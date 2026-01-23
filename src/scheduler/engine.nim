import std/[times, options, os, strutils, asyncdispatch, logging, heapqueue, tables, sequtils]
import db_connector/db_sqlite
import ../models/types
import ../models/job
import ../models/execution
import ../models/task
import ../db/connection
import ../executor/local
import ../executor/remote
import ../executor/process
import ../alert/email
import ../services/cleanup_service
import ../config/types
import triggers

type
  # Item for the Priority Queue
  ScheduledItem* = object
    triggerTime*: DateTime
    taskId*: int
    
  # Track running sequential tasks
  RunningTask* = object
    taskId*: int
    currentJobId*: int       # The ID of the job currently running
    currentExecId*: int      # The ID of the execution record
    logPath*: string         # Path to check for .exit file
    jobIds*: seq[int]        # List of job IDs in order for this task
    jobIndex*: int           # Current index in jobIds

  SchedulerEngine* = ref object
    db: DbConn
    running*: bool
    checkInterval: int       # milliseconds (now intended for micro-sleep in loop)
    localExecutor: LocalExecutor
    remoteExecutor: RemoteExecutor
    smtpConfig: SmtpConfig
    cleanupManager: CleanupManager
    serverStartTime*: DateTime
    
    # New In-Memory Schedule State
    dailySchedule*: HeapQueue[ScheduledItem] 
    runningTasks*: Table[int, RunningTask]
    lastScheduleUpdate*: DateTime

# Comparison for HeapQueue (Min-Heap based on triggerTime)
proc `<`*(a, b: ScheduledItem): bool =
  a.triggerTime < b.triggerTime


proc newSchedulerEngine*(db: DbConn, localExec: LocalExecutor, remoteExec: RemoteExecutor, cfg: Config, checkInterval = 30000): SchedulerEngine =
  new(result)
  result.db = db
  result.checkInterval = checkInterval
  result.running = false
  result.serverStartTime = now().utc
  result.localExecutor = localExec
  result.remoteExecutor = remoteExec
  result.smtpConfig = cfg.smtp
  # Initialize Cleanup Manager
  let logsDir = getCurrentDir() / "logs" # Or passed in? LocalExecutor uses this too.
  createDir(logsDir)
  
  # Setup Logging
  let fileLogger = newFileLogger(logsDir / "scheduler.log", fmtStr="$date $time - ")
  let consoleLogger = newConsoleLogger(fmtStr="$date $time - ")
  addHandler(fileLogger)
  addHandler(consoleLogger)

  result.cleanupManager = newCleanupManager(db, logsDir, cfg.internal)
  
  # Initialize In-Memory State
  result.dailySchedule = initHeapQueue[ScheduledItem]()
  result.runningTasks = initTable[int, RunningTask]()


proc logMsg(engine: SchedulerEngine, msg: string) =
  info("[Scheduler] " & msg)

proc dispatchJob(engine: SchedulerEngine, jobId: int, job: Job): tuple[execId: int, pid: int, logPath: string] =
  engine.logMsg("Starting job: " & job.name & " (ID: " & $jobId & ")")
  result = (0, 0, "")

  # Fetch Task details using helper
  let taskOpt = getTaskById(engine.db, job.taskId)
  if taskOpt.isNone:
    engine.logMsg("Error: Task not found for job " & $jobId)
    return
    
  var (_, task) = taskOpt.get

  # Create Execution record
  var exec = Execution(
    jobId: jobId,
    status: esScheduled,
    startTime: now().utc,
    endTime: now().utc
  )
  
  var execId = 0
  try:
    execId = createExecution(engine.db, jobId, esScheduled)
    engine.logMsg("Created Execution ID: " & $execId)
    
    # Dispatch based on Task Type
    var procInfo: ProcessInfo
    
    case task.taskType
    of ttLocal:
      procInfo = engine.localExecutor.execute(job, task, execId)
    of ttRemote:
      procInfo = engine.remoteExecutor.execute(job, task, execId)
      
      
    if procInfo.pid > 0:
      engine.logMsg("Started process PID: " & $procInfo.pid)
      # Update execution with PID
      try:
        let execOpt = getExecutionById(engine.db, execId)
        if execOpt.isSome:
          var (dbId, e) = execOpt.get()
          e.pid = procInfo.pid
          e.status = esRunning
          updateRowExecution(engine.db, dbId, e)
      except:
        engine.logMsg("Warning: Failed to update PID in database")
        
      return (execId, procInfo.pid, procInfo.logPath)
    else:
      engine.logMsg("Failed to start process")
      asyncCheck sendAlert(jobId, job, execId, exec, engine.smtpConfig) 
      try:
         updateExecutionStatus(engine.db, execId, esFailed, "Failed to launch process")
      except: discard
      return (execId, 0, "")

  except Exception as e:
    engine.logMsg("Failed to dispatch job " & $jobId & ": " & e.msg)
    exec.errorMessage = e.msg
    exec.status = esFailed
    asyncCheck sendAlert(jobId, job, execId, exec, engine.smtpConfig)
    return (execId, 0, "")


proc generateDailySchedule(engine: SchedulerEngine) =
  engine.logMsg("Generating daily schedule...")
  engine.dailySchedule = initHeapQueue[ScheduledItem]()
  let tasks = getAllTasksOrdered(engine.db)
  let now = now().utc
  # Schedule until the end of the next 24 hours to cover full day cycles
  let horizonUtc = now + 1.days
  
  for t in tasks:
    if not t.data.enabled: continue
    
    let task = t.data
    var lastRun = getLastTaskExecutionTime(engine.db, t.dbId)
    
    # We start searching from now
    var searchTime = now
    
    # Safety limit to prevent infinite loops or massive schedules
    var count = 0 
    while count < 200: 
       # Helper for next trigger
       let nextOpt = getNextTrigger(task, searchTime, lastRun, some(engine.serverStartTime))
       if nextOpt.isNone: break
       let next = nextOpt.get()
       
       if next > horizonUtc: break
       
       # Add to schedule
       engine.dailySchedule.push(ScheduledItem(triggerTime: next, taskId: t.dbId))
       
       # Advance search time to just after this trigger to find the next one
       # We use the trigger time as the new "now" for the search
       searchTime = next
       lastRun = some(next) 
       count.inc
    
    if count >= 200:
       engine.logMsg("Warning: Task " & task.name & " has too many triggers scheduled. Limit reached.")

  engine.lastScheduleUpdate = now
  engine.logMsg("Schedule generated. Items: " & $engine.dailySchedule.len)

proc recoverRunningTasks(engine: SchedulerEngine) =
  engine.logMsg("Recovering running tasks...")
  # Find all executions that are marked as Running
  let rows = engine.db.getAllRows(sql"SELECT _dbID, jobId, pid, logFile FROM ExecutionTable WHERE status = 'Running'")
  
  for row in rows:
    try:
      let execId = parseInt(row[0])
      let jobId = parseInt(row[1]) 
      let pid = if row[2] == "": 0 else: parseInt(row[2])
      let logPath = row[3]
      
      # Check if process is actually running
      if pid > 0 and isProcessRunning(pid):
         engine.logMsg("Recovered running execution ID: " & $execId & " (PID: " & $pid & ")")
         
         # We need to reconstruct RunningTask state
         let jobOpt = getJobById(engine.db, jobId)
         if jobOpt.isSome:
            let (_, job) = jobOpt.get
            let taskId = job.taskId
            
            # Check if we already have this task running
            if not engine.runningTasks.hasKey(taskId):
               # Get all jobs for task to know index
               let jobs = getJobsByTaskIdOrdered(engine.db, taskId)
               var idx = -1
               for i, j in jobs:
                 if j[0] == jobId: 
                   idx = i
                   break
               
               if idx >= 0:
                  engine.runningTasks[taskId] = RunningTask(
                    taskId: taskId,
                    currentJobId: jobId,
                    currentExecId: execId,
                    logPath: logPath,
                    jobIds: jobs.mapIt(it[0]),
                    jobIndex: idx
                  )
      else:
         # It's dead. Mark as Failed.
         engine.logMsg("Found dead execution ID: " & $execId & ". Marking as Failed.")
         updateExecutionStatus(engine.db, execId, esFailed, "Scheduler recovered: Process not running.")
         
    except Exception as e:
      engine.logMsg("Error recovering execution: " & e.msg)

proc dispatchTask(engine: SchedulerEngine, taskId: int, task: Task) =
  engine.logMsg("Dispatching task: " & task.name & " (ID: " & $taskId & ")")
  
  if not task.enabled:
     engine.logMsg("Skipping task " & task.name & ": Disabled.")
     return

  # Find jobs
  let jobs = getJobsByTaskIdOrdered(engine.db, taskId)
  
  if jobs.len == 0:
    engine.logMsg("Task " & task.name & " has no jobs to run.")
    return

  if task.parallel:
    engine.logMsg("Dispatching task " & task.name & " in PARALLEL mode (" & $jobs.len & " jobs)")
    for jobInfo in jobs:
       let (jId, jData) = jobInfo
       discard engine.dispatchJob(jId, jData)
  else:
    # Sequential
    # Check if already running?
    if engine.runningTasks.hasKey(taskId):
       engine.logMsg("Task " & task.name & " is already running. Skipping trigger.")
       return

    # Start first job
    let (jobId, firstJob) = jobs[0] 
    
    let (execId, pid, logPath) = engine.dispatchJob(jobId, firstJob)
    
    if pid > 0:
       engine.runningTasks[taskId] = RunningTask(
          taskId: taskId,
          currentJobId: jobId,
          currentExecId: execId,
          logPath: logPath,
          jobIds: jobs.mapIt(it[0]),
          jobIndex: 0
       )

proc runLoop(engine: SchedulerEngine) {.async.} =
  engine.running = true
  
  # Initial Schedule & Recovery
  engine.recoverRunningTasks()
  engine.generateDailySchedule()
  
  var lastPlanTime = now().utc
  
  while engine.running:
    let loopStart = now().utc
    
    # 1. Daily Plan Refresh (At midnight or if empty? Just check date change)
    if loopStart.yearday != lastPlanTime.yearday:
       engine.generateDailySchedule()
       lastPlanTime = loopStart
       
    # 2. Trigger Tasks
    while engine.dailySchedule.len > 0:
       # Peek
       let item = engine.dailySchedule[0]
       if item.triggerTime <= loopStart:
          discard engine.dailySchedule.pop()
          
          # Fetch task to dispatch
          let taskOpt = getTaskById(engine.db, item.taskId)
          if taskOpt.isSome:
             engine.dispatchTask(item.taskId, taskOpt.get.data)
       else:
          break 
          
    # 3. Monitor Running Tasks (Sequential)
    # Use toSeq to allow modification if we need to remove keys
    let currentTasks = toSeq(engine.runningTasks.pairs) 
    for (taskId, runInfo) in currentTasks:
       # Check if current execution finished
       let exitPath = runInfo.logPath.changeFileExt("exit")
       
       if fileExists(exitPath):
          engine.logMsg("Execution finished for Task " & $taskId & " Job " & $runInfo.currentJobId)
          
          # Read exit code
          var exitCode = -1
          try:
             let content = readFile(exitPath).strip()
             exitCode = parseInt(content)
          except:
             engine.logMsg("Error reading exit code.")
             
          # Update Status
          let status = if exitCode == 0: esSuccess else: esFailed
          updateExecutionStatus(engine.db, runInfo.currentExecId, status)
          
          # Handle Next Step
          if status == esSuccess:
             let nextIdx = runInfo.jobIndex + 1
             if nextIdx < runInfo.jobIds.len:
                # Run Next Job
                let nextJobId = runInfo.jobIds[nextIdx]
                let jobOpt = getJobById(engine.db, nextJobId)
                if jobOpt.isSome:
                   let (_, nextJob) = jobOpt.get
                   let (execId, pid, logPath) = engine.dispatchJob(nextJobId, nextJob)
                   
                   if pid > 0:
                      # Update RunningTask
                      var nextRun = runInfo
                      nextRun.currentJobId = nextJobId
                      nextRun.currentExecId = execId
                      nextRun.jobIndex = nextIdx
                      nextRun.logPath = logPath
                      engine.runningTasks[taskId] = nextRun
                   else:
                      engine.runningTasks.del(taskId)
                else:
                   engine.logMsg("Next job not found.")
                   engine.runningTasks.del(taskId)
             else:
                engine.logMsg("Task " & $taskId & " completed all jobs.")
                engine.runningTasks.del(taskId)
          else:
             # Failed
             engine.logMsg("Task " & $taskId & " failed at job " & $runInfo.currentJobId)
             engine.runningTasks.del(taskId)
             
    # Sleep (High Frequency)
    await sleepAsync(100) 

proc start*(engine: SchedulerEngine) =
  if not engine.running:
    engine.logMsg("Starting scheduler loop (High Frequency)...")
    engine.cleanupManager.start()
    asyncCheck engine.runLoop()


proc triggerJob*(engine: SchedulerEngine, jobId: int): Future[bool] {.async.} =
  engine.logMsg("Manual trigger for job ID: " & $jobId)
  
  try:
    # Get job with full details using helper
    let jobOpt = getJobWithDetails(engine.db, jobId)
    if jobOpt.isNone:
      engine.logMsg("Trigger failed: Job not found " & $jobId)
      return false

    var (id, job) = jobOpt.get
    
    # We need to dispatch using ID
    discard engine.dispatchJob(id, job)
    return true
  except Exception as e:
    engine.logMsg("Error triggering job " & $jobId & ": " & e.msg)
    return false

proc cancelExecution*(engine: SchedulerEngine, execId: int): bool =
  engine.logMsg("Cancelling execution ID: " & $execId)
  
  try:
    let execOpt = getExecutionById(engine.db, execId)
    if execOpt.isNone: return false
    let (_, execution) = execOpt.get
    
    let jobOpt = getJobById(engine.db, execution.jobId)
    if jobOpt.isNone: return false
    let (jobDbId, job) = jobOpt.get
    
    let taskOpt = getTaskById(engine.db, job.taskId)
    if taskOpt.isNone: return false
    let (_, task) = taskOpt.get
    
    var cancelled = false
    case task.taskType
    of ttLocal:
      cancelled = engine.localExecutor.cancel(execId)
    of ttRemote:
      cancelled = engine.remoteExecutor.cancel(execId, task)
    
    # Send alert for cancelled execution
    if cancelled:
      asyncCheck sendAlert(jobDbId, job, execId, execution, engine.smtpConfig)
    
    return cancelled
  except Exception as e:
    engine.logMsg("Error cancelling execution " & $execId & ": " & e.msg)
    return false

proc stop*(engine: SchedulerEngine) =
  engine.running = false
  engine.cleanupManager.stop()
  engine.logMsg("Stopping scheduler loop...")
