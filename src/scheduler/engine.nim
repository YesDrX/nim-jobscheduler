import std/[times, options, os, strutils, asyncdispatch, logging]
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
  SchedulerEngine* = ref object
    db: DbConn
    running*: bool
    checkInterval: int # milliseconds
    localExecutor: LocalExecutor
    remoteExecutor: RemoteExecutor
    smtpConfig: SmtpConfig
    cleanupManager: CleanupManager
    serverStartTime*: DateTime

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

proc logMsg(engine: SchedulerEngine, msg: string) =
  info("[Scheduler] " & msg)

proc dispatchJob(engine: SchedulerEngine, jobId: int, job: Job) =
  engine.logMsg("Starting job: " & job.name & " (ID: " & $jobId & ")")

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
    else:
      engine.logMsg("Failed to start process")
      asyncCheck sendAlert(jobId, job, execId, exec, engine.smtpConfig) 
      try:
         updateExecutionStatus(engine.db, execId, esFailed, "Failed to launch process")
      except: discard

  except Exception as e:
    engine.logMsg("Failed to dispatch job " & $jobId & ": " & e.msg)
    exec.errorMessage = e.msg
    exec.status = esFailed
    asyncCheck sendAlert(jobId, job, execId, exec, engine.smtpConfig)

proc dispatchTask(engine: SchedulerEngine, taskId: int, task: Task) =
  engine.logMsg("Dispatching task: " & task.name & " (ID: " & $taskId & ")")
  
  if not task.enabled:
     engine.logMsg("Skipping task " & task.name & ": Disabled.")
     return

  # Find the first job (orderIdx 1)
  let jobs = getJobsByTaskIdOrdered(engine.db, taskId)
  
  if jobs.len == 0:
    engine.logMsg("Task " & task.name & " has no jobs to run.")
    return
    
  let (jobId, firstJob) = jobs[0] # Ordered by orderIdx ASC
  
  # Dispatch this job
  engine.dispatchJob(jobId, firstJob)

proc poll(engine: SchedulerEngine) =
  # Fetch all enabled Tasks
  try:
    let tasks = getAllTasksOrdered(engine.db) 
    
    for t in tasks:
      let taskId = t.dbId
      let task = t.data
      
      if not task.enabled: continue
      
      # Check Trigger
      # We need last execution time of the TASK (start time of first job)
      let lastRun = getLastTaskExecutionTime(engine.db, taskId)
      
      if shouldTrigger(task, now(), lastRun, engine.serverStartTime):
         engine.dispatchTask(taskId, task)
        
  except Exception as e:
    engine.logMsg("Error in poll loop: " & e.msg)

proc runLoop(engine: SchedulerEngine) {.async.} =
  engine.running = true
  while engine.running:
    engine.poll()
    await sleepAsync(engine.checkInterval)

proc start*(engine: SchedulerEngine) =
  if not engine.running:
    engine.logMsg("Starting scheduler loop...")
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
    engine.dispatchJob(id, job)
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
