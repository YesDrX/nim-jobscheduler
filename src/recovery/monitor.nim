import std/[asyncdispatch, strutils, options, os, logging]
import db_connector/db_sqlite
import ../models/types
import ../models/task
import ../models/job
import ../models/execution
import ../executor/local
import ../executor/remote
import ../executor/process
import ../db/connection
import ../alert/email
import ../config/types

type
  SchedulerTriggerProc* = proc(jobId: int) {.gcsafe, async.} # Callback to trigger next job

type
  ProcessMonitor* = ref object
    db*: DbConn
    localExecutor*: LocalExecutor
    remoteExecutor*: RemoteExecutor
    triggerNextJob*: SchedulerTriggerProc
    checkIntervalSeconds*: int
    running*: bool

proc newProcessMonitor*(db: DbConn, localExec: LocalExecutor,
    remoteExec: RemoteExecutor, checkIntervalSeconds = 5): ProcessMonitor =
  new(result)
  result.db = db
  result.localExecutor = localExec
  result.remoteExecutor = remoteExec
  result.checkIntervalSeconds = checkIntervalSeconds
  result.running = false

proc checkJob(monitor: ProcessMonitor, execId: int,
    execution: Execution, smtpConfig: SmtpConfig,
        externalUrl: string) {.async.} =
  try:
    let jobId = execution.jobId
    let pid = execution.pid

    # Determine Task Type and Details
    # 1. Get Job -> TaskId
    let jobOpt = monitor.db.getJobById(jobId)
    if jobOpt.isNone: return # Orphaned execution?
    let (_, job) = jobOpt.get
    let taskId = job.taskId

    # 2. Get Task details
    let taskOpt = monitor.db.getTaskById(taskId)
    if taskOpt.isNone: return
    let (_, task) = taskOpt.get

    var isRunning = false
    case task.taskType
    of ttLocal:
      isRunning = isProcessRunning(pid)
    of ttRemote:
      # Use Async version
      isRunning = await monitor.remoteExecutor.checkRemoteStatusAsync(task, pid)

    if not isRunning:
      # Process has finished. Check outcome.
      var success = false
      var exitCode = -1
      var hasExitCode = false

      if task.taskType == ttRemote:
        # Check if local exit file already exists (may have been fetched by engine)
        let exitFile = execution.logFile.changeFileExt("exit")
        if fileExists(exitFile):
          # Already fetched by engine, just read the local file
          try:
            exitCode = parseInt(readFile(exitFile).strip())
            hasExitCode = true
          except:
            hasExitCode = false
        else:
          # Fetch remote result
          exitCode = await monitor.remoteExecutor.fetchRemoteResult(task,
              job.name, execId, execution.logFile)
          if exitCode != -1: hasExitCode = true
      else:
        # Local check
        let exitFile = execution.logFile.changeFileExt("exit")
        if fileExists(exitFile):
          try:
            exitCode = parseInt(readFile(exitFile).strip())
            hasExitCode = true
          except: discard

      if hasExitCode:
        if exitCode == 0:
          success = true
          updateExecutionStatus(monitor.db, execId, esSuccess)
        else:
          updateExecutionStatus(monitor.db, execId, esFailed, "Exit Code: " & $exitCode)
      else:
        updateExecutionStatus(monitor.db, execId, esFailed, "Process exited without exit code")

      if success:
        # Check if there's a next job in the chain and trigger it
        let currentOrder = job.orderIdx

        # Find next job: Same Task, Order > Current, Limit 1
        let jobs = monitor.db.getJobsByTaskIdOrdered(taskId)
        var nextJobId = 0
        var found = false

        for row in jobs:
          let j = row.data
          if j.orderIdx > currentOrder:
            if j.enabled:
              nextJobId = row.dbId
              found = true
            break # First one > currentOrder is enough because sorted ASC

        if found:
          if monitor.triggerNextJob != nil:
            asyncCheck monitor.triggerNextJob(nextJobId)

      else:
        info "[Monitor] Execution " & $execId & " failed."
        await sendAlert(jobId, job, execId, execution, smtpConfig, externalUrl)

  except Exception as e:
    info "[Monitor] Error checking execution " & $execId & ": " & e.msg

proc checkRunningJobs(monitor: ProcessMonitor,
    smtpConfig: SmtpConfig, externalUrl: string) {.async.} =
  try:
    # Query all running executions
    let runningExecutions = monitor.db.getRunningExecutions()

    var futures: seq[Future[void]] = @[]
    for row in runningExecutions:
      futures.add(monitor.checkJob(row.dbId, row.data, smtpConfig, externalUrl))

    if futures.len > 0:
      await all(futures)

  except Exception as e:
    info "[Monitor] Error checking jobs: " & e.msg

proc monitorLoop(monitor: ProcessMonitor, smtpConfig: SmtpConfig,
    externalUrl: string) {.async.} =
  monitor.running = true
  while monitor.running:
    await monitor.checkRunningJobs(smtpConfig, externalUrl)
    await sleepAsync(monitor.checkIntervalSeconds * 1000)

proc startMonitoring*(monitor: ProcessMonitor, smtpConfig: SmtpConfig,
    externalUrl: string) =
  if not monitor.running:
    asyncCheck monitor.monitorLoop(smtpConfig, externalUrl)

proc stopMonitoring*(monitor: ProcessMonitor) =
  monitor.running = false
