import std/[options, logging]
import db_connector/db_sqlite
import monitor
import ../executor/process
import ../executor/remote
import ../models/types
import ../models/job
import ../models/execution
import ../models/task

type
  RecoveryManager* = ref object
    db: DbConn
    monitor: ProcessMonitor

proc newRecoveryManager*(db: DbConn, monitor: ProcessMonitor): RecoveryManager =
  new(result)
  result.db = db
  result.monitor = monitor

proc recoverRunningJobs*(manager: RecoveryManager) =
  info "[Recovery] Starting recovery process..."
  var countRunning = 0
  var countFailed = 0
  
  try:
    # 1. Handle Scheduled/Running jobs
    let runningExecs = getExecutionsRunningOrScheduled(manager.db)
    
    for execRow in runningExecs:
      let execId = execRow.dbId
      let execution = execRow.data
      let jobId = execution.jobId
      let pid = execution.pid
      let status = execution.status
      
      if status == esScheduled:
        # Scheduler crashed while job was queued but process likely hadn't started or we lost track.
        # Mark as Failed for consistency.
        updateExecutionStatus(manager.db, execId, esFailed, "Scheduler restarted while scheduled (Interrupted)")
        countFailed.inc
        continue

      if pid == 0:
         # Running state but no PID means data inconsistency or crash before PID update.
         updateExecutionStatus(manager.db, execId, esFailed, "Process state invalid (No PID)")
         countFailed.inc
         continue
         
      # Check if running
      # Fetch Task from Job
      try:
         let jobOpt = getJobById(manager.db, jobId)
         if jobOpt.isNone: continue
         let job = jobOpt.get().data
         let taskId = job.taskId
         
         let taskOpt = getTaskById(manager.db, taskId)
         if taskOpt.isNone: continue
         let task = taskOpt.get().data
         
         var isRunning = false
         case task.taskType
         of ttLocal:
           isRunning = isProcessRunning(pid)
         of ttRemote:
           # Remote check
           # Relying on remoteExecutor in monitor for config
           isRunning = manager.monitor.remoteExecutor.isRemoteProcessRunning(task, pid)
           
         if isRunning:
           countRunning.inc
         else:
           # Process is gone.
           # Since we are in recovery (startup), we assume it died while we were down.
           # Or it finished successfully while we were down.
           # Without checking logs or exit codes (hard if local process gone), we can't reliably say Success.
           # However, if it's not running, it's definitely not "Running".
           # Safer to mark Failed/Unknown.
           updateExecutionStatus(manager.db, execId, esFailed, "Process terminated unexpectedly during scheduler downtime")
           countFailed.inc
      except Exception as e:
         info "[Recovery] Error checking job " & $execId & ": " & e.msg
         
    info "[Recovery] Recovery Check Complete. Running: ", countRunning, ", Marked Failed: ", countFailed
    
  except Exception as e:
    info "[Recovery] Fatal Error: " & e.msg
