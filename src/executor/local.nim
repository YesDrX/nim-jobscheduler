import std/[os, strutils, times]
import db_connector/db_sqlite
import ../models/job
import ../models/task
import ../models/types
import ../models/execution
import process
import logger

type
  LocalExecutor* = ref object
    db: DbConn
    logsDir: string

proc newLocalExecutor*(db: DbConn, logsDir: string): LocalExecutor =
  new(result)
  result.db = db
  result.logsDir = logsDir
  ensureLogDir(logsDir)

proc execute*(executor: LocalExecutor, job: Job, task: Task,
    execId: int): ProcessInfo =
  ## Execute a job locally

  # 1. Prepare Log Path
  let safeTaskName = task.name.replace(" ", "_").replace("/", "-")
  # job.name optional? check if empty
  let jobName = if job.name != "": job.name else: "job"
  let safeJobName = jobName.replace(" ", "_").replace("/", "-")

  let jobLogDir = executor.logsDir / safeTaskName / safeJobName
  ensureLogDir(jobLogDir)

  let timestamp = now().format("yyyyMMddHHmmss")
  let logPath = jobLogDir / (timestamp & "_" & $execId & ".log")
  let exitPath = logPath.changeFileExt("exit")

  # Write Header
  let header = generateLogHeader(task.name, jobName, false, command = job.command)
  writeFile(logPath, header)

  # 2. Determine Command
  var cmd = job.command

  if cmd == "":
    # Error state
    return ProcessInfo()

  # 3. Start Detached
  # execution ID is used for log file, but PID depends on OS
  result = startDetached(cmd, logPath, exitPath)

  # 4. Update Execution Record (PID, LogPath, Status)

  try:
    setExecutionRunning(executor.db, execId, result.pid, logPath)
  except:
    discard # Log error?

  return result

proc cancel*(executor: LocalExecutor, execId: int): bool =
  ## Cancel a local execution
  try:
    let pid = getExecutionPid(executor.db, execId)
    if pid <= 0: return false

    # Check if running and kill
    if isProcessRunning(pid):
      killProcess(pid, force = true)
      updateExecutionStatus(executor.db, execId, esCancelled)
      return true
    return false
  except:
    return false
