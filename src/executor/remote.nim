import std/[os, times, strutils, osproc, asyncdispatch]
import db_connector/db_sqlite
import ../models/job
import ../models/task
import ../models/types
import ../models/execution
import process
import logger

type
  RemoteExecutor* = ref object
    db: DbConn
    logsDir: string

proc newRemoteExecutor*(db: DbConn, logsDir: string): RemoteExecutor =
  new(result)
  result.db = db
  result.logsDir = logsDir
  ensureLogDir(logsDir)

proc execute*(executor: RemoteExecutor, job: Job, task: Task,
    execId: int): ProcessInfo =
  ## Execute a job remotely via SSH

  # 1. Prepare Local Log Path (for stream capture)
  let safeTaskName = task.name.replace(" ", "_").replace("/", "-")
  let jobName = if job.name != "": job.name else: "job"
  let safeJobName = jobName.replace(" ", "_").replace("/", "-")
  let jobLogDir = executor.logsDir / safeTaskName / safeJobName
  ensureLogDir(jobLogDir)
  let timestamp = now().format("yyyyMMddHHmmss")
  let logPath = jobLogDir / (timestamp & "_" & $execId & ".log")

  # Write Header
  let header = generateLogHeader(task.name, jobName, true, task.sshHost, task.sshUser, command = job.command)
  writeFile(logPath, header)

  # 2. Determine Command
  var cmd = job.command
  if cmd == "": return ProcessInfo()

  # 3. Construct SSH Command
  let remoteLog = "/tmp/jobscheduler_" & $execId & ".log"
  let remoteExit = remoteLog & ".exit"

  # Wrap in sh -c to ensure exit code capture
  # escape single quotes in cmd
  let safeCmd = cmd.replace("'", "'\\''")
  let wrappedCmd = "sh -c '" & safeCmd & "; echo $? > " & remoteExit & "'"

  # nohup the wrapped command
  let remoteCmd = "nohup " & wrappedCmd & " > " & remoteLog & " 2>&1 & echo $!"

  # SSH connection args
  var sshArgs: seq[string] = @[]
  if task.sshKeyPath != "":
    sshArgs.add("-i")
    sshArgs.add(task.sshKeyPath)
  if task.sshPort > 0:
    sshArgs.add("-p")
    sshArgs.add($task.sshPort)
  sshArgs.add("-o")
  sshArgs.add("StrictHostKeyChecking=no")

  let target = task.sshUser & "@" & task.sshHost

  # Run the start command
  let (output, exitCode) = execCmdEx("ssh " & sshArgs.join(" ") & " " & target &
      " " & quoteShell(remoteCmd))

  if exitCode != 0:
    try:
      updateExecutionStatus(executor.db, execId, esFailed,
          "SSH launch failed: " & output)
    except: discard
    return ProcessInfo()

  let remotePid = parseInt(output.strip())

  try:
    setExecutionRunning(executor.db, execId, remotePid, logPath)
  except: discard

  result = ProcessInfo(
    pid: remotePid,
    command: cmd,
    logPath: logPath,
    startTime: now()
  )

proc isRemoteProcessRunning*(executor: RemoteExecutor, task: Task,
    pid: int): bool =
  ## Check if a remote process is running via SSH
  if pid <= 0: return false

  var sshArgs: seq[string] = @[]
  if task.sshKeyPath != "":
    sshArgs.add("-i")
    sshArgs.add(task.sshKeyPath)
  if task.sshPort > 0:
    sshArgs.add("-p")
    sshArgs.add($task.sshPort)
  sshArgs.add("-o")
  sshArgs.add("StrictHostKeyChecking=no")

  let target = task.sshUser & "@" & task.sshHost
  let checkCmd = "kill -0 " & $pid

  let (_, exitCode) = execCmdEx("ssh " & sshArgs.join(" ") & " " & target &
      " " & quoteShell(checkCmd))

  return exitCode == 0

proc checkRemoteStatusAsync*(executor: RemoteExecutor, task: Task,
    pid: int): Future[bool] {.async.} =
  ## Check if a remote process is running via SSH (Async)
  if pid <= 0: return false

  var sshArgs: seq[string] = @[]
  if task.sshKeyPath != "":
    sshArgs.add("-i")
    sshArgs.add(task.sshKeyPath)
  if task.sshPort > 0:
    sshArgs.add("-p")
    sshArgs.add($task.sshPort)
  sshArgs.add("-o")
  sshArgs.add("StrictHostKeyChecking=no")

  let target = task.sshUser & "@" & task.sshHost
  let checkCmd = "kill -0 " & $pid

  var args = sshArgs
  args.add(target)
  args.add(checkCmd)

  var p = startProcess("ssh", args = args, options = {poUsePath,
      poStdErrToStdOut})

  try:
    while p.running:
      await sleepAsync(100)

    let exitCode = p.peekExitCode
    p.close()
    return exitCode == 0
  except:
    if not p.isNil: p.close()
    return false

proc fetchRemoteResult*(executor: RemoteExecutor, task: Task, jobName: string,
    execId: int, localLogPath: string): Future[int] {.async.} =
  ## Fetch remote log and exit code. Returns exit code (-1 if missing).
  ## This is called when the process is known to be finished.

  let remoteLog = "/tmp/jobscheduler_" & $execId & ".log"
  let remoteExit = remoteLog & ".exit"

  var sshArgs: seq[string] = @[]
  if task.sshKeyPath != "":
    sshArgs.add("-i")
    sshArgs.add(task.sshKeyPath)
  if task.sshPort > 0:
    sshArgs.add("-p")
    sshArgs.add($task.sshPort)
  sshArgs.add("-o")
  sshArgs.add("StrictHostKeyChecking=no")
  let target = task.sshUser & "@" & task.sshHost

  # 1. Cat Log
  # We append to local log path
  let catLogCmd = "cat " & remoteLog

  # Construct SSH command line
  let sshBase = "ssh " & sshArgs.join(" ") & " " & target
  let fullLogCmd = sshBase & " " & quoteShell(catLogCmd)

  # Blocking call here, but tolerable
  let (logContent, _) = execCmdEx(fullLogCmd)

  if logContent.len > 0:
    if fileExists(localLogPath):
      # Append to existing file (preserves header with correct start time)
      let f = open(localLogPath, fmAppend)
      try:
        f.write(logContent)
      finally:
        f.close()
    else:
      # Create new with header (fallback)
      let header = generateLogHeader(task.name, jobName, true, task.sshHost, task.sshUser)
      writeFile(localLogPath, header & logContent)

  # 2. Cat Exit Code
  let catExitCmd = "cat " & remoteExit
  let fullExitCmd = sshBase & " " & quoteShell(catExitCmd)

  let (exitContent, exitCode) = execCmdEx(fullExitCmd)

  if exitCode == 0 and exitContent.strip().len > 0:
    try:
      # Cleanup remote files
      let cleanupCmd = "rm " & remoteLog & " " & remoteExit
      discard execCmdEx(sshBase & " " & quoteShell(cleanupCmd))

      return parseInt(exitContent.strip())
    except:
      return -1
  else:
    # also try to cleanup remote files?
    # maybe later
    return -1

proc cancel*(executor: RemoteExecutor, execId: int, task: Task): bool =
  ## Cancel a remote execution
  try:
    let pid = getExecutionPid(executor.db, execId)
    if pid <= 0: return false

    # Construct SSH kill command
    var sshArgs: seq[string] = @[]
    if task.sshKeyPath != "":
      sshArgs.add("-i")
      sshArgs.add(task.sshKeyPath)
    if task.sshPort > 0:
      sshArgs.add("-p")
      sshArgs.add($task.sshPort)
    sshArgs.add("-o")
    sshArgs.add("StrictHostKeyChecking=no")

    let target = task.sshUser & "@" & task.sshHost
    let killCmd = "kill " & $pid

    let (_, exitCode) = execCmdEx("ssh " & sshArgs.join(" ") & " " & target &
        " " & quoteShell(killCmd))

    if exitCode == 0:
      updateExecutionStatus(executor.db, execId, esCancelled)
      return true
    return false
  except:
    return false
