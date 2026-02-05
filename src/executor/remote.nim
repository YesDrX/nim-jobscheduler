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
  let header = generateLogHeader(task.name, jobName, true, task.sshHost,
      task.sshUser, command = job.command)
  writeFile(logPath, header)

  # 2. Determine Command
  var cmd = job.command
  if cmd == "": return ProcessInfo()

  # 3. Construct SSH Command
  let remoteLog = "/tmp/jobscheduler_" & $execId & ".log"
  let remoteExit = remoteLog & ".exit"

  # Wrap in sh -c to ensure exit code capture, accessing safeCmd via subshell to handle 'exec' usage case.
  # escape single quotes in cmd
  let safeCmd = cmd.replace("'", "'\\''")
  let wrappedCmd = "sh -c '(" & safeCmd & "); echo $? > " & remoteExit & "'"

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
    pid: int, expectedCmd: string = ""): bool =
  ## Check if a remote process is running via SSH
  ## If expectedCmd is provided, checks if strict overlap exists (to avoid PID reuse issues)
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

  if expectedCmd != "":
    # Use ps to check command line
    # -p PID -o args= -> returns just the command args
    # We check if output CONTAINS a significant part of the expected command
    # Note: 'ps' behavior varies by OS, but 'ps -p <PID> -o args=' is standard-ish POSIX.
    let checkCmd = "ps -p " & $pid & " -o args="
    let (output, exitCode) = execCmdEx("ssh " & sshArgs.join(" ") & " " &
        target & " " & quoteShell(checkCmd))

    if exitCode != 0: return false

    # We do a loose containment check because of how shells wrap commands (nohup, sh -c, etc.)
    # But we should be careful.
    # If expected command is "python foo.py", we hope to see "python foo.py" or "/bin/sh -c python foo.py"
    return output.contains(expectedCmd) or expectedCmd.contains(output.strip())
  else:
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

proc syncLogs*(executor: RemoteExecutor, task: Task, execId: int,
    localLogPath: string) {.async.} =
  ## Fetch remote log and update local file
  let remoteLog = "/tmp/jobscheduler_" & $execId & ".log"

  var sshArgs: seq[string] = @[]
  if task.sshKeyPath != "":
    sshArgs.add("-i"); sshArgs.add(task.sshKeyPath)
  if task.sshPort > 0:
    sshArgs.add("-p"); sshArgs.add($task.sshPort)
  sshArgs.add("-o"); sshArgs.add("StrictHostKeyChecking=no")
  let target = task.sshUser & "@" & task.sshHost

  let catLogCmd = "cat " & remoteLog
  let fullCmd = "ssh " & sshArgs.join(" ") & " " & target & " " & quoteShell(catLogCmd)

  # Note: execCmdEx is blocking, but keeps simplicity consistent with fetchRemoteResult
  let (content, _) = execCmdEx(fullCmd)

  if content.len > 0:
    # Read existing header from local file to preserve it
    var header = ""
    try:
      let f = open(localLogPath, fmRead)
      header = f.readLine() & "\n"
      f.close()
    except:
      discard

    # Overwrite file with Header + Current Content
    try:
      writeFile(localLogPath, header & content)
    except:
      discard

proc fetchRemoteResult*(executor: RemoteExecutor, task: Task, jobName: string,
    execId: int, localLogPath: string): Future[int] {.async.} =
  ## Fetch remote log and exit code. Returns exit code (-1 if missing).
  ## This is called when the process is known to be finished.

  # Check if already fetched (local exit file exists from previous fetch)
  let localExitPath = localLogPath.changeFileExt("exit")
  if fileExists(localExitPath):
    try:
      return parseInt(readFile(localExitPath).strip())
    except:
      return -1

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
      let finalExit = exitContent.strip()
      # Write local exit file explicitly so engine sees it immediately
      let exitPath = localLogPath.changeFileExt("exit")
      writeFile(exitPath, finalExit)

      # Cleanup remote files
      let cleanupCmd = "rm " & remoteLog & " " & remoteExit
      discard execCmdEx(sshBase & " " & quoteShell(cleanupCmd))

      return parseInt(finalExit)
    except:
      return -1
  else:
    # Exit code missing/remote file deleted.
    # To prevent infinite loop in engine (which keeps calling fetchRemoteResult until local exit exists),
    # we MUST generate a local exit file here, assuming failure.
    try:
      let exitPath = localLogPath.changeFileExt("exit")
      writeFile(exitPath, "1") # Mark as failed
       
       # Log this event
      let errorMsg = "\n[Scheduler] Remote exit file not found. Assuming process failed/killed."
      if fileExists(localLogPath):
        let f = open(localLogPath, fmAppend)
        f.write(errorMsg)
        f.close()
    except:
      discard

    return 1 # Return failed code

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

    # Try to kill children first (pkill -P), then the process itself
    # Use || true to ignore errors if no children found
    let killCmd = "pkill -P " & $pid & " || true; kill " & $pid

    let (_, exitCode) = execCmdEx("ssh " & sshArgs.join(" ") & " " & target &
        " " & quoteShell(killCmd))

    if exitCode == 0:
      updateExecutionStatus(executor.db, execId, esCancelled)
      return true
    return false
  except:
    return false
