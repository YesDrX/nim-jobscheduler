import std/[os, osproc, times, strutils]
when not defined(windows):
  import posix

type
  ProcessInfo* = object
    pid*: int
    command*: string
    logPath*: string
    startTime*: DateTime

proc isProcessRunning*(pid: int, expectedCmd: string = ""): bool =
  ## Checks if a process with the given PID is running.
  ## If expectedCmd provided, verifies generic containment to avoid PID reuse.
  if pid <= 0: return false

  when defined(windows):
    let cmd = "tasklist /FI \"PID eq " & $pid & "\" /NH"
    let (output, exitCode) = execCmdEx(cmd)
    if output.contains("No tasks are running"):
      return false
    return output.contains($pid)
  else:
    try:
      if not dirExists("/proc/" & $pid): return false
      let statPath = "/proc/" & $pid & "/stat"
      if not fileExists(statPath): return false

      let content = readFile(statPath)
      let lastParen = content.rfind(')')
      if lastParen != -1 and lastParen + 2 < content.len:
        let state = content[lastParen + 2]
        if state == 'Z': return false # Zombie is effectively dead for our purposes

      if expectedCmd != "":
        # Check cmdline to match expected command
        let cmdlinePath = "/proc/" & $pid & "/cmdline"
        if fileExists(cmdlinePath):
          let cmdContent = readFile(cmdlinePath).replace("\0", " ")
          return cmdContent.contains(expectedCmd) or expectedCmd.contains(cmdContent)
        return false

      return true
    except:
      return false

proc startDetached*(command: string, logPath: string, exitPath: string,
    workDir: string = ""): ProcessInfo =
  ## Starts a process that will survive parent death.
  ## Uses fork/exec on Unix, standard process on Windows.

  # Ensure log/exit path directory exists
  let logDir = logPath.splitFile.dir
  createDir(logDir)

  when defined(windows):
    # Windows: Create truly detached process
    # Use poDemon to create detached process (CREATE_NEW_PROCESS_GROUP)
    # Use /V:ON to enable delayed environment variable expansion for !errorlevel!
    let fullCmd = "cmd /V:ON /C \"" & command & " >> " & quoteShell(logPath) &
        " 2>&1 & echo !errorlevel! > " & quoteShell(exitPath) & "\""

    # poDemon creates a detached process that survives parent death
    let p = startProcess(
      fullCmd,
      workingDir = workDir,
      options = {poEvalCommand, poStdErrToStdOut, poDaemon}
    )
    let pid = p.processID

    # Close handle - process continues independently
    p.close()

    result = ProcessInfo(
      pid: pid,
      command: command,
      logPath: logPath,
      startTime: now()
    )
  else:
    # Unix: Use fork/exec
    # Child automatically orphaned to init if parent dies
    let pid = fork()
    if pid < 0:
      raise newException(OSError, "Fork failed")

    if pid == 0:
      # Child process
      discard setsid()

      # Redirect stdout/stderr to log file
      let logFd = open(logPath.cstring, O_WRONLY or O_CREAT or O_APPEND, 0o644)
      discard dup2(logFd, STDOUT_FILENO)
      discard dup2(logFd, STDERR_FILENO)
      discard close(logFd)

      # Close stdin
      let devNull = open("/dev/null", O_RDONLY)
      discard dup2(devNull, STDIN_FILENO)
      discard close(devNull)

      # Change directory if specified
      if workDir != "":
        discard chdir(workDir.cstring)

      # Execute command with exit code capture
      let shellCmd = "(" & command & "); echo $? > " & quoteShell(exitPath)
      discard execl("/bin/sh", "sh", "-c", shellCmd.cstring, nil)

      # If exec fails
      exitnow(127)

    # Parent process
    result = ProcessInfo(
      pid: pid,
      command: command,
      logPath: logPath,
      startTime: now()
    )

proc killProcess*(pid: int, force: bool = false) =
  ## Kills the process with the given PID.
  if pid <= 0: return

  when defined(windows):
    var cmd = "taskkill /PID " & $pid
    if force: cmd.add(" /F")
    discard execShellCmd(cmd)
  else:
    # Use OS kill command
    let signal = if force: "-9" else: "-15" # SIGKILL or SIGTERM
    discard execShellCmd("kill " & signal & " " & $pid)
