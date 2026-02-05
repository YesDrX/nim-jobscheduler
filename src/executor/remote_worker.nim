import std/[asyncdispatch, os, osproc, times, strutils, tables, locks, options,
    streams, sequtils]
import ../models/types
import ../models/task
import ../db/connection

type
  RemoteTaskStatus* = object
    taskId*: int
    execId*: int
    pid*: int
    logPath*: string
    task*: Task
    jobCommand*: string
    isRunning*: bool
    lastChecked*: DateTime
    lastLogSync*: DateTime
    exitCode*: int # -1 if not finished
    errorMessage*: string

var
  sharedStatuses*: Table[int, RemoteTaskStatus] # taskId -> status
  statusLock*: Lock
  workerRunning*: bool = false

# GC-safe wrappers for table access
proc getTaskIds(): seq[int] =
  {.gcsafe.}:
    acquire(statusLock)
    result = toSeq(sharedStatuses.keys)
    release(statusLock)

proc getTaskCopy(taskId: int): Option[RemoteTaskStatus] =
  {.gcsafe.}:
    acquire(statusLock)
    if sharedStatuses.hasKey(taskId):
      result = some(sharedStatuses[taskId])
    else:
      result = none(RemoteTaskStatus)
    release(statusLock)

proc updateTaskRunning(taskId: int, isRunning: bool,
    lastChecked: DateTime) =
  {.gcsafe.}:
    acquire(statusLock)
    if sharedStatuses.hasKey(taskId):
      sharedStatuses[taskId].isRunning = isRunning
      sharedStatuses[taskId].lastChecked = lastChecked
    release(statusLock)

proc updateTaskLogSync(taskId: int, lastLogSync: DateTime) =
  {.gcsafe.}:
    acquire(statusLock)
    if sharedStatuses.hasKey(taskId):
      sharedStatuses[taskId].lastLogSync = lastLogSync
    release(statusLock)

proc updateTaskExitCode(taskId: int, exitCode: int) =
  {.gcsafe.}:
    acquire(statusLock)
    if sharedStatuses.hasKey(taskId):
      sharedStatuses[taskId].exitCode = exitCode
    release(statusLock)

proc buildSshArgs(task: Task): seq[string] =
  result = @[]
  if task.sshKeyPath != "":
    result.add("-i")
    result.add(task.sshKeyPath)
  if task.sshPort > 0:
    result.add("-p")
    result.add($task.sshPort)
  result.add("-o")
  result.add("StrictHostKeyChecking=no")

proc checkRemoteProcessAsync(task: Task, pid: int,
    expectedCmd: string = ""): Future[bool] {.async.} =
  if pid <= 0: return false

  let target = task.sshUser & "@" & task.sshHost
  let checkCmd = if expectedCmd != "":
    "ps -p " & $pid & " -o args="
  else:
    "kill -0 " & $pid

  var args = buildSshArgs(task)
  args.add(target)
  args.add(checkCmd)

  var p = startProcess("ssh", args = args, options = {poUsePath,
      poStdErrToStdOut})

  try:
    while p.running:
      await sleepAsync(100)

    let exitCode = p.peekExitCode
    if expectedCmd != "" and exitCode == 0:
      var output = ""
      let stream = p.outputStream
      var line = ""
      while stream.readLine(line):
        output.add(line & "\n")
      p.close()
      return output.contains(expectedCmd) or expectedCmd.contains(output.strip())
    else:
      p.close()
      return exitCode == 0
  except:
    if not p.isNil: p.close()
    return false

proc syncLogsAsync(task: Task, execId: int, localLogPath: string) {.async.} =
  let remoteLog = "/tmp/jobscheduler_" & $execId & ".log"
  let target = task.sshUser & "@" & task.sshHost

  let catLogCmd = "cat " & remoteLog
  var args = buildSshArgs(task)
  args.add(target)
  args.add(quoteShell(catLogCmd))

  var p = startProcess("ssh", args = args, options = {poUsePath,
      poStdErrToStdOut})

  try:
    while p.running:
      await sleepAsync(100)

    var content = ""
    let stream = p.outputStream
    var line = ""
    while stream.readLine(line):
      content.add(line & "\n")
    p.close()

    if content.len > 0:
      var header = ""
      try:
        if fileExists(localLogPath):
          let f = open(localLogPath, fmRead)
          header = f.readLine() & "\n"
          f.close()
      except:
        discard

      try:
        writeFile(localLogPath, header & content)
      except:
        discard
  except:
    if not p.isNil: p.close()

proc fetchRemoteResultAsync(task: Task, execId: int, localLogPath: string,
    jobName: string): Future[int] {.async.} =
  let localExitPath = localLogPath.changeFileExt("exit")
  if fileExists(localExitPath):
    try:
      return parseInt(readFile(localExitPath).strip())
    except:
      return -1

  let remoteLog = "/tmp/jobscheduler_" & $execId & ".log"
  let remoteExit = remoteLog & ".exit"
  let target = task.sshUser & "@" & task.sshHost

  var args = buildSshArgs(task)
  args.add(target)

  let catLogCmd = "cat " & remoteLog
  args.add(quoteShell(catLogCmd))

  var p = startProcess("ssh", args = args, options = {poUsePath,
      poStdErrToStdOut})

  var logContent = ""
  try:
    while p.running:
      await sleepAsync(100)
    let stream = p.outputStream
    var line = ""
    while stream.readLine(line):
      logContent.add(line & "\n")
    p.close()
  except:
    if not p.isNil: p.close()

  if logContent.len > 0:
    if fileExists(localLogPath):
      let f = open(localLogPath, fmAppend)
      try:
        f.write(logContent)
      finally:
        f.close()
    else:
      let header = "=== Job: " & jobName & " | Remote: " & task.sshHost & " ===\n"
      writeFile(localLogPath, header & logContent)

  args[args.len - 1] = quoteShell("cat " & remoteExit)

  var pExit = startProcess("ssh", args = args, options = {poUsePath,
      poStdErrToStdOut})

  var exitContent = ""
  var exitCodeInt = 1
  try:
    while pExit.running:
      await sleepAsync(100)

    let exitCode = pExit.peekExitCode
    let stream = pExit.outputStream
    var line = ""
    while stream.readLine(line):
      exitContent.add(line & "\n")
    pExit.close()

    if exitCode == 0 and exitContent.strip().len > 0:
      try:
        exitCodeInt = parseInt(exitContent.strip())
        writeFile(localExitPath, $exitCodeInt)

        let cleanupCmd = "rm " & remoteLog & " " & remoteExit
        args[args.len - 1] = quoteShell(cleanupCmd)
        discard startProcess("ssh", args = args, options = {poUsePath})

        return exitCodeInt
      except:
        discard
  except:
    if not pExit.isNil: pExit.close()

  try:
    writeFile(localExitPath, "1")
    if fileExists(localLogPath):
      let f = open(localLogPath, fmAppend)
      f.write("\n[Worker] Remote exit file not found. Assuming process failed/killed.\n")
      f.close()
  except:
    discard

  return 1

proc workerLoop() {.thread.} =
  proc monitorTasks() {.async, gcsafe.} =
    while workerRunning:
      let taskIds = getTaskIds()
      let now = now().utc

      for taskId in taskIds:
        let statusOpt = getTaskCopy(taskId)
        if statusOpt.isNone:
          continue

        let status = statusOpt.get

        let isRunning = await checkRemoteProcessAsync(status.task, status.pid,
            status.jobCommand)
        updateTaskRunning(taskId, isRunning, now)

        if isRunning and (now - status.lastLogSync).inSeconds >= 5:
          asyncCheck syncLogsAsync(status.task, status.execId, status.logPath)
          updateTaskLogSync(taskId, now)

        if not isRunning and status.exitCode == -1:
          let exitCode = await fetchRemoteResultAsync(status.task,
              status.execId, status.logPath, "")
          updateTaskExitCode(taskId, exitCode)

      await sleepAsync(500)

  # Don't use waitFor on infinite loop - it blocks the thread creation!
  asyncCheck monitorTasks()
  while workerRunning:
    poll(timeout = 100)

proc startWorker*() =
  if workerRunning:
    return

  sharedStatuses = initTable[int, RemoteTaskStatus]()
  initLock(statusLock)
  workerRunning = true

  var thr: Thread[void]
  # Thread-safe via locks, cast to bypass compiler check
  type WorkerProc = proc() {.thread, gcsafe, nimcall.}
  createThread(thr, cast[WorkerProc](workerLoop))

proc stopWorker*() =
  workerRunning = false

proc addTaskToMonitor*(taskId: int, execId: int, pid: int, logPath: string,
                       task: Task, jobCommand: string) =
  let now = now().utc
  let status = RemoteTaskStatus(
    taskId: taskId,
    execId: execId,
    pid: pid,
    logPath: logPath,
    task: task,
    jobCommand: jobCommand,
    isRunning: true,
    lastChecked: now,
    lastLogSync: now,
    exitCode: -1,
    errorMessage: ""
  )

  acquire(statusLock)
  sharedStatuses[taskId] = status
  release(statusLock)

proc removeTaskFromMonitor*(taskId: int) =
  acquire(statusLock)
  sharedStatuses.del(taskId)
  release(statusLock)

proc getTaskStatus*(taskId: int): Option[RemoteTaskStatus] =
  acquire(statusLock)
  if sharedStatuses.hasKey(taskId):
    result = some(sharedStatuses[taskId])
  else:
    result = none(RemoteTaskStatus)
  release(statusLock)
