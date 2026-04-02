import std/[tables, sequtils, os, osproc, times, strutils]
import ./[types, database, orm, utils]

when not defined(windows):
  import posix
else:
  import winlean

proc runScript*(scriptPath: string): ExecutionProcess = 
  let fullPath = scriptPath.absolutePath()
  debug "Running script: " & fullPath
  if not fileExists(fullPath):
    raise newException(OSError, "Script not found: " & fullPath)
  
  when defined(windows):
    let p = startProcess("cmd.exe", args = ["/c", fullPath], 
                         options = {poDaemon, poParentStreams})
    return p.processID().ExecutionProcess

  else:
    let pid = fork()
    
    if pid < 0:
      raise newException(OSError, "Fork failed")
    
    if pid == 0:
      discard setsid() # Create new session (detaches from parent/terminal)
      discard execv(fullPath.cstring, nil.cstringArray)
    else:
      return pid.ExecutionProcess

proc isRunning*(p: ExecutionProcess): bool =
  when defined(windows):
    let handle = openProcess(SYNCHRONIZE, 0, p.int.DWORD)
    if handle == 0:
      return false  # Process doesn't exist or no permission
    defer: discard closeHandle(handle)
    let res = waitForSingleObject(handle, 0)
    return res == WAIT_TIMEOUT  # Still running if timeout (not yet signaled)

  else:
    # waitpid with WNOHANG returns 0 if process is still running
    var status: cint
    let res = waitpid(p.Pid, status, WNOHANG)
    if res == 0:
      return true   # Still running
    elif res == p.int:
      return false  # Exited
    else:
      # res == -1: ECHILD means no such child (already reaped or never a child)
      return false

proc getExitCode*(execution: Execution, p: ExecutionProcess): int =
  ## Called only when the process is no longer running.
  ## First checks the .exit sidecar file written by the script.
  ## Falls back to OS-level exit code query, or returns -1 if unavailable.
  let exitFile = execution.exitCodeFilename
  if fileExists(exitFile):
    try:
      let code = readFile(exitFile).strip().parseInt()
      return code
    except ValueError:
      debug "Exit file " & exitFile & " is malformed: " & readFile(exitFile)
  else:
    debug "Exit file not found: " & exitFile

  # .exit file not present: fall back to OS-level query (best-effort).
  when defined(windows):
    let handle = openProcess(SYNCHRONIZE or PROCESS_QUERY_INFORMATION, 0, p.int.DWORD)
    if handle == 0:
      return -1
    defer: discard closeHandle(handle)
    discard waitForSingleObject(handle, INFINITE)  # Block until done
    var code: DWORD
    if getExitCodeProcess(handle, code) == 0:
      return -1
    return code.int

  else:
    var status: cint
    let res = waitpid(p.Pid, status, WNOHANG)  # Non-blocking: process already gone
    if res == p.int:
      if WIFEXITED(status):
        return WEXITSTATUS(status)
      elif WIFSIGNALED(status):
        return 128 + WTERMSIG(status)  # Unix convention: 128 + signal number
    # Process has already been reaped or not found – it crashed without leaving an exit file
    return -1

proc terminate*(p: ExecutionProcess) =
  when defined(windows):
    let handle = openProcess(PROCESS_TERMINATE, 0, p.int.DWORD)
    if handle == 0:
      return
    defer: discard closeHandle(handle)
    discard terminateProcess(handle, 1)

  else:
    # First attempt graceful shutdown via SIGTERM
    let termRes = kill(p.Pid, SIGTERM)
    if termRes != 0:
      return  # Process already gone

    # Give it up to 5 seconds to exit cleanly
    var status: cint
    for _ in 0 ..< 50:
      let res = waitpid(p.Pid, status, WNOHANG)
      if res == p.Pid:
        return  # Exited cleanly
      sleep(100)

    # Force kill if still alive
    discard kill(p.Pid, SIGKILL)
    discard waitpid(p.Pid, status, 0)  # Reap the zombie

proc runLocalCommand*(
    executor: Executor,
    taskId: int,
    task: Task,
    jobId: int,
    job: Job,
    nextJobId: int,
    manualTriggered: bool = false
): tuple[exec: Execution, p: ExecutionProcess] =
    let dt = now().format("yyyyMMdd") & "_" & now().format("HHmmss")
    let logDir = (executor.cfg.workingDir.expandTilde / task.name /
            job.name).sanitizeFileName
    createDir(logDir)

    let logFile = logDir / (dt & ".log")

    var scriptPath: string
    when defined(windows):
        scriptPath = logDir / (dt & ".bat")
        let exitFile = scriptPath & ".exit"
        let scriptContent = "@echo off\r\ncall " & job.command & " > \"" &
                logFile & "\" 2>&1\r\necho %ERRORLEVEL% > \"" & exitFile & "\"\r\n"
        writeFile(scriptPath, scriptContent)
    else:
        scriptPath = logDir / (dt & ".sh")
        let exitFile = scriptPath & ".exit"
        let scriptContent = "#!/bin/bash\nexec > \"" & logFile & "\" 2>&1\n" &
                job.command & "\n" &
                "echo $? > \"" & exitFile & "\"\n"
        writeFile(scriptPath, scriptContent)
        inclFilePermissions(scriptPath, {fpUserExec, fpGroupExec, fpOthersExec})

    let p = runScript(scriptPath)

    var execution = Execution(
        jobId: jobId,
        nextJobId: nextJobId,
        taskId: taskId,
        jobName: job.name,
        taskName: task.name,
        pid: p.int,
        processStartTime: now().toTime().toUnix(),
        startTime: now(),
        status: esRunning,
        logFile: logFile,
        scriptFilename: scriptPath,
        manualTriggered: manualTriggered,
        exitCodeFilename: exitFile
    )

    return (execution, p)

proc runRemoteCommand*(
    executor: Executor,
    taskId: int,
    task: Task,
    jobId: int,
    job: Job,
    nextJobId: int,
    manualTriggered: bool = false
): tuple[exec: Execution, p: ExecutionProcess] =
    let dt = now().format("yyyyMMdd") & "_" & now().format("HHmmss")
    let logDir = (executor.cfg.workingDir.expandTilde / task.name /
            job.name).sanitizeFileName
    createDir(logDir)

    let logFile = logDir / (dt & ".log")

    let keyPath = (if task.sshKeyPath.len >
            0: task.sshKeyPath else: executor.cfg.ssh.defaultKeyPath).expandTilde
    let sshPort = if task.sshPort > 0: task.sshPort else: 22
    let sshUser = task.sshUser
    let sshHost = task.sshHost

    var scriptPath: string
    let remoteScriptPath = ("/tmp/" & task.name & "_" & job.name & "_" & dt &
            ".sh").sanitizeFileName
    
    let jobCommandFile = logDir / (dt & "_cmd.sh")
    let cmdScriptContent = "#!/bin/bash\n" & job.command & "\n"
    writeFile(jobCommandFile, cmdScriptContent)

    let scpCmd = "scp -q -o StrictHostKeyChecking=no -o BatchMode=yes -P " & $sshPort & " -i \"" & keyPath &
            "\" \"" & jobCommandFile & "\" " & sshUser & "@" & sshHost & ":" & remoteScriptPath
    let sshCmd = "ssh -o ServerAliveInterval=60 -o StrictHostKeyChecking=no -o BatchMode=yes -p " &
            $sshPort & " -i \"" & keyPath & "\" " & sshUser & "@" & sshHost &
            " \"bash " & remoteScriptPath & "\""

    when defined(windows):
        scriptPath = logDir / (dt & "_remote.bat")
        let exitFile = scriptPath & ".exit"
        let scriptContent = "@echo off\r\ncall " & scpCmd & "\r\ncall " &
                sshCmd & " > \"" & logFile & "\" 2>&1\r\necho %ERRORLEVEL% > \"" &
                exitFile & "\"\r\n"
        writeFile(scriptPath, scriptContent)
    else:
        scriptPath = logDir / (dt & "_remote.sh")
        let exitFile = scriptPath & ".exit"
        let scriptContent = "#!/bin/bash\n" & scpCmd & "\nexec > \"" & logFile &
                "\" 2>&1\n" & sshCmd & "\n" &
                "echo $? > \"" & exitFile & "\"\n"
        writeFile(scriptPath, scriptContent)
        inclFilePermissions(scriptPath, {fpUserExec, fpGroupExec, fpOthersExec})

    let p = runScript(scriptPath)

    var execution = Execution(
        jobId: jobId,
        nextJobId: nextJobId,
        taskId: taskId,
        jobName: job.name,
        taskName: task.name,
        pid: p.int,
        processStartTime: now().toTime().toUnix(),
        startTime: now(),
        status: esRunning,
        logFile: logFile,
        scriptFilename: scriptPath,
        remoteScriptFilename: remoteScriptPath,
        manualTriggered: manualTriggered,
        exitCodeFilename: exitFile,
        remoteSshHost: sshHost,
        remoteSshPort: sshPort,
        remoteSshUser: sshUser,
        remoteSshKeyPath: keyPath
    )

    return (execution, p)

proc cleanupScripts*(executor: Executor, execution: Execution, task: Task) =
  debug "Cleaning up scripts for task " & $task.name
  if task.taskType == ttRemote:
    if execution.remoteScriptFilename.len > 0:
      debug "Removing remote script file: " & execution.remoteScriptFilename & " from host " & execution.remoteSshHost
      let rmCmd = "ssh -o StrictHostKeyChecking=no -o BatchMode=yes -p " & $execution.remoteSshPort & " -i " & execution.remoteSshKeyPath & " " & execution.remoteSshUser & "@" & execution.remoteSshHost & " \"rm -f " & execution.remoteScriptFilename & "\""
      try:
        discard execShellCmd(rmCmd)
      except:
        debug "Failed to remove remote script file: " & execution.remoteScriptFilename & " from host " & execution.remoteSshHost

  if fileExists(execution.scriptFilename):
    debug "Removing script file: " & execution.scriptFilename
    try: removeFile(execution.scriptFilename)
    except OSError: discard
  
  if fileExists(execution.exitCodeFilename):
    debug "Removing exit file: " & execution.exitCodeFilename
    try: removeFile(execution.exitCodeFilename)
    except OSError: discard

proc checkProcessStatus*(execution: var Execution,
        p: ExecutionProcess): ExecutionStatus =
    var status: ExecutionStatus
    if p.int <= 0:
        status = esLost
    else:
        if isRunning(p):
            status = esRunning
        else:
            let exitCode = getExitCode(execution, p)
            execution.exitCode = exitCode
            if exitCode == 0:
                status = esSuccess
            else:
                status = esFailed
            debug "Execution with pid " & $p.int & " finished with exit code " & $exitCode
    return status

proc executeJob*(
    executor: Executor,
    taskId: int,
    task: Task,
    jobId: int,
    job: Job,
    nextJobId: int,
    nextJob: Job,
    jobsTuple: seq[tuple[dbId: int, data: Job]],
    manualTriggered: bool = false
) =
    info "Executing job " & $jobId & " " & job.name & " for task " & $taskId &
            " " & task.name
    let (exec, p) = if task.taskType == ttLocal:
        executor.runLocalCommand(taskId, task, jobId, job, nextJobId)
    else:
        executor.runRemoteCommand(taskId, task, jobId, job, nextJobId)

    var resultCh = create(Channel[int])
    resultCh[].open()
    executor.dbChan[].send(DbMessage(
        kind: dbInsertExecution,
        execution: exec,
        executionResultCh: resultCh
    ))
    var dbExec = exec
    let execId = resultCh[].recv()
    resultCh[].close()
    dealloc(resultCh)
    executor.liveExecutions[execId] = (execution: dbExec, p: p, task: task,
            jobsTuple: jobsTuple)

proc executeTask*(
    executor: Executor,
    taskId: int,
    task: Task,
    jobs: seq[tuple[dbId: int, data: Job]],
    manualTriggered: bool = false
) =
    info "Executing task " & $taskId & " " & task.name
    if jobs.len == 0:
        return

    if task.parallel:
        for j in jobs:
            executor.executeJob(taskId, task, j.dbId, j.data, -1, Job(), jobs)
    else:
        let firstJob = jobs[0]
        let nextJobId = if jobs.len > 1: jobs[1].dbId else: -1
        let nextJob = if jobs.len > 1: jobs[1].data else: Job()
        executor.executeJob(taskId, task, firstJob.dbId, firstJob.data,
                nextJobId, nextJob, jobs)

proc cancelExecution*(executor: Executor, executionId: int) =
    info "Cancelling execution " & $executionId
    if executor.liveExecutions.hasKey(executionId):
        var execTuple = executor.liveExecutions[executionId]
        let p = execTuple.p
        if p.int > 0 and p.isRunning():
            p.terminate()
            executor.monitorChan[].send(SchedulerMonitorSignal(
                kind: smmAlert,
                messageTitle: "Execution cancelled: " & execTuple.task.name & " " & execTuple.execution.jobName,
                message: "Execution " & $executionId & " cancelled by user"
            ))

        executor.cleanupScripts(execTuple.execution, execTuple.task)

        executor.dbChan[].send(DbMessage(
            kind: dbUpdateExecutionStatus,
            statusExecutionId: executionId,
            newStatus: esCancelled,
            newEndTime: now(),
            newExitCode: -1
        ))
        executor.liveExecutions.del(executionId)

proc checkLiveExecutions*(executor: Executor) =
    var toRemove: seq[int] = @[]

    for execId in toSeq(executor.liveExecutions.keys):
        var pair = executor.liveExecutions[execId]
        let status = checkProcessStatus(pair.execution, pair.p)

        if status != esRunning:
            toRemove.add(execId)

            executor.dbChan[].send(DbMessage(
                kind: dbUpdateExecutionStatus,
                statusExecutionId: execId,
                newStatus: status,
                newEndTime: now(),
                newExitCode: pair.execution.exitCode
            ))

            executor.cleanupScripts(pair.execution, pair.task)

            if status == esSuccess and pair.execution.nextJobId != -1:
                let jobRows = pair.jobsTuple
                var nextIdx = -1
                for i, r in jobRows:
                    if r.dbId == pair.execution.nextJobId:
                        nextIdx = i
                        break
                if nextIdx != -1:
                    let nJob = jobRows[nextIdx]
                    let nextNextJobId = if nextIdx + 1 <
                            jobRows.len: jobRows[nextIdx+1].dbId else: -1
                    let nextNextJob = if nextIdx + 1 < jobRows.len: jobRows[
                            nextIdx+1].data else: Job()
                    executor.executeJob(pair.execution.taskId, pair.task,
                            nJob.dbId, nJob.data, nextNextJobId, nextNextJob, jobRows)
            elif status == esFailed or status == esLost:
                var messageContent = "Jobscheduler Alert:\n" &
                        "Execution Log: " & executor.cfg.server.externalHost &
                                "/execution_log?id=" &
                        $execId & "\n" &
                        "Task: " & pair.task.name & "\n" &
                        "Job: " & pair.execution.jobName & "\n" &
                        "Status: " & $status & "\n" &
                        "Log: \n"
                if fileExists(pair.execution.logFile):
                    let logContent = readFile(pair.execution.logFile)
                    messageContent &= logContent

                executor.monitorChan[].send(SchedulerMonitorSignal(
                    kind: smmAlert,
                    messageTitle: "Executon for " & pair.task.name &
                    " failed/lost",
                    message: messageContent
                ))

    for id in toRemove:
        executor.liveExecutions.del(id)

proc runExecutor*(executor: Executor) =
    info "Starting executor"
    let db = getReaderDb(executor.cfg.database.path)
    defer: db.close()

    # change execution status on startup
    let liveExecutions = db.queryRowsExecution("""status = '"Running"'""")
    var lostExecutionMessages: seq[string]
    for (execId, exec) in liveExecutions:
        var execCopy = exec
        execCopy.status = esLost
        executor.dbChan[].send(DbMessage(
            kind: dbUpdateExecutionStatus,
            statusExecutionId: execId,
            newStatus: esLost,
            newEndTime: now(),
            newExitCode: -1
        ))
        lostExecutionMessages.add("Task: " & exec.taskName & " Job: " & exec.jobName & " Execution ID: " & $execId & " PID: " & $exec.pid)
    if lostExecutionMessages.len > 0:
        var messageContent = "On startup, the following executions were found as running. They are now marked as lost. Please check if they are still running and cancel them if necessary.\n"
        executor.monitorChan[].send(SchedulerMonitorSignal(
            kind: smmAlert,
            messageTitle: "Running Jobs Found on Startup",
            message: messageContent & lostExecutionMessages.join("\n")
        ))

    while getIsRunning():
        try:
            while executor.executorChan[].peek() > 0:
                let msg = executor.executorChan[].recv()
                debug "Received executor message: " & $msg.kind
                case msg.kind:
                    of ExecutorSignalType.estTriggerTask:
                        let jobs = db.queryRowsJob("taskId = " &
                                $msg.triggerTaskId & " ORDER BY orderIdx ASC")
                        executor.executeTask(msg.triggerTaskId, msg.triggerTaskTask,
                                jobs, msg.triggerTaskManualTriggered)
                        
                    of ExecutorSignalType.estTriggerJob:
                        let jobs = db.queryRowsJob("taskId = " &
                                $msg.triggerJobTaskId & " ORDER BY orderIdx ASC")
                        executor.executeJob(msg.triggerJobTaskId,
                                msg.triggerJobTask, msg.triggerJobJobId,
                                msg.triggerJobJob, nextJobId = -1,
                                nextJob = Job(),
                                jobsTuple = jobs,
                                manualTriggered = msg.triggerJobManualTriggered)
                        
                    of ExecutorSignalType.estCancelExecution:
                        executor.cancelExecution(msg.cancelExecutionId)

            executor.checkLiveExecutions()
        except Exception as e:
            error "Error in executor loop: " & getCurrentExceptionMsg()
            executor.monitorChan[].send(SchedulerMonitorSignal(
                kind: smmAlert,
                messageTitle: "Executor Error: " & getCurrentExceptionMsg(),
                message: e.getStackTrace()
            ))

        sleep(1000)
    info "Stopping executor"

