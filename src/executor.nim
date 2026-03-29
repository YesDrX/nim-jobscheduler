import std/[tables, sequtils, os, osproc, times]
import ./[types, database, orm, utils]

proc runLocalCommand*(
    executor: Executor,
    taskId: int,
    task: Task,
    jobId: int,
    job: Job,
    nextJobId: int,
    manualTriggered: bool = false
): tuple[exec: Execution, p: Process] =
    let dt = now().format("yyyyMMdd") & "_" & now().format("HHmmss")
    let logDir = (executor.cfg.workingDir.expandTilde / task.name /
            job.name).sanitizeFileName
    createDir(logDir)

    let logFile = logDir / (dt & ".log")

    var scriptPath: string
    when defined(windows):
        scriptPath = logDir / (dt & ".bat")
        let scriptContent = "@echo off\r\ncall " & job.command & " > \"" &
                logFile & "\" 2>&1\r\n"
        writeFile(scriptPath, scriptContent)
    else:
        scriptPath = logDir / (dt & ".sh")
        let scriptContent = "#!/bin/bash\nexec > \"" & logFile & "\" 2>&1\n" &
                job.command & "\n"
        writeFile(scriptPath, scriptContent)
        inclFilePermissions(scriptPath, {fpUserExec, fpGroupExec, fpOthersExec})

    let p = startProcess(scriptPath, options = {poEvalCommand, poParentStreams})

    var execution = Execution(
        jobId: jobId,
        nextJobId: nextJobId,
        taskId: taskId,
        jobName: job.name,
        taskName: task.name,
        pid: p.processID(),
        processStartTime: now().toTime().toUnix(),
        startTime: now(),
        status: esRunning,
        logFile: logFile,
        scriptFilename: scriptPath,
        manualTriggered: manualTriggered
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
): tuple[exec: Execution, p: Process] =
    let dt = now().format("yyyyMMdd") & "_" & now().format("HHmmss")
    let logDir = (executor.cfg.workingDir.expandTilde / task.name /
            job.name).sanitizeFileName
    createDir(logDir)

    let logFile = logDir / (dt & ".log")

    let keyPath = if task.sshKeyPath.len >
            0: task.sshKeyPath else: executor.cfg.ssh.defaultKeyPath
    let sshPort = if task.sshPort > 0: task.sshPort else: 22
    let sshUser = task.sshUser
    let sshHost = task.sshHost

    var scriptPath: string
    let remoteScriptPath = ("/tmp/" & task.name & "_" & job.name & "_" & dt &
            ".sh").sanitizeFileName

    let jobCommandFile = logDir / (dt & "_cmd.sh")
    let cmdScriptContent = "#!/bin/bash\n" & job.command & "\n"
    writeFile(jobCommandFile, cmdScriptContent)

    let scpCmd = "scp -q -o BatchMode=yes -P " & $sshPort & " -i \"" & keyPath &
            "\" \"" & jobCommandFile & "\" " & sshUser & "@" & sshHost & ":" & remoteScriptPath
    let sshCmd = "ssh -o ServerAliveInterval=60 -o BatchMode=yes -p " &
            $sshPort & " -i \"" & keyPath & "\" " & sshUser & "@" & sshHost &
            " \"bash " & remoteScriptPath & "\""

    when defined(windows):
        scriptPath = logDir / (dt & "_remote.bat")
        let scriptContent = "@echo off\r\ncall " & scpCmd & "\r\ncall " &
                sshCmd & " > \"" & logFile & "\" 2>&1\r\n"
        writeFile(scriptPath, scriptContent)
    else:
        scriptPath = logDir / (dt & "_remote.sh")
        let scriptContent = "#!/bin/bash\n" & scpCmd & "\nexec > \"" & logFile &
                "\" 2>&1\n" & sshCmd & "\n"
        writeFile(scriptPath, scriptContent)
        inclFilePermissions(scriptPath, {fpUserExec, fpGroupExec, fpOthersExec})

    let p = startProcess(scriptPath, options = {poEvalCommand, poParentStreams})

    var execution = Execution(
        jobId: jobId,
        nextJobId: nextJobId,
        taskId: taskId,
        jobName: job.name,
        taskName: task.name,
        pid: p.processID(),
        processStartTime: now().toTime().toUnix(),
        startTime: now(),
        status: esRunning,
        logFile: logFile,
        scriptFilename: remoteScriptPath,
        manualTriggered: manualTriggered
    )

    return (execution, p)

proc cleanupScripts*(executor: Executor, execution: Execution, task: Task) =
    if execution.scriptFilename.len == 0: return

    if task.taskType == ttRemote:
        let keyPath = if task.sshKeyPath.len >
                0: task.sshKeyPath else: executor.cfg.ssh.defaultKeyPath
        let sshPort = if task.sshPort > 0: task.sshPort else: 22
        let rmCmd = "ssh -o BatchMode=yes -p " & $sshPort & " -i \"" & keyPath &
                "\" " & task.sshUser & "@" & task.sshHost & " \"rm -f " &
                execution.scriptFilename & "\""
        discard execShellCmd(rmCmd)

        let localDir = executor.cfg.workingDir.expandTilde / task.name /
                execution.jobName
        for file in walkPattern(localDir / "*_remote.*"):
            try: removeFile(file)
            except OSError: discard
        for file in walkPattern(localDir / "*_cmd.sh"):
            try: removeFile(file)
            except OSError: discard
    else:
        if fileExists(execution.scriptFilename):
            try: removeFile(execution.scriptFilename)
            except OSError: discard

proc checkProcessStatus*(execution: var Execution,
        p: Process): ExecutionStatus =
    var status: ExecutionStatus
    if p != nil:
        if p.running():
            status = esRunning
        else:
            let exitCode = p.peekExitCode()
            execution.exitCode = exitCode
            if exitCode == 0:
                status = esSuccess
            else:
                status = esFailed
    else:
        # Process handle missing, check system processes
        let pid = execution.pid
        if pid <= 0:
            status = esLost
        else:
            when defined(posix):
                let code = execShellCmd("kill -0 " & $pid & " > /dev/null 2>&1")
                if code == 0:
                    status = esRunning
                else:
                    status = esLost
            elif defined(windows):
                let code = execShellCmd("tasklist /FI \"PID eq " & $pid &
                        "\" | findstr " & $pid & " > nul 2>&1")
                if code == 0:
                    status = esRunning
                else:
                    status = esLost
            else:
                status = esLost

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
        if p != nil and p.running():
            p.terminate()

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

    while getIsRunning():
        try:
            while executor.executorChan[].peek() > 0:
                let msg = executor.executorChan[].recv()
                debug "Received executor message: " & $msg.kind
                case msg.kind:
                    of ExecutorSignalType.estTriggerTask:
                        let jobs = db.queryRowsJob("taskId = " &
                                $msg.triggerTaskId & " ORDER BY orderIdx ASC")
                        executor.executeTask(msg.triggerTaskId,
                                msg.triggerTaskTask,

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

