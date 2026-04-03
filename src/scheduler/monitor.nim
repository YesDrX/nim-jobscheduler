import std/[times, options, os, streams, strutils,
    tables, json, sequtils]
import yaml/tojson
import smtp
import ../[database, orm, types, utils]

proc loadTasksFromDir*(s: SchedulerMonitor) =
  var anyChange = false

  proc getTaskPath(task: Task): string =
    return task.name & "||" & task.sourceFile

  if not dirExists(s.cfg.tasksDir):
    return

  let db = getReaderDb(s.cfg.database.path)
  defer: db.close()

  let tasks = getAllTasksOrdered(db)
  var taskPaths: seq[string] = @[]
  for (taskId, task) in tasks:
    if task.sourceFile.len > 0 and not fileExists(task.sourceFile):
      info "Removing task " & task.name & " from " & task.sourceFile
      s.dbChan[].send(DbMessage(
          kind: dbDeleteTask,
          deleteTaskId: taskId
      ))
      anyChange = true
    else:
      taskPaths.add(task.getTaskPath())

  for path in walkDirRec(s.cfg.tasksDir):
    if fileExists(path) and (path.endsWith(".yaml") or path.endsWith(".yml")):
      if path notin s.lastFileModTime or path.getFileInfo.lastWriteTime !=
          s.lastFileModTime[path]:
        s.lastFileModTime[path] = path.getFileInfo.lastWriteTime
      else:
        continue

      var fs = newFileStream(path)
      if fs.isNil: continue
      defer: fs.close()

      try:
        let jsonNode = loadToJson(fs)[0]
        var task = deserializeJson[Task](jsonNode)
        if "enabled" notin jsonNode:
          task.enabled = true
        task.sourceFile = path
        task.folder = path.parentDir
        task.groupName = path.parentDir.absolutePath.replace(
            s.cfg.tasksDir.absolutePath, "").strip(chars = {'/', '\\'})
        let resolvedCalendarPath = resolveCalendarPath(task.calendarPath, path)
        if resolvedCalendarPath.len > 0:
          if fileExists(resolvedCalendarPath):
            task.dateList = readFile(resolvedCalendarPath).splitLines()
          else:
            warn "Calendar file not found: " & resolvedCalendarPath

        let configHash = calculateConfigHash(task, resolvedCalendarPath)
        task.configHash = configHash

        if task.getTaskPath in taskPaths:
          let existingTasks = tasks.filterIt((it.data.sourceFile ==
              task.sourceFile) and (it.data.name == task.name))
          if existingTasks.len > 0:
            let (existingTaskId, existingTask) = existingTasks[0]
            if existingTask.configHash != task.configHash:
              info "Task " & task.folder & "/" & task.name & " has changed. Reloading."
              s.dbChan[].send(DbMessage(
                  kind: dbUpdateTask,
                  updateTaskId: existingTaskId,
                  updatedTask: task
              ))
              anyChange = true
          else:
            warn "Task not found in database: " & task.folder & "/" & task.name
        else:
          info "Adding task " & task.folder & "/" & task.name & " from " & path
          s.dbChan[].send(DbMessage(
              kind: dbInsertTask,
              task: task,
              taskResultCh: nil
          ))
          anyChange = true

      except:
        error "Error loading YAML " & path & ": " & getCurrentExceptionMsg()

  s.lastTasksScanTime = now().utc
  if anyChange:
    s.schedulerChan[].send(SchedulerSignal(kind: ssReloadTasks))

proc runSchedulerMonitor*(s: SchedulerMonitor) {.gcsafe.} =
  info "Starting scheduler monitor"
  let db = getReaderDb(s.cfg.database.path)
  defer: db.close()

  s.loadTasksFromDir()

  while getIsRunning():
    try:
      let referenceNowTime = now().utc

      if (referenceNowTime - s.lastTasksScanTime).inSeconds > 15:
        s.loadTasksFromDir()
        s.lastTasksScanTime = referenceNowTime

      if not s.lastExecutionsCleanupTime.isInitialized or (referenceNowTime -
          s.lastExecutionsCleanupTime).inHours > 1:
        s.dbChan[].send(DbMessage(kind: dbCleanupExecutions))
        s.lastExecutionsCleanupTime = referenceNowTime

      while s.monitorChan[].peek() > 0:
        let msg = s.monitorChan[].recv()
        debug "Received monitor signal: " & $msg.kind
        case msg.kind:
          of smmAlert:
            try:
              let client = newSmtp(useSsl = s.cfg.smtp.useSSL)
              defer: client.close()
              client.connect(s.cfg.smtp.host, s.cfg.smtp.port.Port)
              if s.cfg.smtp.fromAddr.len > 0 and s.cfg.smtp.password.len > 0:
                client.auth(s.cfg.smtp.fromAddr, s.cfg.smtp.password)
              let emailContent = createMessage(
                mSubject = "Jobscheduler Alert: " & msg.messageTitle.replace(
                    "\n", " ").replace("\r", " "),
                mBody = msg.message,
                sender = s.cfg.smtp.fromAddr,
                mTo = s.cfg.smtp.toAddrs
              )
              client.sendMail(s.cfg.smtp.fromAddr, s.cfg.smtp.toAddrs, $emailContent)
              info "Alert email sent to " & s.cfg.smtp.toAddrs.join(",") &
                  " : " & msg.messageTitle
            except:
              error "Error sending alert: " & getCurrentExceptionMsg()
              error "Alert message:\n" & msg.message
    except Exception as e:
      error "Error in monitor loop: " & getCurrentExceptionMsg()
      s.monitorChan[].send(SchedulerMonitorSignal(
        kind: smmAlert,
        messageTitle: "Monitor Error: " & getCurrentExceptionMsg(),
        message: e.getStackTrace()
      ))

    sleep 1000
  info "Stopping scheduler monitor"
