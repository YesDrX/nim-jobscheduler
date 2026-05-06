import std/[asyncdispatch, strutils, json, times, os, uri,
    sequtils, asynchttpserver, tables, options, strformat]

when defined(posix):
  import posix

import db_connector/db_sqlite
import ./auth
import ../[database, orm, types, config, utils, serialize]
import ../webui/views
import ../scheduler/triggers

const StaticFiles = {
  "/static/app.js": staticRead("../webui/static/app.js"),
  "/static/pages.css": staticRead("../webui/static/pages.css"),
  "/static/style.css": staticRead("../webui/static/style.css"),
  "/static/toggles.css": staticRead("../webui/static/toggles.css")
}.toTable

# http helpers
proc toHeaders(headers: Table[string, string]): HttpHeaders =
  var httpHeaders = newHttpHeaders()
  for key, value in headers:
    httpHeaders[key] = value
  return httpHeaders

proc decodeQueryAsTable(query: string): Table[string, string] =
  result = initTable[string, string]()
  for key, val in decodeQuery(query):
    result[key] = val

# Response Helpers
proc resp(req: Request, code: HttpCode, content: string,
    headers: HttpHeaders = newHttpHeaders()) {.async.} =
  await req.respond(code, content, headers)

proc respJson(req: Request, content: JsonNode,
    headers: HttpHeaders = newHttpHeaders()) {.async.} =
  var headersCopy = headers
  headersCopy["Content-Type"] = "application/json"
  await req.respond(Http200, $content, headersCopy)

proc respHtml(req: Request, content: string) {.async.} =
  var headers = newHttpHeaders()
  headers["Content-Type"] = "text/html;charset=utf-8"
  await req.respond(Http200, content, headers)

proc respFile(req: Request, content, filename: string) {.async.} =
  var headers = newHttpHeaders()
  headers["Content-Type"] = "text/html;charset=utf-8"
  headers["Content-Disposition"] = "attachment; filename=" & filename
  await req.respond(Http200, content, headers)

proc resp404(req: Request, msg: string = "Not Found") {.async.} =
  await req.respond(Http404, msg)

proc resp500(req: Request, msg: string) {.async.} =
  await req.respond(Http500, "Internal Server Error: " & msg)

proc redirect(req: Request, location: string) {.async.} =
  var headers = newHttpHeaders()
  headers["Location"] = location
  await req.respond(Http302, "", headers)

proc getTokenFromCookie(req: Request): string =
  if req.headers.hasKey("cookie"):
    let cookieSeq = req.headers.table["cookie"]
    for cookieStr in cookieSeq:
      if "session=" in cookieStr:
        let parts = cookieStr.split(";")
        for part in parts:
          let trimmed = part.strip()
          if trimmed.startsWith("session="):
            return trimmed[8..^1]
  return ""

proc isAuthenticated(req: Request, sessions: Table[string, int], db: DbConn): int =
  # Check for Authorization header (Bearer token from API)
  if req.headers.hasKey("authorization"):
    let authHeaders = req.headers.table.getOrDefault("authorization", @[])
    for authHeader in authHeaders:
      if authHeader.startsWith("Bearer "):
        debug "Authorization header: " & authHeader
        if authHeader.len > 7:
          let token = authHeader[7..^1]
          if sessions.hasKey(token):
            return sessions[token]
          # Also check DB TokenTable for API tokens
          let dbTokens = queryRowsToken(db, "token = '" & token.serialize() & "'")
          if dbTokens.len > 0:
            return dbTokens[0].data.userId

  # Check for session cookie
  let token = getTokenFromCookie(req)
  if token != "":
    if sessions.hasKey(token):
      return sessions[token]

  return -1

proc runWebServer*(webServer: WebServer) =
  let db = getReaderDb(webServer.cfg.database.path)
  defer: db.close()
  var sessions: Table[string, int] = initTable[string, int]()
  let cfg = webServer.cfg
  var
    dbChan = webServer.dbChan
    schedulerChan = webServer.schedulerChan
    executorChan = webServer.executorChan
    monitorChan = webServer.monitorChan

  proc handleRequest(req: Request) {.async, gcsafe.} =
    let path = req.url.path
    let httpMethod = req.reqMethod

    debug "Request: " & $httpMethod & " " & path

    # --- End points without authentication ---
    if path == "/login" and httpMethod == HttpGet:
      if isAuthenticated(req, sessions, db) > 0:
        await redirect(req, "/dashboard")
        return
      else:
        await respHtml(req, renderLogin())
        return

    if path == "/api/login" and httpMethod == HttpPost:
      let body = req.body
      let data = parseJson(body)
      let username = data["username"].getStr
      let password = data["password"].getStr
      info "Login request  Username=" & username

      let userOpt = getUserByUsername(db, username)
      if userOpt.isNone:
        await resp(req, Http401, "Invalid credentials")
        return

      let user = userOpt.get()
      if user.data.passwordHash != password:
        await resp(req, Http401, "Invalid credentials")
        return

      let sessionToken = genToken()
      sessions[sessionToken] = user.dbId

      # Set session cookie
      let headers = {"Set-Cookie": "session=" & sessionToken &
          "; Path=/; HttpOnly; SameSite=Strict"}.toTable.toHeaders
      let response = %*{"status": "ok", "token": sessionToken,
          "userId": user.dbId}
      await respJson(req, response, headers)
      return

    if path == "/isHealthy" and httpMethod == HttpGet:
      await respJson(req, %*{
        "status": "ok",
        "timestamp": now().format("yyyy-MM-dd'T'HH:mm:sszzz")
      })
      return

    if path == "/api/publicKey" and httpMethod == HttpGet:
      await respJson(req, %*{"publicKey": getPassphrase()})
      return

    # -- Other end points need authentication ---
    let userId = isAuthenticated(req, sessions, db)
    if userId < 0:
      await redirect(req, "/login")
      return

    # --- Static Files ---
    if path.startsWith("/static/"):
      let realPath = if "?" in path: path.split("?")[0] else: path
      if realPath in StaticFiles:
        var mime = "application/octet-stream"
        if realPath.endsWith(".css"): mime = "text/css"
        elif realPath.endsWith(".js"): mime = "application/javascript"
        elif realPath.endsWith(".png"): mime = "image/png"
        elif realPath.endsWith(".svg"): mime = "image/svg+xml"
        var headers = {"Content-Type": mime}.toTable().toHeaders()
        await resp(req, Http200, StaticFiles[realPath], headers)
      else:
        await resp404(req)
      return

    # Wegpages
    # --- Dashboard / Root ---
    if (path == "/" or path == "/dashboard") and httpMethod == HttpGet:
      let executions = queryRowsExecution(db, "1=1 ORDER BY startTime DESC LIMIT 200")
      await respHtml(req, renderDashboard(executions))
      return

    # --- Tasks Page ---
    if path == "/tasks" and httpMethod == HttpGet:
      let tasks = getAllTasksOrdered(db)
      let executions = queryRowsExecution(db, "1=1 ORDER BY taskId, startTime DESC")
      var lastTaskExecutions: Table[int, Execution] = initTable[int, Execution]()
      for (execId, exec) in executions:
        if exec.taskId notin lastTaskExecutions:
          lastTaskExecutions[exec.taskId] = exec
        elif lastTaskExecutions[exec.taskId].startTime < exec.startTime:
          lastTaskExecutions[exec.taskId] = exec
      var nextTaskRuntims: seq[DateTime]
      for (taskId, task) in tasks:
        if taskId in lastTaskExecutions:
          let nextRunTime = getNextTrigger(task, now(), some(lastTaskExecutions[
              taskId].startTime), none(DateTime))
          if nextRunTime.isSome():
            nextTaskRuntims.add(nextRunTime.get())
          else:
            nextTaskRuntims.add(DateTime())
        else:
          let nextRunTime = getNextTrigger(task, now(), none(DateTime), none(DateTime))
          if nextRunTime.isSome():
            nextTaskRuntims.add(nextRunTime.get())
          else:
            nextTaskRuntims.add(DateTime())
      await respHtml(req, renderTasks(tasks, nextTaskRuntims))
      return

    # --- Task Detail ---
    if path.startsWith("/task_detail") and httpMethod == HttpGet:
      let queryJson = req.url.query.decodeQueryAsTable()
      let id = queryJson.getOrDefault("id", "-1").parseInt
      debug "Loading task details for id=" & $id

      let tOpt = getTaskById(db, id)
      if tOpt.isNone:
        debug "Task not found in db: " & $id
        await resp404(req, "Task not found")
        return

      var task = tOpt.get().data
      let jobs = getJobsByTaskIdOrdered(db, id)
      let recentExecs = queryRowsExecution(db, "taskId = " & $id &
          " ORDER BY startTime DESC LIMIT 10")
      let lastExec = if recentExecs.len > 0: some(recentExecs[
          0].data) else: none(Execution)
      var nextRunTime: DateTime
      if lastExec.isSome():
        let nextRunTimeOpt = getNextTrigger(task, now(), some(lastExec.get(
          ).startTime), none(DateTime))
        if nextRunTimeOpt.isSome():
          nextRunTime = nextRunTimeOpt.get()
      else:
        let nextRunTimeOpt = getNextTrigger(task, now(), none(DateTime), none(DateTime))
        if nextRunTimeOpt.isSome():
          nextRunTime = nextRunTimeOpt.get()

      await respHtml(req, renderTaskDetail(id, task, jobs, recentExecs,
          if nextRunTime.isInitialized: nextRunTime.serialize() else: "'-'"))

    # --- New Task ---
    if path.startsWith("/new_task") and httpMethod == HttpGet:
      await respHtml(req, renderTaskNew())
      return

    # --- Edit Task ---
    if path.startsWith("/edit_task") and httpMethod == HttpGet:
      let queryJson = req.url.query.decodeQueryAsTable()
      let id = queryJson.getOrDefault("id", "-1").parseInt
      debug "Loading task details for id=" & $id

      if id == -1:
        debug "Invalid task id: " & $id
        await resp404(req, "Task not found")
        return

      let tOpt = getTaskById(db, id)
      if tOpt.isNone:
        debug "Task not found in db: " & $id
        await resp404(req, "Task not found")
        return

      var task = tOpt.get().data
      await respHtml(req, renderTaskEdit(id, task))
      return

    # --- Executions Page ---
    if path == "/executions" and httpMethod == HttpGet:
      let allExecutions = queryRowsExecution(db, "1=1 ORDER BY CASE WHEN status = '" &
          esRunning.serialize() & "' THEN 0 ELSE 1 END ASC, startTime DESC")
      await respHtml(req, renderExecutions(allExecutions))
      return

    # --- Job Execution History ---
    if path == "/job_history" and httpMethod == HttpGet:
      let queryJson = req.url.query.decodeQueryAsTable()
      let jobId = queryJson.getOrDefault("id", "-1").parseInt
      let jobOpt = getJobById(db, jobId)
      if jobOpt.isNone:
        await resp404(req, "Job not found")
        return
      let job = jobOpt.get()
      let taskOpt = getTaskById(db, job.data.taskId)
      if taskOpt.isNone:
        await resp404(req, "Task not found")
        return
      let task = taskOpt.get()
      let executions = queryRowsExecution(db,
          fmt"""jobId = {jobId} OR (taskName = '{task.data.name.serialize()}' AND jobName = '{job.data.name.serialize()}') ORDER BY startTime DESC""")
      await respHtml(req, renderJobHistory(jobId, job.data.name, executions))
      return

    # --- Execution Log ---
    if path == "/execution_log" and httpMethod == HttpGet:
      let queryJson = req.url.query.decodeQueryAsTable()
      let maxLen = queryJson.getOrDefault("maxLen", "16384").parseInt
      let execId = queryJson.getOrDefault("id", "-1").parseInt
      let execs = queryRowsExecution(db, "_dbID = " & $execId)
      if execs.len == 0:
        await resp404(req, "Execution not found")
        return
      let exec = execs[0].data
      await respHtml(req, renderLogViewer(execId, exec, maxLen))
      return

    # --- Users Page ---
    if path == "/users" and httpMethod == HttpGet:
      let user = getUserById(db, userId).get().data
      let users = getAllUsers(db)
      await respHtml(req, renderUsers(userId, user, users))
      return

    # --- Tokens Page ---
    if path == "/tokens" and httpMethod == HttpGet:
      let tokens = getTokensByUserId(db, userId)
      await respHtml(req, renderTokens(tokens, userId))
      return

    # --- Logout Page ---
    if path == "/logout":
      let token = getTokenFromCookie(req)
      if token != "" and sessions.hasKey(token):
        sessions.del(token)
      await redirect(req, "/login")
      return

    # --- Schedule Page ---
    if path == "/schedule" and httpMethod == HttpGet:
      schedulerChan[].send(SchedulerSignal(kind: ssPrintSchedule))

      var schedule: seq[ScheduledTask] = @[]
      let tasks = db.queryRowsTask()
      let referenceTime = dateTime(
        year = now().year(),
        month = now().month(),
        monthday = now().monthday(),
        zone = local()
      )

      for (taskId, task) in tasks:
        if task.scheduleType == stInterval and task.intervalMinutes > 60:
          var lastRunTime: DateTime
          while not lastRunTime.isInitialized or lastRunTime < referenceTime +
              hours(24):
            let nextRunTime = getNextTrigger(task, referenceTime,
                if lastRunTime.isInitialized: some(lastRunTime) else: none(
                DateTime), none(DateTime))
            if nextRunTime.isSome():
              schedule.add(ScheduledTask(
                triggerTime: nextRunTime.get(),
                taskId: taskId,
                taskName: task.name
              ))
              lastRunTime = schedule[^1].triggerTime
            else:
              break
        elif task.scheduleType != stInterval:
          let nextRunTime = getNextTrigger(task, referenceTime, none(DateTime),
              none(DateTime))
          if nextRunTime.isSome:
            schedule.add(ScheduledTask(
              triggerTime: nextRunTime.get(),
              taskId: taskId,
              taskName: task.name
            ))

      let scheduleJson: string = $(%*(schedule))
      let tasksJson: string = $(%*(tasks.mapIt(it.data)))
      let taskIdsJson: string = $(%*(tasks.mapIt(it.dbId)))
      await respHtml(req, renderSchedule(scheduleJson, tasksJson, taskIdsJson))
      return

    # --- API End Points ---
    # Save Task
    if path.startsWith("/api/save_task") and httpMethod == HttpPost:
      debug "Saving task query: " & req.url.query
      let queryJson = req.url.query.decodeQueryAsTable()
      let id = queryJson.getOrDefault("id", "-1").parseInt
      var task = deserializeJson[Task](req.body.parseJson)
      let taskPath = if id == -1: cfg.tasksDir / task.name &
          ".yaml" else: task.sourceFile
      let resolvedCalendarPath = resolveCalendarPath(task.calendarPath,
          taskPath.absolutePath())
      debug "Saving task to: " & $taskPath
      writeFile(taskPath, toYamlString(task))
      if id == -1:
        task.sourceFile = taskPath
      else:
        let existingTask = getTaskById(db, id)
        if existingTask.isSome():
          task.sourceFile = existingTask.get().data.sourceFile
          task.folder = existingTask.get().data.folder
      task.configHash = calculateConfigHash(task, resolvedCalendarPath)
      if id == -1:
        # new task
        dbChan[].send(DbMessage(kind: dbInsertTask, task: task))
      else:
        # update task
        dbChan[].send(DbMessage(kind: dbUpdateTask, updateTaskId: id,
            updatedTask: task))
      schedulerChan[].send(SchedulerSignal(kind: ssReloadTasks))

      await respJson(req, %*{"status": "ok"})
      return

    # --- API: Stats ---
    if path == "/api/stats" and httpMethod == HttpGet:
      let tasksCount = getTasksCount(db)
      let executions = queryRowsExecution(db)
      let numRunning = executions.filterIt(it.data.status == esRunning).len
      let numSuccess = executions.filterIt(it.data.status == esSuccess).len
      let numFailed = executions.filterIt(it.data.status == esFailed).len
      let total = executions.len

      let stats = %*{
        "tasks": tasksCount,
        "total": total,
        "running": numRunning,
        "success": numSuccess,
        "failed": numFailed
      }
      await respJson(req, stats)
      return

    # --- API: get execution log ---
    if path == "/api/execution_log" and httpMethod == HttpPost:
      let queryJson = decodeQueryAsTable(req.url.query)
      let id = queryJson.getOrDefault("id", "-1").parseInt
      let maxLen = queryJson.getOrDefault("maxLen", "16384").parseInt
      let execs = queryRowsExecution(db, "_dbID = " & $id)
      if execs.len == 0:
        await resp404(req, "Execution not found")
        return
      let execution = execs[0].data
      var content = ""
      var skippedBytes = false
      if fileExists(execution.logFile):
        (skippedBytes, content) = readFile(execution.logFile, maxLen = maxLen)
      else:
        let absPath = absolutePath(execution.logFile)
        warn "Log file not found at: " & absPath
        content = "Log file not found at " & absPath
      await respJson(req, %*{"content": content, "status": $execution.status,
          "skippedBytes": skippedBytes})
      return

    if path == "/api/download_log" and httpMethod == HttpGet:
      let queryJson = decodeQueryAsTable(req.url.query)
      let id = queryJson.getOrDefault("id", "-1").parseInt
      let execs = queryRowsExecution(db, "_dbID = " & $id)
      if execs.len == 0:
        await resp404(req, "Execution not found")
        return
      let execution = execs[0].data
      var content = ""
      if fileExists(execution.logFile):
        content = readFile(execution.logFile)
      else:
        let absPath = absolutePath(execution.logFile)
        warn "Log file not found at: " & absPath
        content = "Log file not found at " & absPath
      await respFile(req, content, execution.logFile)
      return

    # --- API: delete executions (bulk) ---
    if path.startsWith("/api/delete_executions") and httpMethod == HttpPost:
      let dataJson = req.body.parseJson()
      let ids = dataJson["ids"].getElems().mapIt(it.getInt())
      for id in ids:
        executorChan[].send(ExecutorSignal(kind: estCancelExecution,
            cancelExecutionId: id))
        dbChan[].send(DbMessage(kind: dbDeleteExecution, deleteExecutionId: id))
      await respJson(req, %*{"status": "ok", "deleted": ids.len})
      return

    # --- API: delete execution (single) ---
    if path.startsWith("/api/delete_execution") and httpMethod == HttpPost:
      let queryJson = decodeQueryAsTable(req.url.query)
      let id = queryJson.getOrDefault("id", "-1").parseInt
      executorChan[].send(ExecutorSignal(kind: estCancelExecution,
          cancelExecutionId: id))
      dbChan[].send(DbMessage(kind: dbDeleteExecution, deleteExecutionId: id))
      await respJson(req, %*{"status": "ok"})
      return

    # --- API: cancel execution ---
    if path == "/api/cancel_execution" and httpMethod == HttpPost:
      let queryJson = decodeQueryAsTable(req.url.query)
      let id = queryJson.getOrDefault("id", "-1").parseInt
      executorChan[].send(ExecutorSignal(kind: estCancelExecution,
          cancelExecutionId: id))
      await respJson(req, %*{"status": "ok"})
      return

    # --- API: toggle task ---
    if path == "/api/toggle_task" and httpMethod == HttpPost:
      let queryJson = decodeQueryAsTable(req.url.query)
      let id = queryJson.getOrDefault("id", "-1").parseInt
      let enabled = queryJson.getOrDefault("enabled", "false").parseBool
      let tasks = queryRowsTask(db, "_dbID = " & $id)
      if tasks.len == 0:
        await resp404(req, "Task not found")
        return
      var (taskId, task) = tasks[0]
      task.enabled = enabled
      dbChan[].send(DbMessage(kind: dbUpdateTask, updateTaskId: id,
          updatedTask: task))
      schedulerChan[].send(SchedulerSignal(kind: ssReloadTasks))
      await respJson(req, %*{"status": "ok"})
      return

    # --- API: delete task ---
    if path == "/api/delete_task" and httpMethod == HttpPost:
      let queryJson = decodeQueryAsTable(req.url.query)
      let id = queryJson.getOrDefault("id", "-1").parseInt
      let tasks = queryRowsTask(db, "_dbID = " & $id)
      if tasks.len == 0:
        await resp404(req, "Task not found")
        return
      let (taskId, task) = tasks[0]
      dbChan[].send(DbMessage(kind: dbDeleteTask, deleteTaskId: id,
          deleteTaskSourceFile: task.sourceFile))
      schedulerChan[].send(SchedulerSignal(kind: ssReloadTasks))
      await respJson(req, %*{"status": "ok"})
      return

    ## -- API: trigger task ---
    if path == "/api/trigger_task" and httpMethod == HttpPost:
      let queryJson = decodeQueryAsTable(req.url.query)
      let id = queryJson.getOrDefault("id", "-1").parseInt
      let tasks = queryRowsTask(db, "_dbID = " & $id)
      if tasks.len == 0:
        await resp404(req, "Task not found")
        return
      let (taskId, task) = tasks[0]
      executorChan[].send(ExecutorSignal(kind: estTriggerTask,
          triggerTaskId: id, triggerTaskTask: task,
          triggerTaskManualTriggered: true))
      await respJson(req, %*{"status": "ok"})
      return

    ## -- API: trigger job ---
    if path == "/api/trigger_job" and httpMethod == HttpPost:
      let queryJson = decodeQueryAsTable(req.url.query)
      let id = queryJson.getOrDefault("id", "-1").parseInt
      let jobs = queryRowsJob(db, "_dbID = " & $id)
      if jobs.len == 0:
        await resp404(req, "Job not found")
        return
      let (jobId, job) = jobs[0]
      let taskId = job.taskId
      let tasks = queryRowsTask(db, "_dbID = " & $taskId)
      if tasks.len == 0:
        await resp404(req, "Task not found")
        return
      let (_, task) = tasks[0]
      executorChan[].send(ExecutorSignal(kind: estTriggerJob,
          triggerJobTaskId: taskId, triggerJobTask: task,
          triggerJobJobId: jobId, triggerJobJob: job,
          triggerJobManualTriggered: true))
      await respJson(req, %*{"status": "ok"})
      return

    ## -- API: create user ---
    if path == "/api/create_user" and httpMethod == HttpPost:
      let dataJson = req.body.parseJson()
      var user = deserializeJson[User](dataJson)
      user.passwordHash = dataJson["password"].getStr.encryptPassword
      user.createdAt = now()
      user.updatedAt = now()

      let existingUsers = db.queryRowsUser("username = '" & dataJson[
          "username"].getStr.serialize() & "'")
      if existingUsers.len > 0:
        await respJson(req, %*{"status": "error",
            "message": "User already exists"})
        return

      dbChan[].send(DbMessage(kind: dbInsertUser, user: user))
      await respJson(req, %*{"status": "ok"})
      return

    ## -- API: delete user ---
    if path == "/api/delete_user" and httpMethod == HttpPost:
      let dataJson = req.body.parseJson()
      let id = dataJson["userId"].getInt()
      dbChan[].send(DbMessage(kind: dbDeleteUser, deleteUserId: id))
      await respJson(req, %*{"status": "ok"})
      return

    ## -- API: update user password ---
    if path == "/api/update_user_password" and httpMethod == HttpPost:
      let dataJson = req.body.parseJson()
      let id = dataJson["userId"].`$`.strip(chars = {'"'}).parseInt()
      let password = dataJson["password"].getStr
      let email = dataJson["email"].getStr
      dbChan[].send(DbMessage(kind: dbUpdateUserPassword, updateUserId: id,
          newPasswordHash: password.encryptPassword, newPasswordEmail: email))
      await respJson(req, %*{"status": "ok"})
      return

    ## -- API: create token ---
    if path == "/api/create_token" and httpMethod == HttpPost:
      let dataJson = req.body.parseJson()
      let tokenName = dataJson["name"].getStr()
      var expireAt: DateTime
      if "expiresAt" in dataJson and dataJson["expiresAt"].getInt() > 0:
        expireAt = (dataJson["expiresAt"].getInt() div 1000).fromUnix().utc()
      let token = genToken()
      dbChan[].send(DbMessage(
        kind: dbInsertToken,
        token: Token(
          userId: userId,
          name: tokenName,
          token: token,
          createdAt: now(),
          expiresAt: expireAt
        )
      ))
      await respJson(req, %*{"status": "ok", "token": token})
      return

    ## --API: delete token ---
    if path == "/api/delete_token" and httpMethod == HttpPost:
      let dataJson = req.body.parseJson()
      let tokenId = dataJson["tokenId"].getInt()
      dbChan[].send(DbMessage(kind: dbDeleteToken, deleteTokenId: tokenId))
      await respJson(req, %*{"status": "ok"})
      return

    # --- 404 Default ---
    await resp404(req)

  proc tryHandleRequest(req: Request) {.async.} =
    try:
      await handleRequest(req)
    except Exception as e:
      warn getCurrentExceptionMsg()
      warn getStackTrace()
      await resp500(req, getCurrentExceptionMsg())
      monitorChan[].send(SchedulerMonitorSignal(kind: smmAlert,
          messageTitle: "Jobscheduler: Web Server Error :" &
          getCurrentExceptionMsg(),
          message: e.getStackTrace()))

  let port = Port(webServer.cfg.server.port)
  var server = newAsyncHttpServer()

  info "Starting web server on " & webServer.cfg.server.host & ":" &
      $webServer.cfg.server.port
  waitFor server.serve(port, tryHandleRequest, webServer.cfg.server.host)
