import std/[asyncdispatch, strutils, json, times, os, uri,
    sequtils, asynchttpserver, tables, options, strformat, algorithm]

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

const DashboardRecentPerJobLimit* = 5
  ## Max non-running executions shown per jobId on the dashboard, so
  ## high-frequency recurring jobs cannot flood the recent-executions view.

# --- HTTP & Parsing Helpers ---

proc toHeaders(headers: Table[string, string]): HttpHeaders =
  var httpHeaders = newHttpHeaders()
  for key, value in headers:
    httpHeaders[key] = value
  return httpHeaders

proc decodeQueryAsTable(query: string): Table[string, string] =
  result = initTable[string, string]()
  for key, val in decodeQuery(query):
    result[key] = val

proc parseIntSafe(s: string, defaultVal: int = -1): int =
  try:
    return parseInt(s.strip())
  except ValueError:
    return defaultVal

proc parseInt64Safe(s: string, defaultVal: int64 = -1'i64): int64 =
  try:
    return parseBiggestInt(s.strip()).int64
  except ValueError:
    return defaultVal

proc sanitizeGroupPath(raw: string): string =
  let normalized = raw.replace("\\", "/").strip(chars = {'/', ' '})
  if normalized.len == 0:
    return ""
  var parts: seq[string] = @[]
  for part in normalized.split('/'):
    let p = part.strip()
    if p.len == 0 or p == "." or p == "..":
      continue
    parts.add(p.sanitizeFileName())
  return parts.join("/")

proc yamlScalar(s: string): string =
  ## Formats a single-line YAML string safely.
  if s.len == 0:
    return "\"\""
  let needsQuote = s.startsWith(" ") or s.endsWith(" ") or
      s.contains(":") or s.contains("#") or s.contains("\"") or
      s.contains("'") or s.contains("*") or s.contains("&") or
      s.contains("!") or s.contains("{") or s.contains("}") or
      s.contains("[") or s.contains("]") or s.contains(",") or
      s.contains("|") or s.contains(">") or s.contains("%") or
      s.contains("@") or s.contains("`") or
      s.toLowerAscii() in ["true", "false", "null", "yes", "no", "on", "off", "~"]
  if needsQuote:
    return escape(s)
  return s

proc taskToCleanYaml(task: Task): string =
  var lines: seq[string] = @[]
  lines.add("name: " & yamlScalar(task.name))
  if task.description.len > 0:
    if '\n' in task.description:
      lines.add("description: |")
      for ln in task.description.splitLines():
        lines.add("  " & ln)
    else:
      lines.add("description: " & yamlScalar(task.description))
  lines.add("taskType: " & $task.taskType)
  lines.add("enabled: " & $task.enabled)
  if task.parallel:
    lines.add("parallel: true")
  lines.add("scheduleType: " & $task.scheduleType)
  if task.timezone.len > 0:
    lines.add("timezone: " & yamlScalar(task.timezone))
  case task.scheduleType
  of stCron:
    if task.cronExpr.len > 0:
      lines.add("cronExpr: " & yamlScalar(task.cronExpr))
  of stTime:
    if task.timeOfDay.len > 0:
      lines.add("timeOfDay: " & yamlScalar(task.timeOfDay))
  of stInterval:
    if task.intervalMinutes > 0:
      lines.add("intervalMinutes: " & $task.intervalMinutes)
    if task.intervalStart.len > 0:
      lines.add("intervalStart: " & yamlScalar(task.intervalStart))
    if task.intervalEnd.len > 0:
      lines.add("intervalEnd: " & yamlScalar(task.intervalEnd))
  of stOnStart, stUndefined:
    discard
  if task.calendarPath.len > 0:
    lines.add("calendarPath: " & yamlScalar(task.calendarPath))
  if task.taskType == ttRemote:
    if task.sshHost.len > 0:
      lines.add("sshHost: " & yamlScalar(task.sshHost))
    if task.sshPort > 0:
      lines.add("sshPort: " & $task.sshPort)
    if task.sshUser.len > 0:
      lines.add("sshUser: " & yamlScalar(task.sshUser))
    if task.sshKeyPath.len > 0:
      lines.add("sshKeyPath: " & yamlScalar(task.sshKeyPath))
  lines.add("jobs:")
  for job in task.jobs:
    lines.add("  - name: " & yamlScalar(job.name))
    if '\n' in job.command:
      lines.add("    command: |")
      for ln in job.command.splitLines():
        lines.add("      " & ln)
    else:
      lines.add("    command: " & yamlScalar(job.command))
  return lines.join("\n") & "\n"

proc updateEnabledInYaml(filePath: string, enabled: bool) =
  if filePath.len == 0 or not fileExists(filePath):
    return
  try:
    var lines = readFile(filePath).splitLines()
    var found = false
    for i in 0..<lines.len:
      let trimmed = lines[i].strip()
      if trimmed.startsWith("enabled:") and not lines[i].startsWith(" ") and not lines[i].startsWith("\t"):
        lines[i] = "enabled: " & $enabled
        found = true
        break
    if not found:
      lines.add("enabled: " & $enabled)
    writeFile(filePath, lines.join("\n"))
  except Exception as e:
    warn "Failed to update enabled flag in YAML " & filePath & ": " & e.msg

# --- Response Helpers ---

proc resp(req: Request, code: HttpCode, content: string,
    headers: HttpHeaders = newHttpHeaders()) {.async.} =
  await req.respond(code, content, headers)

proc respJson(req: Request, content: JsonNode,
    headers: HttpHeaders = newHttpHeaders()) {.async.} =
  var headersCopy = headers
  headersCopy["Content-Type"] = "application/json; charset=utf-8"
  headersCopy["Cache-Control"] = "no-store"
  await req.respond(Http200, $content, headersCopy)

proc respJsonCode(req: Request, code: HttpCode, content: JsonNode,
    headers: HttpHeaders = newHttpHeaders()) {.async.} =
  var headersCopy = headers
  headersCopy["Content-Type"] = "application/json; charset=utf-8"
  headersCopy["Cache-Control"] = "no-store"
  await req.respond(code, $content, headersCopy)

proc respHtml(req: Request, content: string) {.async.} =
  var headers = newHttpHeaders()
  headers["Content-Type"] = "text/html; charset=utf-8"
  headers["Cache-Control"] = "no-store"
  await req.respond(Http200, content, headers)

proc respFile(req: Request, content, filepath: string) {.async.} =
  let fname = extractFilename(filepath)
  let safeName = if fname.len > 0: fname else: "execution.log"
  var headers = newHttpHeaders()
  headers["Content-Type"] = "text/plain; charset=utf-8"
  headers["Content-Disposition"] = "attachment; filename=\"" & safeName.replace("\"", "") & "\""
  await req.respond(Http200, content, headers)

proc resp404(req: Request, msg: string = "Not Found") {.async.} =
  await req.respond(Http404, msg)

proc resp500(req: Request, msg: string) {.async.} =
  await req.respond(Http500, "Internal Server Error: " & msg)

proc redirect(req: Request, location: string) {.async.} =
  var headers = newHttpHeaders()
  headers["Location"] = location
  await req.respond(Http302, "", headers)

# --- Actor Sync Helpers (Non-blocking on Async Loop) ---

proc sendDbSync(dbChan: ptr DbChannel, msg: DbMessage): Future[int] {.async.} =
  var ch = create(Channel[int])
  ch[].open()
  var msgCopy = msg
  msgCopy.ackCh = ch
  dbChan[].send(msgCopy)
  while true:
    let (ok, val) = ch[].tryRecv()
    if ok:
      ch[].close()
      dealloc(ch)
      return val
    await sleepAsync(5)

proc sendExecutorSync(executorChan: ptr ExecutorChannel,
    sig: ExecutorSignal): Future[int] {.async.} =
  var ch = create(Channel[int])
  ch[].open()
  var sigCopy = sig
  sigCopy.resultCh = ch
  executorChan[].send(sigCopy)
  while true:
    let (ok, val) = ch[].tryRecv()
    if ok:
      ch[].close()
      dealloc(ch)
      return val
    await sleepAsync(10)

# --- Auth Helpers ---

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
        if authHeader.len > 7:
          let token = authHeader[7..^1]
          if sessions.hasKey(token):
            return sessions[token]
          # Also check DB TokenTable for API tokens
          let dbTokens = queryRowsToken(db, "token = '" & token.serialize().replace("'", "''") & "'")
          if dbTokens.len > 0:
            return dbTokens[0].data.userId

  # Check for session cookie
  let token = getTokenFromCookie(req)
  if token != "":
    if sessions.hasKey(token):
      return sessions[token]

  return -1

# --- JSON View-Model Builders ---

proc execToJson(dbId: int, exec: Execution): JsonNode =
  result = %*exec
  result["id"] = %dbId
  result["executionId"] = %dbId

proc buildStatsJson(db: DbConn): JsonNode =
  let tasksCount = getTasksCount(db)
  let enabledTasks = getEnabledTasksCount(db)
  let counts = getExecutionStatusCounts(db)
  let numRunning = counts.getOrDefault($esRunning, 0)
  let numSuccess = counts.getOrDefault($esSuccess, 0)
  let numFailed = counts.getOrDefault($esFailed, 0)
  let numCancelled = counts.getOrDefault($esCancelled, 0)
  let numLost = counts.getOrDefault($esLost, 0)
  let numScheduled = counts.getOrDefault($esScheduled, 0)
  var total = 0
  for _, c in counts:
    total += c

  return %*{
    "tasks": tasksCount,
    "enabledTasks": enabledTasks,
    "total": total,
    "running": numRunning,
    "success": numSuccess,
    "failed": numFailed,
    "cancelled": numCancelled,
    "lost": numLost,
    "scheduled": numScheduled
  }

proc buildDashboardJson(db: DbConn): JsonNode =
  let stats = buildStatsJson(db)
  let runningExecs = queryRowsExecution(db, "status = '" &
      esRunning.serialize() & "' ORDER BY startTime DESC")
  let recentExecs = queryRowsExecution(db,
    "_dbID IN (SELECT _dbID FROM (SELECT _dbID, row_number() OVER " &
    "(PARTITION BY jobId ORDER BY startTime DESC) AS rn FROM " &
    "ExecutionTable WHERE status != '" & esRunning.serialize() &
    "') WHERE rn <= " & $DashboardRecentPerJobLimit & ") " &
    "ORDER BY startTime DESC")
  var executions = runningExecs & recentExecs
  executions.sort(proc (a, b: tuple[dbId: int, data: Execution]): int =
    if a.data.status == esRunning and b.data.status != esRunning:
      return -1
    elif a.data.status != esRunning and b.data.status == esRunning:
      return 1
    else:
      return cmp(b.data.startTime, a.data.startTime))

  var runningArr = newJArray()
  for (eid, e) in runningExecs:
    runningArr.add(execToJson(eid, e))

  var recentArr = newJArray()
  for (eid, e) in executions:
    recentArr.add(execToJson(eid, e))

  return %*{
    "stats": stats,
    "running": runningArr,
    "executions": recentArr,
    "serverTime": now().format("yyyy-MM-dd HH:mm:ss zzzz")
  }

proc buildTasksListJson(db: DbConn): JsonNode =
  let tasks = getAllTasksOrdered(db)

  # Fetch latest execution per taskId (lightweight indexed aggregate)
  let latestRows = queryRowsExecution(db,
      "_dbID IN (SELECT MAX(_dbID) FROM ExecutionTable GROUP BY taskId)")
  var lastTaskExec = initTable[int, tuple[dbId: int, data: Execution]]()
  for item in latestRows:
    lastTaskExec[item.data.taskId] = item

  # Fetch currently running executions
  let runningRows = queryRowsExecution(db, "status = '" &
      esRunning.serialize() & "' ORDER BY startTime DESC")
  var runningByTask = initTable[int, int]()
  for (eid, e) in runningRows:
    if e.taskId notin runningByTask:
      runningByTask[e.taskId] = eid

  # Count jobs per task
  var jobCounts = initTable[int, int]()
  for row in db.getAllRows(sql"SELECT taskId, COUNT(*) FROM JobTable GROUP BY taskId"):
    if row.len >= 2:
      try:
        jobCounts[parseInt(row[0])] = parseInt(row[1])
      except ValueError:
        discard

  var arr = newJArray()
  let refNow = now()
  for (taskId, task) in tasks:
    var obj = %*task
    obj["id"] = %taskId
    obj["jobCount"] = %jobCounts.getOrDefault(taskId, task.jobs.len)

    let isRunning = taskId in runningByTask
    obj["isRunning"] = %isRunning
    obj["runningExecId"] = %(if isRunning: runningByTask[taskId] else: -1)

    if taskId in lastTaskExec:
      let (lastId, lastE) = lastTaskExec[taskId]
      let nextOpt = getNextTrigger(task, refNow, some(lastE.startTime), none(DateTime))
      obj["nextRunTime"] = if nextOpt.isSome(): %nextOpt.get() else: %""
      obj["lastExecution"] = %*{
        "id": lastId,
        "status": $lastE.status,
        "startTime": %lastE.startTime,
        "endTime": %lastE.endTime,
        "jobName": lastE.jobName
      }
    else:
      let nextOpt = getNextTrigger(task, refNow, none(DateTime), none(DateTime))
      obj["nextRunTime"] = if nextOpt.isSome(): %nextOpt.get() else: %""
      obj["lastExecution"] = newJNull()

    arr.add(obj)

  return %*{
    "tasks": arr,
    "runningCount": runningRows.len,
    "serverTime": refNow.format("yyyy-MM-dd HH:mm:ss zzzz")
  }

proc buildTaskDetailJson(db: DbConn, id: int): Option[JsonNode] =
  let tOpt = getTaskById(db, id)
  if tOpt.isNone:
    return none(JsonNode)

  let task = tOpt.get().data
  let jobs = getJobsByTaskIdOrdered(db, id)
  let recentExecs = queryRowsExecution(db, "taskId = " & $id &
      " ORDER BY startTime DESC LIMIT 30")

  let refNow = now()
  let lastStart = if recentExecs.len > 0: some(recentExecs[0].data.startTime) else: none(DateTime)
  let nextOpt = getNextTrigger(task, refNow, lastStart, none(DateTime))
  let nextRunNode = if nextOpt.isSome(): %nextOpt.get() else: %""

  # Latest execution per jobId across full history (not just top 30)
  var latestByJob = initTable[int, tuple[dbId: int, data: Execution]]()
  var runningByJob = initTable[int, int]()
  for (eid, e) in recentExecs:
    if e.jobId notin latestByJob:
      latestByJob[e.jobId] = (eid, e)
    if e.status == esRunning and e.jobId notin runningByJob:
      runningByJob[e.jobId] = eid

  var jobsArr = newJArray()
  for (jobId, job) in jobs:
    var jObj = %*job
    jObj["id"] = %jobId
    let isRun = jobId in runningByJob
    jObj["isRunning"] = %isRun
    jObj["runningExecId"] = %(if isRun: runningByJob[jobId] else: -1)
    if jobId in latestByJob:
      let (lid, le) = latestByJob[jobId]
      jObj["lastExecution"] = execToJson(lid, le)
    else:
      # Fallback query for older jobs outside the top 30 task executions
      let jLatest = queryRowsExecution(db, "jobId = " & $jobId & " ORDER BY startTime DESC LIMIT 1")
      if jLatest.len > 0:
        jObj["lastExecution"] = execToJson(jLatest[0].dbId, jLatest[0].data)
      else:
        jObj["lastExecution"] = newJNull()
    jobsArr.add(jObj)

  var execsArr = newJArray()
  for (eid, e) in recentExecs:
    execsArr.add(execToJson(eid, e))

  var taskJson = %*task
  taskJson["id"] = %id
  let sourceContent = if task.sourceFile.len > 0 and fileExists(task.sourceFile):
    readFile(task.sourceFile)
  else:
    "# Source file not found: " & task.sourceFile

  return some(%*{
    "id": id,
    "task": taskJson,
    "sourceContent": sourceContent,
    "nextRunTime": nextRunNode,
    "jobs": jobsArr,
    "recentExecutions": execsArr,
    "isRunning": runningByJob.len > 0,
    "serverTime": refNow.format("yyyy-MM-dd HH:mm:ss zzzz")
  })

proc buildExecutionsJson(db: DbConn, search: string = "", status: string = "",
    taskId: int = -1, jobId: int = -1, page: int = 1, limit: int = 50): JsonNode =
  let safePage = max(1, page)
  let safeLimit = max(1, min(limit, 200))
  let (rows, total) = queryExecutionsPage(
      db, search = search, status = status, taskId = taskId, jobId = jobId,
      page = safePage, limit = safeLimit)
  let totalPages = max(1, (total + safeLimit - 1) div safeLimit)
  var arr = newJArray()
  for (eid, e) in rows:
    arr.add(execToJson(eid, e))
  return %*{
    "executions": arr,
    "total": total,
    "page": safePage,
    "limit": safeLimit,
    "totalPages": totalPages,
    "serverTime": now().format("yyyy-MM-dd HH:mm:ss zzzz")
  }

proc buildJobHistoryJson(db: DbConn, jobId: int, status: string = "",
    page: int = 1, limit: int = 50): Option[JsonNode] =
  let jobOpt = getJobById(db, jobId)
  if jobOpt.isNone:
    return none(JsonNode)
  let job = jobOpt.get()
  let taskOpt = getTaskById(db, job.data.taskId)
  if taskOpt.isNone:
    return none(JsonNode)
  let task = taskOpt.get()

  let safePage = max(1, page)
  let safeLimit = max(1, min(limit, 200))
  let (rows, total) = queryExecutionsPage(
      db, status = status, jobId = jobId,
      matchTaskName = task.data.name, matchJobName = job.data.name,
      page = safePage, limit = safeLimit)
  let totalPages = max(1, (total + safeLimit - 1) div safeLimit)
  var arr = newJArray()
  for (eid, e) in rows:
    arr.add(execToJson(eid, e))

  return some(%*{
    "job": {
      "id": jobId,
      "name": job.data.name,
      "command": job.data.command,
      "taskId": job.data.taskId,
      "taskName": task.data.name,
      "taskGroup": task.data.groupName
    },
    "executions": arr,
    "total": total,
    "page": safePage,
    "limit": safeLimit,
    "totalPages": totalPages,
    "serverTime": now().format("yyyy-MM-dd HH:mm:ss zzzz")
  })

proc buildScheduleJson(db: DbConn): JsonNode =
  let tasks = getAllTasksOrdered(db)
  let refNow = now()
  let referenceTime = dateTime(
    year = refNow.year(),
    month = refNow.month(),
    monthday = refNow.monthday(),
    zone = local()
  )

  var scheduleItems = newJArray()
  var frequentTasks = newJArray()

  for (taskId, task) in tasks:
    if task.scheduleType == stInterval and task.intervalMinutes <= 60:
      frequentTasks.add(%*{
        "taskId": taskId,
        "taskName": task.name,
        "description": task.description,
        "enabled": task.enabled,
        "taskType": $task.taskType,
        "intervalMinutes": task.intervalMinutes,
        "intervalStart": task.intervalStart,
        "intervalEnd": task.intervalEnd,
        "timezone": task.timezone,
        "groupName": task.groupName
      })
    elif task.scheduleType == stInterval and task.intervalMinutes > 60:
      var lastRunTime: DateTime
      var guard = 0
      while (not lastRunTime.isInitialized or lastRunTime <
          referenceTime + hours(24)) and guard < 100:
        inc guard
        let nextRunTime = getNextTrigger(task, referenceTime,
            if lastRunTime.isInitialized: some(lastRunTime) else: none(DateTime),
            none(DateTime))
        if nextRunTime.isSome():
          if lastRunTime.isInitialized and nextRunTime.get() <= lastRunTime:
            break
          let t = nextRunTime.get()
          if t >= referenceTime + hours(24):
            break
          scheduleItems.add(%*{
            "triggerTime": %t,
            "taskId": taskId,
            "taskName": task.name,
            "description": task.description,
            "enabled": task.enabled,
            "taskType": $task.taskType,
            "scheduleType": $task.scheduleType,
            "cronExpr": task.cronExpr,
            "timeOfDay": task.timeOfDay,
            "intervalMinutes": task.intervalMinutes,
            "groupName": task.groupName
          })
          lastRunTime = t
        else:
          break
    elif task.scheduleType != stInterval:
      let nextRunTime = getNextTrigger(task, referenceTime, none(DateTime), none(DateTime))
      if nextRunTime.isSome:
        scheduleItems.add(%*{
          "triggerTime": %nextRunTime.get(),
          "taskId": taskId,
          "taskName": task.name,
          "description": task.description,
          "enabled": task.enabled,
          "taskType": $task.taskType,
          "scheduleType": $task.scheduleType,
          "cronExpr": task.cronExpr,
          "timeOfDay": task.timeOfDay,
          "groupName": task.groupName
        })

  return %*{
    "schedule": scheduleItems,
    "frequentTasks": frequentTasks,
    "serverTime": refNow.format("yyyy-MM-dd HH:mm:ss zzzz")
  }

proc buildUsersJson(db: DbConn, currentUserId: int): JsonNode =
  let users = getAllUsers(db)
  var arr = newJArray()
  for (uid, u) in users:
    arr.add(%*{
      "id": uid,
      "username": u.username,
      "email": u.email,
      "createdAt": %u.createdAt,
      "updatedAt": %u.updatedAt
    })
  return %*{
    "currentUserId": currentUserId,
    "users": arr
  }

proc buildTokensJson(db: DbConn, userId: int): JsonNode =
  let tokens = getTokensByUserId(db, userId)
  var arr = newJArray()
  for (tid, tok) in tokens:
    let preview = if tok.token.len > 10: tok.token[0..9] & "••••••••" else: "••••••••"
    arr.add(%*{
      "id": tid,
      "name": tok.name,
      "tokenPreview": preview,
      "createdAt": %tok.createdAt,
      "expiresAt": %tok.expiresAt
    })
  return %*{
    "userId": userId,
    "tokens": arr
  }

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

    # --- Static Files (public so /login can load CSS/JS) ---
    if path.startsWith("/static/"):
      let realPath = if "?" in path: path.split("?")[0] else: path
      if realPath in StaticFiles:
        var mime = "application/octet-stream"
        if realPath.endsWith(".css"): mime = "text/css; charset=utf-8"
        elif realPath.endsWith(".js"): mime = "application/javascript; charset=utf-8"
        elif realPath.endsWith(".png"): mime = "image/png"
        elif realPath.endsWith(".svg"): mime = "image/svg+xml"
        let headers = {
          "Content-Type": mime,
          "Cache-Control": "public, max-age=300"
        }.toTable().toHeaders()
        await resp(req, Http200, StaticFiles[realPath], headers)
      else:
        await resp404(req)
      return

    # --- Public Endpoints ---
    if path == "/login" and httpMethod == HttpGet:
      if isAuthenticated(req, sessions, db) > 0:
        await redirect(req, "/dashboard")
        return
      await respHtml(req, renderLogin())
      return

    if path == "/api/login" and httpMethod == HttpPost:
      let data = parseJson(req.body)
      let username = data["username"].getStr
      let password = data["password"].getStr
      info "Login request Username=" & username

      let userOpt = getUserByUsername(db, username)
      if userOpt.isNone:
        await respJsonCode(req, Http401, %*{"status": "error", "message": "Invalid credentials"})
        return

      let user = userOpt.get()
      if user.data.passwordHash != password:
        await respJsonCode(req, Http401, %*{"status": "error", "message": "Invalid credentials"})
        return

      let sessionToken = genToken()
      sessions[sessionToken] = user.dbId

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

    # --- Authentication Required Beyond This Point ---
    let userId = isAuthenticated(req, sessions, db)
    if userId < 0:
      if path.startsWith("/api/"):
        await respJsonCode(req, Http401, %*{"status": "error", "message": "Unauthorized"})
      else:
        await redirect(req, "/login")
      return

    # =========================
    # HTML Pages
    # =========================

    if (path == "/" or path == "/dashboard") and httpMethod == HttpGet:
      let data = buildDashboardJson(db)
      await respHtml(req, renderDashboard($data))
      return

    if path == "/tasks" and httpMethod == HttpGet:
      let data = buildTasksListJson(db)
      await respHtml(req, renderTasks($data))
      return

    if path == "/task_detail" and httpMethod == HttpGet:
      let query = req.url.query.decodeQueryAsTable()
      let id = parseIntSafe(query.getOrDefault("id", "-1"))
      let detailOpt = buildTaskDetailJson(db, id)
      if detailOpt.isNone:
        await resp404(req, "Task not found")
        return
      let detail = detailOpt.get()
      let taskName = detail["task"]["name"].getStr("Task")
      await respHtml(req, renderTaskDetail(id, taskName, $detail))
      return

    if path == "/new_task" and httpMethod == HttpGet:
      await respHtml(req, renderTaskNew())
      return

    if path == "/edit_task" and httpMethod == HttpGet:
      let query = req.url.query.decodeQueryAsTable()
      let id = parseIntSafe(query.getOrDefault("id", "-1"))
      if id <= 0:
        await resp404(req, "Task not found")
        return
      let tOpt = getTaskById(db, id)
      if tOpt.isNone:
        await resp404(req, "Task not found")
        return
      await respHtml(req, renderTaskEdit(id, tOpt.get().data))
      return

    if path == "/executions" and httpMethod == HttpGet:
      let query = req.url.query.decodeQueryAsTable()
      let page = max(1, parseIntSafe(query.getOrDefault("page", "1"), 1))
      let limit = max(1, min(parseIntSafe(query.getOrDefault("limit", "50"), 50), 200))
      let search = query.getOrDefault("q", query.getOrDefault("search", ""))
      let status = query.getOrDefault("status", "")
      let taskId = parseIntSafe(query.getOrDefault("taskId", "-1"))
      let data = buildExecutionsJson(db, search = search, status = status,
          taskId = taskId, page = page, limit = limit)
      await respHtml(req, renderExecutions($data))
      return

    if path == "/job_history" and httpMethod == HttpGet:
      let query = req.url.query.decodeQueryAsTable()
      let jobId = parseIntSafe(query.getOrDefault("id", "-1"))
      let status = query.getOrDefault("status", "")
      let page = max(1, parseIntSafe(query.getOrDefault("page", "1"), 1))
      let dataOpt = buildJobHistoryJson(db, jobId, status = status, page = page)
      if dataOpt.isNone:
        await resp404(req, "Job not found")
        return
      let data = dataOpt.get()
      let jobName = data["job"]["name"].getStr("Job")
      await respHtml(req, renderJobHistory(jobId, jobName, $data))
      return

    if path == "/execution_log" and httpMethod == HttpGet:
      let query = req.url.query.decodeQueryAsTable()
      let maxLen = max(1024, parseIntSafe(query.getOrDefault("maxLen", "65536"), 65536))
      let execId = parseIntSafe(query.getOrDefault("id", "-1"))
      let execs = queryRowsExecution(db, "_dbID = " & $execId)
      if execs.len == 0:
        await resp404(req, "Execution not found")
        return
      let payload = %*{
        "execId": execId,
        "maxLen": maxLen,
        "execution": execToJson(execId, execs[0].data)
      }
      await respHtml(req, renderLogViewer(execId, $payload))
      return

    if path == "/schedule" and httpMethod == HttpGet:
      schedulerChan[].send(SchedulerSignal(kind: ssPrintSchedule))
      let data = buildScheduleJson(db)
      await respHtml(req, renderSchedule($data))
      return

    if path == "/users" and httpMethod == HttpGet:
      let data = buildUsersJson(db, userId)
      await respHtml(req, renderUsers($data))
      return

    if path == "/tokens" and httpMethod == HttpGet:
      let data = buildTokensJson(db, userId)
      await respHtml(req, renderTokens($data))
      return

    if path == "/logout":
      let token = getTokenFromCookie(req)
      if token != "" and sessions.hasKey(token):
        sessions.del(token)
      let headers = {
        "Location": "/login",
        "Set-Cookie": "session=; Path=/; Max-Age=0; HttpOnly; SameSite=Strict"
      }.toTable.toHeaders
      await resp(req, Http302, "", headers)
      return

    # =========================
    # JSON Read APIs
    # =========================

    if path == "/api/stats" and httpMethod == HttpGet:
      await respJson(req, buildStatsJson(db))
      return

    if path == "/api/dashboard" and httpMethod == HttpGet:
      await respJson(req, buildDashboardJson(db))
      return

    if path == "/api/tasks" and httpMethod == HttpGet:
      await respJson(req, buildTasksListJson(db))
      return

    if path == "/api/task" and httpMethod == HttpGet:
      let query = req.url.query.decodeQueryAsTable()
      let id = parseIntSafe(query.getOrDefault("id", "-1"))
      let detailOpt = buildTaskDetailJson(db, id)
      if detailOpt.isNone:
        await respJsonCode(req, Http404, %*{"status": "error", "message": "Task not found"})
        return
      await respJson(req, detailOpt.get())
      return

    if path == "/api/executions" and httpMethod == HttpGet:
      let query = req.url.query.decodeQueryAsTable()
      let page = max(1, parseIntSafe(query.getOrDefault("page", "1"), 1))
      let limit = max(1, min(parseIntSafe(query.getOrDefault("limit", "50"), 50), 200))
      let search = query.getOrDefault("q", query.getOrDefault("search", ""))
      let status = query.getOrDefault("status", "")
      let taskId = parseIntSafe(query.getOrDefault("taskId", "-1"))
      let jobId = parseIntSafe(query.getOrDefault("jobId", "-1"))
      await respJson(req, buildExecutionsJson(db, search = search, status = status,
          taskId = taskId, jobId = jobId, page = page, limit = limit))
      return

    if path == "/api/job_history" and httpMethod == HttpGet:
      let query = req.url.query.decodeQueryAsTable()
      let jobId = parseIntSafe(query.getOrDefault("id", "-1"))
      let status = query.getOrDefault("status", "")
      let page = max(1, parseIntSafe(query.getOrDefault("page", "1"), 1))
      let limit = max(1, min(parseIntSafe(query.getOrDefault("limit", "50"), 50), 200))
      let dataOpt = buildJobHistoryJson(db, jobId, status = status, page = page, limit = limit)
      if dataOpt.isNone:
        await respJsonCode(req, Http404, %*{"status": "error", "message": "Job not found"})
        return
      await respJson(req, dataOpt.get())
      return

    if path == "/api/schedule" and httpMethod == HttpGet:
      await respJson(req, buildScheduleJson(db))
      return

    if path == "/api/users" and httpMethod == HttpGet:
      await respJson(req, buildUsersJson(db, userId))
      return

    if path == "/api/tokens" and httpMethod == HttpGet:
      await respJson(req, buildTokensJson(db, userId))
      return

    # --- Execution Log (supports both GET and POST + incremental offset) ---
    if path == "/api/execution_log" and (httpMethod == HttpGet or httpMethod == HttpPost):
      let query = decodeQueryAsTable(req.url.query)
      let id = parseIntSafe(query.getOrDefault("id", "-1"))
      let maxLen = max(1024, min(parseIntSafe(query.getOrDefault("maxLen", "65536"), 65536), 524288))
      let offset = parseInt64Safe(query.getOrDefault("offset", "-1"), -1'i64)
      let execs = queryRowsExecution(db, "_dbID = " & $id)
      if execs.len == 0:
        await respJsonCode(req, Http404, %*{"status": "error", "message": "Execution not found"})
        return
      let execution = execs[0].data
      var content = ""
      var skippedBytes = false
      var nextOffset: int64 = 0
      var fileSize: int64 = 0
      var reset = false
      var logFound = false
      if execution.logFile.len > 0 and fileExists(execution.logFile):
        logFound = true
        (skippedBytes, content, nextOffset, fileSize, reset) =
            readFileChunk(execution.logFile, offset, maxLen)
      else:
        let absPath = if execution.logFile.len > 0: absolutePath(execution.logFile) else: "(none)"
        content = if offset < 0: "Log file not found at " & absPath else: ""
      await respJson(req, %*{
        "id": id,
        "taskId": execution.taskId,
        "taskName": execution.taskName,
        "jobId": execution.jobId,
        "jobName": execution.jobName,
        "pid": execution.pid,
        "exitCode": execution.exitCode,
        "startTime": %execution.startTime,
        "endTime": %execution.endTime,
        "logFile": execution.logFile,
        "status": $execution.status,
        "content": content,
        "offset": nextOffset,
        "fileSize": fileSize,
        "skippedBytes": skippedBytes,
        "reset": reset,
        "logFound": logFound
      })
      return

    if path == "/api/download_log" and httpMethod == HttpGet:
      let query = decodeQueryAsTable(req.url.query)
      let id = parseIntSafe(query.getOrDefault("id", "-1"))
      let execs = queryRowsExecution(db, "_dbID = " & $id)
      if execs.len == 0:
        await resp404(req, "Execution not found")
        return
      let execution = execs[0].data
      var content = ""
      if execution.logFile.len > 0 and fileExists(execution.logFile):
        content = readFile(execution.logFile)
      else:
        let absPath = absolutePath(execution.logFile)
        warn "Log file not found at: " & absPath
        content = "Log file not found at " & absPath
      await respFile(req, content, execution.logFile)
      return

    # =========================
    # Mutation APIs (Synchronized with DbWorker / Executor)
    # =========================

    # --- Save Task (Create or Update) ---
    if path.startsWith("/api/save_task") and httpMethod == HttpPost:
      let query = req.url.query.decodeQueryAsTable()
      let id = parseIntSafe(query.getOrDefault("id", "-1"))
      let bodyJson = req.body.parseJson()
      var task = deserializeJson[Task](bodyJson)
      task.name = task.name.strip()
      if task.name.len == 0:
        await respJsonCode(req, Http400, %*{"status": "error", "message": "Task name is required"})
        return
      if task.jobs.len == 0:
        await respJsonCode(req, Http400, %*{"status": "error", "message": "At least one job is required"})
        return

      let safeGroup = sanitizeGroupPath(task.groupName)
      var taskPath: string
      if id <= 0:
        let targetDir = if safeGroup.len > 0: cfg.tasksDir / safeGroup else: cfg.tasksDir
        createDir(targetDir)
        let baseName = task.name.sanitizeFileName()
        taskPath = targetDir / (baseName & ".yaml")
        task.sourceFile = taskPath
        task.folder = targetDir
        task.groupName = safeGroup
        task.createdAt = now()
        task.updatedAt = now()
      else:
        let existingTaskOpt = getTaskById(db, id)
        if existingTaskOpt.isNone:
          await respJsonCode(req, Http404, %*{"status": "error", "message": "Task not found"})
          return
        let existing = existingTaskOpt.get().data
        taskPath = if existing.sourceFile.len > 0: existing.sourceFile else: cfg.tasksDir / (task.name.sanitizeFileName() & ".yaml")
        task.sourceFile = taskPath
        task.folder = existing.folder
        task.groupName = existing.groupName
        task.createdAt = existing.createdAt
        task.updatedAt = now()

      let resolvedCalendarPath = resolveCalendarPath(task.calendarPath, taskPath.absolutePath())
      if resolvedCalendarPath.len > 0 and fileExists(resolvedCalendarPath):
        task.dateList = readFile(resolvedCalendarPath).splitLines()
      debug "Saving task to: " & taskPath
      createDir(taskPath.parentDir)
      writeFile(taskPath, taskToCleanYaml(task))
      task.configHash = calculateConfigHash(task, resolvedCalendarPath)

      var savedId = id
      if id <= 0:
        savedId = await sendDbSync(dbChan, DbMessage(kind: dbInsertTask, task: task))
      else:
        savedId = await sendDbSync(dbChan, DbMessage(kind: dbUpdateTask,
            updateTaskId: id, updatedTask: task))

      schedulerChan[].send(SchedulerSignal(kind: ssReloadTasks))
      if savedId < 0:
        await respJsonCode(req, Http500, %*{"status": "error", "message": "Database write failed"})
        return
      await respJson(req, %*{"status": "ok", "id": savedId})
      return

    # --- Delete Executions (Bulk) ---
    if path == "/api/delete_executions" and httpMethod == HttpPost:
      let dataJson = req.body.parseJson()
      let ids = dataJson["ids"].getElems().mapIt(it.getInt())
      for id in ids:
        discard await sendExecutorSync(executorChan, ExecutorSignal(
            kind: estCancelExecution, cancelExecutionId: id))
        discard await sendDbSync(dbChan, DbMessage(
            kind: dbDeleteExecution, deleteExecutionId: id))
      await respJson(req, %*{"status": "ok", "deleted": ids.len})
      return

    # --- Delete Execution (Single) ---
    if path == "/api/delete_execution" and httpMethod == HttpPost:
      let query = decodeQueryAsTable(req.url.query)
      let id = parseIntSafe(query.getOrDefault("id", "-1"))
      if id <= 0:
        await respJsonCode(req, Http400, %*{"status": "error", "message": "Invalid execution ID"})
        return
      discard await sendExecutorSync(executorChan, ExecutorSignal(
          kind: estCancelExecution, cancelExecutionId: id))
      discard await sendDbSync(dbChan, DbMessage(
          kind: dbDeleteExecution, deleteExecutionId: id))
      await respJson(req, %*{"status": "ok"})
      return

    # --- Cancel Execution ---
    if path == "/api/cancel_execution" and httpMethod == HttpPost:
      let query = decodeQueryAsTable(req.url.query)
      let id = parseIntSafe(query.getOrDefault("id", "-1"))
      if id <= 0:
        await respJsonCode(req, Http400, %*{"status": "error", "message": "Invalid execution ID"})
        return
      discard await sendExecutorSync(executorChan, ExecutorSignal(
          kind: estCancelExecution, cancelExecutionId: id))
      await respJson(req, %*{"status": "ok"})
      return

    # --- Toggle Task ---
    if path == "/api/toggle_task" and httpMethod == HttpPost:
      let query = decodeQueryAsTable(req.url.query)
      let id = parseIntSafe(query.getOrDefault("id", "-1"))
      let enabled = query.getOrDefault("enabled", "false").parseBool
      let tasks = queryRowsTask(db, "_dbID = " & $id)
      if tasks.len == 0:
        await respJsonCode(req, Http404, %*{"status": "error", "message": "Task not found"})
        return
      var (_, task) = tasks[0]
      task.enabled = enabled
      if task.sourceFile.len > 0 and fileExists(task.sourceFile):
        updateEnabledInYaml(task.sourceFile, enabled)
      let resolvedCal = resolveCalendarPath(task.calendarPath,
          if task.sourceFile.len > 0: task.sourceFile.absolutePath() else: "")
      let newHash = calculateConfigHash(task, resolvedCal)
      discard await sendDbSync(dbChan, DbMessage(
          kind: dbToggleTask,
          toggleTaskId: id,
          toggleEnabled: enabled,
          toggleConfigHash: newHash))
      schedulerChan[].send(SchedulerSignal(kind: ssReloadTasks))
      await respJson(req, %*{"status": "ok", "id": id, "enabled": enabled})
      return

    # --- Delete Task ---
    if path == "/api/delete_task" and httpMethod == HttpPost:
      let query = decodeQueryAsTable(req.url.query)
      let id = parseIntSafe(query.getOrDefault("id", "-1"))
      let tasks = queryRowsTask(db, "_dbID = " & $id)
      if tasks.len == 0:
        await respJsonCode(req, Http404, %*{"status": "error", "message": "Task not found"})
        return
      let (_, task) = tasks[0]
      discard await sendDbSync(dbChan, DbMessage(
          kind: dbDeleteTask, deleteTaskId: id,
          deleteTaskSourceFile: task.sourceFile))
      schedulerChan[].send(SchedulerSignal(kind: ssReloadTasks))
      await respJson(req, %*{"status": "ok"})
      return

    # --- Trigger Task ---
    if path == "/api/trigger_task" and httpMethod == HttpPost:
      let query = decodeQueryAsTable(req.url.query)
      let id = parseIntSafe(query.getOrDefault("id", "-1"))
      let tasks = queryRowsTask(db, "_dbID = " & $id)
      if tasks.len == 0:
        await respJsonCode(req, Http404, %*{"status": "error", "message": "Task not found"})
        return
      let (_, task) = tasks[0]
      let execId = await sendExecutorSync(executorChan, ExecutorSignal(
          kind: estTriggerTask,
          triggerTaskId: id,
          triggerTaskTask: task,
          triggerTaskManualTriggered: true))
      await respJson(req, %*{
        "status": "ok",
        "taskId": id,
        "taskName": task.name,
        "executionId": execId
      })
      return

    # --- Trigger Job ---
    if path == "/api/trigger_job" and httpMethod == HttpPost:
      let query = decodeQueryAsTable(req.url.query)
      let id = parseIntSafe(query.getOrDefault("id", "-1"))
      let jobs = queryRowsJob(db, "_dbID = " & $id)
      if jobs.len == 0:
        await respJsonCode(req, Http404, %*{"status": "error", "message": "Job not found"})
        return
      let (jobId, job) = jobs[0]
      let taskId = job.taskId
      let tasks = queryRowsTask(db, "_dbID = " & $taskId)
      if tasks.len == 0:
        await respJsonCode(req, Http404, %*{"status": "error", "message": "Task not found"})
        return
      let (_, task) = tasks[0]
      let execId = await sendExecutorSync(executorChan, ExecutorSignal(
          kind: estTriggerJob,
          triggerJobTaskId: taskId,
          triggerJobTask: task,
          triggerJobJobId: jobId,
          triggerJobJob: job,
          triggerJobManualTriggered: true))
      await respJson(req, %*{
        "status": "ok",
        "taskId": taskId,
        "taskName": task.name,
        "jobId": jobId,
        "jobName": job.name,
        "executionId": execId
      })
      return

    # --- Create User ---
    if path == "/api/create_user" and httpMethod == HttpPost:
      let dataJson = req.body.parseJson()
      let uname = dataJson["username"].getStr().strip()
      if uname.len == 0:
        await respJsonCode(req, Http400, %*{"status": "error", "message": "Username is required"})
        return
      var user = deserializeJson[User](dataJson)
      user.username = uname
      user.passwordHash = dataJson["password"].getStr.encryptPassword
      user.createdAt = now()
      user.updatedAt = now()

      let existingUsers = db.queryRowsUser("username = '" & uname.serialize().replace("'", "''") & "'")
      if existingUsers.len > 0:
        await respJsonCode(req, Http409, %*{"status": "error",
            "message": "User already exists"})
        return

      let newId = await sendDbSync(dbChan, DbMessage(kind: dbInsertUser, user: user))
      if newId < 0:
        await respJsonCode(req, Http409, %*{"status": "error", "message": "Failed to create user"})
        return
      await respJson(req, %*{"status": "ok", "id": newId})
      return

    # --- Delete User ---
    if path == "/api/delete_user" and httpMethod == HttpPost:
      let dataJson = req.body.parseJson()
      let id = dataJson["userId"].getInt()
      if id == userId:
        await respJsonCode(req, Http400, %*{"status": "error",
            "message": "Cannot delete your own active account"})
        return
      discard await sendDbSync(dbChan, DbMessage(kind: dbDeleteUser, deleteUserId: id))
      await respJson(req, %*{"status": "ok"})
      return

    # --- Update User Password ---
    if path == "/api/update_user_password" and httpMethod == HttpPost:
      let dataJson = req.body.parseJson()
      let id = dataJson["userId"].`$`.strip(chars = {'"'}).parseIntSafe()
      let password = dataJson["password"].getStr()
      let email = if "email" in dataJson: dataJson["email"].getStr() else: ""
      if id <= 0 or password.len == 0:
        await respJsonCode(req, Http400, %*{"status": "error", "message": "Invalid user or password"})
        return
      discard await sendDbSync(dbChan, DbMessage(kind: dbUpdateUserPassword,
          updateUserId: id,
          newPasswordHash: password.encryptPassword,
          newPasswordEmail: email))
      await respJson(req, %*{"status": "ok"})
      return

    # --- Create Token ---
    if path == "/api/create_token" and httpMethod == HttpPost:
      let dataJson = req.body.parseJson()
      let tokenName = dataJson["name"].getStr().strip()
      if tokenName.len == 0:
        await respJsonCode(req, Http400, %*{"status": "error", "message": "Token name is required"})
        return
      var expireAt: DateTime
      if "expiresAt" in dataJson and dataJson["expiresAt"].kind in {JInt, JFloat} and
          dataJson["expiresAt"].getBiggestInt() > 0:
        expireAt = (dataJson["expiresAt"].getBiggestInt() div 1000).fromUnix().utc()
      let token = genToken()
      let tokId = await sendDbSync(dbChan, DbMessage(
        kind: dbInsertToken,
        token: Token(
          userId: userId,
          name: tokenName,
          token: token,
          createdAt: now(),
          expiresAt: expireAt
        )
      ))
      await respJson(req, %*{"status": "ok", "id": tokId, "token": token})
      return

    # --- Delete Token ---
    if path == "/api/delete_token" and httpMethod == HttpPost:
      let dataJson = req.body.parseJson()
      let tokenId = dataJson["tokenId"].getInt()
      if getTokenOwnerId(db, tokenId) != userId:
        await respJsonCode(req, Http403, %*{"status": "error", "message": "Forbidden"})
        return
      discard await sendDbSync(dbChan, DbMessage(kind: dbDeleteToken, deleteTokenId: tokenId))
      await respJson(req, %*{"status": "ok"})
      return

    # --- 404 Default ---
    if path.startsWith("/api/"):
      await respJsonCode(req, Http404, %*{"status": "error", "message": "Endpoint not found"})
    else:
      await resp404(req)

  proc tryHandleRequest(req: Request) {.async.} =
    try:
      await handleRequest(req)
    except Exception as e:
      warn getCurrentExceptionMsg()
      warn getStackTrace()
      try:
        if req.url.path.startsWith("/api/"):
          await respJsonCode(req, Http500, %*{"status": "error", "message": getCurrentExceptionMsg()})
        else:
          await resp500(req, getCurrentExceptionMsg())
      except:
        discard
      monitorChan[].send(SchedulerMonitorSignal(kind: smmAlert,
          messageTitle: "Jobscheduler: Web Server Error :" &
          getCurrentExceptionMsg(),
          message: e.getStackTrace()))

  let port = Port(webServer.cfg.server.port)
  var server = newAsyncHttpServer()

  info "Starting web server on " & webServer.cfg.server.host & ":" &
      $webServer.cfg.server.port
  waitFor server.serve(port, tryHandleRequest, webServer.cfg.server.host)
