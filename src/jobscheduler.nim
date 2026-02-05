import std/[asynchttpserver, asyncdispatch, json, times, strutils, uri, os,
    base64, tables, random, options, re, sequtils, logging]
import db_connector/db_sqlite
import checksums/sha1
import nimja/parser
import db/connection
import scheduler/engine
import scheduler/triggers
import scheduler/projection
import executor/local
import executor/remote
import recovery/monitor
import recovery/manager
import config/types
import config/loader
import models/types as model_types
import models/user
import models/task
import models/job
import models/execution
import models/token
import services/sync_service
import services/task_file_service

# Global config-derived values
var dbPath = "jobscheduler.db"
var tasksDir {.threadvar.}: string

# In-memory session storage (thread-local for async GC safety)
var sessions {.threadvar.}: Table[string, string]
var dummyHash {.threadvar.}: string

var scheduler {.threadvar.}: SchedulerEngine

const TemplateDir = currentSourcePath().parentDir() / "webui/templates/"

proc deleteTaskFile(task: Task, tasksDir: string) =
  var path = task.sourceFile
  if path == "":
      var targetDir = tasksDir
      if task.groupName.len > 0: targetDir = targetDir / task.groupName
      let filename = task.name.replace(" ", "_").replace("/", "-") & ".yaml"
      path = targetDir / filename
      
  if fileExists(path):
    try: removeFile(path)
    except: error "Failed to delete task file: " & path

proc syncTaskToFile(db: DbConn, taskId: int, tasksDir: string) =
  {.gcsafe.}:
    let taskOpt = getTaskById(db, taskId)
    if taskOpt.isSome:
      let (dbId, t) = taskOpt.get
      let jobs = getJobsByTaskIdOrdered(db, taskId).mapIt(it.data)
      
      saveTaskToYaml(t, jobs, tasksDir)
      
      # Update hash
      var targetDir = tasksDir
      if t.groupName.len > 0: targetDir = targetDir / t.groupName
      let filename = t.name.replace(" ", "_").replace("/", "-") & ".yaml"
      let path = targetDir / filename
      
      if fileExists(path):
          let content = readFile(path)
          let hash = calculateConfigHash(content)
          
          # Avoid infinite loop of updates by checking if hash changed
          if t.configHash != hash:
            var updatedTask = t
            updatedTask.configHash = hash
            updateRowTask(db, dbId, updatedTask)

proc genSalt(rounds: int = 12): string =
  var rng = initRand()
  var s = ""
  for i in 0..<16:
    s.add(char(rng.rand(255)))
  return "$simple$" & encode(s)

proc hash(password: string, salt: string): string =
  var realSalt = salt
  if salt.startsWith("$simple$"):
    let parts = salt.split('$')
    if parts.len >= 3:
      realSalt = parts[2]

  return "$simple$" & realSalt & "$" & $secureHash(realSalt & password)

proc generateToken(): string =
  var rng = initRand()
  result = ""
  for i in 0..<32:
    result.add(char(rng.rand(255)))
  result = encode(result, safe = true)

proc checkAuth(req: Request, db: DbConn): string =
  ## Returns username if authenticated, empty string otherwise

  # 1. Check Authorization Header (Bearer)
  let authHeader = req.headers.getOrDefault("Authorization")
  if authHeader.startsWith("Bearer "):
    let token = authHeader[7..^1]
    
    # First check session tokens (login sessions)
    let sessionUser = sessions.getOrDefault(token, "")
    if sessionUser != "":
      return sessionUser
    
    # Then check API tokens (database)
    let tokens = queryRowsToken(db, "token = '" & token.replace("'", "''") & "'")
    if tokens.len > 0:
      let t = tokens[0].data
      # Check if token is expired
      if t.expiresAt > now().utc:
        # Get username from userId
        let users = queryRowsUser(db, "_dbID = " & $t.userId)
        if users.len > 0:
          return users[0].data.username
    
    return ""

  # 2. Check Cookie (auth_token)
  let cookieHeader = req.headers.getOrDefault("Cookie")
  if cookieHeader != "":
    for cookie in cookieHeader.split(";"):
      let pair = cookie.strip().split("=", 1)
      if pair.len == 2 and pair[0] == "auth_token":
        return sessions.getOrDefault(pair[1], "")

  return ""

proc renderDashboard(): string =
  const page = "dashboard"
  compileTemplateFile("dashboard.html", baseDir = TemplateDir)

proc renderTasks(): string =
  const page = "tasks"
  compileTemplateFile("tasks.html", baseDir = TemplateDir)

proc renderTaskDetail(taskId: int, taskName: string): string =
  const page = "tasks"
  compileTemplateFile("task_detail.html", baseDir = TemplateDir)

proc renderTaskEdit(taskId: string): string =
  const page = "tasks"
  compileTemplateFile("task_edit.html", baseDir = TemplateDir)

proc renderJobHistory(jobId: int, jobName: string): string =
  const page = "executions"
  compileTemplateFile("job_history.html", baseDir = TemplateDir)

proc renderLogViewer(execId: int): string =
  const page = "executions"
  compileTemplateFile("log_viewer.html", baseDir = TemplateDir)

proc renderUsers(): string =
  const page = "users"
  compileTemplateFile("users.html", baseDir = TemplateDir)

proc renderExecutions(): string =
  const page = "executions"
  compileTemplateFile("executions.html", baseDir = TemplateDir)

proc renderTokens(): string =
  const page = "tokens"
  compileTemplateFile("tokens.html", baseDir = TemplateDir)

proc renderSchedule(): string =
  const page = "schedule"
  compileTemplateFile("schedule.html", baseDir = TemplateDir)

# Embed static files at compile time
const staticFiles = {
  "style.css": (staticRead("webui/static/style.css"), "text/css"),
  "pages.css": (staticRead("webui/static/pages.css"), "text/css"),
  "toggles.css": (staticRead("webui/static/toggles.css"), "text/css"),
  "app.js": (staticRead("webui/static/app.js"), "application/javascript")
}.toTable

const loginHtml = staticRead("webui/templates/login.html")

proc serveStatic(path: string): tuple[content: string, contentType: string] =
  if staticFiles.hasKey(path):
    return staticFiles[path]
  else:
    return ("", "")

proc addCorsHeaders(headers: var HttpHeaders) =
  headers["Access-Control-Allow-Origin"] = "*"
  headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
  headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"

proc getDb(): DbConn {.gcsafe.} =
  {.gcSafe.}:
    return openDatabase(dbPath)

proc matchPath(path: string, pattern: string, captures: var seq[string]): bool =
  ## Helper function for regex URL matching
  var matches: array[10, string]
  let matched = path.match(re(pattern), matches)
  if matched:
    captures = @[]
    for i in 0..<matches.len:
      if matches[i].len > 0:
        captures.add(matches[i])
    return true
  else:
    return false

proc handleRequest(req: Request) {.async, gcsafe.} =
  let path = req.url.path
  var headers = newHttpHeaders()
  addCorsHeaders(headers)

  let db = getDb()
  defer: db.close()

  try:
    # Shared variable for regex captures (only one branch executes)
    var captures: seq[string]

    # OPTIONS for CORS
    if req.reqMethod == HttpOptions:
      await req.respond(Http200, "", headers)
      return

    # Static files - allow query string for cache busting
    if matchPath(path, "^/static/([^?]+)", captures):
      let (content, contentType) = serveStatic(captures[0])
      if content.len > 0:
        headers["Content-Type"] = contentType
        await req.respond(Http200, content, headers)
      else:
        await req.respond(Http404, "Not found", headers)
      return

    # === GLOBAL AUTH MIDDLEWARE ===
    let username = checkAuth(req, db)

    # 1. Handle Login Page Redirection (If already authenticated)
    if path == "/login" and username != "":
      headers["Location"] = "/"
      await req.respond(Http302, "", headers)
      return

    # 2. Define Public Routes
    let isPublic = (path == "/api/login" and req.reqMethod == HttpPost) or
                   (path == "/api/health") or
                   (path == "/isHealthy") or
                   (path == "/login")

    # 3. Enforce Authentication
    if not isPublic and username == "":
      if path.startsWith("/api/"):
        await req.respond(Http401, $(%*{"error": "Unauthorized"}), headers)
      else:
        headers["Location"] = "/login"
        await req.respond(Http302, "", headers)
      return

    # === API ENDPOINTS ===

    # Login
    if path == "/api/login" and req.reqMethod == HttpPost:
      var username = ""
      var password = ""
      try:
        let body = parseJson(req.body)
        username = body["username"].getStr
        password = body["password"].getStr
      except:
        await req.respond(Http400, $(%*{"error": "Invalid request body"}), headers)
        return

      if username.len == 0 or password.len == 0:
        await req.respond(Http400, $(%*{
            "error": "Username and password required"}), headers)
        return

      let userOpt = getUserByUsername(db, username)
      if userOpt.isSome:
        let (_, uData) = userOpt.get
        let hashed = uData.passwordHash

        # Workaround: bcrypt.compare is failing, so we manually re-hash and check equality.
        let checkHash = hash(password, hashed)
        let match = (checkHash == hashed)

        if match:
          let token = generateToken()
          sessions[token] = username
          headers["Content-Type"] = "application/json"
          # Set HttpOnly cookie for web access
          headers["Set-Cookie"] = "auth_token=" & token & "; HttpOnly; Path=/; Max-Age=86400"
          await req.respond(Http200, $(%*{"token": token,
              "username": username}), headers)
        else:
          await req.respond(Http401, $(%*{"error": "Invalid credentials"}), headers)
      else:
        info "Login attempt for '", username, "' - User not found."
        # Timing attack mitigation
        discard hash(password, dummyHash)
        await req.respond(Http401, $(%*{"error": "Invalid credentials"}), headers)


    # Logout
    elif path == "/api/logout" and req.reqMethod == HttpPost:
      # Username guaranteed by middleware
      # Remove session
      for token, user in sessions.pairs:
        if user == username:
          sessions.del(token)
          break

      # Clear cookie
      headers["Set-Cookie"] = "auth_token=; HttpOnly; Path=/; Max-Age=0"
      await req.respond(Http200, $(%*{"message": "Logged out"}), headers)

    # Health
    elif path == "/api/health" or path == "/isHealthy":
      headers["Content-Type"] = "application/json"
      await req.respond(Http200, $(%*{"status": "ok", "timestamp": $now()}), headers)

    # Stats
    elif path == "/api/stats":
      let stats = getExecutionStats(db)
      headers["Content-Type"] = "application/json"
      await req.respond(Http200, $(%*{
        "total": stats.total,
        "success": stats.success,
        "failed": stats.failed,
        "running": stats.running
      }), headers)

    # Get all tasks
    elif path == "/api/tasks" and req.reqMethod == HttpGet:
      try:
          # Parse pagination parameters
          var page = 1
          var limit = 50
          if req.url.query != "":
            for pair in req.url.query.split("&"):
              if pair.startsWith("page="):
                try:
                  page = parseInt(pair.split("=")[1])
                  if page < 1: page = 1
                except: discard
              elif pair.startsWith("limit="):
                try:
                  limit = parseInt(pair.split("=")[1])
                  if limit < 1: limit = 50
                except: discard

          let offset = (page - 1) * limit
          let tasks = getAllTasksOrdered(db) 
          
          let total = tasks.len
          # Manual splice
          var pagedTasks: seq[tuple[dbId: int, data: Task]] = @[]
          if offset < tasks.len:
            let endIdx = min(offset + limit, tasks.len)
            pagedTasks = tasks[offset..<endIdx]

          var list = newJArray()
          for row in pagedTasks:
            let t = row.data
            let warnings = getTaskWarnings(t)
            var warningJArray = newJArray()
            for w in warnings: warningJArray.add(%w)
            
            # Calculate next trigger using scheduler's server start time
            let lastRun = getLastTaskExecutionTime(db, row.dbId)
            let serverStart = if scheduler != nil: some(scheduler.serverStartTime) else: none(DateTime)
            let nextRun = getNextTrigger(t, now().utc, lastRun, serverStart)
            let nextRunStr = if nextRun.isSome: $nextRun.get() else: ""
            
            list.add(%*{"id": row.dbId, "name": t.name, "description": t.description,
                       "type": t.taskType, "enabled": t.enabled, "warnings": warningJArray,
                       "nextTrigger": nextRunStr, "groupName": t.groupName, "timezone": t.timezone})

          var response = %*{
            "data": list,
            "pagination": {
              "page": page,
              "limit": limit,
              "total": total,
              "totalPages": (total + limit - 1) div limit
            }
          }

          headers["Content-Type"] = "application/json"
          await req.respond(Http200, $response, headers)
      except:
          let e = getCurrentException()
          error "Error in /api/tasks: " & e.msg
          await req.respond(Http500, $(%*{"error": "Failed to list tasks"}), headers)

    # Delete task
    elif matchPath(path, "^/api/tasks/(\\d+)$", captures) and req.reqMethod == HttpDelete:
      let taskId = parseInt(captures[0])
      
      # Get task to delete file
      let taskOpt = getTaskById(db, taskId)
      if taskOpt.isSome:
         deleteTaskFile(taskOpt.get.data, tasksDir)
      
      # Cascade delete: executions -> jobs -> task
      let jobs = queryRowsJob(db, "taskId = " & $taskId)
      for j in jobs:
        # Delete all executions for this job
        let executions = queryRowsExecution(db, "jobId = " & $j.dbId)
        for e in executions:
          # Clean up log files
          let logPath = e.data.logFile
          if logPath != "":
            try:
              if fileExists(logPath):
                removeFile(logPath)
              let exitPath = logPath.changeFileExt("exit")
              if fileExists(exitPath):
                removeFile(exitPath)
            except:
              discard
          deleteRowExecution(db, e.dbId)
        
        # Delete the job
        deleteRowJob(db, j.dbId)
      
      # Delete the task
      deleteRowTask(db, taskId)

      headers["Content-Type"] = "application/json"
      await req.respond(Http200, $(%*{"message": "Deleted"}), headers)

    # Get task jobs
    elif matchPath(path, "^/api/tasks/(\\d+)/jobs$", captures) and
        req.reqMethod == HttpGet:
      let taskId = parseInt(captures[0])
      let jobs = getJobsByTaskIdOrdered(db, taskId)
      var list = newJArray()
      for row in jobs:
        let j = row.data
        # Get last execution ID
        var lastExecId = 0
        let lastExecTimeOpt = getLastExecutionTime(db, row.dbId)
        # Wait, getLastExecutionTime returns time, not ID. 
        # I need a helper to get ID or fetches latest execution row.
        # Let's use raw SQL for efficiency here or add a helper?
        # A simple query is fine.
        let lastExecRow = db.getRow(sql"SELECT _dbID FROM ExecutionTable WHERE jobId = ? ORDER BY startTime DESC LIMIT 1", row.dbId)
        if lastExecRow[0] != "":
             lastExecId = parseInt(lastExecRow[0])

        list.add(%*{"id": row.dbId, "name": j.name, "command": j.command,
            "enabled": j.enabled, "lastExecutionId": lastExecId})
      headers["Content-Type"] = "application/json"
      await req.respond(Http200, $list, headers)

    # Add job to task
    elif matchPath(path, "^/api/tasks/(\\d+)/jobs$", captures) and
        req.reqMethod == HttpPost:
      try:
        let taskId = parseInt(captures[0])
        let body = parseJson(req.body)

        # Get next order using helper function
        let nextOrder = getNextJobOrder(db, taskId)

        var j = Job(
          taskId: taskId,
          name: body["name"].getStr,
          command: body["command"].getStr,
          orderIdx: nextOrder,
          enabled: body.getOrDefault("enabled").getBool(true),
          # createdAt: now().utc
        )
        let jobId = insertRowJob(db, j)
        
        # Sync to file
        syncTaskToFile(db, taskId, tasksDir)

        headers["Content-Type"] = "application/json"
        await req.respond(Http200, $(%*{"success": true, "jobId": jobId,
            "order": nextOrder}), headers)
      except:
        let e = getCurrentException()
        info "Error adding job: ", e.msg
        await req.respond(Http500, $(%*{"error": "Failed to add job"}), headers)

    # Get single task (must be after more specific /tasks/* routes)
    elif matchPath(path, "^/api/tasks/(\\d+)$", captures) and req.reqMethod == HttpGet:
      let taskId = parseInt(captures[0])
      let taskOpt = getTaskById(db, taskId)
      if taskOpt.isSome:
        let (dbId, t) = taskOpt.get

        # Fetch commands from jobs
        let jobs = getJobsByTaskIdOrdered(db, taskId)
        let commands = jobs.mapIt(it.data.command)

        # Calculate next trigger
        let lastRun = getLastTaskExecutionTime(db, dbId)
        let serverStart = if scheduler != nil: some(scheduler.serverStartTime) else: none(DateTime)
        let nextRun = getNextTrigger(t, now().utc, lastRun, serverStart)
        let nextRunStr = if nextRun.isSome: $nextRun.get() else: ""
        
        var sourceContent = ""
        if t.sourceFile != "" and fileExists(t.sourceFile):
           try: sourceContent = readFile(t.sourceFile)
           except: sourceContent = "# Error reading file"

        headers["Content-Type"] = "application/json"
        await req.respond(Http200, $(%*{"id": dbId, "name": t.name, "description": t.description,
                                        "groupName": t.groupName,
                                        "type": t.taskType,
                                        "sshHost": t.sshHost,
                                        "sshUser": t.sshUser,
                                        "sshKeyPath": t.sshKeyPath,
                                        "scheduleType": t.scheduleType,
                                        "cronExpr": t.cronExpr,
                                        "timeOfDay": t.timeOfDay,
                                        "timezone": t.timezone,
                                        "intervalMinutes": t.intervalMinutes,
                                        "intervalStart": t.intervalStart,
                                        "intervalEnd": t.intervalEnd,
                                        "calendarPath": t.calendarPath,
                                        "enabled": t.enabled,
                                        "warnings": %getTaskWarnings(t),
                                        "commands": commands,
                                        "sourceContent": sourceContent,
                                        "nextTrigger": nextRunStr}), headers)
      else:
        await req.respond(Http404, $(%*{"error": "Not found"}), headers)


    # Task Toggle
    elif matchPath(path, "^/api/tasks/(\\d+)/toggle$", captures) and
        req.reqMethod == HttpPost:
      try:
        let taskId = parseInt(captures[0])
        let body = parseJson(req.body)
        let enabled = body["enabled"].getBool

        toggleTask(db, taskId, enabled) # Use helper from task.nim
        
        # Sync to file
        syncTaskToFile(db, taskId, tasksDir)
        
        headers["Content-Type"] = "application/json"
        await req.respond(Http200, $(%*{"success": true}), headers)
      except:
        await req.respond(Http500, $(%*{"error": "Failed to toggle task"}), headers)

    # Update Task
    elif matchPath(path, "^/api/tasks/(\\d+)$", captures) and req.reqMethod == HttpPut:
      try:
        let taskId = parseInt(captures[0])
        let body = parseJson(req.body)
        let name = body["name"].getStr
        let desc = body["description"].getStr
        let taskTypeStr = body["type"].getStr
        let sshHost = body["sshHost"].getStr
        let sshUser = body["sshUser"].getStr
        let enabled = body["enabled"].getBool

        # Schedule params
        let scheduleTypeStr = body.getOrDefault("scheduleType").getStr("cron")
        let cronExpr = body.getOrDefault("cronExpr").getStr("")
        let timeOfDay = body.getOrDefault("timeOfDay").getStr("")
        let timezone = body.getOrDefault("timezone").getStr("")
        let intervalMinutes = body.getOrDefault("intervalMinutes").getInt(0)
        let intervalStart = body.getOrDefault("intervalStart").getStr("")
        let intervalEnd = body.getOrDefault("intervalEnd").getStr("")
        let calendarPath = body.getOrDefault("calendarPath").getStr("")

        var t = Task(
          name: name,
          description: desc,
          taskType: parseEnum[model_types.TaskType](taskTypeStr),
          sshHost: sshHost,
          sshUser: sshUser,
          enabled: enabled,

          scheduleType: parseEnum[model_types.ScheduleType](scheduleTypeStr),
          cronExpr: cronExpr,
          timeOfDay: timeOfDay,
          timezone: timezone,
          intervalMinutes: intervalMinutes,
          intervalStart: intervalStart,
          intervalEnd: intervalEnd,
          calendarPath: calendarPath
        )

        let existingOpt = getTaskById(db, taskId)
        var oldTask: Option[Task] = none(Task)
        if existingOpt.isSome:
          t.createdAt = existingOpt.get.data.createdAt
          oldTask = some(existingOpt.get.data)
        else:
          t.createdAt = now().utc

        t.updatedAt = now().utc

        updateRowTask(db, taskId, t)
        
        # Sync Jobs
        if body.hasKey("commands") and body["commands"].kind == JArray:
          let newCommandsJ = body["commands"]
          var newCommands: seq[string] = @[]
          for c in newCommandsJ: newCommands.add(c.getStr)
          
          if newCommands.len > 0:
             let existingJobs = getJobsByTaskIdOrdered(db, taskId)
             
             # 1. Update existing or Create new
             for i in 0..<newCommands.len:
                 let cmd = newCommands[i]
                 let order = i + 1
                 
                 if i < existingJobs.len:
                     # Update
                     var j = existingJobs[i].data
                     j.command = cmd
                     j.orderIdx = order
                     # Update name if it matches default pattern to keep it clean
                     if j.name.startsWith(name) or j.name.startsWith("Job"):
                          j.name = if newCommands.len > 1: name & " Job " & $order else: name & " Job"
                     updateRowJob(db, existingJobs[i].dbId, j)
                 else:
                     # Create
                     var j = Job(
                       taskId: taskId,
                       name: if newCommands.len > 1: name & " Job " & $order else: name & " Job",
                       command: cmd,
                       orderIdx: order,
                       enabled: true
                     )
                     discard insertRowJob(db, j)
                     
             # 2. Delete excess jobs
             if existingJobs.len > newCommands.len:
                 for i in newCommands.len..<existingJobs.len:
                     let jId = existingJobs[i].dbId
                     # Delete executions
                     let executions = queryRowsExecution(db, "jobId = " & $jId)
                     for e in executions:
                       let logPath = e.data.logFile
                       if logPath != "":
                          try:
                            if fileExists(logPath): removeFile(logPath)
                            let exitPath = logPath.changeFileExt("exit")
                            if fileExists(exitPath): removeFile(exitPath)
                          except: discard
                       deleteRowExecution(db, e.dbId)
                     deleteRowJob(db, jId)
        
        if oldTask.isSome and oldTask.get.name != name:
             deleteTaskFile(oldTask.get, tasksDir)
             
        syncTaskToFile(db, taskId, tasksDir)
        
        headers["Content-Type"] = "application/json"
        await req.respond(Http200, $(%*{"success": true}), headers)
      except:
        let e = getCurrentException()
        info "Error updating task: ", e.msg
        await req.respond(Http500, $(%*{"error": "Failed to update task"}), headers)

    # Create Task
    elif path == "/api/tasks" and req.reqMethod == HttpPost:
      try:
        let body = parseJson(req.body)
        let name = body["name"].getStr
        let desc = body["description"].getStr
        let taskTypeStr = body["type"].getStr
        let sshHost = body["sshHost"].getStr
        let sshUser = body["sshUser"].getStr
        let sshKeyPath = body.getOrDefault("sshKeyPath").getStr("")
        let enabled = body["enabled"].getBool

        # Get commands - support both single command and commands array for backward compatibility
        var commands: seq[string] = @[]
        if body.hasKey("commands") and body["commands"].kind == JArray:
          for cmd in body["commands"]:
            if cmd.kind == JString and cmd.getStr.len > 0:
              commands.add(cmd.getStr)
        elif body.hasKey("command"):
          let singleCmd = body["command"].getStr
          if singleCmd.len > 0:
            commands.add(singleCmd)

        if commands.len == 0:
          await req.respond(Http400, $(%*{
              "error": "At least one command is required"}), headers)
          return

        # Schedule params
        let scheduleTypeStr = body.getOrDefault("scheduleType").getStr("cron")
        let cronExpr = body.getOrDefault("cronExpr").getStr("")
        let timeOfDay = body.getOrDefault("timeOfDay").getStr("")
        let timezone = body.getOrDefault("timezone").getStr("")
        let intervalMinutes = body.getOrDefault("intervalMinutes").getInt(0)
        let intervalStart = body.getOrDefault("intervalStart").getStr("")
        let intervalEnd = body.getOrDefault("intervalEnd").getStr("")
        let calendarPath = body.getOrDefault("calendarPath").getStr("")

        # 1. Create Task
        var t = Task(
          name: name,
          description: desc,
          taskType: parseEnum[model_types.TaskType](taskTypeStr),
          sshHost: sshHost,
          sshUser: sshUser,
          sshKeyPath: sshKeyPath,

          scheduleType: parseEnum[model_types.ScheduleType](scheduleTypeStr),
          cronExpr: cronExpr,
          timeOfDay: timeOfDay,
          timezone: timezone,
          intervalMinutes: intervalMinutes,
          intervalStart: intervalStart,
          intervalEnd: intervalEnd,
          calendarPath: calendarPath,

          enabled: enabled,
          createdAt: now().utc,
          updatedAt: now().utc
        )
        let taskId = insertRowTask(db, t)

        # 2. Create Jobs (one per command)
        var orderIdx = 1
        for cmd in commands:
          var j = Job(
              taskId: taskId,
              name: if commands.len > 1: name & " Job " & $orderIdx else: name &
              " Job",
              command: cmd,
              orderIdx: orderIdx,
              enabled: true,
              # createdAt: now().utc
          )
          discard insertRowJob(db, j)
          inc(orderIdx)

        # Sync to file
        syncTaskToFile(db, taskId, tasksDir)

        await req.respond(Http200, $(%*{"success": true, "taskId": taskId,
            "jobsCreated": commands.len}), headers)
      except:
        let e = getCurrentException()
        info "Error creating task: ", e.msg
        await req.respond(Http500, $(%*{"error": "Failed to create task"}), headers)

    # Trigger task manually (runs all jobs in sequence)
    elif matchPath(path, "^/api/tasks/(\\d+)/trigger$", captures) and
        req.reqMethod == HttpPost:
      let taskId = parseInt(captures[0])
      let jobs = getJobsByTaskIdOrdered(db, taskId)

      if jobs.len == 0:
        headers["Content-Type"] = "application/json"
        await req.respond(Http400, $(%*{
            "error": "No jobs found for this task"}), headers)
      else:
        # Trigger the first job - rest will chain automatically
        let firstJobId = jobs[0].dbId
        let triggered = await scheduler.triggerJob(firstJobId)
        if triggered:
          headers["Content-Type"] = "application/json"
          await req.respond(Http200, $(%*{"message": "Task triggered",
              "firstJobId": firstJobId, "totalJobs": jobs.len}), headers)
        else:
          await req.respond(Http400, $(%*{"error": "Failed to trigger task"}), headers)

    # Trigger job manually
    elif matchPath(path, "^/api/jobs/(\\d+)/trigger$", captures) and
        req.reqMethod == HttpPost:
      let jobId = parseInt(captures[0])
      let triggered = await scheduler.triggerJob(jobId)
      if triggered:
        headers["Content-Type"] = "application/json"
        await req.respond(Http200, $(%*{"message": "Job triggered"}), headers)
      else:
        await req.respond(Http400, $(%*{"error": "Failed to trigger job"}), headers)

    # Update job
    elif matchPath(path, "^/api/jobs/(\\d+)$", captures) and req.reqMethod == HttpPut:
      try:
        let jobId = parseInt(captures[0])
        let body = parseJson(req.body)

        # Get existing job to preserve fields not in update
        let jobOpt = getJobById(db, jobId)
        if jobOpt.isNone:
          await req.respond(Http404, $(%*{"error": "Job not found"}), headers)
        else:
          var (dbId, j) = jobOpt.get
          j.name = body.getOrDefault("name").getStr(j.name)
          j.command = body.getOrDefault("command").getStr(j.command)
          j.enabled = body.getOrDefault("enabled").getBool(j.enabled)

          updateRowJob(db, dbId, j)
          
          # Sync to file
          syncTaskToFile(db, j.taskId, tasksDir)
          
          headers["Content-Type"] = "application/json"
          await req.respond(Http200, $(%*{"success": true}), headers)
      except:
        let e = getCurrentException()
        info "Error updating job: ", e.msg
        await req.respond(Http500, $(%*{"error": "Failed to update job"}), headers)

    # Delete job
    elif matchPath(path, "^/api/jobs/(\\d+)$", captures) and req.reqMethod == HttpDelete:
      try:
        let jobId = parseInt(captures[0])
        # Need taskId to sync
        let jobOpt = getJobById(db, jobId)
        if jobOpt.isSome:
            let taskId = jobOpt.get.data.taskId
            deleteJobById(db, jobId) # Use helper that handles reordering
            
            # Sync to file
            syncTaskToFile(db, taskId, tasksDir)
            
        headers["Content-Type"] = "application/json"
        await req.respond(Http200, $(%*{"success": true}), headers)
      except:
        let e = getCurrentException()
        info "Error deleting job: ", e.msg
        await req.respond(Http500, $(%*{"error": "Failed to delete job"}), headers)


    # Job history
    elif path.startsWith("/api/jobs/") and path.endsWith("/history"):
      let jobId = path.split("/")[3]
      let jobIdInt = parseInt(jobId)
      let executions = getJobHistory(db, jobIdInt)
      var list = newJArray()
      for row in executions:
        let e = row.data
        list.add(%*{"id": row.dbId, "startTime": $e.startTime, "endTime": $e.endTime,
                   "status": e.status, "pid": e.pid, "exitCode": e.exitCode})
      headers["Content-Type"] = "application/json"
      await req.respond(Http200, $list, headers)

    # === EXECUTIONS API ===

    # GET /api/running-executions - get all currently running executions
    if path == "/api/running-executions" and req.reqMethod == HttpGet:
      try:
        let runningExecs = getRunningExecutionsSummary(db)
        var list = newJArray()
        for exec in runningExecs:
          list.add(%*{"id": exec.id, "taskId": exec.taskId, "taskName": exec.taskName,
                     "jobName": exec.jobName, "status": exec.status,
                     "startTime": $exec.startTime, "endTime": $exec.endTime,
                     "pid": exec.pid})
        headers["Content-Type"] = "application/json"
        await req.respond(Http200, $list, headers)
      except:
        let e = getCurrentException()
        error "Error in /api/running-executions: " & e.msg
        await req.respond(Http500, $(%*{"error": "Internal Error"}), headers)
      return


    # GET /api/stats - get dashboard statistics (for today)
    if path == "/api/stats" and req.reqMethod == HttpGet:
      let stats = getTodayExecutionStats(db)
      let resp = %*{"total": stats.total, "success": stats.success,
          "failed": stats.failed, "running": stats.running}
      headers["Content-Type"] = "application/json"
      await req.respond(Http200, $resp, headers)
      return

    # GET /api/executions - list all or get single execution
    if path == "/api/executions" and req.reqMethod == HttpGet:
      try:
          var execId = 0
          var page = 1
          var limit = 50
          
          if req.url.query != "":
             for pair in req.url.query.split("&"):
                let parts = pair.split("=")
                if parts.len >= 2:
                   let key = parts[0]
                   let val = parts[1]
                   if key == "id": 
                      try: execId = parseInt(val)
                      except: discard
                   elif key == "page":
                      try: page = parseInt(val)
                      except: discard
                   elif key == "limit":
                      try: limit = parseInt(val)
                      except: discard

          if execId > 0:
              let execOpt = getExecutionById(db, execId)
              var list = newJArray()
              if execOpt.isSome:
                let (dbId, e) = execOpt.get
                
                # Fetch Job and Task info
                var taskName = "Unknown"
                var jobName = "Unknown"
                var taskId = 0
                
                let jobOpt = getJobById(db, e.jobId)
                if jobOpt.isSome:
                    let j = jobOpt.get.data
                    jobName = j.name
                    taskId = j.taskId
                    
                    let taskOpt = getTaskById(db, taskId)
                    if taskOpt.isSome:
                        taskName = taskOpt.get.data.name

                list.add(%*{"id": dbId, "taskId": taskId, "taskName": taskName,
                          "jobId": e.jobId, "jobName": jobName, "status": e.status,
                          "startTime": $e.startTime, "endTime": $e.endTime,
                          "pid": e.pid, "exitCode": e.exitCode, "logFile": e.logFile})
              headers["Content-Type"] = "application/json"
              await req.respond(Http200, $(%*{"data": list}), headers)
          else:
             if page < 1: page = 1
             if limit < 1: limit = 50
             let offset = (page - 1) * limit
            
             let executions = getRecentExecutionsPaged(db, limit, offset)
             let totalStr = db.getValue(sql"SELECT COUNT(*) FROM ExecutionTable")
             let total = try: parseInt(totalStr) except: 0
            
             var list = newJArray()
             for e in executions:
               list.add(%*{"id": e.id, "taskId": e.taskId, "taskName": e.taskName,
                         "jobId": 0, "jobName": e.jobName, "status": e.status, 
                         "startTime": $e.startTime, "endTime": $e.endTime,
                         "pid": e.pid, "exitCode": 0})
                          
             var response = %*{
               "data": list,
               "pagination": {
                 "page": page,
                 "limit": limit,
                 "total": total,
                 "totalPages": (total + limit - 1) div limit
               }
             }
             headers["Content-Type"] = "application/json"
             await req.respond(Http200, $response, headers)
      except:
          let e = getCurrentException()
          error "Error in /api/executions: " & e.msg & "\n" & e.getStackTrace()
          await req.respond(Http500, $(%*{"error": "Internal Error"}), headers)
          
    # Schedule API
    elif path == "/api/schedule" and req.reqMethod == HttpGet:
      try:
          var dateStr = ""
          if req.url.query.contains("date="):
             for pair in req.url.query.split("&"):
                 if pair.startsWith("date="):
                     dateStr = pair.split("=")[1]
          
          var targetDate = now().utc
          if dateStr != "":
              try:
                  let parts = dateStr.split("-")
                  if parts.len == 3:
                      targetDate = dateTime(parseInt(parts[0]), parseEnum[Month](parts[1]), parseInt(parts[2]), 0, 0, 0, 0, utc())
              except:
                  discard
          
          let tasks = getAllTasksOrdered(db)
          var list = newJArray()
          
          for row in tasks:
              let t = row.data
              # Use projection module
              let proj = projectTask(t, targetDate)
              
              if proj.triggers.len > 0 or proj.summary != "":
                  var triggerStrList = newJArray()
                  for d in proj.triggers:
                      triggerStrList.add(%($d))
                      
                  list.add(%*{
                      "taskId": row.dbId,
                      "taskName": t.name,
                      "type": t.taskType,
                      "scheduleType": t.scheduleType,
                      "triggers": triggerStrList,
                      "summary": proj.summary,
                      "isHighFrequency": proj.isHighFrequency,
                      "groupName": t.groupName 
                  })
                  
          headers["Content-Type"] = "application/json"
          await req.respond(Http200, $list, headers)
      except:
          let e = getCurrentException()
          error "Error in /api/schedule: " & e.msg
          await req.respond(Http500, $(%*{"error": "Failed to get schedule"}), headers)

    # Serve Schedule Page
    elif path == "/schedule":
       await req.respond(Http200, renderSchedule(), headers)
       return

    # GET /api/logs/{id} - get execution log file
    elif matchPath(path, "^/api/logs/(\\d+)$", captures) and req.reqMethod == HttpGet:
      let execId = parseInt(captures[0])
      let logPath = getExecutionLogPath(db, execId)
      if logPath != "" and fileExists(logPath):
        if req.url.query.contains("download=1"):
          headers["Content-Disposition"] = "attachment; filename=\"execution_" &
              $execId & ".log\""
        headers["Content-Type"] = "text/plain"
        await req.respond(Http200, readFile(logPath), headers)
      else:
        await req.respond(Http404, "Log not found", headers)

    # DELETE /api/executions/{id} - delete execution record
    elif matchPath(path, "^/api/executions/(\\d+)$", captures) and req.reqMethod == HttpDelete:
      try:
        let execId = parseInt(captures[0])
        # Get log path before deletion to clean up files
        let logPath = getExecutionLogPath(db, execId)
        
        # Delete from database
        deleteRowExecution(db, execId)
        
        # Clean up log files if they exist
        if logPath != "":
          try:
            if fileExists(logPath):
              removeFile(logPath)
            let exitPath = logPath.changeFileExt("exit")
            if fileExists(exitPath):
              removeFile(exitPath)
          except:
            discard # Log cleanup failure is non-critical
        
        headers["Content-Type"] = "application/json"
        await req.respond(Http200, $(%*{"success": true}), headers)
      except:
        await req.respond(Http500, $(%*{"error": "Failed to delete execution"}), headers)

    # POST /api/executions/{id}/cancel - cancel running execution
    elif matchPath(path, "^/api/executions/(\\d+)/cancel$", captures) and req.reqMethod == HttpPost:
      try:
        let execId = parseInt(captures[0])
        let success = scheduler.cancelExecution(execId)
        
        headers["Content-Type"] = "application/json"
        if success:
          await req.respond(Http200, $(%*{"success": true}), headers)
        else:
          await req.respond(Http400, $(%*{"error": "Failed to cancel execution"}), headers)
      except:
        await req.respond(Http500, $(%*{"error": "Failed to cancel execution"}), headers)


    # === TOKENS API ===
    elif path == "/api/tokens" and req.reqMethod == HttpGet:
      # Use global auth username
      let userOpt = getUserByUsername(db, username)
      if userOpt.isNone:
        await req.respond(Http401, $(%*{"error": "User not found"}), headers)
        return
      let userId = userOpt.get.dbId

      # Fetch tokens
      let tokens = getTokensByUserId(db, userId)
      var list = newJArray()
      for row in tokens:
        let t = row.data
        let prefix = if t.token.len > 10: t.token[0..9] else: t.token
        list.add(%*{"id": row.dbId, "name": t.name, "tokenPrefix": prefix,
            "expiresAt": $t.expiresAt})

      headers["Content-Type"] = "application/json"
      await req.respond(Http200, $list, headers)

    elif path == "/api/tokens" and req.reqMethod == HttpPost:
      let userOpt = getUserByUsername(db, username)
      if userOpt.isNone:
        await req.respond(Http401, $(%*{"error": "User not found"}), headers)
        return
      let userId = userOpt.get.dbId

      try:
        let body = parseJson(req.body)
        let name = body["name"].getStr

        let tokenVal = generateToken()
        # Read expiration from request, default to 100 years if not provided
        var expires: DateTime
        if body.hasKey("expiresAt") and body["expiresAt"].kind != JNull:
          try:
            # Expecting ISO format or similar
            expires = parse(body["expiresAt"].getStr, "yyyy-MM-dd'T'HH:mm:ss",
                utc())
          except:
            # If parsing fails, use default
            expires = now().utc + years(100)
        else:
          expires = now().utc + years(100)

        var t = Token(token: tokenVal, userId: userId, name: name,
            expiresAt: expires)
        discard insertRowToken(db, t)

        headers["Content-Type"] = "application/json"
        await req.respond(Http200, $(%*{"success": true, "token": tokenVal}), headers)
      except:
        await req.respond(Http500, $(%*{"error": "Failed to create token"}), headers)

    elif matchPath(path, "^/api/tokens/(\\d+)$", captures) and req.reqMethod == HttpDelete:
      try:
        let tokenId = parseInt(captures[0])
        let ownerId = getTokenOwnerId(db, tokenId)
        if ownerId == 0:
          headers["Content-Type"] = "application/json"
          await req.respond(Http404, $(%*{"error": "Token not found"}), headers)
          return

        let userOpt = getUserByUsername(db, username)
        if userOpt.isNone or userOpt.get.dbId != ownerId:
          headers["Content-Type"] = "application/json"
          await req.respond(Http403, $(%*{"error": "Forbidden"}), headers)
          return

        deleteToken(db, tokenId)
        headers["Content-Type"] = "application/json"
        await req.respond(Http200, $(%*{"success": true}), headers)
      except:
        await req.respond(Http400, $(%*{"error": "Invalid token ID"}), headers)

    # === USERS API ===
    elif path == "/api/users" and req.reqMethod == HttpGet:
      let users = getAllUsers(db)
      var list = newJArray()
      for row in users:
        let u = row.data
        list.add(%*{"id": row.dbId, "username": u.username})

      headers["Content-Type"] = "application/json"
      await req.respond(Http200, $list, headers)

    elif path == "/api/users" and req.reqMethod == HttpPost:
      try:
        let body = parseJson(req.body)
        let newUsername = body["username"].getStr
        let newPassword = body["password"].getStr

        # Check exists
        if getUserByUsername(db, newUsername).isSome:
          await req.respond(Http400, $(%*{"error": "Username already exists"}), headers)
          return

        let salt = genSalt(12)
        let hash = hash(newPassword, salt)

        var u = User(username: newUsername, passwordHash: hash)
        discard insertRowUser(db, u)

        await req.respond(Http200, $(%*{"success": true}), headers)
      except:
        await req.respond(Http500, $(%*{"error": "Failed to create user"}), headers)

    elif matchPath(path, "^/api/users/(\\d+)/password$", captures) and
        req.reqMethod == HttpPut:
      try:
        let userId = parseInt(captures[0])
        let body = parseJson(req.body)
        let newPassword = body["password"].getStr

        let salt = genSalt(12)
        let hash = hash(newPassword, salt)

        updateUserPassword(db, userId, hash)
        headers["Content-Type"] = "application/json"
        await req.respond(Http200, $(%*{"success": true}), headers)
      except:
        await req.respond(Http500, $(%*{"error": "Failed to update password"}), headers)

    elif matchPath(path, "^/api/users/(\\d+)$", captures) and req.reqMethod == HttpDelete:
      try:
        let userId = parseInt(captures[0])
        let userOpt = getUserByUsername(db, username)
        if userOpt.isSome and userOpt.get.dbId == userId:
          headers["Content-Type"] = "application/json"
          await req.respond(Http400, $(%*{"error": "Cannot delete yourself"}), headers)
          return

        deleteUser(db, userId)
        headers["Content-Type"] = "application/json"
        await req.respond(Http200, $(%*{"success": true}), headers)
      except:
        await req.respond(Http500, $(%*{"error": "Failed to delete user"}), headers)


    # === WEB PAGES ===

    elif path == "/" or path == "/dashboard":
      headers["Content-Type"] = "text/html"
      await req.respond(Http200, renderDashboard(), headers)

    elif path == "/login":
      headers["Content-Type"] = "text/html"
      if username != "":
        discard
      await req.respond(Http200, loginHtml, headers)

    elif path == "/logout":
      # Clear session
      let cookie = req.headers.getOrDefault("Cookie")
      for c in cookie.split(";"):
        let pair = c.strip().split("=", 1)
        if pair.len == 2 and pair[0] == "auth_token":
          let sid = pair[1]
          if sessions.hasKey(sid):
            sessions.del(sid)

      headers["Set-Cookie"] = "auth_token=; Path=/; Max-Age=0"
      headers["Location"] = "/login"
      await req.respond(Http303, "", headers)

    elif path == "/tasks" and req.reqMethod == HttpGet:
      headers["Content-Type"] = "text/html"
      await req.respond(Http200, renderTasks(), headers)

    elif path == "/tasks/new":
      headers["Content-Type"] = "text/html"
      await req.respond(Http200, renderTaskEdit("new"), headers)

    elif matchPath(path, "^/tasks/(\\d+)/edit$", captures):
      let taskId = captures[0]
      headers["Content-Type"] = "text/html"
      await req.respond(Http200, renderTaskEdit(taskId), headers)

    elif matchPath(path, "^/tasks/(\\d+)$", captures):
      let taskId = parseInt(captures[0])
      let taskOpt = getTaskById(db, taskId)
      let taskName = if taskOpt.isSome: taskOpt.get.data.name else: "Unknown"
      headers["Content-Type"] = "text/html"
      await req.respond(Http200, renderTaskDetail(taskId, taskName), headers)

    elif matchPath(path, "^/jobs/(\\d+)/history$", captures):
      let jobId = parseInt(captures[0])
      let jobOpt = getJobById(db, jobId)
      let jobName = if jobOpt.isSome: jobOpt.get.data.name else: "Unknown"
      headers["Content-Type"] = "text/html"
      await req.respond(Http200, renderJobHistory(jobId, jobName), headers)

    elif matchPath(path, "^/executions/(\\d+)/log$", captures):
      let execId = parseInt(captures[0])
      headers["Content-Type"] = "text/html"
      await req.respond(Http200, renderLogViewer(execId), headers)

    elif path == "/users":
      headers["Content-Type"] = "text/html"
      await req.respond(Http200, renderUsers(), headers)

    elif path == "/executions":
      headers["Content-Type"] = "text/html"
      await req.respond(Http200, renderExecutions(), headers)

    elif path == "/tokens":
      headers["Content-Type"] = "text/html"
      await req.respond(Http200, renderTokens(), headers)

    else:
      await req.respond(Http404, "Not found", headers)

  except Exception as e:
    info "Error: ", e.msg
    await req.respond(Http500, "Error: " & e.msg, headers)

proc mainAsync(configFilename: string) {.async.} =
  sessions = initTable[string, string]()

  if not fileExists(configFilename):
    raise newException(IOError, "config.yaml not found")

  let cfg = loadConfig(configFilename)

  # Apply Config
  dbPath = cfg.database.path
  tasksDir = cfg.tasksDir.absolutePath

  # Initialize Scheduler and Recovery (Background)
  let schedulerDb = openDatabase(dbPath)
  let logsDir = getCurrentDir() / "logs"

  # Initialize DB Tables (Use Macro Generated Procs)
  createTableUser(schedulerDb)
  createTableTask(schedulerDb)
  
  # Migration: Check if TaskTable has groupName column
  try:
      let columns = schedulerDb.getAllRows(sql"PRAGMA table_info(TaskTable)")
      var hasGroupName = false
      for col in columns:
        if col[1] == "groupName":
           hasGroupName = true
           break
           
      if not hasGroupName:
         info "Migrating TaskTable: adding groupName column"
         schedulerDb.exec(sql"ALTER TABLE TaskTable ADD COLUMN groupName TEXT DEFAULT ''")
  except:
      error "Migration failed: ", getCurrentExceptionMsg()
  createTableJob(schedulerDb)
  createTableExecution(schedulerDb)
  createTableToken(schedulerDb)

  # Sync Tasks from File System
  syncTasks(schedulerDb, tasksDir)
  
  # Start Background File Watcher
  asyncCheck (proc() {.async.} =
    while true:
      await sleepAsync(5000) # Check every 5 seconds
      try:
        checkForChanges(schedulerDb, tasksDir)
      except:
        error "Error in file watcher: " & getCurrentExceptionMsg()
  )()

  # Seed Initial User from Config
  dummyHash = hash("dummy", genSalt(12)) # Initialize timing mitigation hash

  let authUser = cfg.auth.username
  let authPass = cfg.auth.password

  if authUser.len > 0 and authPass.len > 0:
    let existing = getUserByUsername(schedulerDb, authUser)
    if existing.isNone:
      let salt = genSalt(12)
      let hash = hash(authPass, salt)
      var u = User(username: authUser, passwordHash: hash)
      try:
        discard insertRowUser(schedulerDb, u)
        info "=================================================================="
        info "Created initial user '", authUser, "' from config."
        info "=================================================================="
      except:
        info "Error creating initial user."
    else:
      info "User '", authUser, "' already exists."
  else:
    info "No auth credentials in config.yaml, skipping user seeding."

  # Executors
  let localExec = newLocalExecutor(schedulerDb, logsDir)
  let remoteExec = newRemoteExecutor(schedulerDb, logsDir)

  # Monitoring & Recovery
  var monitor = newProcessMonitor(schedulerDb, localExec, remoteExec)
  let recovery = newRecoveryManager(schedulerDb, monitor)

  # 1. Recover state (Sync)
  recovery.recoverRunningJobs()

  # 3. Start Scheduler
  scheduler = newSchedulerEngine(schedulerDb, localExec, remoteExec, cfg)
  scheduler.start()

  # Set up monitor callback for sequential job triggering
  proc triggerCallback(jobId: int) {.async, gcsafe.} =
    discard await scheduler.triggerJob(jobId)
  monitor.triggerNextJob = triggerCallback

  # 2. Start Monitor (Async) - after scheduler is set up
  monitor.startMonitoring(cfg.smtp, cfg.server.externalHost & ":" &
      $cfg.server.port)

  let server = newAsyncHttpServer()
  info "Job Scheduler starting on http://" & cfg.server.host & ":" &
      $cfg.server.port
  await server.serve(Port(cfg.server.port), handleRequest, cfg.server.host)

proc main(configFilename: string = "./config.yaml") =
  waitFor mainAsync(configFilename)

when isMainModule:
  import cligen
  dispatch main
