import std/[options, tables, strutils, os, times]
import db_connector/db_sqlite
import ./[database, types]

# orm
proc getReaderDb*(dbPath: string): DbConn =
  return open(dbPath, "", "", "")

proc getUserByUsername*(db: DbConn, username: string): Option[tuple[dbId: int, data: User]] =
  let users = queryRowsUser(db, "username = '" & username.serialize() & "'")
  if users.len > 0:
    return some(users[0])
  return none(tuple[dbId: int, data: User])

proc getUserById*(db: DbConn, id: int): Option[tuple[dbId: int, data: User]] =
  let users = queryRowsUser(db, "_dbID = " & $id)
  if users.len > 0:
    return some(users[0])
  return none(tuple[dbId: int, data: User])

proc getAllUsers*(db: DbConn): seq[tuple[dbId: int, data: User]] =
  return queryRowsUser(db, "1=1 ORDER BY username")

proc getTaskById*(db: DbConn, id: int): Option[tuple[dbId: int, data: Task]] =
  let res = queryRowsTask(db, "_dbID = " & $id)
  if res.len > 0:
    return some(res[0])
  else:
    return none(tuple[dbId: int, data: Task])

proc getTaskBySourceFile*(db: DbConn, sourceFile: string): Option[tuple[
    dbId: int, data: Task]] =
  let res = queryRowsTask(db, "sourceFile = '" & sourceFile.serialize() & "'")
  if res.len > 0:
    return some(res[0])
  else:
    return none(tuple[dbId: int, data: Task])

proc getAllTasksOrdered*(db: DbConn): seq[tuple[dbId: int, data: Task]] =
  return queryRowsTask(db, "1=1 ORDER BY name")

proc getAllTasksTable*(db: DbConn): Table[int, Task] =
  let tasks = getAllTasksOrdered(db)
  for (taskId, task) in tasks:
    result[taskId] = task

proc getTasksCount*(db: DbConn): int =
  let count = db.getValue(sql"SELECT COUNT(*) FROM TaskTable")
  if count == "": return 0
  return parseInt(count)

proc getEnabledTasksCount*(db: DbConn): int =
  let count = db.getValue(sql"SELECT COUNT(*) FROM TaskTable WHERE enabled = ?", serialize(true))
  if count == "": return 0
  return parseInt(count)

proc getExecutionStatusCounts*(db: DbConn): Table[string, int] =
  result = initTable[string, int]()
  for row in db.getAllRows(sql"SELECT status, COUNT(*) FROM ExecutionTable GROUP BY status"):
    if row.len >= 2:
      try:
        let st = $deserialize[ExecutionStatus](row[0])
        result[st] = parseInt(row[1])
      except:
        discard

proc getValueSeq(db: DbConn, q: SqlQuery, args: seq[string]): string =
  case args.len
  of 0: db.getValue(q)
  of 1: db.getValue(q, args[0])
  of 2: db.getValue(q, args[0], args[1])
  of 3: db.getValue(q, args[0], args[1], args[2])
  of 4: db.getValue(q, args[0], args[1], args[2], args[3])
  of 5: db.getValue(q, args[0], args[1], args[2], args[3], args[4])
  of 6: db.getValue(q, args[0], args[1], args[2], args[3], args[4], args[5])
  of 7: db.getValue(q, args[0], args[1], args[2], args[3], args[4], args[5], args[6])
  of 8: db.getValue(q, args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7])
  of 9: db.getValue(q, args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8])
  else: db.getValue(q, args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9])

proc getAllRowsSeq(db: DbConn, q: SqlQuery, args: seq[string]): seq[Row] =
  case args.len
  of 0: db.getAllRows(q)
  of 1: db.getAllRows(q, args[0])
  of 2: db.getAllRows(q, args[0], args[1])
  of 3: db.getAllRows(q, args[0], args[1], args[2])
  of 4: db.getAllRows(q, args[0], args[1], args[2], args[3])
  of 5: db.getAllRows(q, args[0], args[1], args[2], args[3], args[4])
  of 6: db.getAllRows(q, args[0], args[1], args[2], args[3], args[4], args[5])
  of 7: db.getAllRows(q, args[0], args[1], args[2], args[3], args[4], args[5], args[6])
  of 8: db.getAllRows(q, args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7])
  of 9: db.getAllRows(q, args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8])
  else: db.getAllRows(q, args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9])

proc queryExecutionsPage*(db: DbConn, search: string = "", status: string = "",
    taskId: int = -1, jobId: int = -1, matchTaskName: string = "",
    matchJobName: string = "", page: int = 1, limit: int = 50): tuple[
    rows: seq[tuple[dbId: int, data: Execution]], total: int] =
  var conds: seq[string] = @[]
  var args: seq[string] = @[]

  if search.len > 0:
    conds.add("(LOWER(taskName) LIKE ? OR LOWER(jobName) LIKE ?)")
    let pattern = "%" & search.toLowerAscii() & "%"
    args.add(pattern)
    args.add(pattern)

  if status.len > 0:
    try:
      let st = parseEnum[ExecutionStatus](status)
      conds.add("status = ?")
      args.add(st.serialize())
    except ValueError:
      return (@[], 0)

  if taskId > 0:
    conds.add("taskId = ?")
    args.add($taskId)

  if jobId > 0:
    if matchTaskName.len > 0 and matchJobName.len > 0:
      conds.add("(jobId = ? OR (taskName = ? AND jobName = ?))")
      args.add($jobId)
      args.add(matchTaskName.serialize())
      args.add(matchJobName.serialize())
    else:
      conds.add("jobId = ?")
      args.add($jobId)

  let whereSql = if conds.len > 0: " WHERE " & conds.join(" AND ") else: ""
  let countStr = db.getValueSeq(sql("SELECT COUNT(*) FROM ExecutionTable" & whereSql), args)
  result.total = if countStr.len > 0: parseInt(countStr) else: 0

  let safePage = max(1, page)
  let safeLimit = max(1, min(limit, 500))
  let offset = (safePage - 1) * safeLimit
  let runningSer = esRunning.serialize()
  let selectSql = "SELECT * FROM ExecutionTable" & whereSql &
      " ORDER BY CASE WHEN status = ? THEN 0 ELSE 1 END ASC, startTime DESC LIMIT ? OFFSET ?"
  var selectArgs = args
  selectArgs.add(runningSer)
  selectArgs.add($safeLimit)
  selectArgs.add($offset)

  let rawRows = db.getAllRowsSeq(sql(selectSql), selectArgs)
  for row in rawRows:
    if row.len > 0:
      let dbId = row[0].parseInt
      let objData = row.dbRowToObj[:Execution]()
      result.rows.add((dbId, objData))

proc getJobHistory*(db: DbConn, jobId: int): seq[tuple[dbId: int,
    data: Execution]] =
  return queryRowsExecution(db, "jobId=" & $jobId & " ORDER BY startTime DESC LIMIT 50")

proc getJobsByTaskIdOrdered*(db: DbConn, taskId: int): seq[tuple[dbId: int, data: Job]] =
  return queryRowsJob(db, "taskId = " & $taskId & " ORDER BY orderIdx ASC")

proc getNextJobOrder*(db: DbConn, taskId: int): int =
  let maxOrderStr = db.getValue(sql"SELECT COALESCE(MAX(orderIdx), 0) FROM JobTable WHERE taskId = ?", taskId)
  if maxOrderStr == "": return 1
  return parseInt(maxOrderStr) + 1

proc getJobWithDetails*(db: DbConn, jobId: int): Option[tuple[dbId: int, data: Job]] =
  let res = queryRowsJob(db, "_dbID = " & $jobId)
  if res.len > 0: return some(res[0])
  else: return none(tuple[dbId: int, data: Job])

proc getJobById*(db: DbConn, jobId: int): Option[tuple[dbId: int, data: Job]] =
  getJobWithDetails(db, jobId)

proc getTokensByUserId*(db: DbConn, userId: int): seq[tuple[dbId: int,
    data: Token]] =
  return queryRowsToken(db, "userId = " & $userId)

proc getTokenOwnerId*(db: DbConn, tokenId: int): int =
  let res = queryRowsToken(db, "_dbID = " & $tokenId)
  if res.len > 0:
    return res[0].data.userId
  return 0

proc setUpInitialUser*(db: DbConn, username: string,
    encryptedPassword: string) =
  let existingUser = getUserByUsername(db, username)
  if existingUser.isSome():
    info "User " & username & " already exists"
  else:
    let user = User(username: username, passwordHash: encryptedPassword)
    info "Creating initial user " & username
    discard db.insertRowUser(user)

proc initDb*(db: DbConn) =
  db.exec(sql"PRAGMA journal_mode=WAL;")
  db.exec(sql"PRAGMA synchronous=NORMAL;")
  db.createTableUser()
  db.createTableTask()
  db.createTableJob()
  db.createTableExecution()
  db.createTableToken()

proc runDbWorker*(dbWorker: DbWorker) =
  info "Starting db worker"
  let db = dbWorker.db

  while getIsRunning():
    var currentAckCh: ptr Channel[int] = nil
    try:
      let msg = dbWorker.ch[].recv()
      currentAckCh = msg.ackCh
      var ackVal = 1
      debug "Received db message: " & $msg.kind

      case msg.kind:
      of dbStop:
        warn "Stopping db worker"
        if currentAckCh != nil:
          currentAckCh[].send(1)
        dbWorker.db.close()
        break

      # User
      of dbInsertUser:
        if getUserByUsername(db, msg.user.username).isSome():
          warn "User " & msg.user.username & " already exists"
          if msg.userResultCh != nil:
            msg.userResultCh[].send(-1)
          if currentAckCh != nil:
            currentAckCh[].send(-1)
          continue

        info "Adding user=" & msg.user.username & " email=" & msg.user.email
        let userId = db.insertRowUser(msg.user)
        ackVal = userId
        if msg.userResultCh != nil:
          msg.userResultCh[].send(userId)

      of dbUpdateUserPassword:
        db.exec(sql"UPDATE UserTable SET passwordHash = ? WHERE _dbID = ?",
            msg.newPasswordHash.serialize(), msg.updateUserId)
        if msg.newPasswordEmail != "":
          info "Updating user " & $msg.updateUserId & " email: " &
              msg.newPasswordEmail
          db.exec(sql"UPDATE UserTable SET email = ? WHERE _dbID = ?",
              msg.newPasswordEmail.serialize(), msg.updateUserId)
        db.exec(sql"UPDATE UserTable SET updatedAt = ? WHERE _dbID = ?",
            now().serialize(), msg.updateUserId)

      of dbDeleteUser:
        db.deleteRowUser(msg.deleteUserId)
        db.exec(sql"DELETE FROM TokenTable WHERE userId = ?", msg.deleteUserId)

      # Task
      of dbInsertTask:
        let taskId = db.insertRowTask(msg.task)
        ackVal = taskId
        for idx, job in msg.task.jobs.pairs:
          discard db.insertRowJob(Job(
              taskId: taskId,
              name: job.name,
              command: job.command,
              orderIdx: idx
          ))
        if msg.taskResultCh != nil:
          msg.taskResultCh[].send(taskId)

      of dbUpdateTask:
        db.updateRowTask(msg.updateTaskId, msg.updatedTask)
        db.exec(sql"DELETE FROM JobTable WHERE taskId = ?", msg.updateTaskId)
        for idx, job in msg.updatedTask.jobs.pairs:
          discard db.insertRowJob(Job(
              taskId: msg.updateTaskId,
              name: job.name,
              command: job.command,
              orderIdx: idx
          ))
        ackVal = msg.updateTaskId

      of dbDeleteTask:
        db.deleteRowTask(msg.deleteTaskId)
        db.exec(sql"DELETE FROM JobTable WHERE taskId = ?", msg.deleteTaskId)
        if msg.deleteTaskSourceFile.len > 0 and fileExists(msg.deleteTaskSourceFile):
          removeFile(msg.deleteTaskSourceFile)

      of dbToggleTask:
        if msg.toggleConfigHash.len > 0:
          db.exec(sql"UPDATE TaskTable SET enabled = ?, configHash = ?, updatedAt = ? WHERE _dbID = ?",
            serialize(msg.toggleEnabled), serialize(msg.toggleConfigHash), now().serialize(), msg.toggleTaskId)
        else:
          db.exec(sql"UPDATE TaskTable SET enabled = ?, updatedAt = ? WHERE _dbID = ?",
            serialize(msg.toggleEnabled), now().serialize(), msg.toggleTaskId)

      # Execution
      of dbInsertExecution:
        let executionId = db.insertRowExecution(msg.execution)
        ackVal = executionId
        if msg.executionResultCh != nil:
          msg.executionResultCh[].send(executionId)

      of dbDeleteExecution:
        let execs = queryRowsExecution(db, "_dbID = " & $msg.deleteExecutionId)
        if execs.len > 0 and execs[0].data.logFile.len > 0 and fileExists(execs[0].data.logFile):
          try: removeFile(execs[0].data.logFile)
          except OSError: discard
        db.deleteRowExecution(msg.deleteExecutionId)

      of dbUpdateExecution:
        db.updateRowExecution(msg.updateExecutionId, msg.updatedExecution)

      of dbUpdateExecutionIdentity:
        db.exec(sql"UPDATE ExecutionTable SET pid = ?, processStartTime = ? WHERE _dbID = ?",
            serialize(msg.newPid), serialize(msg.newIdentity),
                msg.identityExecutionId)

      of dbUpdateExecutionStatus:
        db.exec(sql"UPDATE ExecutionTable SET status = ?, endTime = ?, exitCode = ? WHERE _dbID = ?",
            serialize(msg.newStatus), serialize(msg.newEndTime), serialize(
                msg.newExitCode), msg.statusExecutionId)

      # Token
      of dbInsertToken:
        let tokenId = db.insertRowToken(msg.token)
        ackVal = tokenId
        if msg.tokenResultCh != nil:
          msg.tokenResultCh[].send(tokenId)

      of dbDeleteToken:
        db.deleteRowToken(msg.deleteTokenId)

      of dbDeleteUserTokens:
        db.exec(sql"DELETE FROM TokenTable WHERE userId = ?",
            msg.deleteTokensUserId)

      of dbCleanupExecutions:
        debug "Cleaning up executions ..."
        debug "Total executions before dropping old ones: " & $db.getAllRows(sql"SELECT COUNT(_dbID) FROM ExecutionTable")
        let refTime = (now() - dbWorker.cfg.internal.logRetentionDays.days).toTime().toUnix()
        db.exec(sql("DELETE FROM ExecutionTable WHERE _dbTimestamp < " &
            $refTime & " AND status != ?"), serialize(esRunning))
        debug "Total executions after dropping ones order than " & $refTime &
            " (" & $dbWorker.cfg.internal.logRetentionDays & " days): " &
                $db.getAllRows(sql"SELECT COUNT(_dbID) FROM ExecutionTable")
        db.exec(sql("""
            DELETE FROM ExecutionTable
            WHERE status != ?
            AND _dbID NOT IN (
                SELECT _dbId FROM (
                    SELECT _dbID, row_number() over (
                      PARTITION by jobId ORDER BY _dbTimestamp DESC
                    ) as row
                    FROM ExecutionTable)
                WHERE row <= """ & $dbWorker.cfg.internal.maxExecutionsByJob &
            ")"), serialize(esRunning))
        debug "Total executions after dropping old ones by Job: " &
            $db.getAllRows(sql"SELECT COUNT(_dbID) FROM ExecutionTable")

      if currentAckCh != nil:
        currentAckCh[].send(ackVal)

    except Exception as e:
      if currentAckCh != nil:
        currentAckCh[].send(-1)
      error "Error in db worker: " & getCurrentExceptionMsg()
      dbWorker.monitorChan[].send(SchedulerMonitorSignal(
        kind: smmAlert,
        messageTitle: "DB Worker Error: " & getCurrentExceptionMsg(),
        message: e.getStackTrace()
      ))

  info "Stopping db worker"

when isMainModule:
  type
    TestObj {.dbTable.} = object
      name: string
      age: int
      weight: float
      isHappy: bool
      createdAt: DateTime
      tags: seq[string]
      metadata: Table[string, string]
