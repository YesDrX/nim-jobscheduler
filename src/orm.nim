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
    try:
      let msg = dbWorker.ch[].recv()
      debug "Received db message: " & $msg.kind

      case msg.kind:
      of dbStop:
        warn "Stopping db worker"
        dbWorker.db.close()
        break

      # User
      of dbInsertUser:
        if getUserByUsername(db, msg.user.username).isSome():
          warn "User " & msg.user.username & " already exists"
          if msg.userResultCh != nil:
            msg.userResultCh[].send(-1)
          continue

        info "Adding user=" & msg.user.username & " email=" & msg.user.email
        let userId = db.insertRowUser(msg.user)
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

      # Task
      of dbInsertTask:
        var taskCopy = msg.task

        let taskId = db.insertRowTask(msg.task)
        if msg.taskResultCh != nil:
          msg.taskResultCh[].send(taskId)
        for idx, job in msg.task.jobs.pairs:
          discard db.insertRowJob(Job(
              taskId: taskId,
              name: job.name,
              command: job.command,
              orderIdx: idx
          ))

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

      of dbDeleteTask:
        db.deleteRowTask(msg.deleteTaskId)
        db.exec(sql"DELETE FROM JobTable WHERE taskId = ?", msg.deleteTaskId)
        if fileExists(msg.deleteTaskSourceFile):
          removeFile(msg.deleteTaskSourceFile)

      of dbToggleTask:
        db.exec(sql"UPDATE TaskTable SET enabled = ? WHERE _dbID = ?",
          if msg.toggleEnabled: 1 else: 0, msg.toggleTaskId)

      # Execution
      of dbInsertExecution:
        let executionId = db.insertRowExecution(msg.execution)
        if msg.executionResultCh != nil:
          msg.executionResultCh[].send(executionId)

      of dbDeleteExecution:
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

    except Exception as e:
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
