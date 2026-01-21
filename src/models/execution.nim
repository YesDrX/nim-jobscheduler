import times, options, strutils, json
import ../db/db_macros
import db_connector/db_sqlite
import types
import ../db/serialize

# Define Execution object using new dbTable macro style
type
  Execution* {.dbTable.} = object
    jobId* : int 
    pid* : int
    
    startTime* : DateTime
    endTime* : DateTime
    
    status* : ExecutionStatus
    exitCode* : int
    logFile* : string
    
    canReconnect* : bool
    errorMessage* : string

# DTOs
type
  ExecutionStats* = tuple[total, success, failed, running: int]
  ExecutionSummary* = object
    id*: int
    taskId*: int
    pid*: int
    startTime*: DateTime
    endTime*: DateTime
    status*: ExecutionStatus
    jobName*: string
    taskName*: string

# Helper Procs
proc getExecutionStats*(db: DbConn): ExecutionStats =
  # dbTable names: ExecutionTable
  let total = db.getValue(sql"SELECT COUNT(*) FROM ExecutionTable")
  let success = db.getValue(sql"SELECT COUNT(*) FROM ExecutionTable WHERE status='Success'")
  let failed = db.getValue(sql"SELECT COUNT(*) FROM ExecutionTable WHERE status='Failed'")
  let running = db.getValue(sql"SELECT COUNT(*) FROM ExecutionTable WHERE status='Running'")
  return (parseInt(total), parseInt(success), parseInt(failed), parseInt(running))

proc getTodayExecutionStats*(db: DbConn): ExecutionStats =
  let today = now().utc.format("yyyy-MM-dd")
  # Use single query for performance
  let query = sql"SELECT COUNT(*), SUM(CASE WHEN status='Success' THEN 1 ELSE 0 END), SUM(CASE WHEN status='Failed' THEN 1 ELSE 0 END), SUM(CASE WHEN status='Running' THEN 1 ELSE 0 END) FROM ExecutionTable WHERE startTime >= ?"
  let row = db.getRow(query, today)
  
  let total = if row[0] == "": 0 else: parseInt(row[0])
  # SUM returns NULL if no rows, or empty string in db_connector?
  # Usually "0" if using SUM(CASE...) unless 0 rows.
  # If 0 rows match WHERE, row will be ["0", "", "", ""] ?
  let success = if row[1] == "": 0 else: parseInt(row[1])
  let failed = if row[2] == "": 0 else: parseInt(row[2])
  let running = if row[3] == "": 0 else: parseInt(row[3])
  
  return (total, success, failed, running)

proc getJobHistory*(db: DbConn, jobId: int): seq[tuple[dbId: int, data: Execution]] =
  return queryRowsExecution(db, "jobId=" & $jobId & " ORDER BY startTime DESC LIMIT 50")

proc getRecentExecutionsPaged*(db: DbConn, limit: int, offset: int): seq[ExecutionSummary] =
  # Join query needed. Manual SQL because macro doesn't do joins.
  # Use correct table names: ExecutionTable, JobTable, TaskTable
  # AND correct columns (macro uses fields as columns, so jobId is jobId)
  var query = """
    SELECT e._dbID, e.pid, e.startTime, e.endTime, e.status, j.name, t.name, t._dbID
    FROM ExecutionTable e 
    JOIN JobTable j ON e.jobId=j._dbID 
    JOIN TaskTable t ON j.taskId=t._dbID
    ORDER BY e.startTime DESC"""
  if limit > 0:
    query &= " LIMIT " & $limit & " OFFSET " & $offset
  let rows = db.getAllRows(sql(query))
  var res: seq[ExecutionSummary] = @[]
  for row in rows:
    var s = ExecutionSummary()
    s.id = parseInt(row[0])
    s.pid = parseInt(row[1])
    s.startTime = row[2].deserialize[:DateTime]
    s.endTime = row[3].deserialize[:DateTime]
    s.status = parseEnum[ExecutionStatus](row[4])
    s.jobName = row[5]
    s.taskName = row[6]
    s.taskId = parseInt(row[7])
    res.add(s)
  return res

proc getRecentExecutions*(db: DbConn): seq[ExecutionSummary] =
  getRecentExecutionsPaged(db, 100, 0) # Default 100 for backward compatibility

proc getExecutionsCount*(db: DbConn): int =
  # Use same JOIN logic as getRecentExecutionsPaged to exclude orphaned executions
  let count = db.getValue(sql"""
    SELECT COUNT(*) 
    FROM ExecutionTable e 
    JOIN JobTable j ON e.jobId=j._dbID 
    JOIN TaskTable t ON j.taskId=t._dbID
  """)
  if count == "": return 0
  return parseInt(count)

proc getExecutionLogPath*(db: DbConn, execId: int): string =
  let row = db.getRow(sql"SELECT logFile FROM ExecutionTable WHERE _dbID=?", execId)
  if row[0] != "":
    return row[0]
  return ""

proc getExecutionPid*(db: DbConn, execId: int): int =
  let row = db.getRow(sql"SELECT pid FROM ExecutionTable WHERE _dbID = ?", execId)
  if row[0] == "": return 0
  try:
    return parseInt(row[0])
  except:
    return 0

proc updateExecutionStatus*(db: DbConn, id: int, status: ExecutionStatus, errorMsg: string = "") =
  # Need to fetch object to use updateRowExecution? Or usage of manual SQL acceptable for partial updates?
  # User asked to use macros.
  # But fetching entire object to update 1 field is inefficient.
  # But arguably safer/cleaner.
  # Let's try raw SQL for partial updates if simple, BUT using table names from macro.
  if errorMsg == "":
    db.exec(sql"UPDATE ExecutionTable SET status = ? WHERE _dbID = ?", $status, id)
  else:
    db.exec(sql"UPDATE ExecutionTable SET status = ?, errorMessage = ? WHERE _dbID = ?", $status, errorMsg, id)

proc setExecutionRunning*(db: DbConn, id: int, pid: int, logPath: string) =
  db.exec(sql"UPDATE ExecutionTable SET pid = ?, logFile = ?, status = ? WHERE _dbID = ?",
    pid, logPath, $esRunning, id)

proc getExecutionsRunningOrScheduled*(db: DbConn): seq[tuple[dbId: int, data: Execution]] =
  return queryRowsExecution(db, "status IN ('" & $esRunning & "', '" & $esScheduled & "')")

proc getExecutionById*(db: DbConn, id: int): Option[tuple[dbId: int, data: Execution]] =
  let res = queryRowsExecution(db, "_dbID = " & $id)
  if res.len > 0: return some(res[0])
  return none(tuple[dbId: int, data: Execution])

proc getLastExecutionTime*(db: DbConn, jobId: int): Option[DateTime] =
  let row = db.getRow(sql"SELECT startTime FROM ExecutionTable WHERE jobId = ? ORDER BY _dbID DESC LIMIT 1", jobId)
  if row[0] == "":
    return none(DateTime)
  
  try:
    return some(row[0].deserialize[:DateTime])
  except:
    return none(DateTime)

proc getLastTaskExecutionTime*(db: DbConn, taskId: int): Option[DateTime] =
  # _dbID for Order by
  let row = db.getRow(sql"""
    SELECT e.startTime 
    FROM ExecutionTable e 
    JOIN JobTable j ON e.jobId = j._dbID
    WHERE j.taskId = ? AND j.orderIdx = 1
    ORDER BY e.startTime DESC LIMIT 1
  """, taskId)
  
  if row[0] == "":
    return none(DateTime)
  
  try:
    return some(row[0].deserialize[:DateTime])
  except:
    return none(DateTime)

proc createExecution*(db: DbConn, jobId: int, status: ExecutionStatus): int =
  var e = Execution()
  e.jobId = jobId
  e.status = status
  e.startTime = now().utc
  e.endTime = now().utc
  e.pid = 0
  e.exitCode = 0
  e.canReconnect = false
  # Macro's create proc returns nothing (void)? No, macro implementation `insertID` returns ID.
  # But `insertRowX` macro does NOT return ID. It returns `void`.
  # `create` proc in macro wrapper: calls `insertID`.
  # The reference implementation `insertRowProc` logic: `db.exec`. No ID return. Wait.
  # User reference: `insertRowProc` calls `db.exec`.
  # BUT I modified it in `db_macros.nim`?
  # Let's check `db_macros.nim`. I copied the user's `db_macros.nim`.
  # User's `insertRowProc` uses `db.exec(sql(insertRowStr))`. No return ID.
  # This is a problem if I need the ID.
  # `db_connector` `insertID` returns ID.
  # I should update `db_macros.nim` to use `tryInsertID` inside `insertRowProc` and return it?
  # User's implementation: `proc insertRowAbc(db : DbConn, obj : Abc)` -> `void`.
  # This implies User doesn't need ID immediately or retrieves it?
  
  # I MUST have the ID for `createExecution`.
  # I will update `db_macros.nim` to return int64/int from `insertRow`.  
  
  # Hold on, I'll assume for this file that `insertRowExecution` returns int. I will fix `db_macros.nim` in next step.
  return insertRowExecution(db, e)

proc getRunningExecutions*(db: DbConn): seq[tuple[dbId: int, data: Execution]] =
  return queryRowsExecution(db, "status = '" & $esRunning & "'")

