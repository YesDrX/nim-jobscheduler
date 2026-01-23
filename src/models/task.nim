import times, options, strutils, json
import ../db/db_macros
import db_connector/db_sqlite
import types
import ../db/serialize # for serialization logic

# Define Task object using new dbTable macro style
type
  Task* {.dbTable.} = object
    name* : string
    description* : string
    taskType* : TaskType
    configHash* : string # For file synchronization
    
    # SSH specific
    sshHost* : string
    sshPort* : int
    sshUser* : string
    sshKeyPath* : string

    # Scheduling
    scheduleType* : ScheduleType
    timezone* : string 
    cronExpr* : string
    timeOfDay* : string 
    intervalStart* : string
    intervalEnd* : string
    intervalMinutes* : int
    dateList* : seq[string]
    calendarPath* : string
    
    parallel* : bool = false

    sourceFile* : string
    enabled* : bool = true
    createdAt* : DateTime
    updatedAt* : DateTime
    groupName* : string

proc getTaskById*(db: DbConn, id: int): Option[tuple[dbId: int, data: Task]] =
  let res = queryRowsTask(db, "_dbID = " & $id)
  if res.len > 0:
    return some(res[0])
  else:
    return none(tuple[dbId: int, data: Task])

proc getAllTasksOrdered*(db: DbConn): seq[tuple[dbId: int, data: Task]] =
  return queryRowsTask(db, "1=1 ORDER BY name")

proc getTasksCount*(db: DbConn): int =
  let count = db.getValue(sql"SELECT COUNT(*) FROM TaskTable")
  if count == "": return 0
  return parseInt(count)

proc toggleTask*(db: DbConn, taskId: int, enabled: bool) =
  var taskOpt = getTaskById(db, taskId)
  if taskOpt.isSome:
    var (dbId, t) = taskOpt.get
    t.enabled = enabled
    updateRowTask(db, dbId, t)

