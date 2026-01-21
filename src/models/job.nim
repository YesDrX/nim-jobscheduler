import times, options, strutils, os
import ../db/db_macros
import db_connector/db_sqlite
import ../db/serialize
import execution

type
  Job* {.dbTable.} = object
    taskId* : int # Foreign key to Task
    name* : string
    command* : string 
    orderIdx* : int
    enabled* : bool

proc getJobsByTaskIdOrdered*(db: DbConn, taskId: int): seq[tuple[dbId: int, data: Job]] =
  return queryRowsJob(db, "taskId = " & $taskId & " ORDER BY orderIdx ASC")

proc deleteJobById*(db: DbConn, jobId: int) =
  let jobList = queryRowsJob(db, "_dbID = " & $jobId)
  if jobList.len > 0:
    let job = jobList[0].data
    
    # Delete all executions for this job first
    let executions = queryRowsExecution(db, "jobId = " & $jobId)
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
    deleteRowJob(db, jobId)
    
    # Reorder remaining jobs
    let remaining = queryRowsJob(db, "taskId = " & $job.taskId & " AND orderIdx > " & $job.orderIdx)
    for row in remaining:
      var j = row.data
      j.orderIdx = j.orderIdx - 1
      updateRowJob(db, row.dbId, j)

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