import strutils, options, logging, tables, os, times
import db_connector/db_sqlite
import ../models/task
import ../models/job
import ../models/execution
import ./task_file_service

# Cache for file modification times (Path -> Time)
var mtimeCache {.threadvar.}: Table[string, Time]

# Helper to sync a single file (used by periodic check)
proc syncSingleFile(db: DbConn, path: string, tasksDir: string) =
  let taskFileOpt = loadTaskFromYaml(path)
  if taskFileOpt.isNone: return

  let taskFile = taskFileOpt.get
  var t = taskFile.task
  let jobs = taskFile.jobs

  # Calculate Group Name based on folder structure
  let relPath = relativePath(path, tasksDir)
  let parentDir = relPath.parentDir
  if parentDir != "." and parentDir != "":
      t.groupName = parentDir
  else:
      t.groupName = ""

  # Calculate hash
  let content = readFile(path)
  let newHash = calculateConfigHash(content)

  # Check DB for task by Name
  let existingRows = queryRowsTask(db, "name = '" & t.name.replace("'", "''") & "'")
  
  if existingRows.len > 0:
    let (dbId, existingTask) = existingRows[0]
    
    if existingTask.configHash != newHash or existingTask.groupName != t.groupName:
      info "Updating changed task file: " & t.name
      var toUpdate = t
      toUpdate.configHash = newHash
      # Preserve timestamps/ID
      toUpdate.createdAt = existingTask.createdAt
      toUpdate.updatedAt = now().utc

      updateRowTask(db, dbId, toUpdate)

      # Sync Jobs: delete all and recreate
      let dbJobs = getJobsByTaskIdOrdered(db, dbId)
      var dbJobMap = initTable[string, tuple[dbId: int, data: Job]]()
      for j in dbJobs: dbJobMap[j.data.name] = j
      
      var processedJobs: seq[string] = @[]
      
      for j in jobs:
          var jobData = j
          jobData.taskId = dbId
          processedJobs.add(jobData.name)
          
          if dbJobMap.hasKey(jobData.name):
             let (jId, _) = dbJobMap[jobData.name]
             updateRowJob(db, jId, jobData)
          else:
             discard insertRowJob(db, jobData)
             
      for jName, jVal in dbJobMap:
          if jName notin processedJobs:
             let execs = queryRowsExecution(db, "jobId = " & $jVal.dbId)
             for e in execs: deleteRowExecution(db, e.dbId)
             deleteRowJob(db, jVal.dbId)
  else:
     # New Task
     info "Creating new task from file: " & t.name
     var newTask = t
     newTask.configHash = newHash
     let newId = insertRowTask(db, newTask)
     for j in jobs:
        var newJob = j
        newJob.taskId = newId
        discard insertRowJob(db, newJob)

proc deleteBySourceFile(db: DbConn, path: string) =
  let rows = queryRowsTask(db, "sourceFile = '" & path.replace("'", "''") & "'")
  if rows.len > 0:
      let (taskId, t) = rows[0]
      info "Deleting task for removed file: " & t.name
      
      # Cascade Delete
      let taskJobs = getJobsByTaskIdOrdered(db, taskId)
      for j in taskJobs:
         let execs = queryRowsExecution(db, "jobId = " & $j.dbId)
         for e in execs: deleteRowExecution(db, e.dbId)
         deleteRowJob(db, j.dbId)
         
      deleteRowTask(db, taskId)

proc syncTasks*(db: DbConn, tasksDir: string) =
  if mtimeCache.len == 0:
      mtimeCache = initTable[string, Time]()

  info "Starting full task synchronization from " & tasksDir
  
  if not dirExists(tasksDir):
    createDir(tasksDir)
    return

  # 1. Get all existing tasks from DB
  var dbTasks = initTable[string, tuple[dbId: int, data: Task]]()
  let allTasks = getAllTasksOrdered(db)
  for t in allTasks:
    dbTasks[t.data.name] = t
    
  # 2. Iterate over YAML files
  let tools = getAllTaskFiles(tasksDir)
  var processedNames: seq[string] = @[]
  
  for file in tools:
    # Update Cache
    try:
       mtimeCache[file] = getLastModificationTime(file)
    except:
       discard

    let taskFileOpt = loadTaskFromYaml(file)
    if taskFileOpt.isNone:
      continue
      
    let taskFile = taskFileOpt.get
    var t = taskFile.task
    let jobs = taskFile.jobs
    
    # Calculate Group Name based on folder structure
    let relPath = relativePath(file, tasksDir)
    let parentDir = relPath.parentDir
    if parentDir != "." and parentDir != "":
        t.groupName = parentDir
    else:
        t.groupName = ""
    
    let content = readFile(file)
    let newHash = calculateConfigHash(content)
    
    processedNames.add(t.name)
    
    if dbTasks.hasKey(t.name):
      let (dbId, existingTask) = dbTasks[t.name]
      
      if existingTask.configHash != newHash or existingTask.groupName != t.groupName:
        info "Updating task: " & t.name
        var toUpdate = t
        toUpdate.configHash = newHash
        toUpdate.updatedAt = now().utc # Refresh update time
        updateRowTask(db, dbId, toUpdate)
        
        # Sync Jobs
        let dbJobs = getJobsByTaskIdOrdered(db, dbId)
        var dbJobMap = initTable[string, tuple[dbId: int, data: Job]]()
        for j in dbJobs: dbJobMap[j.data.name] = j
        
        var processedJobs: seq[string] = @[]
        
        for j in jobs:
          var jobData = j
          jobData.taskId = dbId
          processedJobs.add(jobData.name)
          if dbJobMap.hasKey(jobData.name):
             let (jId, _) = dbJobMap[jobData.name]
             updateRowJob(db, jId, jobData)
          else:
             discard insertRowJob(db, jobData)
        
        for jName, jVal in dbJobMap:
          if jName notin processedJobs:
             let execs = queryRowsExecution(db, "jobId = " & $jVal.dbId)
             for e in execs: deleteRowExecution(db, e.dbId)
             deleteRowJob(db, jVal.dbId)
      else:
        debug "Task up to date: " & t.name
    else:
      info "Creating new task: " & t.name
      var newTask = t
      newTask.configHash = newHash
      let newId = insertRowTask(db, newTask)
      for j in jobs:
        var newJob = j
        newJob.taskId = newId
        discard insertRowJob(db, newJob)
        
  # 3. Delete tasks not in YAML
  for name, val in dbTasks:
    if name notin processedNames:
      info "Deleting orphaned task: " & name
      let taskId = val.dbId
      let taskJobs = getJobsByTaskIdOrdered(db, taskId)
      for j in taskJobs:
         let execs = queryRowsExecution(db, "jobId = " & $j.dbId)
         for e in execs: deleteRowExecution(db, e.dbId)
         deleteRowJob(db, j.dbId)
      deleteRowTask(db, taskId)

proc checkForChanges*(db: DbConn, tasksDir: string) =
  if mtimeCache.len == 0:
      # If cache empty, maybe run full sync? Or just initialize.
      mtimeCache = initTable[string, Time]()
  
  if not dirExists(tasksDir): return
  
  var currentFiles = initTable[string, bool]()
  
  for path in walkDirRec(tasksDir):
    if path.endsWith(".yaml") and fileExists(path):
       currentFiles[path] = true
       
       let mtime = getLastModificationTime(path)
       
       var changed = false
       if not mtimeCache.hasKey(path):
          changed = true
       elif mtime > mtimeCache[path]:
          changed = true
          
       if changed:
          # Sync this file
          try:
             syncSingleFile(db, path, tasksDir)
             mtimeCache[path] = mtime
          except:
             error "Failed to sync changed file: " & path
             
  # Check for deletions
  var deletedFiles: seq[string] = @[]
  for path in mtimeCache.keys:
      if not currentFiles.hasKey(path):
         deletedFiles.add(path)
         
  for path in deletedFiles:
      try:
         deleteBySourceFile(db, path)
         mtimeCache.del(path)
      except:
         error "Failed to delete task for missing file: " & path
