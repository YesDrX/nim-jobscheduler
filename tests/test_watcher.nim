import unittest, os, strutils, times, options, logging, tables
import db_connector/db_sqlite
import models/task
import models/job
import models/execution
import models/types
import services/sync_service
import services/task_file_service
import db/db_macros

const TestDbPath = "test_watcher.db"
const TestTasksDir = "test_watcher_tasks"

suite "File Watcher Tests":
  setup:
    if fileExists(TestDbPath): removeFile(TestDbPath)
    if dirExists(TestTasksDir): removeDir(TestTasksDir)
    createDir(TestTasksDir)
    
    let setupDb = open(TestDbPath, "", "", "")
    createTableTask(setupDb)
    createTableJob(setupDb)
    createTableExecution(setupDb)
    setupDb.close()

  teardown:
    if fileExists(TestDbPath): removeFile(TestDbPath)
    if dirExists(TestTasksDir): removeDir(TestTasksDir)

  test "Watcher detects new file":
    let db = open(TestDbPath, "", "", "")
    defer: db.close()
    
    # 1. Initial Sync (Empty)
    syncTasks(db, TestTasksDir)
    check getAllTasksOrdered(db).len == 0

    # 2. Add File
    let t = Task(name: "WatcherTask", taskType: ttLocal, enabled: true, cronExpr: "* * * * *")
    saveTaskToYaml(t, @[], TestTasksDir)
    
    # Sleep briefly to ensure mtime diff? (Not needed if file didn't exist)
    # 3. Check for Changes
    checkForChanges(db, TestTasksDir)
    
    let tasks = getAllTasksOrdered(db)
    check tasks.len == 1
    check tasks[0].data.name == "WatcherTask"

  test "Watcher detects modification":
    let db = open(TestDbPath, "", "", "")
    defer: db.close()
    
    var t = Task(name: "ModTask", description: "Original", taskType: ttLocal, enabled: true, cronExpr: "* * * * *")
    saveTaskToYaml(t, @[], TestTasksDir)
    
    syncTasks(db, TestTasksDir)
    check getAllTasksOrdered(db)[0].data.description == "Original"
    
    # Modify File
    sleep(1100) # Wait 1.1s for mtime change (FS resolution)
    t.description = "Modified"
    saveTaskToYaml(t, @[], TestTasksDir)
    
    checkForChanges(db, TestTasksDir)
    
    check getAllTasksOrdered(db)[0].data.description == "Modified"

  test "Watcher detects deletion":
    let db = open(TestDbPath, "", "", "")
    defer: db.close()
    
    var t = Task(name: "DelTask", taskType: ttLocal, enabled: true, cronExpr: "* * * * *")
    saveTaskToYaml(t, @[], TestTasksDir)
    
    syncTasks(db, TestTasksDir)
    check getAllTasksOrdered(db).len == 1
    
    # Delete File
    let filename = t.name & ".yaml"
    removeFile(TestTasksDir / filename)
    
    checkForChanges(db, TestTasksDir)
    
    check getAllTasksOrdered(db).len == 0
