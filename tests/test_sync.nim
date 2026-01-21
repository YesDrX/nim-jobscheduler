import unittest, os, times, options, logging, strutils
import db_connector/db_sqlite
import services/sync_service
import services/task_file_service
import models/task
import models/job
import models/types # For TaskType enum
import jobscheduler # to get access to helpers if exported, or just use services directly

# We need to manually initialize tables since we are not running the main app
import models/execution
import models/user
import models/token

const TestDbPath = "test_sync.db"
const TestTasksDir = "test_tasks"

suite "Task Sync Tests":
  setup:
    if fileExists(TestDbPath): removeFile(TestDbPath)
    if dirExists(TestTasksDir): removeDir(TestTasksDir)
    
    let setupDb = open(TestDbPath, "", "", "")
    createTableTask(setupDb)
    createTableJob(setupDb)
    createTableExecution(setupDb)
    
    createDir(TestTasksDir)
    
    setupDb.close()

  teardown:
    if fileExists(TestDbPath): removeFile(TestDbPath)
    if dirExists(TestTasksDir): removeDir(TestTasksDir)

  test "Sync from YAML (Create)":
    let db = open(TestDbPath, "", "", "")
    defer: db.close()
    
    # 1. Create a task YAML file manually
    var t = Task(
      name: "SyncTestTask",
      description: "A task created via YAML",
      taskType: ttLocal,
      enabled: true,
      # Wait, yaml load needs correct structure.
      # TaskFile = object(task: Task, jobs: seq[Job])
      cronExpr: "* * * * *"
    )
    
    var j1 = Job(
        name: "Job1",
        command: "echo test",
        orderIdx: 1,
        enabled: true
    )
    
    saveTaskToYaml(t, @[j1], TestTasksDir)
    
    # Verify file exists
    check fileExists(TestTasksDir / "SyncTestTask.yaml")
    
    # 2. Run Sync
    syncTasks(db, TestTasksDir)
    
    # 3. Verify DB
    let tasks = getAllTasksOrdered(db)
    check tasks.len == 1
    check tasks[0].data.name == "SyncTestTask"
    check tasks[0].data.name == "SyncTestTask"
    check tasks[0].data.configHash.len > 0
    check tasks[0].data.sourceFile.endsWith("SyncTestTask.yaml")
    
    let jobs = getJobsByTaskIdOrdered(db, tasks[0].dbId)
    check jobs.len == 1
    check jobs[0].data.name == "Job1"

  test "Sync from YAML (Update)":
    let db = open(TestDbPath, "", "", "")
    defer: db.close()
    
    # 1. Create Initial State
    var t = Task(name: "UpdateTask", description: "Original", enabled: true)
    var j1 = Job(name: "Job1", command: "echo 1", orderIdx: 1, enabled: true)
    
    # Create via service to populate YAML
    saveTaskToYaml(t, @[j1], TestTasksDir)
    syncTasks(db, TestTasksDir)
    
    let tasksInitial = getAllTasksOrdered(db)
    check tasksInitial.len == 1
    let originalHash = tasksInitial[0].data.configHash
    
    # 2. Update YAML
    t.description = "Updated"
    j1.command = "echo 2"
    
    saveTaskToYaml(t, @[j1], TestTasksDir)
    
    # 3. Run Sync
    syncTasks(db, TestTasksDir)
    
    # 4. Verify DB
    let tasksUpdated = getAllTasksOrdered(db)
    check tasksUpdated.len == 1
    check tasksUpdated[0].data.description == "Updated"
    check tasksUpdated[0].data.configHash != originalHash
    
    let jobsUpdated = getJobsByTaskIdOrdered(db, tasksUpdated[0].dbId)
    check jobsUpdated.len == 1
    check jobsUpdated[0].data.command == "echo 2"


  test "Sync from YAML (Delete)":
    let db = open(TestDbPath, "", "", "")
    defer: db.close()

    # 1. Create Task
    var t = Task(name: "DeleteTask", enabled: true)
    saveTaskToYaml(t, @[], TestTasksDir)
    syncTasks(db, TestTasksDir)
    
    check getAllTasksOrdered(db).len == 1
    
    # 2. Delete YAML
    removeFile(TestTasksDir / "DeleteTask.yaml")
    
    # 3. Run Sync
    syncTasks(db, TestTasksDir)
    
    # 4. Verify DB
    check getAllTasksOrdered(db).len == 0
