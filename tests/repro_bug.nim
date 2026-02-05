
import std/[os, strutils, options, times, logging]
import db_connector/db_sqlite
import models/task
import models/job
import config/task_loader

# Configure logging
var consoleLogger = newConsoleLogger()
addHandler(consoleLogger)

# Setup DB
let db = open(":memory:", "", "", "")
db.exec(sql"PRAGMA foreign_keys = ON;")
createTableTask(db)
createTableJob(db)

# Setup Environment
let testDataDir = "tests/repro_data"
let tasksDir = testDataDir / "tasks"
if dirExists(testDataDir):
  removeDir(testDataDir)
createDir(tasksDir)

let calFile = tasksDir / "cal.txt"
writeFile(calFile, "2023-01-01")

let taskFile = tasksDir / "task.yaml"
writeFile(taskFile, """
task:
  name: ReproTask
  description: "Test Task"
  taskType: local
  calendarPath: cal.txt
  sshHost: null
  sshPort: 22
  sshUser: null
  sshKeyPath: null
  scheduleType: cron
  cronExpr: null
  timeOfDay: null
  intervalStart: null
  intervalEnd: null
  intervalMinutes: 0
  timezone: null
  parallel: false
jobs: []
""")

# Initial Load
echo "Loading tasks (Initial)..."
loadTasks(tasksDir, db)
let tasks1 = getAllTasksOrdered(db)
doAssert tasks1.len == 1, "Task not loaded"
doAssert tasks1[0].data.dateList == @["2023-01-01"], "Initial dateList incorrect"
echo "Initial load success. DateList: ", tasks1[0].data.dateList

# Update Calendar
echo "Updating calendar file..."
sleep(1100) # Ensure mtime differs
writeFile(calFile, "2023-01-01\n2023-01-02")
setLastModificationTime(calFile, (now() + 1.seconds).toTime()) # Force mtime update just in case

# Load again
echo "Loading tasks (After Update)..."
loadTasks(tasksDir, db)
let tasks2 = getAllTasksOrdered(db)
echo "DateList: ", tasks2[0].data.dateList

if tasks2[0].data.dateList.len == 2:
  echo "SUCCESS: DateList updated."
else:
  echo "FAILURE: DateList not updated."
  quit(1)
