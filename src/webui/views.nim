import std/[times, os, json, sequtils, json, strutils]
import nimja
import ../[types, serialize]

const ViewDir = currentSourcePath.parentDir

# Helper for templates
proc formatDateTime(dt: DateTime): string =
  if dt == default(DateTime): return "-"
  dt.format("yyyy-MM-dd HH:mm:ss")

# Views using inline staticRead to ensure proper compile-time expansion

proc renderDashboard*(executions: seq[tuple[dbId: int,
    data: Execution]]): string =
  let page = "dashboard"
  let executionsJson = $(%*executions.mapIt(it.data))
  let executionIdsJson = $(%*executions.mapIt(it.dbId))

  compileTemplateFile("dashboard.html", baseDir = currentSourcePath.parentDir)

proc renderTasks*(tasks: seq[tuple[dbId: int, data: Task]], nextRunTimes: seq[
    DateTime]): string =
  let page = "tasks"
  var allTasksJsonData = newJArray()
  for (dbId, task) in tasks:
    var taskJsonData = %*task
    taskJsonData["id"] = %dbId
    allTasksJsonData.add(taskJsonData)
  let allTasksJson = $allTasksJsonData
  let nextRunTimesJson = "[" & nextRunTimes.mapIt(if it.serialize().len >
      0: it.serialize() else: "''").join(", ") & "]"
  compileTemplateFile("tasks.html", baseDir = currentSourcePath.parentDir)

proc renderTaskDetail*(taskId: int, task: Task, jobs: seq[tuple[dbId: int,
    data: Job]],
    recentExecs: seq[tuple[dbId: int, data: Execution]],
        nextRunTime: string): string =
  let page = "tasks"
  let taskName = task.name
  var taskJsonData = %*task
  if fileExists(task.sourceFile):
    taskJsonData["sourceContent"] = %readFile(task.sourceFile)
  else:
    taskJsonData["sourceContent"] = %("# Source file not found: " &
        task.sourceFile)
  var taskJson = $taskJsonData
  let jobsJson = $(%*(jobs.mapIt(it.data)))
  let jobsIdJson = $(%*(jobs.mapIt(it.dbId)))
  let executionsJson = $(%*(recentExecs.mapIt(it.data)))
  var executionsByJob: seq[seq[Execution]]
  var executionIdsByJob: seq[seq[int]]
  for (jobId, job) in jobs:
    executionsByJob.add(@[])
    executionIdsByJob.add(@[])
    for (execId, exec) in recentExecs:
      if exec.jobId == jobId:
        executionsByJob[^1].add(exec)
        executionIdsByJob[^1].add(execId)
  let executionsByJobJson = $(%*executionsByJob)
  let executionIdsByJobJson = $(%*executionIdsByJob)
  compileTemplateFile("task_detail.html", baseDir = currentSourcePath.parentDir)

proc renderTaskEdit*(taskId: int, task: Task): string =
  let isNew = false
  let page = "tasks"
  let taskJson = $(%*task)
  compileTemplateFile("task_edit.html", baseDir = currentSourcePath.parentDir)

proc renderTaskNew*(): string =
  let isNew = true
  let page = "tasks"
  var task: Task # Empty
  let taskJson = $(%*task)
  let taskId = -1
  compileTemplateFile("task_edit.html", baseDir = currentSourcePath.parentDir)

proc renderExecutions*(executions: seq[tuple[dbId: int, data: Execution]],
    limit: int, currentPage: int, totalPages: int, totalExecutions: int,
    search: string = "", statusFilter: string = ""): string =
  let page = "executions"
  let allExecutionsJson = $(%*(executions.mapIt(it.data)))
  let executionIdsJson = $(%*(executions.mapIt(it.dbId)))
  compileTemplateFile("executions.html", baseDir = currentSourcePath.parentDir)

proc renderLogViewer*(execId: int, execution: Execution,
    logContent: string): string =
  let page = "executions"
  let executionJson = $(%*execution)
  compileTemplateFile("log_viewer.html", baseDir = currentSourcePath.parentDir)

proc renderSchedule*(scheduleJson: string, tasksJson: string,
    taskIdsJson: string): string =
  let page = "schedule"
  compileTemplateFile("schedule.html", baseDir = currentSourcePath.parentDir)

proc renderLogin*(): string =
  let page = ""
  compileTemplateFile("login.html", baseDir = currentSourcePath.parentDir)

proc renderUsers*(users: seq[tuple[dbId: int, data: User]]): string =
  let page = "users"
  let usersJson = $(%*(users.mapIt(it.data)))
  let userIdsJson = $(%*(users.mapIt(it.dbId)))
  compileTemplateFile("users.html", baseDir = currentSourcePath.parentDir)

proc renderTokens*(tokens: seq[tuple[dbId: int, data: Token]],
    userId: int): string =
  let page = "tokens"
  let tokensJson = $(%*(tokens.mapIt(it.data)))
  let tokenIdsJson = $(%*(tokens.mapIt(it.dbId)))
  compileTemplateFile("tokens.html", baseDir = currentSourcePath.parentDir)

proc renderJobHistory*(jobId: int, jobName: string, executions: seq[tuple[
    dbId: int, data: Execution]]): string =
  let page = "executions"
  let executionsJson = $(%*(executions.mapIt(it.data)))
  let executionIdsJson = $(%*(executions.mapIt(it.dbId)))

  compileTemplateFile("job_history.html", baseDir = currentSourcePath.parentDir)
