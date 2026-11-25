import std/[times, os, json, strutils]
import nimja
import ../[types, serialize]

const ViewDir = currentSourcePath.parentDir

proc safeScriptJson*(j: JsonNode): string =
  ($j).replace("</", "<\\/")

proc safeScriptJson*(s: string): string =
  s.replace("</", "<\\/")

proc formatDateTime*(dt: DateTime): string =
  if dt == default(DateTime): return "-"
  dt.format("yyyy-MM-dd HH:mm:ss")

proc renderDashboard*(initialDataJson: string): string =
  let page = "dashboard"
  let initialJson = safeScriptJson(initialDataJson)
  compileTemplateFile("dashboard.html", baseDir = currentSourcePath.parentDir)

proc renderTasks*(initialDataJson: string): string =
  let page = "tasks"
  let initialJson = safeScriptJson(initialDataJson)
  compileTemplateFile("tasks.html", baseDir = currentSourcePath.parentDir)

proc renderTaskDetail*(taskId: int, taskName: string,
    initialDataJson: string): string =
  let page = "tasks"
  let initialJson = safeScriptJson(initialDataJson)
  compileTemplateFile("task_detail.html", baseDir = currentSourcePath.parentDir)

proc renderTaskEdit*(taskId: int, task: Task): string =
  let isNew = false
  let page = "tasks"
  let taskJson = safeScriptJson(%*task)
  compileTemplateFile("task_edit.html", baseDir = currentSourcePath.parentDir)

proc renderTaskNew*(): string =
  let isNew = true
  let page = "tasks"
  var task: Task
  task.enabled = true
  task.taskType = ttLocal
  task.scheduleType = stCron
  task.sshPort = 22
  let taskJson = safeScriptJson(%*task)
  let taskId = -1
  compileTemplateFile("task_edit.html", baseDir = currentSourcePath.parentDir)

proc renderExecutions*(initialDataJson: string): string =
  let page = "executions"
  let initialJson = safeScriptJson(initialDataJson)
  compileTemplateFile("executions.html", baseDir = currentSourcePath.parentDir)

proc renderLogViewer*(execId: int, initialDataJson: string): string =
  let page = "executions"
  let initialJson = safeScriptJson(initialDataJson)
  compileTemplateFile("log_viewer.html", baseDir = currentSourcePath.parentDir)

proc renderSchedule*(initialDataJson: string): string =
  let page = "schedule"
  let initialJson = safeScriptJson(initialDataJson)
  compileTemplateFile("schedule.html", baseDir = currentSourcePath.parentDir)

proc renderLogin*(): string =
  let page = ""
  compileTemplateFile("login.html", baseDir = currentSourcePath.parentDir)

proc renderUsers*(initialDataJson: string): string =
  let page = "users"
  let initialJson = safeScriptJson(initialDataJson)
  compileTemplateFile("users.html", baseDir = currentSourcePath.parentDir)

proc renderTokens*(initialDataJson: string): string =
  let page = "tokens"
  let initialJson = safeScriptJson(initialDataJson)
  compileTemplateFile("tokens.html", baseDir = currentSourcePath.parentDir)

proc renderJobHistory*(jobId: int, jobName: string,
    initialDataJson: string): string =
  let page = "executions"
  let initialJson = safeScriptJson(initialDataJson)
  compileTemplateFile("job_history.html", baseDir = currentSourcePath.parentDir)
