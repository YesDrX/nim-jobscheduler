import streams, os, strutils, options, logging, times
import std/md5
import db_connector/db_sqlite
import ../models/task
import ../models/job
import ../models/types
import ../db/db_macros
import yaml

type
  JobYAML = object
    name: string
    command: string

  TaskConfigYAML = object
    # Required fields
    name: string

    # Optional fields with Option types
    description: Option[string]
    taskType: Option[string]
    sshHost: Option[string]
    sshPort: Option[int]
    sshUser: Option[string]
    sshKeyPath: Option[string]

    # Schedule fields
    scheduleType: Option[string]
    cronExpr: Option[string]
    timeOfDay: Option[string]
    intervalStart: Option[string]
    intervalEnd: Option[string]
    intervalMinutes: Option[int]
    timezone: Option[string]
    calendarPath: Option[string]
    parallel: Option[bool]

  TaskYAML = object
    task: TaskConfigYAML
    jobs: Option[seq[JobYAML]]

proc resolveCalendarPath*(calendarPath: string, yamlFilePath: string): string =
  ## Resolve calendar path relative to task YAML file if it's a relative path
  ## Cross-platform: handles Windows (C:, D:) and Unix (/) absolute paths
  if calendarPath == "":
    return ""

  # Check if path is absolute
  if isAbsolute(calendarPath):
    return calendarPath

  # Relative path: resolve relative to YAML file directory
  # strictly convert to absolute path so that checking code doesn't need to worry about CWD
  let absYamlPath = absolutePath(yamlFilePath)
  let yamlDir = parentDir(absYamlPath)
  let resolved = joinPath(yamlDir, calendarPath)

  return resolved

proc calculateConfigHash(taskConfig: TaskConfigYAML,
    resolvedCalendarPath: string): string =
  ## Calculate MD5 hash of task configuration for change detection
  var content = $taskConfig

  if resolvedCalendarPath.len > 0 and fileExists(resolvedCalendarPath):
    content &= $getLastModificationTime(resolvedCalendarPath)

  return $toMD5(content)

proc loadTasks*(tasksDir: string, db: DbConn) =
  if not dirExists(tasksDir):
    return

  for path in walkDirRec(tasksDir):
    if path.endsWith(".yaml") and fileExists(path):
      var s = newFileStream(path)
      var taskYaml: TaskYAML
      try:
        load(s, taskYaml)
      except:
        logging.error "Error loading task file: ", path
        logging.error getCurrentExceptionMsg()
        s.close()
        continue
      s.close()

      # Upsert Task
      var taskTypeEnum = ttLocal
      if taskYaml.task.taskType.isSome:
        try:
          taskTypeEnum = parseEnum[TaskType](taskYaml.task.taskType.get())
        except:
          info "Invalid task type in ", path, ": ", taskYaml.task.taskType.get()
          continue

      var scheduleTypeEnum = stCron
      if taskYaml.task.scheduleType.isSome:
        try:
          scheduleTypeEnum = parseEnum[ScheduleType](
              taskYaml.task.scheduleType.get())
        except:
          info "Invalid schedule type in ", path, ": ",
              taskYaml.task.scheduleType.get()
          continue

      # Resolve paths
      let resolvedCalendarPath = resolveCalendarPath(
          taskYaml.task.calendarPath.get(""), path)

      # Calculate config hash for change detection
      let configHash = calculateConfigHash(taskYaml.task, resolvedCalendarPath)

      # Find existing task by sourceFile
      let existingTasks = queryRowsTask(db, "sourceFile = '" & path.replace("'",
          "''") & "'")
      var taskId: int = 0

      if existingTasks.len > 0:
        # Task exists - check if config has changed
        var (dbId, tData) = existingTasks[0]

        if tData.configHash == configHash and tData.calendarPath == resolvedCalendarPath:
          # Config unchanged, skip update
          taskId = dbId
        else:
          # Config changed - update task
          var t = tData
          t.name = taskYaml.task.name
          t.description = taskYaml.task.description.get("")
          t.taskType = taskTypeEnum
          t.configHash = configHash
          t.sshHost = taskYaml.task.sshHost.get("")
          t.sshPort = taskYaml.task.sshPort.get(22)
          t.sshUser = taskYaml.task.sshUser.get("")
          t.sshKeyPath = taskYaml.task.sshKeyPath.get("")

          t.scheduleType = scheduleTypeEnum
          t.timezone = taskYaml.task.timezone.get("")
          t.cronExpr = taskYaml.task.cronExpr.get("")
          t.timeOfDay = taskYaml.task.timeOfDay.get("")
          t.intervalStart = taskYaml.task.intervalStart.get("")
          t.intervalEnd = taskYaml.task.intervalEnd.get("")
          t.intervalMinutes = taskYaml.task.intervalMinutes.get(0)

          t.calendarPath = resolvedCalendarPath
          t.parallel = taskYaml.task.parallel.get(false)
          t.sourceFile = path

          if resolvedCalendarPath.len > 0:
            if not fileExists(resolvedCalendarPath):
              error "Calendar file not found: ", resolvedCalendarPath
            else:
              info "Reading calendar file: ", resolvedCalendarPath
              t.dateList = readFile(resolvedCalendarPath).splitLines()

          info "Updated task '", t.name, "' with calendarPath: '",
              t.calendarPath, "'"
          updateRowTask(db, dbId, t)
          taskId = dbId
      else:
        # Create new task
        var t = Task(
          name: taskYaml.task.name,
          description: taskYaml.task.description.get(""),
          taskType: taskTypeEnum,
          configHash: configHash,
          sshHost: taskYaml.task.sshHost.get(""),
          sshPort: taskYaml.task.sshPort.get(22),
          sshUser: taskYaml.task.sshUser.get(""),
          sshKeyPath: taskYaml.task.sshKeyPath.get(""),
          scheduleType: scheduleTypeEnum,
          timezone: taskYaml.task.timezone.get(""),
          cronExpr: taskYaml.task.cronExpr.get(""),
          timeOfDay: taskYaml.task.timeOfDay.get(""),
          intervalStart: taskYaml.task.intervalStart.get(""),
          intervalEnd: taskYaml.task.intervalEnd.get(""),
          intervalMinutes: taskYaml.task.intervalMinutes.get(0),

          calendarPath: resolvedCalendarPath,
          parallel: taskYaml.task.parallel.get(false),
          sourceFile: path,
          createdAt: now().utc,
          updatedAt: now().utc
        )

        if resolvedCalendarPath.len > 0:
          if not fileExists(resolvedCalendarPath):
            error "Calendar file not found: ", resolvedCalendarPath
          else:
            info "Reading calendar file: ", resolvedCalendarPath
            t.dateList = readFile(resolvedCalendarPath).splitLines()

        info "Created task '", t.name, "' with calendarPath: '", t.calendarPath, "'"
        taskId = insertRowTask(db, t)

      # Sync Jobs
      let existingJobs = queryRowsJob(db, "taskId = " & $taskId)
      var jobsInYaml: seq[string] = @[]

      # Only process jobs if they exist in the YAML
      if taskYaml.jobs.isSome:
        var orderIdx = 0
        for jobYaml in taskYaml.jobs.get():
          orderIdx.inc
          jobsInYaml.add(jobYaml.name)

          # Check if job exists
          var matchingJobId = 0
          var matchingJobData: Option[Job] = none(Job)

          for row in existingJobs:
            if row.data.name == jobYaml.name:
              matchingJobId = row.dbId
              matchingJobData = some(row.data)
              break

          if matchingJobData.isSome:
            var j = matchingJobData.get
            j.command = jobYaml.command
            j.orderIdx = orderIdx
            updateRowJob(db, matchingJobId, j)
          else:
            var j = Job(
              taskId: taskId,
              name: jobYaml.name,
              command: jobYaml.command,
              orderIdx: orderIdx,
              enabled: true
            )
            discard insertRowJob(db, j)

      # Delete jobs not in YAML
      for row in existingJobs:
        if row.data.name notin jobsInYaml:
          deleteJobById(db, row.dbId)

