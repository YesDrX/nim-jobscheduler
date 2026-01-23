import yaml, streams, os, strutils, options, logging, times
import yaml/native
import yaml/annotations
import checksums/sha1
import ../models/task
import ../models/job
import ../models/types
import ../config/task_loader

# Custom serialization for DateTime
proc yamlTag*(T: typedesc[DateTime]): Tag = yTagTimestamp

proc constructObject*(ctx: var ConstructionContext, result: var DateTime) =
  ctx.input.constructScalarItem(item, DateTime):
    try:
      result = parse(item.scalarContent, "yyyy-MM-dd'T'HH:mm:sszzz")
    except:
      # Try parsing without Z if fails?
      try:
         result = parse(item.scalarContent, "yyyy-MM-dd'T'HH:mm:ss")
      except:
         result = now().utc

proc representObject*(ctx: var SerializationContext, value: DateTime, tag: Tag) =
  if value == default(DateTime) or value.year == 0:
     ctx.put(scalarEvent("1970-01-01T00:00:00Z", tag, yAnchorNone, ssAny))
  else:
     ctx.put(scalarEvent(format(value, "yyyy-MM-dd'T'HH:mm:sszzz"), tag, yAnchorNone, ssAny))

type
  # DTO for YAML with Optional fields
  TaskYamlDTO = object
    name* : string # Required
    description* {.defaultVal: none(string).}: Option[string]
    taskType* {.defaultVal: none(TaskType).}: Option[TaskType]
    configHash* {.defaultVal: none(string).} : Option[string] 
    
    # SSH specific
    sshHost* {.defaultVal: none(string).}: Option[string]
    sshPort* {.defaultVal: none(int).}: Option[int]
    sshUser* {.defaultVal: none(string).}: Option[string]
    sshKeyPath* {.defaultVal: none(string).}: Option[string]

    # Scheduling
    scheduleType* {.defaultVal: none(ScheduleType).}: Option[ScheduleType]
    timezone* {.defaultVal: none(string).}: Option[string] 
    cronExpr* {.defaultVal: none(string).}: Option[string]
    timeOfDay* {.defaultVal: none(string).}: Option[string] 
    intervalStart* {.defaultVal: none(string).}: Option[string]
    intervalEnd* {.defaultVal: none(string).}: Option[string]
    intervalMinutes* {.defaultVal: none(int).}: Option[int]
    calendarPath* {.defaultVal: none(string).}: Option[string]
    
    enabled* {.defaultVal: none(bool).}: Option[bool]
    createdAt* {.defaultVal: none(DateTime).}: Option[DateTime]
    updatedAt* {.defaultVal: none(DateTime).}: Option[DateTime]
    parallel* {.defaultVal: none(bool).}: Option[bool]

  JobYamlDTO = object
    name* {.defaultVal: none(string).}: Option[string]
    command* : string
    enabled* {.defaultVal: none(bool).}: Option[bool]
    # Legacy fields to ignore (absorb)
    taskId* {.defaultVal: none(int).}: Option[int]
    orderIdx* {.defaultVal: none(int).}: Option[int]

  TaskFileYaml* = object
    task*: TaskYamlDTO
    jobs*: seq[JobYamlDTO]

  # Public types
  TaskFile* = object
    task*: Task
    jobs*: seq[Job]

proc calculateConfigHash*(content: string): string =
  return $secureHash(content)

proc loadTaskFromYaml*(path: string): Option[TaskFile] =
  if not fileExists(path):
    return none(TaskFile)
  
  try:
    var s = newFileStream(path)
    defer: s.close()
    
    var resYaml: TaskFileYaml
    load(s, resYaml)
    
    # Map DTO to Task
    let dto = resYaml.task
    var t = Task(
        name: dto.name,
        description: dto.description.get(""),
        taskType: dto.taskType.get(ttLocal),
        configHash: dto.configHash.get(""),
        sshHost: dto.sshHost.get(""),
        sshPort: dto.sshPort.get(0),
        sshUser: dto.sshUser.get(""),
        sshKeyPath: dto.sshKeyPath.get(""),
        scheduleType: dto.scheduleType.get(stCron),
        timezone: dto.timezone.get(""),
        cronExpr: dto.cronExpr.get(""),
        timeOfDay: dto.timeOfDay.get(""),
        intervalStart: dto.intervalStart.get(""),
        intervalEnd: dto.intervalEnd.get(""),
        intervalMinutes: dto.intervalMinutes.get(0),
        calendarPath: dto.calendarPath.get(""),
        enabled: dto.enabled.get(true),
        createdAt: dto.createdAt.get(now().utc),
        updatedAt: dto.updatedAt.get(now().utc),
        sourceFile: path,
        parallel: dto.parallel.get(false)
    )
    let resolvedCalendarPath = resolveCalendarPath(t.calendarPath, path)
    if resolvedCalendarPath.len > 0:
      if not fileExists(resolvedCalendarPath):
        error "Calendar file not found: ", resolvedCalendarPath
      else:
        t.dateList = readFile(resolvedCalendarPath).splitLines()
    
    # Map Jobs DTO to Job
    var jobs: seq[Job] = @[]
    for i, jDto in resYaml.jobs:
       var job = Job(
         taskId: 0, # To be set by caller or DB interact
         name: jDto.name.get("job_" & $i),
         command: jDto.command,
         orderIdx: i + 1,
         enabled: jDto.enabled.get(true)
       )
       jobs.add(job)
       
    var res = TaskFile(task: t, jobs: jobs)
    return some(res)
  except:
    let e = getCurrentException()
    logging.error "Failed to load task from YAML: " & path & " Error: " & e.msg
    return none(TaskFile)

proc saveTaskToYaml*(task: Task, jobs: seq[Job], dir: string) =
  var targetDir = dir
  if task.groupName.len > 0:
      targetDir = targetDir / task.groupName
      
  if not dirExists(targetDir):
    createDir(targetDir)
    
  # Use task name as filename, sanitized
  let filename = task.name.replace(" ", "_").replace("/", "-") & ".yaml"
  let path = targetDir / filename
  
  # Convert Task to DTO
  var dto = TaskYamlDTO(
    name: task.name,
    description: some(task.description),
    taskType: some(task.taskType),
    configHash: none(string), 
    sshHost: some(task.sshHost),
    sshPort: some(task.sshPort),
    sshUser: some(task.sshUser),
    sshKeyPath: some(task.sshKeyPath),
    scheduleType: some(task.scheduleType),
    timezone: some(task.timezone),
    cronExpr: some(task.cronExpr),
    timeOfDay: some(task.timeOfDay),
    intervalStart: some(task.intervalStart),
    intervalEnd: some(task.intervalEnd),
    intervalMinutes: some(task.intervalMinutes),
    calendarPath: some(task.calendarPath),
    enabled: some(task.enabled),
    createdAt: some(task.createdAt),
    updatedAt: some(task.updatedAt),
    parallel: some(task.parallel)
  )
  
  # Convert Jobs to DTO
  var jobsDto: seq[JobYamlDTO] = @[]
  for j in jobs:
     jobsDto.add(JobYamlDTO(
       name: some(j.name),
       command: j.command,
       enabled: some(j.enabled)
     ))
  
  var tfYaml = TaskFileYaml(task: dto, jobs: jobsDto)
  
  try:
    var s = newFileStream(path, fmWrite)
    defer: s.close()
    
    var dumper = Dumper()
    dumper.dump(tfYaml, s)
  except:
    let e = getCurrentException()
    logging.error "Failed to save task to YAML: " & path & " Error: " & e.msg

proc getAllTaskFiles*(dir: string): seq[string] =
  result = @[]
  if not dirExists(dir): return
  
  for path in walkDirRec(dir):
    if path.endsWith(".yaml") and fileExists(path):
      result.add(path)
