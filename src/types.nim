import std/[strutils, times, options, os, tables, osproc]
import checksums/md5
import db_connector/db_sqlite
import ./[config, database, serialize]

# Enums
type
  TaskType* = enum
    ttUndefined = ""
    ttLocal = "local"
    ttRemote = "remote"

  ScheduleType* = enum
    stUndefined = ""
    stCron = "cron"
    stTime = "time"
    stInterval = "interval"
    stOnStart = "onstart"

  ExecutionStatus* = enum
    esUndefined = ""
    esScheduled = "Scheduled"
    esTriggered = "Triggered"
    esRunning = "Running"
    esSuccess = "Success"
    esFailed = "Failed"
    esCancelled = "Cancelled"
    esLost = "Lost" # Added esLost as per plan.md

# Value Objects (Not DB Entities directly, used inside Task)
type
  JobConfig* = object
    name*: string
    command*: string

# Database Entities

# User
type
  User* {.dbTable.} = object
    username*: string
    passwordHash*: string
    email*: string
    createdAt*: DateTime
    updatedAt*: DateTime

# Task
type
  Task* {.dbTable.} = object
    name*: string
    description*: string
    taskType*: TaskType
    configHash*: string # For file synchronization

    # SSH specific
    sshHost*: string
    sshPort*: int
    sshUser*: string
    sshKeyPath*: string

    # Scheduling
    scheduleType*: ScheduleType
    timezone*: string
    cronExpr*: string
    timeOfDay*: string
    intervalStart*: string
    intervalEnd*: string
    intervalMinutes*: int
    dateList*: seq[string]
    calendarPath*: string

    parallel*: bool = false # if parallel is true, jobs will be executed in parallel rather than sequentially

    sourceFile*: string
    folder*: string
    enabled*: bool = true
    createdAt*: DateTime
    updatedAt*: DateTime
    groupName*: string

    jobs*: seq[JobConfig]

# Execution
type
  Execution* {.dbTable.} = object
    manualTriggered*: bool
    jobId*: int
    nextJobId*: int = -1
    taskId*: int
    jobName*: string
    taskName*: string
    pid*: int
    processStartTime*: int64

    startTime*: DateTime
    endTime*: DateTime

    status*: ExecutionStatus
    exitCode*: int
    logFile*: string

    canReconnect*: bool
    errorMessage*: string
    scriptFilename*: string

# Job
type
  # Each Task can be associated with one or multiple jobs.
  Job* {.dbTable.} = object
    taskId*: int # Foreign key to Task
    name*: string
    command*: string
    orderIdx*: int

# API Token
type
  Token* {.dbTable.} = object
    name*: string
    token*: string
    userId*: int
    createdAt*: DateTime
    expiresAt*: DateTime

# Channel Messages
type
  # Database Messages
  DbMsgKind* = enum
    dbStop

    # User
    dbInsertUser
    dbUpdateUserPassword
    dbDeleteUser

    # Task
    dbInsertTask
    dbUpdateTask
    dbDeleteTask
    dbToggleTask

    # Execution
    dbInsertExecution
    dbUpdateExecution
    dbDeleteExecution
    dbUpdateExecutionIdentity
    dbUpdateExecutionStatus

    # Token
    dbInsertToken
    dbDeleteToken
    dbDeleteUserTokens

    # Cleanup
    dbCleanupExecutions

  DbMessage* = object
    case kind*: DbMsgKind
    of dbStop: discard

    # User
    of dbInsertUser:
      user*: User
      userResultCh*: ptr Channel[int] # Optional: to return generated ID
    of dbUpdateUserPassword:
      updateUserId*: int
      newPasswordHash*: string
    of dbDeleteUser:
      deleteUserId*: int

    # Task
    of dbInsertTask:
      task*: Task
      taskResultCh*: ptr Channel[int]
    of dbUpdateTask:
      updateTaskId*: int
      updatedTask*: Task
    of dbDeleteTask:
      deleteTaskId*: int
      deleteTaskSourceFile*: string
    of dbToggleTask:
      toggleTaskId*: int
      toggleEnabled*: bool

    of dbInsertExecution:
      execution*: Execution
      executionResultCh*: ptr Channel[int]
    of dbUpdateExecution:
      updateExecutionId*: int
      updatedExecution*: Execution
    of dbDeleteExecution:
      deleteExecutionId*: int
    of dbUpdateExecutionIdentity:
      identityExecutionId*: int
      newPid*: int
      newIdentity*: int64
    of dbUpdateExecutionStatus:
      statusExecutionId*: int
      newStatus*: ExecutionStatus
      newEndTime*: DateTime
      newExitCode*: int

    # Token
    of dbInsertToken:
      token*: Token
      tokenResultCh*: ptr Channel[int]
    of dbDeleteToken:
      deleteTokenId*: int
    of dbDeleteUserTokens:
      deleteTokensUserId*: int

    # Cleanup
    of dbCleanupExecutions:
      discard

  # Scheduler Messages
  SchedulerSignalKind* = enum
    ssStop
    ssReloadTasks
    ssReloadSchedule
    ssPrintSchedule

  SchedulerSignal* = object
    case kind*: SchedulerSignalKind
    of ssStop: discard
    of ssReloadTasks: discard
    of ssReloadSchedule: discard
    of ssPrintSchedule: discard

  # Scheduler Monitor Messages
  SchedulerMonitorSignalKind* = enum
    smmAlert

  SchedulerMonitorSignal* = object
    case kind*: SchedulerMonitorSignalKind
    of smmAlert:
      messageTitle*: string
      message*: string

  # Executor Messages
  ExecutorSignalType* = enum
    estTriggerTask
    estTriggerJob
    estCancelExecution

  ExecutorSignal* = object
    case kind*: ExecutorSignalType
    of estTriggerTask:
      triggerTaskId*: int
      triggerTaskTask*: Task
      triggerTaskManualTriggered*: bool
    of estTriggerJob:
      triggerJobTaskId*: int
      triggerJobTask*: Task
      triggerJobJobId*: int
      triggerJobJob*: Job
      triggerJobManualTriggered*: bool
    of estCancelExecution:
      cancelExecutionId*: int

# Channels
type
  DbChannel* = Channel[DbMessage]
  SchedulerChannel* = Channel[SchedulerSignal]
  SchedulerMonitorChannel* = Channel[SchedulerMonitorSignal]
  ExecutorChannel* = Channel[ExecutorSignal]

# Actor Objects
type
  ScheduledTask* = object
    triggerTime*: DateTime
    taskId*: int
    taskName*: string

  Scheduler* = ref object
    dbChan*: ptr DbChannel
    schedulerChan*: ptr SchedulerChannel
    executorChan*: ptr ExecutorChannel
    monitorChan*: ptr SchedulerMonitorChannel
    cfg*: Config
    taskSchedule*: seq[ScheduledTask]
    tasks*: Table[int, Task]
    jobs*: Table[int, seq[tuple[dbId: int, data: Job]]]
    jobsToTaskMap*: Table[int, int]
    lastScheduleRefreshTime*: DateTime

  SchedulerMonitor* = ref object
    dbChan*: ptr DbChannel
    schedulerChan*: ptr SchedulerChannel
    executorChan*: ptr ExecutorChannel
    monitorChan*: ptr SchedulerMonitorChannel
    cfg*: Config
    lastTasksScanTime*: DateTime
    lastFileModTime*: Table[string, Time]
    lastExecutionsCleanupTime*: DateTime

  Executor* = ref object
    dbChan*: ptr DbChannel
    schedulerChan*: ptr SchedulerChannel
    executorChan*: ptr ExecutorChannel
    monitorChan*: ptr SchedulerMonitorChannel
    cfg*: Config
    liveExecutions*: Table[int, tuple[execution: Execution, p: Process,
        task: Task, jobsTuple: seq[tuple[dbId: int, data: Job]]]]

  DbWorker* = ref object
    db*: DbConn
    ch*: ptr DbChannel
    monitorChan*: ptr SchedulerMonitorChannel

  WebServer* = ref object
    dbChan*: ptr DbChannel
    schedulerChan*: ptr SchedulerChannel
    executorChan*: ptr ExecutorChannel
    monitorChan*: ptr SchedulerMonitorChannel
    scheduler*: Scheduler
    cfg*: Config

# Actor Object Constructors
proc newScheduler*(dbChan: ptr DbChannel, schedulerChan: ptr SchedulerChannel,
    executorChan: ptr ExecutorChannel, monitorChan: ptr SchedulerMonitorChannel,
    cfg: Config): Scheduler =
  new(result)
  result.dbChan = dbChan
  result.schedulerChan = schedulerChan
  result.executorChan = executorChan
  result.monitorChan = monitorChan
  result.cfg = cfg
  result.taskSchedule = @[]
  result.tasks = initTable[int, Task]()

proc newSchedulerMonitor*(dbChan: ptr DbChannel,
    schedulerChan: ptr SchedulerChannel, executorChan: ptr ExecutorChannel,
    monitorChan: ptr SchedulerMonitorChannel, cfg: Config): SchedulerMonitor =
  new(result)
  result.dbChan = dbChan
  result.schedulerChan = schedulerChan
  result.executorChan = executorChan
  result.monitorChan = monitorChan
  result.cfg = cfg

proc newExecutor*(dbChan: ptr DbChannel, schedulerChan: ptr SchedulerChannel,
    executorChan: ptr ExecutorChannel, monitorChan: ptr SchedulerMonitorChannel,
    cfg: Config): Executor =
  new(result)
  result.dbChan = dbChan
  result.schedulerChan = schedulerChan
  result.executorChan = executorChan
  result.monitorChan = monitorChan
  result.cfg = cfg

proc newDbWorker*(dbPath: string, ch: ptr DbChannel,
    monitorChan: ptr SchedulerMonitorChannel): DbWorker =
  new(result)
  result.db = open(dbPath, "", "", "")
  result.ch = ch
  result.monitorChan = monitorChan

proc newWebServer*(dbChan: ptr DbChannel, schedulerChan: ptr SchedulerChannel,
    executorChan: ptr ExecutorChannel, monitorChan: ptr SchedulerMonitorChannel,
    scheduler: Scheduler, cfg: Config): WebServer =
  new(result)
  result.dbChan = dbChan
  result.schedulerChan = schedulerChan
  result.executorChan = executorChan
  result.monitorChan = monitorChan
  result.scheduler = scheduler
  result.cfg = cfg

proc calculateConfigHash*(task: Task, resolvedCalendarPath: string): string =
  var taskJson = %*(task)
  if "dateList" in taskJson: taskJson.delete("dateList")
  var content = $taskJson
  if resolvedCalendarPath.len > 0 and fileExists(resolvedCalendarPath):
    content &= resolvedCalendarPath.getFileInfo.lastWriteTime.`$`
  return $toMD5(content)

# Global variable
var
  isRunning = false

proc setIsRunning*(value: bool) {.inline.} =
  {.gcsafe.}:
    isRunning = value

proc getIsRunning*(): bool {.inline.} =
  {.gcsafe.}:
    return isRunning

