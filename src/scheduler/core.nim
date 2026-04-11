import std/[tables, times, options, sequtils, os, algorithm, strutils]
import ./[triggers]
import ../[database, orm, config, types, utils]

proc refreshSchedule*(s: Scheduler) =
  info "Refreshing schedule..."
  let db = getReaderDb(s.cfg.database.path)
  defer: db.close()

  let referenceNowTime = now().utc
  let taskIds = s.tasks.keys.toSeq
  var taskSchedule: seq[ScheduledTask]

  # keep old tasks
  for scheduled_task in s.taskSchedule:
    if scheduled_task.taskId in taskIds and scheduled_task.triggerTime <= referenceNowTime:
      taskSchedule.add(scheduled_task)

  # add new tasks
  for (taskId, task) in s.tasks.pairs:
    if not task.enabled: continue
    let execs = queryRowsExecution(db, "taskId = " & $taskId &
        " AND manualTriggered = " & false.serialize() & " ORDER BY startTime DESC LIMIT 1")
    var lastRun: Option[DateTime] = none(DateTime)
    if execs.len > 0: lastRun = some(execs[0].data.startTime)
    let next = getNextTrigger(task, referenceNowTime, lastRun)
    if next.isSome and next.get() - referenceNowTime < initDuration(days = 1):
      taskSchedule.add(ScheduledTask(triggerTime: next.get(),
          taskId: taskId, taskName: task.name))

  taskSchedule.sort(proc(a, b: ScheduledTask): int = cmp(a.triggerTime,
      b.triggerTime))
  s.taskSchedule = taskSchedule

  s.lastScheduleRefreshTime = referenceNowTime
  debug "Task Schedule: " & $s.taskSchedule

proc reloadSchedulerTasks*(s: Scheduler) =
  info "Reloading tasks..."
  let db = getReaderDb(s.cfg.database.path)
  defer: db.close()

  s.tasks = getAllTasksTable(db)
  s.jobs = initTable[int, seq[tuple[dbId: int, data: Job]]]()
  for (jobId, job) in queryRowsJob(db, "1=1 ORDER BY taskId, orderIdx ASC"):
    if job.taskId notin s.jobs:
      s.jobs[job.taskId] = @[]
    s.jobs[job.taskId].add((jobId, job))
    s.jobsToTaskMap[jobId] = job.taskId

proc triggerOnStartTasks*(s: Scheduler) =
  for (taskId, task) in s.tasks.pairs:
    if task.enabled and task.scheduleType == stOnStart:
      info "Triggering task on start: " & task.name
      s.executorChan[].send(ExecutorSignal(
        kind: estTriggerTask,
        triggerTaskId: taskId,
        triggerTaskTask: task
      ))

proc startScheduler*(s: Scheduler) =
  s.reloadSchedulerTasks()
  s.refreshSchedule()
  s.triggerOnStartTasks()

  while getIsRunning():
    try:
      let referenceNowTime = now().utc

      let (hasData, signal) = s.schedulerChan[].tryRecv()
      if hasData:
        debug "Received scheduler signal: " & $signal.kind
        case signal.kind:
        of ssStop:
          info "Stopping scheduler..."
          setIsRunning(false)
          break
        of ssReloadTasks:
          s.reloadSchedulerTasks()
          s.refreshSchedule()
        of ssReloadSchedule:
          s.refreshSchedule()
        of ssPrintSchedule:
          info "Reference Time: " & $referenceNowTime
          info "\n===================\nTask Schedule: " & s.taskSchedule.mapIt(
              $it.triggerTime & ": " & it.taskName).
            join("\n") & "\n==================="

      if not s.lastScheduleRefreshTime.isInitialized or (referenceNowTime -
          s.lastScheduleRefreshTime > initDuration(hours = 6)):
        s.reloadSchedulerTasks()
        s.refreshSchedule()

      # Execute tasks
      var executedTaskIdx = -1
      let taskScheduleLen = s.taskSchedule.len
      for i in 0 ..< taskScheduleLen:
        let scheduledTask = s.taskSchedule[i]
        if scheduledTask.triggerTime <= referenceNowTime:
          if scheduledTask.taskId in s.tasks:
            info "Triggering task: " & s.tasks[scheduledTask.taskId].name
            s.executorChan[].send(ExecutorSignal(
              kind: estTriggerTask,
              triggerTaskId: scheduledTask.taskId,
              triggerTaskTask: s.tasks[scheduledTask.taskId]
            ))
            executedTaskIdx += 1
            let next = getNextTrigger(s.tasks[scheduledTask.taskId],
                referenceNowTime, some(referenceNowTime))
            if next.isSome and next.get() - referenceNowTime <
                initDuration(days = 1):
              info "Adding next trigger for task: " & s.tasks[
                  scheduledTask.taskId].name & " at " & $next.get()
              s.taskSchedule.add(ScheduledTask(triggerTime: next.get(
                ), taskId: scheduledTask.taskId,
                    taskName: scheduledTask.taskName))
              info "Task Schedule after adding next trigger: " & $s.taskSchedule
        else:
          break

      if executedTaskIdx >= 0:
        s.taskSchedule.delete(0 .. executedTaskIdx)
        s.taskSchedule.sort(proc(a, b: ScheduledTask): int =
          cmp(a.triggerTime, b.triggerTime))
    except Exception as e:
      error "Error in scheduler loop: " & getCurrentExceptionMsg()
      s.monitorChan[].send(SchedulerMonitorSignal(
        kind: smmAlert,
        messageTitle: "Scheduler Error: " & getCurrentExceptionMsg(),
        message: e.getStackTrace()
      ))

    sleep 1000

  info "Scheduler stopped."
