type
  TaskType* = enum
    ttLocal = "local"
    ttRemote = "remote"

  ScheduleType* = enum
    stCron = "cron"
    stTime = "time"
    stInterval = "interval"
    stOnStart = "onstart"

  ExecutionStatus* = enum
    esScheduled = "Scheduled"
    esTriggered = "Triggered"
    esRunning = "Running"
    esSuccess = "Success"
    esFailed = "Failed"
    esCancelled = "Cancelled"
