import std/[times, strutils, asyncdispatch, logging]
import db_connector/db_sqlite
import ../executor/logger
import ../config/types

type
  CleanupManager* = ref object
    db: DbConn
    logsDir: string
    config: InternalConfig
    running: bool

proc newCleanupManager*(db: DbConn, logsDir: string, config: InternalConfig): CleanupManager =
  new(result)
  result.db = db
  result.logsDir = logsDir
  result.config = config
  result.running = false

proc pruneExecutionHistory(manager: CleanupManager) =
  try:
    if manager.config.logRetentionDays <= 0: return

    # Calculate cutoff date
    let retentionSeconds = manager.config.logRetentionDays * 24 * 3600
    let cutoff = now().utc - initDuration(seconds = retentionSeconds)
    let cutoffStr = cutoff.format("yyyy-MM-dd HH:mm:ss")
    
    # 1. Prune Database with limit to avoid locking
    manager.db.exec(sql"DELETE FROM ExecutionTable WHERE endTime < ? AND endTime IS NOT NULL", cutoffStr)
    
    # 2. Prune Logs
    rotateLogs(manager.logsDir, manager.config.logRetentionDays)
    
  except Exception as e:
    info "[Cleanup] Error during cleanup: " & e.msg

proc runLoop(manager: CleanupManager) {.async.} =
  manager.running = true
  while manager.running:
    manager.pruneExecutionHistory()
    # Run once every hour? Or every 24 hours?
    # Let's run every hour.
    await sleepAsync(3600 * 1000)

proc start*(manager: CleanupManager) =
  if not manager.running:
    asyncCheck manager.runLoop()

proc stop*(manager: CleanupManager) =
  manager.running = false
