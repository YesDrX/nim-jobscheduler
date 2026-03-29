import std/[os, strutils]
import cligen
import types, utils
import config
import scheduler/[core, monitor]
import web/server
import web/auth
import ./[types, database, orm]
import ./executor

const
  DefaultConfigPath = "config.yaml"

proc start(config = DefaultConfigPath) =
  ## Starts the Job Scheduler service.

  setIsRunning(true)

  # 1. Config
  if not fileExists(config):
    fatal "Config file not found: " & config

  let cfg = loadConfig(config)
  setLogLevel(cfg.logLevel)

  # 2. Channels
  var
    dbChan: DbChannel
    schedulerChan: SchedulerChannel
    executorChan: ExecutorChannel
    monitorChan: SchedulerMonitorChannel
  dbChan.open()
  schedulerChan.open()
  executorChan.open()
  monitorChan.open()

  defer:
    dbChan.close()
    schedulerChan.close()
    executorChan.close()
    monitorChan.close()

  # 3. Database Actor
  let dbPath = cfg.database.path
  let dbWorker = newDbWorker(dbPath, dbChan.addr)
  dbWorker.db.initDb()
  dbWorker.db.setUpInitialUser(cfg.auth.username,
      cfg.auth.password.encryptPassword)

  # 4. Scheduler
  let scheduler = newScheduler(dbChan.addr, schedulerChan.addr,
      executorChan.addr, monitorChan.addr, cfg)

  # 5. Executor
  let executor = newExecutor(dbChan.addr, schedulerChan.addr, executorChan.addr,
      monitorChan.addr, cfg)

  # 6. Scheduler Monitor
  let schedulerMonitor = newSchedulerMonitor(dbChan.addr, schedulerChan.addr,
      executorChan.addr, monitorChan.addr, cfg)

  # 7. Web Server
  let webServer = newWebServer(dbChan.addr, schedulerChan.addr,
      executorChan.addr, monitorChan.addr, scheduler, cfg)

  # 8. Start
  ## Start DB Worker
  var dbWorkerThread: Thread[DbWorker]
  dbWorkerThread.createThread(runDbWorker, dbWorker)
  defer: dbWorkerThread.joinThread()

  ## Start Executor
  var executorThread: Thread[Executor]
  executorThread.createThread(runExecutor, executor)
  defer: executorThread.joinThread()

  ## Start Scheduler Monitor
  var schedulerMonitorThread: Thread[SchedulerMonitor]
  schedulerMonitorThread.createThread(runSchedulerMonitor, schedulerMonitor)
  defer: schedulerMonitorThread.joinThread()

  ## Start Web Server
  var webServerThread: Thread[WebServer]
  webServerThread.createThread(runWebServer, webServer)
  defer: webServerThread.joinThread()

  ## Start Scheduler
  startScheduler(scheduler)

when isMainModule:
  setLogLevel(DEBUG)
  dispatch(start, help = {
    "config": "Path to the configuration file"
  })
