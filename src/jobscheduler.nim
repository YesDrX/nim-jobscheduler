import std/[os, strutils]
import cligen
import types, utils
import config
import scheduler/[core, monitor]
import web/server
import web/auth
import ./[types, database, orm]
import ./executor
from ./usermgmt import userMain, printTopHelp, printUserHelp

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
  debug "Config loaded: \n" & cfg.toYamlString()
  if cfg.auth.password != "":
    var cfgCopy = cfg
    cfgCopy.auth.password = ""
    try:
      # remove password from config file
      writeFile(config, cfgCopy.toYamlString())
    except:
      discard

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
  let dbWorker = newDbWorker(dbPath, dbChan.addr, monitorChan.addr, cfg)
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
  let cliParams = commandLineParams()
  # --- User management CLI: `jobscheduler user ...` (same binary) ---
  if cliParams.len >= 1 and cliParams[0] in ["user", "users"]:
    quit(userMain(if cliParams.len > 1: cliParams[1..^1] else: @[]))
  elif cliParams.len >= 1 and cliParams[0] in ["help", "--help", "-h"] and
      cliParams.len == 1:
    # Top-level help covering both service and user management.
    printTopHelp()
    quit(0)
  elif cliParams.len >= 2 and cliParams[0] == "help" and cliParams[1] in [
      "user", "users"]:
    printUserHelp()
    quit(0)
  elif cliParams.len >= 1 and cliParams[0] == "start":
    # Explicit `jobscheduler start [--config=PATH]` (same as default action).
    var cfgPath = DefaultConfigPath
    var wantHelp = false
    var i = 1
    var badOpt = ""
    while i < cliParams.len:
      let a = cliParams[i]
      if a.startsWith("--config="):
        cfgPath = a["--config=".len .. ^1]
      elif a == "--config" and i + 1 < cliParams.len:
        inc i
        cfgPath = cliParams[i]
      elif a.startsWith("-c="):
        cfgPath = a["-c=".len .. ^1]
      elif a == "-c" and i + 1 < cliParams.len:
        inc i
        cfgPath = cliParams[i]
      elif a in ["-h", "--help"]:
        wantHelp = true
      elif a.startsWith("-"):
        badOpt = a
        break
      else:
        badOpt = a
        break
      inc i
    if wantHelp or badOpt != "":
      if badOpt != "":
        stderr.writeLine("Unknown argument for 'start': " & badOpt)
      echo "Usage:\n  jobscheduler start [-c|--config=PATH]\n\nStarts the Job Scheduler service.\n\nOptions:\n  -c, --config=PATH  Path to the configuration file (default: config.yaml)\n  -h, --help         Show this help."
      quit(if wantHelp and badOpt == "": 0 else: 2)
    start(cfgPath)
  else:
    # Default / backward-compatible: `jobscheduler [--config=PATH]`
    # starts the service via cligen.
    dispatch(start, help = {
      "config": "Path to the configuration file"
    })
