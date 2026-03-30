import yaml, streams, os, json
import yaml/tojson
import ./[utils, serialize]

type
  DatabaseConfig* = object
    path*: string

  ServerConfig* = object
    host*: string = "0.0.0.0"
    port*: int = 8080
    externalHost*: string
    sslCert*: string = ""
    sslKey*: string = ""

  SmtpConfig* = object
    enabled*: bool
    host*: string
    port*: int
    useSSL*: bool
    password*: string
    fromAddr*: string
    toAddrs*: seq[string]

  SshConfig* = object
    defaultKeyPath*: string

  InternalConfig* = object
    logRetentionDays*: int

  AuthConfig* = object
    username*: string
    password*: string

  Config* = object
    tasksDir*: string
    workingDir*: string
    logLevel*: LogLevel
    database*: DatabaseConfig
    server*: ServerConfig
    smtp*: SmtpConfig
    ssh*: SshConfig
    internal*: InternalConfig
    auth*: AuthConfig


proc loadConfig*(path: string): Config =
  if not fileExists(path):
    raise newException(IOError, "Config file not found: " & path)
  let fs = newFileStream(path, fmRead)
  defer: fs.close()
  let cfgJson = loadToJson(fs)
  result = deserializeJson[Config](cfgJson[0])
  info "Loaded config from " & path

proc saveConfig*(path: string, cfg: Config) =
  var s = newFileStream(path, fmWrite)
  defer: s.close()
  var dumper = Dumper()
  dumper.dump(cfg, s)
