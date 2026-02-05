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
    database*: DatabaseConfig
    server*: ServerConfig
    smtp*: SmtpConfig
    ssh*: SshConfig
    internal*: InternalConfig
    auth*: AuthConfig
