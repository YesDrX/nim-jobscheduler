## User management library + CLI for the jobscheduler single binary.
##
## Core DB operations work directly on the SQLite file (no actor threads),
## using the same schema / password hashing as the web server so results
## stay compatible with the running service.
##
## CLI usage (via `jobscheduler user ...`):
##   jobscheduler user list [--config=PATH] [--json]
##   jobscheduler user show USER [--config=PATH] [--json]
##   jobscheduler user add USER [--password=PASS] [--email=EMAIL] [--config=PATH]
##   jobscheduler user delete USER [--yes] [--config=PATH]
##   jobscheduler user passwd USER [--password=PASS] [--email=EMAIL] [--config=PATH]
##   jobscheduler user set-email USER --email=EMAIL [--config=PATH]

import std/[os, strutils, strformat, times, options, json, terminal]
import db_connector/db_sqlite
import ./[config, database, orm, types, serialize]
import ./web/auth

export options

const
  DefaultConfigPath* = "config.yaml"

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

proc resolveDbPath*(configPath: string): string =
  ## Load config and resolve the SQLite path.
  ## Keeps backward compat (path used as-is, relative to CWD) but also
  ## falls back to config-dir-relative and ~/ expansion when helpful.
  if not fileExists(configPath):
    raise newException(IOError, "Config file not found: " & configPath)
  let cfg = loadConfig(configPath)
  var dbPath = cfg.database.path.expandTilde
  if dbPath == "":
    raise newException(ValueError, "database.path is empty in " & configPath)
  if isAbsolute(dbPath) or fileExists(dbPath):
    return dbPath
  # Fall back: resolve relative to the config file directory.
  let cfgDir = parentDir(absolutePath(configPath))
  let candidate = joinPath(cfgDir, dbPath)
  if fileExists(candidate):
    return candidate
  # Default: keep CWD-relative behaviour (file may be created on first use).
  return dbPath

proc openMgmtDb*(configPath: string): DbConn =
  ## Open (and ensure schema for) the management DB.
  let dbPath = resolveDbPath(configPath)
  let parent = parentDir(dbPath)
  if parent != "" and not dirExists(parent):
    createDir(parent)
  result = open(dbPath, "", "", "")
  result.initDb()

proc findUser*(db: DbConn, ident: string): Option[tuple[dbId: int, data: User]] =
  ## Find a user by numeric ID or by username. ID is tried first when
  ## `ident` parses as an integer.
  let s = ident.strip()
  if s == "":
    return none(tuple[dbId: int, data: User])
  try:
    let id = parseInt(s)
    let byId = getUserById(db, id)
    if byId.isSome:
      return byId
  except ValueError:
    discard
  return getUserByUsername(db, s)

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

proc validateUsername*(username: string): string =
  result = username.strip()
  if result == "":
    raise newException(ValueError, "username must not be empty")
  if result.len > 64:
    raise newException(ValueError, "username too long (max 64 characters)")
  for ch in result:
    if ch in {' ', '\t', '\n', '\r'}:
      raise newException(ValueError, "username must not contain whitespace")
  if result != username.strip():
    raise newException(ValueError, "username must not have leading/trailing spaces")

proc validatePassword*(password: string) =
  if password == "":
    raise newException(ValueError, "password must not be empty")

proc validateEmail*(email: string) =
  let e = email.strip()
  if e == "":
    return # empty email means "not set" — allowed
  if "@" notin e or "." notin e.split('@')[^1]:
    raise newException(ValueError, "invalid email address: " & email)

proc formatUserTime*(dt: DateTime): string =
  if not dt.isInitialized:
    return "-"
  return dt.format("yyyy-MM-dd HH:mm:ss")

# ---------------------------------------------------------------------------
# Core operations (direct SQLite, synchronous)
# ---------------------------------------------------------------------------

proc listUsers*(db: DbConn): seq[tuple[dbId: int, data: User]] =
  return getAllUsers(db)

proc addUser*(db: DbConn, username, password, email: string): int =
  let uname = validateUsername(username)
  validatePassword(password)
  validateEmail(email)
  if getUserByUsername(db, uname).isSome:
    raise newException(ValueError, "user already exists: " & uname)
  let nowTs = now()
  let user = User(
    username: uname,
    passwordHash: encryptPassword(password),
    email: email.strip(),
    createdAt: nowTs,
    updatedAt: nowTs
  )
  result = db.insertRowUser(user)

proc deleteUser*(db: DbConn, ident: string): tuple[dbId: int, username: string] =
  let found = findUser(db, ident)
  if found.isNone:
    raise newException(ValueError, "user not found: " & ident)
  let (dbId, data) = found.get()
  db.deleteRowUser(dbId)
  # Clean up API tokens owned by the user (web API leaves them orphaned).
  try:
    db.exec(sql"DELETE FROM TokenTable WHERE userId = ?", dbId)
  except:
    discard
  return (dbId, data.username)

proc setUserPassword*(db: DbConn, ident, newPassword: string,
    newEmail: string = "\x00"): tuple[dbId: int, username: string] =
  ## Change a user's password. When `newEmail` is "\x00" (sentinel) the
  ## email is left untouched; otherwise it is validated and updated
  ## (empty string clears it).
  let found = findUser(db, ident)
  if found.isNone:
    raise newException(ValueError, "user not found: " & ident)
  let (dbId, data) = found.get()
  validatePassword(newPassword)
  db.exec(sql"UPDATE UserTable SET passwordHash = ? WHERE _dbID = ?",
    newPassword.encryptPassword.serialize(), dbId)
  if newEmail != "\x00":
    validateEmail(newEmail)
    db.exec(sql"UPDATE UserTable SET email = ? WHERE _dbID = ?",
      newEmail.strip().serialize(), dbId)
  db.exec(sql"UPDATE UserTable SET updatedAt = ? WHERE _dbID = ?",
    now().serialize(), dbId)
  return (dbId, data.username)

proc setUserEmail*(db: DbConn, ident, email: string): tuple[dbId: int,
    username: string] =
  let found = findUser(db, ident)
  if found.isNone:
    raise newException(ValueError, "user not found: " & ident)
  let (dbId, data) = found.get()
  validateEmail(email)
  if email.strip() == "":
    raise newException(ValueError, "email must not be empty (use --email=EMAIL)")
  db.exec(sql"UPDATE UserTable SET email = ? WHERE _dbID = ?",
    email.strip().serialize(), dbId)
  db.exec(sql"UPDATE UserTable SET updatedAt = ? WHERE _dbID = ?",
    now().serialize(), dbId)
  return (dbId, data.username)

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

proc usersToJson*(users: seq[tuple[dbId: int, data: User]]): JsonNode =
  result = newJArray()
  for (dbId, u) in users:
    result.add(%*{
      "id": dbId,
      "username": u.username,
      "email": u.email,
      "createdAt": formatUserTime(u.createdAt),
      "updatedAt": formatUserTime(u.updatedAt)
    })

proc printUsersTable*(users: seq[tuple[dbId: int, data: User]]) =
  if users.len == 0:
    echo "No users found."
    return
  var wId = len("ID")
  var wName = len("USERNAME")
  var wEmail = len("EMAIL")
  var wCreated = len("CREATED")
  var wUpdated = len("UPDATED")
  var rows: seq[tuple[id, name, email, created, updated: string]] = @[]
  for (dbId, u) in users:
    let r = ($dbId, u.username, if u.email == "": "-" else: u.email,
      formatUserTime(u.createdAt), formatUserTime(u.updatedAt))
    rows.add(r)
    wId = max(wId, r[0].len)
    wName = max(wName, r[1].len)
    wEmail = max(wEmail, r[2].len)
    wCreated = max(wCreated, r[3].len)
    wUpdated = max(wUpdated, r[4].len)
  echo alignLeft("ID", wId) & "  " & alignLeft("USERNAME", wName) & "  " &
    alignLeft("EMAIL", wEmail) & "  " & alignLeft("CREATED", wCreated) &
    "  " & alignLeft("UPDATED", wUpdated)
  echo repeat('-', wId) & "  " & repeat('-', wName) & "  " &
    repeat('-', wEmail) & "  " & repeat('-', wCreated) & "  " &
    repeat('-', wUpdated)
  for r in rows:
    echo alignLeft(r[0], wId) & "  " & alignLeft(r[1], wName) & "  " &
      alignLeft(r[2], wEmail) & "  " & alignLeft(r[3], wCreated) & "  " &
      alignLeft(r[4], wUpdated)

# ---------------------------------------------------------------------------
# CLI parsing helpers
# ---------------------------------------------------------------------------

proc printUserHelp*() =
  echo """User management for jobscheduler.

Usage:
  jobscheduler user <command> [args] [options]
  jobscheduler users <command> [args] [options]

Commands:
  list                        List all users
  show USER                   Show one user (by username or ID)
  add USER                    Add a user
  delete USER                 Delete a user (and their API tokens)
  passwd USER                 Change a user's password
  set-email USER              Change a user's email address
  help                        Show this help

Arguments:
  USER                        Username or numeric user ID

Options:
  -c, --config=PATH           Path to config file (default: config.yaml)
                              Used to locate database.path.
      --password=PASS         Password (add/passwd). If omitted, you are
                              prompted securely (with confirmation).
      --email=EMAIL           Email address (add/passwd/set-email).
                              For passwd: if omitted, email is unchanged.
                              Use --email="" with passwd to clear it.
  -y, --yes                   Skip confirmation prompt (delete).
      --json                  Machine-readable JSON output (list/show).
  -h, --help                  Show this help.

Examples:
  jobscheduler user list
  jobscheduler user list --config=/etc/jobscheduler/config.yaml
  jobscheduler user show alice
  jobscheduler user add alice --email=alice@example.com
  jobscheduler user add bob --password=s3cret --email=bob@example.com
  jobscheduler user passwd alice
  jobscheduler user set-email alice --email=new@example.com
  jobscheduler user delete bob --yes
"""

proc printTopHelp*() =
  echo """jobscheduler — job scheduler service + user management CLI.

Usage:
  jobscheduler [--config=PATH]               Start the scheduler service
  jobscheduler start [--config=PATH]         Start the scheduler service
  jobscheduler user <command> [...]          Manage users (see below)
  jobscheduler --help                        Show this help

Service options:
  -c, --config=PATH           Path to config file (default: config.yaml)

User management:
  jobscheduler user list [--config=PATH] [--json]
  jobscheduler user show USER [--config=PATH] [--json]
  jobscheduler user add USER [--password=PASS] [--email=EMAIL] [--config=PATH]
  jobscheduler user delete USER [--yes] [--config=PATH]
  jobscheduler user passwd USER [--password=PASS] [--email=EMAIL] [--config=PATH]
  jobscheduler user set-email USER --email=EMAIL [--config=PATH]

Run 'jobscheduler user help' for full user-management help.
"""

proc promptPassword*(prompt: string): string =
  ## Securely prompt twice and require a match.
  let p1 =
    try: readPasswordFromStdin(prompt & ": ")
    except EOFError: ""
  if p1 == "":
    raise newException(ValueError, "password must not be empty")
  let p2 =
    try: readPasswordFromStdin("Confirm " & prompt.toLowerAscii() & ": ")
    except EOFError: ""
  if p1 != p2:
    raise newException(ValueError, "passwords do not match")
  return p1

proc promptConfirm*(question: string): bool =
  stdout.write(question & " [y/N]: ")
  stdout.flushFile()
  let ans =
    try: stdin.readLine().strip().toLowerAscii()
    except EOFError, IOError: ""
  return ans in ["y", "yes"]

type
  UserCliOpts* = object
    config*: string
    password*: string
    hasPassword*: bool
    email*: string
    hasEmail*: bool
    yes*: bool
    json*: bool
    help*: bool

proc parseUserArgs*(args: seq[string], positional: var seq[string]): UserCliOpts =
  ## Minimal `--opt=value` / `--opt value` / `-c value` parser.
  ## Returns parsed options; non-option tokens go into `positional`.
  result = UserCliOpts(config: DefaultConfigPath)
  var i = 0
  while i < args.len:
    let a = args[i]
    if a.startsWith("--config="):
      result.config = a["--config=".len .. ^1]
    elif a == "--config" and i + 1 < args.len:
      inc i
      result.config = args[i]
    elif a.startsWith("-c="):
      result.config = a["-c=".len .. ^1]
    elif a == "-c" and i + 1 < args.len:
      inc i
      result.config = args[i]
    elif a.startsWith("--password="):
      result.password = a["--password=".len .. ^1]
      result.hasPassword = true
    elif a == "--password" and i + 1 < args.len:
      inc i
      result.password = args[i]
      result.hasPassword = true
    elif a.startsWith("--email="):
      result.email = a["--email=".len .. ^1]
      result.hasEmail = true
    elif a == "--email" and i + 1 < args.len:
      inc i
      result.email = args[i]
      result.hasEmail = true
    elif a in ["-y", "--yes"]:
      result.yes = true
    elif a == "--json":
      result.json = true
    elif a in ["-h", "--help"]:
      result.help = true
    elif a == "--":
      for j in i + 1 ..< args.len:
        positional.add(args[j])
      break
    elif a.startsWith("-"):
      raise newException(ValueError, "unknown option: " & a &
        "\nRun 'jobscheduler user help' for usage.")
    else:
      positional.add(a)
    inc i

proc normalizeUserSubcommand*(s: string): string =
  case s.strip().toLowerAscii()
  of "list", "ls": "list"
  of "show", "get", "info", "view": "show"
  of "add", "create", "new": "add"
  of "delete", "del", "rm", "remove": "delete"
  of "passwd", "password", "set-password", "change-password", "chpasswd",
      "reset-password": "passwd"
  of "set-email", "setemail", "email", "update-email", "change-email": "set-email"
  of "help", "--help", "-h": "help"
  else: s.strip().toLowerAscii()

proc userMain*(args: seq[string]): int =
  ## Entry point for `jobscheduler user ...`. Returns process exit code.
  # Keep CLI output clean (and --json parseable): suppress INFO/DEBUG logs.
  # Config loading itself logs at INFO, so silence it for user commands.
  setLogLevel(WARN)
  var positional: seq[string] = @[]
  var opts: UserCliOpts
  try:
    opts = parseUserArgs(args, positional)
  except ValueError as e:
    stderr.writeLine("Error: " & e.msg)
    return 2

  if opts.help or (positional.len == 0):
    # `jobscheduler user` with no subcommand shows help (exit 2 when
    # no subcommand, 0 for explicit help) to aid discoverability.
    printUserHelp()
    return if opts.help or (positional.len > 0 and positional[0] == "help"): 0 else: 2

  let sub = normalizeUserSubcommand(positional[0])
  let rest = if positional.len > 1: positional[1 .. ^1] else: @[]

  case sub
  of "help":
    printUserHelp()
    return 0
  of "list":
    if rest.len > 0:
      stderr.writeLine("Error: 'list' takes no arguments.")
      return 2
    try:
      let db = openMgmtDb(opts.config)
      defer: db.close()
      let users = db.listUsers()
      if opts.json:
        echo $usersToJson(users)
      else:
        printUsersTable(users)
      return 0
    except Exception as e:
      stderr.writeLine("Error: " & e.msg)
      return 1
  of "show":
    if rest.len != 1:
      stderr.writeLine("Error: usage: jobscheduler user show USER [--config=PATH] [--json]")
      return 2
    try:
      let db = openMgmtDb(opts.config)
      defer: db.close()
      let found = db.findUser(rest[0])
      if found.isNone:
        stderr.writeLine("Error: user not found: " & rest[0])
        return 1
      let (dbId, u) = found.get()
      if opts.json:
        echo $(%*{"id": dbId, "username": u.username, "email": u.email,
          "createdAt": formatUserTime(u.createdAt),
          "updatedAt": formatUserTime(u.updatedAt)})
      else:
        printUsersTable(@[(dbId, u)])
      return 0
    except Exception as e:
      stderr.writeLine("Error: " & e.msg)
      return 1
  of "add":
    if rest.len != 1:
      stderr.writeLine("Error: usage: jobscheduler user add USER [--password=PASS] [--email=EMAIL] [--config=PATH]")
      return 2
    var password = opts.password
    try:
      if not opts.hasPassword:
        if not stdin.isatty():
          stderr.writeLine("Error: --password is required when stdin is not a terminal.")
          return 2
        echo "Adding user '" & rest[0].strip() & "'."
        password = promptPassword("Password")
      let email = if opts.hasEmail: opts.email else: ""
      let db = openMgmtDb(opts.config)
      defer: db.close()
      let id = db.addUser(rest[0], password, email)
      echo fmt"User '{$rest[0].strip()}' added with ID {id}."
      return 0
    except ValueError as e:
      stderr.writeLine("Error: " & e.msg)
      return 1
    except Exception as e:
      stderr.writeLine("Error: " & e.msg)
      return 1
  of "delete":
    if rest.len != 1:
      stderr.writeLine("Error: usage: jobscheduler user delete USER [--yes] [--config=PATH]")
      return 2
    try:
      let db = openMgmtDb(opts.config)
      defer: db.close()
      let found = db.findUser(rest[0])
      if found.isNone:
        stderr.writeLine("Error: user not found: " & rest[0])
        return 1
      let (dbId, data) = found.get()
      let total = db.listUsers().len
      if total <= 1:
        stderr.writeLine("Warning: this is the last user. Deleting it will leave no login accounts")
        stderr.writeLine("(the initial user from config.yaml will be recreated on next service start).")
      if not opts.yes:
        if not stdin.isatty():
          stderr.writeLine(fmt"Refusing to delete user '{data.username}' (ID {dbId}) without --yes when stdin is not a terminal.")
          return 2
        if not promptConfirm(fmt"Delete user '{data.username}' (ID {dbId}) and their API tokens?"):
          echo "Aborted."
          return 1
      discard db.deleteUser($dbId)
      echo fmt"User '{data.username}' (ID {dbId}) deleted."
      return 0
    except ValueError as e:
      stderr.writeLine("Error: " & e.msg)
      return 1
    except Exception as e:
      stderr.writeLine("Error: " & e.msg)
      return 1
  of "passwd":
    if rest.len != 1:
      stderr.writeLine("Error: usage: jobscheduler user passwd USER [--password=PASS] [--email=EMAIL] [--config=PATH]")
      return 2
    var password = opts.password
    try:
      if not opts.hasPassword:
        if not stdin.isatty():
          stderr.writeLine("Error: --password is required when stdin is not a terminal.")
          return 2
        echo "Changing password for '" & rest[0].strip() & "'."
        password = promptPassword("New password")
      let db = openMgmtDb(opts.config)
      defer: db.close()
      if opts.hasEmail:
        discard db.setUserPassword(rest[0], password, opts.email)
      else:
        discard db.setUserPassword(rest[0], password)
      echo fmt"Password updated for user '{rest[0].strip()}'."
      return 0
    except ValueError as e:
      stderr.writeLine("Error: " & e.msg)
      return 1
    except Exception as e:
      stderr.writeLine("Error: " & e.msg)
      return 1
  of "set-email":
    if rest.len != 1:
      stderr.writeLine("Error: usage: jobscheduler user set-email USER --email=EMAIL [--config=PATH]")
      return 2
    if not opts.hasEmail:
      stderr.writeLine("Error: --email=EMAIL is required.")
      return 2
    try:
      let db = openMgmtDb(opts.config)
      defer: db.close()
      discard db.setUserEmail(rest[0], opts.email)
      echo fmt"Email updated for user '{rest[0].strip()}'."
      return 0
    except ValueError as e:
      stderr.writeLine("Error: " & e.msg)
      return 1
    except Exception as e:
      stderr.writeLine("Error: " & e.msg)
      return 1
  else:
    stderr.writeLine("Error: unknown user command: '" & positional[0] & "'.")
    stderr.writeLine("Run 'jobscheduler user help' for usage.")
    return 2
