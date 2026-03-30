# nim-jobscheduler

A full-featured job scheduler written in [Nim](https://nim-lang.org/), featuring a web-based management UI, flexible scheduling options, SSH remote execution, email alerting, and a SQLite-backed execution history.

---

## Features

- **Multiple Schedule Types** — cron expressions, daily time-of-day, fixed intervals, on-startup triggers
- **Timezone Awareness** — each task can run in its own IANA timezone
- **Calendar Constraints** — restrict time/interval tasks to a list of allowed dates (e.g. business-day calendars)
- **Local & Remote Execution** — run commands on the local machine or over SSH
- **Sequential & Parallel Jobs** — each task contains one or more jobs that can run sequentially (chained) or in parallel
- **Web UI** — dashboard, task manager, execution history, live log viewer, schedule calendar, user management, API token management
- **REST API** — trigger/cancel tasks and jobs, query stats, stream execution logs via Server-Sent Events
- **Email Alerts** — SMTP notifications on job failure or system errors
- **Hot Reload** — task YAML files are scanned every 15 seconds; changes are applied without restarting the service
- **Execution Cleanup** — configurable log retention with automatic daily pruning
- **Basic Auth** — session-cookie-based authentication with optional API bearer tokens
- **SSL Support** — built with `-d:ssl` for HTTPS

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         nimjobscheduler                          │
│                                                                  │
│  ┌──────────┐   ┌─────────────┐   ┌──────────┐   ┌──────────┐  │
│  │Scheduler │   │  Executor   │   │ Monitor  │   │Web Server│  │
│  │  (core)  │   │             │   │          │   │          │  │
│  └────┬─────┘   └──────┬──────┘   └────┬─────┘   └────┬─────┘  │
│       │                │               │               │        │
│       └────────────────┴───────────────┴───────────────┘        │
│                              │                                   │
│                     ┌────────▼────────┐                          │
│                     │   DB Worker     │                          │
│                     │  (SQLite actor) │                          │
│                     └─────────────────┘                          │
└─────────────────────────────────────────────────────────────────┘
```

Each component runs in its own thread and communicates via typed Nim channels:

| Component | Responsibility |
|---|---|
| **Scheduler** | Evaluates trigger conditions every second and sends jobs to the executor |
| **Executor** | Launches and monitors local (`fork`/`exec`) and SSH processes |
| **Monitor (SchedulerMonitor)** | Scans the tasks directory for YAML changes, sends failure email alerts, cleans up old execution logs |
| **DB Worker** | Serializes all SQLite writes through a single actor thread |
| **Web Server** | Async HTTP server serving the UI and REST API |

---

## Requirements

- Nim ≥ 2.0.0
- A C compiler (standard on Linux/macOS)

### Nim Package Dependencies

| Package | Purpose |
|---|---|
| `yaml` | Parsing task YAML files and the config |
| `db_connector` | SQLite database access |
| `checksums` | MD5 config-hash for hot-reload diffing |
| `smtp` | Email alert delivery |
| `timezones` | IANA timezone database |
| `nimja` | Jinja-style HTML template engine |
| `cligen` | CLI argument parsing |
| `nimcrypto` | Password hashing |

---

## Building

```bash
# Install dependencies
nimble install jobscheduler

# Compile (release build)
cd nim-jobscheduler
nimble build -d:release
```

---

## Running

```bash
./jobscheduler --config=config.yaml
```

If `--config` is omitted, the scheduler looks for `config.yaml` in the current directory.

---

## Configuration

Copy `sample_config.yaml` and edit it to suit your environment:

```bash
cp sample_config.yaml config.yaml
```

### Configuration Reference

```yaml
# Directory where task YAML files are discovered (recursively)
tasksDir: "./tasks"

# Working directory for log files and temporary scripts
workingDir: "~/.jobscheduler/"

# Logging verbosity: DEBUG | INFO | WARN | ERROR
logLevel: "INFO"

database:
  path: "jobscheduler.db"

server:
  host: "0.0.0.0"
  port: 8080
  sslCert: ""           # Path to TLS cert (requires -d:ssl build)
  sslKey: ""            # Path to TLS key
  externalHost: "https://example.com"   # Used in alert emails

smtp:
  enabled: true
  host: "smtp.gmail.com"
  port: 465
  useSSL: true
  password: "your_app_password"
  fromAddr: "you@gmail.com"
  toAddrs: ["you@gmail.com"]

ssh:
  defaultKeyPath: "~/.ssh/id_rsa"

internal:
  logRetentionDays: 30   # Execution logs older than this are purged

auth:
  username: "admin"
  password: "changeme"   # Plain text; stored as a hash in the DB
```

---

## Task Files

Tasks are defined as YAML files placed anywhere inside `tasksDir` (subdirectories are supported and treated as groups).

### Minimal Example

```yaml
# tasks/hello_world.yaml
name: HelloWorld
taskType: local         # local | remote
enabled: true
scheduleType: cron
cronExpr: "*/5 * * * *"   # every 5 minutes

jobs:
  - name: Greet
    command: echo "Hello, World!"
```

### Full Task Schema

| Field | Type | Description |
|---|---|---|
| `name` | string | Unique task name |
| `description` | string | Optional human-readable description |
| `taskType` | `local` \| `remote` | Execution target |
| `enabled` | bool | Whether the task is active (default `true`) |
| `scheduleType` | `cron` \| `time` \| `interval` \| `onstart` | Trigger mode |
| `timezone` | string | IANA timezone (e.g. `America/New_York`), default local |
| `cronExpr` | string | Standard 5-field cron expression (when `scheduleType: cron`) |
| `timeOfDay` | `HH:MM` | Daily trigger time (when `scheduleType: time`) |
| `intervalMinutes` | int | Repeat interval in minutes (when `scheduleType: interval`) |
| `intervalStart` | `HH:MM` | Window start for interval mode |
| `intervalEnd` | `HH:MM` | Window end for interval mode |
| `calendarPath` | string | Path to a date list file (one `YYYY-MM-DD` per line) |
| `parallel` | bool | Run all jobs in parallel instead of sequentially (default `false`) |
| `sshHost` | string | Remote host (when `taskType: remote`) |
| `sshPort` | int | SSH port (default `22`) |
| `sshUser` | string | SSH user |
| `sshKeyPath` | string | Path to SSH private key (falls back to `ssh.defaultKeyPath`) |
| `jobs` | list | One or more jobs to execute |

### Job Schema

```yaml
jobs:
  - name: Step 1
    command: "echo first step"
  - name: Step 2
    command: "echo second step"   # Runs after Step 1 succeeds (sequential mode)
```

### Schedule Types

| Type | Description |
|---|---|
| `cron` | Standard cron expression. Example: `"0 9 * * 1-5"` (weekdays at 9 AM) |
| `time` | Run once per day at a fixed `HH:MM` time |
| `interval` | Repeat every N minutes, optionally within a time window |
| `onstart` | Run once when the scheduler starts (or restarts) |

### Calendar Constraint

For `time` and `interval` tasks you can supply a `calendarPath` pointing to a plain-text file listing allowed run dates:

```
# business_days.txt
2024-01-02
2024-01-03
20240104      # compact format also accepted
```

---

## Web UI

Once running, open `http://localhost:8080` in your browser. Log in with the credentials defined in `config.yaml → auth`.

| Page | URL | Description |
|---|---|---|
| Dashboard | `/dashboard` | Recent execution summary and live stats |
| Tasks | `/tasks` | All configured tasks with next-run times and enable/disable toggles |
| Task Detail | `/task_detail?id=N` | Task info, jobs, recent executions, manual trigger button |
| Executions | `/executions` | Paginated execution history with status/name filters |
| Job History | `/job_history?id=N` | Execution history for a specific job |
| Execution Log | `/execution_log?id=N` | Full log output for a single execution |
| Schedule | `/schedule` | 24-hour forward schedule visualisation |
| Users | `/users` | User management (create / delete / change password) |
| Tokens | `/tokens` | API bearer token management |

---

## REST API

All API endpoints require authentication (Bearer token or session cookie).

### Public Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/isHealthy` | Health check — returns `{"status":"ok","timestamp":"..."}` |

### Authenticated Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/stats` | Execution counts by status |
| `POST` | `/api/trigger_task?id=N` | Manually trigger a task |
| `POST` | `/api/trigger_job?id=N` | Manually trigger a single job |
| `POST` | `/api/cancel_execution?id=N` | Cancel a running execution |
| `POST` | `/api/delete_execution?id=N` | Delete an execution record |
| `POST` | `/api/toggle_task?id=N&enabled=true` | Enable or disable a task |
| `POST` | `/api/delete_task?id=N` | Delete a task |
| `POST` | `/api/save_task[?id=N]` | Create or update a task (JSON body) |
| `POST` | `/api/execution_log?id=N` | Fetch full execution log as JSON |
| `GET` | `/api/stream_execution_log?id=N` | Stream execution log via Server-Sent Events |
| `GET` | `/api/download_log?id=N` | Download log file as an attachment |
| `POST` | `/api/create_user` | Create a new user |
| `POST` | `/api/delete_user` | Delete a user |
| `POST` | `/api/update_user_password` | Change a user's password |
| `POST` | `/api/create_token` | Generate a new API token |
| `POST` | `/api/delete_token` | Delete an API token |

### Example: Trigger a Task via curl

```bash
curl -X POST http://localhost:8080/api/trigger_task?id=1 \
  -H "Authorization: Bearer <your_token>"
```

---

## Project Structure

```
nim-jobscheduler/
├── src/
│   ├── jobscheduler.nim       # Entry point, wires all components together
│   ├── config.nim             # Config file loading (YAML → Config struct)
│   ├── types.nim              # All core types, actors, channel message types
│   ├── database.nim           # SQLite connection helpers
│   ├── orm.nim                # Query helpers / lightweight ORM
│   ├── executor.nim           # Process management (local fork/exec, SSH, cancel)
│   ├── serialize.nim          # JSON / YAML serialization utilities
│   ├── utils.nim              # Logging, crypto helpers, misc utilities
│   ├── timezones.json         # Bundled IANA timezone database
│   ├── scheduler/
│   │   ├── core.nim           # Scheduler loop — evaluates triggers and fires jobs
│   │   ├── cron.nim           # Cron expression parser and next-run calculator
│   │   ├── triggers.nim       # All trigger type logic (cron, time, interval, onstart, calendar)
│   │   └── monitor.nim        # File-system watcher, hot-reload, SMTP alert sender
│   └── web/
│       ├── server.nim         # Async HTTP server with all route handlers
│       ├── auth.nim           # Token generation and password hashing
│       └── webui/
│           ├── views.nim      # Nimja template rendering procedures
│           ├── base.html      # Shared layout template
│           ├── dashboard.html
│           ├── tasks.html
│           ├── task_detail.html
│           ├── task_edit.html
│           ├── executions.html
│           ├── job_history.html
│           ├── log_viewer.html
│           ├── schedule.html
│           ├── users.html
│           ├── tokens.html
│           ├── login.html
│           └── static/
│               ├── style.css
│               ├── pages.css
│               ├── toggles.css
│               └── app.js
├── tasks/                     # Example task definitions
├── sample_config.yaml         # Annotated configuration template
├── jobscheduler.nimble        # Nimble package manifest
└── config.nims                # Nim compiler switches (-d:ssl)
```

---

## License

MIT
