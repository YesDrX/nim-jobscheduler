# Job Scheduler

A robust, full-featured job scheduler application written in Nim.

## Features

- **Flexible Scheduling**: Support for Cron expressions, specific times of day, fixed intervals, and start/end dates.
- **Task Management**: Organize jobs into tasks. Supports sequential job execution (multi-job tasks).
- **Web Interface**:
  - Dashboard with execution statistics.
  - Task and Job management (Create, Edit, Delete).
  - Execution history and real-time status.
  - Live log viewer for running and completed jobs.
  - User and API Token management.
- **Execution Environments**:
  - **Local**: Execute commands on the host machine.
  - **Remote**: Execute commands via SSH on remote servers (supports key-based auth).
- **Resilience**:
  - PID tracking and process monitoring.
  - Automatic recovery of orphaned jobs on restart.
  - Detached process management (jobs survive scheduler restarts).
- **API**: Full REST API for automation and integration.
- **Security**:
  - Session-based authentication.
  - API Token support (with expiration).
  - Secure password storage.

## Prerequisites

- **Nim** (version 2.0.0 or higher)
- **SQLite3**

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/YesDrX/nim-jobscheduler.git
   cd nim-jobscheduler
   ```

2. Install
   ```bash
   nimble install
   ```

Or

1. Install from nimble registry
   ```bash
   nimble install jobscheduler
   ```

## Configuration

The application requires a `config.yaml` file in the working directory.

Example `config.yaml`:
```yaml
server:
  host: "0.0.0.0"
  port: 8080

database:
  path: "jobscheduler.db"

tasksDir: "tasks"  # Directory to store task YAML files

auth:
  username: "admin"
  password: "adminpassword" # Initial user setup

smtp: # Optional, for alerts
  host: "smtp.example.com"
  port: 587
  username: "user"
  password: "password"
  fromAddr: "scheduler@example.com"
```

## Usage

Run the compiled binary:

```bash
jobscheduler -h
```

Or with a specific config file:

```bash
jobscheduler --configFilename=myconfig.yaml
```

Access the web interface at `http://localhost:8080` (default).

## API Overview

The application provides a comprehensive REST API under `/api/`.

- **Tasks**
  - `GET /api/tasks`: List all tasks.
  - `POST /api/tasks`: Create a new task.
  - `GET /api/tasks/{id}`: Get task details.
  - `PUT /api/tasks/{id}`: Update a task.
  - `DELETE /api/tasks/{id}`: Delete a task.
  - `POST /api/tasks/{id}/trigger`: Manually trigger a task.

- **Jobs**
  - `POST /api/tasks/{id}/jobs`: Add a job to a task.
  - `PUT /api/jobs/{id}`: Update a job.
  - `DELETE /api/jobs/{id}`: Delete a job.
  - `POST /api/jobs/{id}/trigger`: Manually trigger a single job.

- **Executions**
  - `GET /api/executions`: View execution history.
  - `GET /api/logs/{id}`: View execution logs.
  - `POST /api/executions/{id}/cancel`: Cancel a running execution.

## Development

To run the application directly from source:

```bash
nim c -r src/jobscheduler.nim
```

## License

MIT
