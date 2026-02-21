# Nagare (流れ)

Nagare (Japanese for "flow" or "stream") is a hyper-lean, minimal, single-binary DAG orchestrator and workflow engine. 

It is designed as a lightweight alternative to heavy data pipeline tools like Apache Airflow. It requires zero external dependencies (no Postgres, no Redis, no Python virtual environments) and is built for rapid, robust pipeline deployments.

## Features
- **YAML DAG Definitions**: Workflows are defined as simple data arrays in `.yaml` files. No need to write complex code to define a pipeline.
- **Embedded State Machine**: Uses SQLite natively out-of-the-box for robust, local state tracking of DAG Runs and Task Instances.
- **Cron Scheduling**: Native evaluation of cron schedules to trigger DAGs exactly when they are needed.
- **Topological Execution**: Evaluates parent/child task dependencies natively (e.g. `process_data` waits for `download_data` to finish).
- **Environment Variables**: Inject environment variables securely into command executions using the `env` YAML block.
- **Task Timeouts**: Prevent runaway scripts by enforcing local task execution limits via `timeout_seconds: <int>` property in the YAML definition.
- **Catchup / Backfill**: Control whether missed schedule intervals should be skipped or executed using the `catchup` boolean in your DAG definition.
- **Process Management**: Gracefully terminate stuck or runaway DAG runs and individual task instances directly from the UI or via API endpoints.
- **Dynamic Map Tasks**: Fan-out workflows dynamically using standard output JSON arrays (`type: map`), natively supporting the Unix philosophy.
- **Worker Pools (Queues)**: Throttle and route specific tasks (e.g. ML inference) to dedicated worker pools via `pool: <queue_name>` and standard global allocations (`nagare.yaml`).
- **Single Binary Web UI**: The entire Next.js + Mantine dashboard is compiled into static files and embedded directly into the Go binary (`//go:embed all:web/out`). Drop the executable onto a server, and you get a production-ready engine + dashboard on port `8080` instantly.

---

## Quick Start

### 1. Download and Run
Download the latest pre-compiled binary for your OS and architecture from the releases page (or build it yourself via `go build -o nagare .`). 

Make it executable and run the engine:
```bash
chmod +x nagare
./nagare
```
On boot, Nagare will automatically:
- Create the `nagare.db` SQLite database if it doesn't exist.
- Provision the schema.
- Create a `dags/` folder if it doesn't exist.
- Start the cron scheduler, launch the worker pool, and boot the embedded Web UI on `http://localhost:8080`.

### 2. Create your first DAG
Drop a YAML file into the `dags/` folder. Nagare watches this folder and will automatically load new DAG definitions.

Create `dags/hello.yaml`:
```yaml
id: hello_world
description: "A fast test pipeline"
schedule: "* * * * *" # Run every minute
tasks:
  - id: t1
    type: command
    command: "echo 'Step 1 complete'"

  - id: t2
    type: command
    command: "echo 'Step 2 complete'"
    depends_on:
      - t1
```
The scheduler evaluates schedules every 5 seconds. Once the cron condition is met, Nagare queues `t1`, waits for it to succeed, and then queues `t2`.

### 3. Environment Variables
You can inject environment variables into your command tasks securely using the `env` block.
```yaml
id: env_test
description: "A pipeline with env vars"
schedule: "workflow_dispatch"
tasks:
  - id: t1
    type: command
    env:
      DB_HOST: "localhost"
      DB_PASS: "secret"
    command: "python run_migration.py"
```

### 4. Managing Execution
If a step takes too long or you need to cancel a DAG run:
- Navigate to the **Dashboard** (`http://localhost:8080`) to see active runs.
- Click the stop icon next to any `RUNNING` workflow to kill the entire run.
- Click the Run ID to view individual step logs.
- Click the stop icon next to any `RUNNING` step to kill that specific task.

### 5. Dynamic Map Tasks
Nagare allows you to dynamically fan-out a pipeline using the standard output of a previous task. If a task outputs a JSON array of strings, you can iterate over it using a `type: map` task.

```yaml
id: map_reduce
description: "Dynamic map-reduce"
schedule: "workflow_dispatch"
tasks:
  - id: get_files
    type: command
    command: "echo '[\"a.csv\", \"b.csv\"]'"

  - id: process_file
    type: map
    map_over: get_files
    command: "python process.py {{item}}"
```
When `get_files` completes, Nagare will dynamically spin up `process_file[0]` and `process_file[1]`, interpolating `{{item}}` in the command. The downstream tasks will naturally wait for all mapped task instances to finish.

### 6. Zero-Config Webhooks
Because Nagare embeds its own web server, DAGs can define ad-hoc webhook endpoints directly in the YAML using the `trigger` block. When a network request hits the endpoint, Nagare parses the JSON payload using `jq` syntax and injects the extracted fields directly into the shell environment of your tasks.

```yaml
id: webhook_demo
description: "A DAG triggered by a webhook"
schedule: "workflow_dispatch"
trigger:
  type: webhook
  path: "/api/webhooks/github"
  method: "POST"
  extract_payload:
    COMMIT_AUTHOR: ".pusher.name"
    COMMIT_HASH: ".head_commit.id"
tasks:
  - id: print_payload
    type: command
    command: |
      echo "Author: $COMMIT_AUTHOR"
      echo "Commit: $COMMIT_HASH"
```

---
## Development

If you want to contribute to the Nagare codebase and run the Next.js React frontend and Go API backend simultaneously with hot-reloading:

### Prerequisites:
- Go 1.20+
- Node.js & npm

### Running the Dev Environment
We use `mprocs` to multiplex the frontend and backend into a single readable terminal, and `air` to hot-reload the Go binary.

```bash
make dev
```
This command automatically:
1. Installs `mprocs` (via Homebrew if on Mac) and `air` (via `go install`) if you don't have them.
2. Boots up the UI on `http://localhost:3000` (`npm run dev`)
3. Boots up the API on `http://localhost:8080` (`air`)
4. Proxies all frontend `/api/*` requests to the Go backend seamlessly.

> **Note on Ports**: During development, you can access the UI on both ports. 
> - **Port 3000** serves the live Next.js development server with hot-reloading. **Always use this port for UI development.**
> - **Port 8080** runs the Go backend, which serves the static built files from your *last* `make build`. UI changes will not reflect here until you rebuild.


### Exiting the Dev Environment
Inside the `mprocs` TUI:
- Press `<C-a> + q` (Control+A, then Q) to safely kill both the frontend and backend processes and exit the multiplexer.
- You can navigate between the running Frontend and Backend logs using the `↑` and `↓` arrow keys.

---
*For information on how Nagare works under the hood, or if you wish to contribute to the Go codebase, please see [ARCHITECTURE.md](ARCHITECTURE.md).*
