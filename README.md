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
- **Container Executor**: Sandbox any task inside a Docker image with a single `image:` field. Supports per-step CPU/memory limits, GPU passthrough, and volume bind-mounts — opt-in, fully backward compatible.
- **Observability Dashboard**: Built-in metrics collection for every task execution — duration, peak memory, and CPU usage — stored in SQLite and surfaced as interactive charts on the `/metrics` dashboard page.
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

### 7. Distributed Workers
Nagare can scale horizontally by running the same binary as a **worker-only node** that connects to a master over HTTP. No external broker or coordination service is needed.

#### Running a master node
The default `./nagare` command already acts as a master. It accepts remote worker connections on the same port (`8080`) as the web UI. No extra flags are needed for a basic setup.

To lock down the cluster with a shared secret:
```bash
./nagare --token mysecret
```

#### Connecting a remote worker
On any machine that can reach the master:
```bash
./nagare --worker --join http://master-host:8080
```

With a token and custom pool:
```bash
./nagare --worker --join http://master-host:8080 --token mysecret --pools gpu,cpu
```

#### All CLI flags
| Flag | Default | Description |
|---|---|---|
| `--worker` | false | Run in worker-only mode |
| `--join` | — | Master address (required in worker mode) |
| `--pools` | `default` | Comma-separated pool names this worker serves |
| `--token` | — | Shared secret (`Authorization: Bearer`) |
| `--port` | `:8080` | Listen address for the master API |
| `--db` | `nagare.db` | SQLite database path |
| `--dags` | `dags` | Directory containing DAG definitions |

#### How it works
- Workers **register** with the master on startup, then **poll** for work every 2 seconds (pull model — no inbound firewall rules needed on workers).
- Workers send **heartbeats** every 10 seconds; the master marks them offline after 60 s of silence.
- Workers stream **log lines** back to the master in batches so the web UI's live log view works identically for remote and local tasks.
- Workers **check for cancellation** every 5 seconds, so killing a run from the UI propagates to remote tasks promptly.
- The master also runs its own **local worker pool** in parallel, so a single-node setup requires no flag changes — the feature is fully backward-compatible.

#### Routing tasks to specific workers
Use `pool:` in your DAG task definition alongside the `--pools` flag on worker nodes:
```yaml
tasks:
  - id: train_model
    type: command
    command: "python train.py"
    pool: gpu          # only dispatched to workers started with --pools gpu
```

### 8. Container Executor
Any task can be sandboxed inside a Docker container by adding an `image:` field. Nagare pulls the image automatically if it is not present locally and streams pull progress to the task log. The container is removed after execution.

```yaml
id: container_demo
description: "Run a Python step inside a Docker container"
schedule: "workflow_dispatch"
tasks:
  - id: run_python
    type: command
    image: "python:3.12-slim"          # Docker image — triggers the container executor
    workdir: "/data"                   # Working directory inside the container
    volumes:
      - "/tmp/input:/data:ro"          # host_path:container_path[:ro|rw]
      - "/tmp/output:/out:rw"
    resources:
      cpus: "1.0"                      # Fractional CPU limit (maps to --cpus)
      memory: "512m"                   # Memory limit: b / k / m / g suffix
      gpus: "all"                      # GPU passthrough: "all" or a positive integer
    command: |
      python3 process.py
```

#### Container field reference

| Field | Required | Description |
|---|---|---|
| `image` | yes (to opt in) | Docker image name, e.g. `python:3.12-slim`, `nvidia/cuda:12.3.1-base-ubuntu22.04` |
| `workdir` | no | Working directory set inside the container (`WORKDIR`) |
| `volumes` | no | List of bind mounts in `host_path:container_path[:ro\|rw]` format |
| `resources.cpus` | no | Fractional CPU count, e.g. `"0.5"`, `"4"` |
| `resources.memory` | no | Memory limit with suffix: `"256m"`, `"2g"`, `"1073741824"` |
| `resources.gpus` | no | `"all"` to pass through every GPU, or a positive integer string (`"1"`, `"2"`) |

**Rules:**
- `image` requires `command` to be set.
- `workdir`, `volumes`, and `resources` all require `image` to be set.
- Tasks without `image` continue to run via the standard `sh -c` local executor — fully backward compatible.
- GPU passthrough requires the [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html) on the host.

See `dags/container_example.yaml` for a working end-to-end example.

---
## Observability

Nagare automatically collects resource metrics for every task execution — no configuration required. Metrics are stored in the embedded SQLite database and exposed via a dedicated dashboard page and REST API.

### What is collected

| Metric | Local process | Docker container |
|---|---|---|
| Wall-clock duration (ms) | yes | yes |
| Peak memory usage (bytes) | yes (`rusage.Maxrss`) | yes (Docker Stats API) |
| CPU user time (ms) | yes | yes (total CPU delta) |
| CPU kernel time (ms) | yes | — |
| Exit code | yes | yes |
| Executor type | `local` | `docker` |

> On Linux, `rusage.Maxrss` is in kilobytes; on macOS it is in bytes. Nagare normalises both to bytes automatically.

### Metrics Dashboard (`/metrics`)

The **Observability → Metrics** page in the web UI provides:

- **Overview stat cards** — total tasks tracked, average duration, peak memory, overall success rate for the selected time window.
- **Duration trend** — area chart of average task duration over time.
- **Memory trend** — bar chart of peak memory usage over time.
- **CPU trend** — area chart of CPU consumption over time.
- **Per-DAG breakdown** — success-rate progress bars and a DAG performance summary table.
- **Slowest tasks** and **recent task executions** tables.

Use the time-window selector (1 h / 6 h / 24 h / 7 d / 30 d) and the DAG filter to narrow the view.

### Inline task metrics

In the **Runs → Run Detail** view, each completed task row displays its duration and peak memory consumption directly next to the task name as small badges — no need to navigate to the metrics page for a quick sanity check.

### Metrics API

| Endpoint | Description |
|---|---|
| `GET /api/metrics/overview?since=24h` | Aggregate stats for the given window |
| `GET /api/metrics/timeseries?since=24h` | Time-series data points (duration, memory, CPU) |
| `GET /api/metrics/tasks/{taskInstanceID}` | Metrics for a specific task instance |
| `GET /api/metrics/runs/{runID}` | All task metrics for a run |
| `GET /api/metrics/dags/{dagID}?since=24h&limit=50` | Per-task metrics for a DAG |

`since` accepts `1h`, `6h`, `24h`, `7d`, or `30d`.

---
## Authentication

By default Nagare starts with all API routes open and logs a warning. To protect the dashboard and API with a shared API key, configure it via any of the following (highest priority first):

**1. CLI flag**
```
./nagare --api-key "your-secret-key"
```

**2. Environment variable**
```
NAGARE_API_KEY="your-secret-key" ./nagare
```

**3. `nagare.yaml`**
```yaml
api_key: "your-secret-key"
```

Once a key is set:
- All `/api/*` routes require `Authorization: Bearer <key>`.
- The web dashboard prompts for the key on first visit and stores it in `localStorage`. A **Disconnect** button in the sidebar clears it.
- `/api/webhooks/` is **exempt** — it uses per-DAG HMAC-SHA256 signature verification instead (see [Zero-Config Webhooks](#6-zero-config-webhooks)).
- Cluster worker routes (`/api/workers/*`) use the separate `--token` flag (unchanged).

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
