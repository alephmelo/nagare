# Nagare Architecture & Development Guide

This document is for developers contributing to the Nagare Go codebase or anyone curious about how the engine works under the hood.

## Core Philosophy
Nagare is built on the principles of **Simplicity**, **Test-Driven Development (TDD)**, and **Go Idioms**:
- **No BS Code**: We avoid over-engineering. If a piece of code doesn't directly contribute to the MVP goals, it is removed.
- **Embedded State**: We leverage SQLite locally rather than requiring developers to spin up Postgres/Redis Docker containers.
- **Native Concurrency**: We use goroutines and channels for our event loops and worker pools, ignoring heavy message brokers like RabbitMQ or Celery.

## The Four Pillars 

Nagare is broken into four distinct core layers, each with strict boundaries.

### 1. The Core Abstractions (`internal/models/dag.go`)
Instead of heavy Python-style base classes, Nagare uses lightweight Go structs. 
- A `DAGDef` holds metadata and a slice of `TaskDef` objects. 
- DAGs are loaded dynamically by unmarshaling YAML bytes, preventing the need to recompile the engine when a user updates a pipeline.

### 2. The Metadata Database (`internal/models/store.go`)
Workflow orchestration relies on a robust state machine. Nagare uses an embedded **SQLite** database (`mattn/go-sqlite3`). 
There are three core tables:
- `dag_runs`: Tracks entire workflow executions (Status, Execution Date).
- `task_instances`: Tracks individual units of work within a run. 
  - Statuses advance linearly: `pending` -> `queued` -> `running` -> `success` | `failed`.
  - Runs and tasks can also be moved to `cancelled` if terminated by the user.
  - Includes a `started_at` column populated by the worker immediately before execution begins.
- `task_metrics`: Stores post-execution resource measurements for every completed task instance (see [Observability](#6-observability)).

### 3. The Scheduler (`internal/scheduler/scheduler.go`)
The brain of the operation. It runs on a continuous `time.Ticker` cycle (e.g. every 5 seconds).
On every "tick", the Scheduler assesses the world:
1. **Triggering**: It parses standard cron expressions (`robfig/cron/v3`) to see if any DAGs fall due. If yes, it creates a `DagRun` and inserts all child `TaskInstances` into the DB dynamically set to `pending`.
2. **Dependency Resolution**: It scans pending tasks. It then executes logic to assert whether all of a task's `depends_on` parents have reached a `success` status. If the topological conditions are met, the task status is promoted to `queued`.

### 4. The Executor (`internal/worker/`)
The muscle. Instead of distributed workers communicating over HTTP/RPC, Nagare uses a localized concurrent worker pool.
- A **Dispatcher** continually polls the SQLite DB for explicitly `queued` tasks and pushes them into a buffered Go **Channel** (`chan models.TaskInstance`).
- A **Worker Pool** (configurable, currently a set of fixed goroutines) constantly listens to this channel.
- Workers pop a task off the channel, update the DB state to `running`, execute the task via the appropriate executor (see below), and report `success` or `failed` back to the store.
- Workers keep a thread-safe registry of active tasks mapped by task ID, each holding a generic `cancel func()`. This enables the API/Scheduler to signal graceful termination (`RunCancelled` / `TaskCancelled`) for both local and container tasks.

#### Executor interface (`internal/worker/executor.go`)
All execution is routed through a single interface:

```go
type Executor interface {
    Run(ctx context.Context, assignment *TaskAssignment, onLine func(string), onCancel func(cancelFn func())) (RunResult, error)
}
```

`NewExecutor(assignment)` is the factory function. It returns:
- **`DockerExecutor`** — when `assignment.Image != ""`
- **`LocalExecutor`** — otherwise (default, backward-compatible path)

#### `LocalExecutor` (`internal/worker/exec_local.go`)
A thin wrapper around `RunCommand` in `exec.go`. Runs the command via `sh -c` in the host process, placing it in its own process group for clean kills.

#### `DockerExecutor` (`internal/worker/exec_docker.go`)
Runs the command inside an ephemeral Docker container using the **Docker SDK for Go** (`github.com/docker/docker v24`). Lifecycle:
1. **Image pull** — checks local image cache; pulls and streams progress via `onLine` if absent.
2. **Container create** — applies `WorkingDir`, bind-mount `Volumes`, and resource limits (`NanoCPUs`, `Memory`, NVIDIA `DeviceRequests` for GPU passthrough).
3. **Cancel hook** — registers a `ContainerKill(SIGKILL)` callback via `onCancel` before starting, so a UI kill propagates immediately.
4. **Start + log stream** — starts the container and attaches a log reader. Docker multiplexes stdout and stderr into a single stream with an 8-byte frame header per chunk; the internal `dockerLogReader` strips these headers before line scanning.
5. **Wait + remove** — waits for the container to exit, collects the exit code, and force-removes the container via a deferred `ContainerRemove`.

#### Shared execution helpers (`internal/worker/exec.go`)
- **`RunCommand(ctx, cmd, env, timeoutSecs, onLine, onStart)`**: pure shell execution via `sh -c`; places the process in its own group (`Setpgid`) for clean kills; calls `onLine` for each output line and `onStart(cancelFn)` after `cmd.Start()` so the caller can register a kill callback. Records `startTime` before `cmd.Start()` and extracts `ProcessState.SysUsage().(*syscall.Rusage)` after `cmd.Wait()` to populate `RunResult` with `DurationMs`, `PeakMemoryBytes`, `CpuUserMs`, and `CpuSystemMs`. `Maxrss` is normalised from kilobytes (Linux) to bytes (macOS) via a `runtime.GOOS` check.
- **`PrepareTaskAssignment(run, ti, dag)`**: resolves a `TaskInstance` + `DagRun` into a `TaskAssignment` by looking up the task definition, applying `{{item}}` substitution, assembling the full environment slice, and copying container fields (`Image`, `Workdir`, `Volumes`, `Resources`).

### 5. The Cluster Layer (`internal/cluster/`)
Nagare can operate as a multi-node cluster using the same binary for both roles. The cluster layer introduces two new components with zero external dependencies — communication happens over plain HTTP JSON.

#### `internal/cluster/protocol.go` — shared wire types
Defines the structs exchanged between master and worker:
- `WorkerRegistration` / `WorkerInfo` — identity and pool membership
- `PollRequest` / `TaskAssignmentDTO` — pull-model task dispatch
- `TaskResult` — execution outcome (status, output, timed_out flag)
- `LogBatch` — line batches forwarded to the master's log broker
- `CancelCheck` — response to a worker's cancellation poll

#### `internal/cluster/master.go` — `Coordinator`
Runs on the master node alongside the existing local worker pool. Responsibilities:
- **Registration / heartbeat**: upserts `WorkerInfo` into an in-memory map; marks workers offline after a configurable timeout.
- **Poll endpoint** (`POST /api/workers/poll`): atomically claims a `queued` task from the store that matches the requesting worker's pool list and returns a `TaskAssignmentDTO`. Returns `204` when there is no matching work.
- **Result endpoint** (`POST /api/workers/result`): updates `task_instances` status + output; closes the log broker entry so SSE subscribers receive EOF.
- **Log endpoint** (`POST /api/workers/log`): publishes batched lines into the existing `logbroker.Broker` — the web UI's SSE log stream works identically for remote and local tasks.
- **Cancel check** (`GET /api/workers/tasks/{id}/cancel`): workers poll this to detect external kills from the UI.
- **Auth middleware**: wraps all routes with `Authorization: Bearer <token>` enforcement when a token is configured. Empty token = no auth.

`Coordinator.Handler()` returns an `http.Handler` that is mounted onto the main mux by `api.Server.WithCoordinator()` — the existing API is untouched when no coordinator is attached.

#### `internal/cluster/worker.go` — `RemoteWorker`
Runs on worker-only nodes (launched with `--worker --join <addr>`). Responsibilities:
- **`Register()`**: POSTs `WorkerRegistration` on startup.
- **`Run(ctx)`**: starts a heartbeat loop and a poll loop; blocks until the context is cancelled.
- **`PollOnce()`**: POSTs a `PollRequest`; if a `TaskAssignmentDTO` is returned, calls `executeAssignment()` in a goroutine.
- **`executeAssignment()`**: converts the `TaskAssignmentDTO` to a `worker.TaskAssignment`, routes it through `worker.NewExecutor()` (the same factory used by local workers), streams log batches to the master every 200 ms, and reports the final `TaskResult`. Container tasks run inside Docker on the remote worker node, not on the master.
- **`cancelCheckLoop()`**: polls `/api/workers/tasks/{id}/cancel` every 5 s; cancels the local execution context on detection.

#### `internal/worker/exec.go` — shared execution helpers
Extracted from `worker.go` to be reusable by both local and remote paths:
- **`RunCommand(ctx, cmd, env, timeoutSecs, onLine, onStart)`**: pure shell execution via `sh -c`; places the process in its own group (`Setpgid`) for clean kills; calls `onLine` for each output line and `onStart(cancelFn)` after `cmd.Start()` to register a kill callback. Records wall-clock duration and extracts `rusage` metrics after `cmd.Wait()` — see [Observability](#6-observability).
- **`PrepareTaskAssignment(run, ti, dag)`**: resolves a `TaskInstance` + `DagRun` into a `TaskAssignment` by looking up the task definition, applying `{{item}}` substitution, assembling the full environment slice, and copying container fields (`Image`, `Workdir`, `Volumes`, `Resources`).

#### CLI entry points (`main.go`)
The binary now accepts flags parsed via stdlib `flag`:
- **No flags** (default): `runMaster()` — identical to the previous standalone mode, plus the coordinator is always initialized (zero overhead when no remote workers connect).
- **`--worker --join <addr>`**: `runWorker()` — registers, runs the poll + heartbeat loop, exits cleanly on SIGINT/SIGTERM.

## Authentication

Nagare supports a single shared API key that protects all `/api/*` routes and the web dashboard.

### Configuration (highest priority first)
1. `--api-key <key>` CLI flag
2. `NAGARE_API_KEY` environment variable
3. `api_key` field in `nagare.yaml`

When no key is configured the server starts with a warning and all routes remain open — matching the existing behaviour for the cluster `--token` flag.

### Backend — `internal/api/server.go`
`apiKeyMiddleware` is applied to every `/api/*` route **except** `/api/webhooks/` (which retains its existing per-DAG HMAC-SHA256 mechanism). It accepts the key via two paths:

- **`Authorization: Bearer <key>` header** — used by all normal fetch requests.
- **`?token=<key>` query parameter** — fallback for the browser `EventSource` API, which cannot send custom headers on SSE connections.

`subtle.ConstantTimeCompare` is used throughout to prevent timing-based side-channel attacks.

### Frontend — `web/src/`
- **`lib/apiFetch.ts`**: drop-in `fetch` wrapper that reads the key from `localStorage` and injects the `Authorization` header. Fires a global callback on any `401` response.
- **`components/AuthProvider.tsx`**: React context provider that probes `GET /api/stats` on mount. If the server returns `401` it renders a Mantine `PasswordInput` lock screen instead of the app. On successful entry the key is persisted to `localStorage` and the probe is re-run. Any subsequent `401` (e.g. key rotated on the server) clears the stored key and returns the user to the lock screen.
- **`components/MainLayout.tsx`**: shows a **Disconnect** nav item (with `IconLogout`) in the sidebar whenever a key is stored, allowing the user to clear it manually.


We adhere strictly to TDD (`go test ./...` should pass cleanly at all times).
When testing database logic, we utilize SQLite's shared in-memory mode (`file::memory:?cache=shared`) to ensure fast, isolated test runs that don't pollute the disk.

## 6. Observability (`internal/worker/`, `internal/models/store.go`, `internal/api/server.go`)

Nagare automatically records resource metrics for every task execution. The design keeps the zero-external-dependency philosophy intact — all data lands in the existing SQLite database.

### Schema

`task_metrics` is created (and migrated on an existing DB) in `store.go`:

```sql
CREATE TABLE IF NOT EXISTS task_metrics (
    id              TEXT PRIMARY KEY,
    task_instance_id TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    dag_id          TEXT NOT NULL,
    task_id         TEXT NOT NULL,
    started_at      DATETIME,
    duration_ms     INTEGER,
    cpu_user_ms     INTEGER,
    cpu_system_ms   INTEGER,
    peak_memory_bytes INTEGER,
    exit_code       INTEGER,
    executor_type   TEXT,
    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

`task_instances` gains a `started_at` column (added via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`).

### Collection — local processes (`exec.go`)

`RunCommand` captures a `startTime` before `cmd.Start()`. After `cmd.Wait()` returns it:
1. Computes `DurationMs = time.Since(startTime).Milliseconds()`.
2. Casts `cmd.ProcessState.SysUsage()` to `*syscall.Rusage` and reads `Maxrss` (peak RSS) and `Utime`/`Stime` (user/kernel CPU time).
3. Normalises `Maxrss` to bytes: Linux reports kilobytes, macOS reports bytes — a `runtime.GOOS == "linux"` guard multiplies by 1024 on Linux.
4. Returns a populated `RunResult` struct carrying all fields.

### Collection — Docker containers (`exec_docker.go`)

`DockerExecutor.Run` launches a background goroutine before `ContainerStart` that calls `client.ContainerStats` in streaming mode. The goroutine:
- Decodes each `types.StatsJSON` frame.
- Updates `peakMemoryBytes` (via `sync/atomic`) whenever `MemoryStats.Usage` exceeds the current peak.
- Accumulates `totalCPUDelta` from `CPUStats.CPUUsage.TotalUsage`.

After the container exits, `statsCancel()` stops the stream and `statsWg.Wait()` ensures the goroutine has flushed its final frame before the values are read into `RunResult`.

### Persistence (`worker.go`)

`executeTask` calls `store.SetTaskStartedAt` immediately before handing off to the executor, then calls `persistMetrics(result)` after execution. `persistMetrics` builds a `models.TaskMetrics` value and calls `store.InsertTaskMetrics`.

### API (`server.go`)

Five new routes are registered **before** the static-file catch-all:

| Method | Path | Handler |
|---|---|---|
| `GET` | `/api/metrics/overview` | `handleGetMetricsOverview` |
| `GET` | `/api/metrics/timeseries` | `handleGetMetricsTimeSeries` |
| `GET` | `/api/metrics/tasks/{id}` | `handleGetTaskMetrics` |
| `GET` | `/api/metrics/runs/{id}` | `handleGetRunMetrics` |
| `GET` | `/api/metrics/dags/{id}` | `handleGetDAGMetrics` |

All routes accept a `since` query parameter (`1h`, `6h`, `24h`, `7d`, `30d`).

`handleGetRunTasks` is also enriched — it now joins `task_metrics` so each task in the run-detail response carries an inline `Metrics` field consumed by the frontend `TaskRow` component.

### Frontend (`web/src/app/metrics/page.tsx`)

The `/metrics` page is a client component that:
- Calls `/api/metrics/overview` and `/api/metrics/timeseries` on mount and whenever the `since` window or DAG filter changes.
- Renders overview stat cards (Mantine `SimpleGrid` + `Paper`).
- Renders `AreaChart` (duration, CPU) and `BarChart` (memory) from `@mantine/charts` / `recharts`.
- Renders per-DAG success-rate `Progress` bars and summary tables.

`MainLayout.tsx` exposes the page under an **Observability** nav group (`IconChartBar`).

`runs/page.tsx` — `TaskRow` shows duration and peak memory badges inline next to the task name when `task.Metrics` is present, with automatic unit formatting (ms → s, bytes → KB/MB/GB).

---

## 7. Autoscaler (`internal/autoscaler/`)

The autoscaler automatically provisions cloud workers when queue depth exceeds configured thresholds and terminates idle workers to keep infrastructure costs proportional to load. It is fully opt-in (`autoscaler.enabled: false` by default).

### Architecture

```
Coordinator (master)
      │ implements StatsSource
      ▼
  Autoscaler ──── tick loop (every 30s)
      │               │
      │      scale-up │ queued > threshold AND below cap AND past cooldown
      │               ▼
      │         CloudProvider.SpinUp()
      │               │ returns WorkerInstance{Status: provisioning}
      │               ▼
      │         models.cloud_instances (persisted for crash recovery)
      │               │
      │               ▼ registers via POST /api/workers/register
      └──────── Coordinator.Register() → TryClaimWorker() → marks IsCloudManaged=true
                      │
      scale-down       │ idle > ScaleDownIdleMins
      CloudProvider.SpinDown() + TerminateCloudInstance()
```

### Key Types

| Type | File | Purpose |
|---|---|---|
| `Autoscaler` | `autoscaler.go` | Core engine: tick loop, scale-up/down decisions |
| `CloudProvider` | `provider.go` | Interface: `SpinUp`, `SpinDown`, `List`, `Name` |
| `WorkerInstance` | `provider.go` | Describes a cloud-provisioned worker |
| `PoolStats` | `provider.go` | Queue depth + worker counts per pool |
| `InstanceStore` | `autoscaler.go` | Interface for persisting instance lifecycle |
| `StatsSource` | `autoscaler.go` | Interface for queue-depth metrics (Coordinator implements this) |
| `DockerProvider` | `docker.go` | Docker daemon backend (dev/test) |
| `AWSProvider` | `aws.go` | AWS EC2 backend (production) |
| `storeAdapter` | `store_adapter.go` | Bridges `models.Store` → `InstanceStore` (avoids circular import) |

### Database

A `cloud_instances` table is added to the SQLite schema:

```sql
CREATE TABLE cloud_instances (
    id              TEXT PRIMARY KEY,        -- "docker-a3f1b2"
    provider        TEXT NOT NULL,           -- "docker" | "aws"
    provider_id     TEXT NOT NULL,           -- container ID or EC2 instance ID
    worker_id       TEXT NOT NULL DEFAULT '', -- nagare worker ID after registration
    pools           TEXT NOT NULL DEFAULT '[]', -- JSON array
    instance_type   TEXT NOT NULL DEFAULT '',
    region          TEXT NOT NULL DEFAULT '',
    status          TEXT NOT NULL DEFAULT 'provisioning',
    cost_per_hour   REAL NOT NULL DEFAULT 0,
    created_at      DATETIME NOT NULL,
    terminated_at   DATETIME
);
```

On master restart, `reconcile()` reloads active instances from this table so the autoscaler doesn't leak cloud resources after a crash.

### Scale-Up Logic (per pool, per tick)

1. `QueuedTasks > ScaleUpThreshold`
2. Total cloud instances < `MaxCloudWorkers`
3. Time since last scale-up for this pool > `CooldownSecs`

### Scale-Down Logic (per cloud instance, per tick)

1. Instance is in `running` state
2. All pools it serves have 0 queued tasks
3. It has been idle for at least `ScaleDownIdleMins`

### Cloud Worker Correlation

When a cloud-provisioned container boots and calls `POST /api/workers/register`, the Coordinator calls `Autoscaler.TryClaimWorker(workerID, pools)`. The autoscaler scans its in-memory provisioning instances for a pool overlap and returns the `instanceID`. The worker is then tagged `IsCloudManaged=true` and `CloudInstanceID` in its `WorkerInfo`. This is a best-effort heuristic that works correctly when workers register sequentially.

### Configuration (`nagare.yaml`)

```yaml
autoscaler:
  enabled: true
  provider: docker           # "docker" | "aws"
  max_cloud_workers: 5
  scale_up_threshold: 3      # queued tasks needed to trigger scale-up
  cooldown_secs: 60
  scale_down_idle_mins: 5
  docker:
    image: nagare:latest
    network: host
  aws:
    region: us-east-1
    instance_type: t3.medium
    gpu_instance_type: g4dn.xlarge
    ami_id: ami-0abc123
    key_name: my-key
    security_group: sg-abc123
    subnet_id: subnet-abc123
```

### API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/autoscaler/status` | Current autoscaler state snapshot |
| `GET` | `/api/autoscaler/costs` | Aggregate cloud cost summary |
| `POST` | `/api/autoscaler/enable` | Runtime enable (persists across restart only via config) |

### AWS Provider

`NewAWSProvider()` currently returns an error stub pending `go get github.com/aws/aws-sdk-go-v2/...`. Once that dependency is added, the `ec2RealClient` wiring in `aws.go` must be completed. All AWS logic and tests already exist; only the SDK import is missing.
