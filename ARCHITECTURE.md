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
There are two core tables:
- `dag_runs`: Tracks entire workflow executions (Status, Execution Date).
- `task_instances`: Tracks individual units of work within a run. 
  - Statuses advance linearly: `pending` -> `queued` -> `running` -> `success` | `failed`.
  - Runs and tasks can also be moved to `cancelled` if terminated by the user.

### 3. The Scheduler (`internal/scheduler/scheduler.go`)
The brain of the operation. It runs on a continuous `time.Ticker` cycle (e.g. every 5 seconds).
On every "tick", the Scheduler assesses the world:
1. **Triggering**: It parses standard cron expressions (`robfig/cron/v3`) to see if any DAGs fall due. If yes, it creates a `DagRun` and inserts all child `TaskInstances` into the DB dynamically set to `pending`.
2. **Dependency Resolution**: It scans pending tasks. It then executes logic to assert whether all of a task's `depends_on` parents have reached a `success` status. If the topological conditions are met, the task status is promoted to `queued`.

### 4. The Executor (`internal/worker/worker.go`)
The muscle. Instead of distributed workers communicating over HTTP/RPC, Nagare uses a localized concurrent worker pool.
- A **Dispatcher** continually polls the SQLite DB for explicitly `queued` tasks and pushes them into a buffered Go **Channel** (`chan models.TaskInstance`).
- A **Worker Pool** (configurable, currently a set of fixed goroutines) constantly listens to this channel.
- Workers pop a task off the channel, update the DB state to `running`, execute the shell command locally via `os/exec`, and report `success` or `failed` back to the store.
- Workers keep a thread-safe registry of active `os/exec.Cmd` processes, mapped to task IDs. This enables the API/Scheduler to signal a process termination (`RunCancelled` / `TaskCancelled`) gracefully.

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
- **`executeAssignment()`**: runs the task via `worker.RunCommand()` (the same shell executor used by local workers); streams log batches to the master every 200 ms; reports the final `TaskResult`.
- **`cancelCheckLoop()`**: polls `/api/workers/tasks/{id}/cancel` every 5 s; cancels the local execution context on detection.

#### `internal/worker/exec.go` — shared execution helpers
Extracted from `worker.go` to be reusable by both local and remote paths:
- **`RunCommand(ctx, cmd, env, timeoutSecs, onLine, onStart)`**: pure shell execution via `sh -c`; places the process in its own group (`Setpgid`) for clean kills; calls `onLine` for each output line and `onStart` after `cmd.Start()`.
- **`PrepareTaskAssignment(run, ti, dag)`**: resolves a `TaskInstance` + `DagRun` into a `TaskAssignment` by looking up the task definition, applying `{{item}}` substitution, and assembling the full environment slice.

#### CLI entry points (`main.go`)
The binary now accepts flags parsed via stdlib `flag`:
- **No flags** (default): `runMaster()` — identical to the previous standalone mode, plus the coordinator is always initialized (zero overhead when no remote workers connect).
- **`--worker --join <addr>`**: `runWorker()` — registers, runs the poll + heartbeat loop, exits cleanly on SIGINT/SIGTERM.

## Developing
We adhere strictly to TDD (`go test ./...` should pass cleanly at all times).
When testing database logic, we utilize SQLite's shared in-memory mode (`file::memory:?cache=shared`) to ensure fast, isolated test runs that don't pollute the disk.