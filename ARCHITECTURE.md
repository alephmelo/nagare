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

## Developing 
We adhere strictly to TDD (`go test ./...` should pass cleanly at all times).
When testing database logic, we utilize SQLite's shared in-memory mode (`file::memory:?cache=shared`) to ensure fast, isolated test runs that don't pollute the disk.
