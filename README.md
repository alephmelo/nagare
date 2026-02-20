# Nagare (流れ)

Nagare (Japanese for "flow" or "stream") is a hyper-lean, minimal, single-binary DAG orchestrator and workflow engine. 

It is designed as a lightweight alternative to heavy data pipeline tools like Apache Airflow. It requires zero external dependencies (no Postgres, no Redis, no Python virtual environments) and is built for rapid, robust pipeline deployments.

## Features
- **YAML DAG Definitions**: Workflows are defined as simple data arrays in `.yaml` files. No need to write complex code to define a pipeline.
- **Embedded State Machine**: Uses SQLite natively out-of-the-box for robust, local state tracking of DAG Runs and Task Instances.
- **Cron Scheduling**: Native evaluation of cron schedules to trigger DAGs exactly when they are needed.
- **Topological Execution**: Evaluates parent/child task dependencies natively (e.g. `process_data` waits for `download_data` to finish).
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

---
*For information on how Nagare works under the hood, or if you wish to contribute to the Go codebase, please see [ARCHITECTURE.md](ARCHITECTURE.md).*
