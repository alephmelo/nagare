# Nagare

## Goal
Build a lean, minimal, single-binary workflow orchestration tool (DAG engine) inspired by Apache Airflow, designed for rapid prototyping and lightweight deployments.

## MVP Nature
- **Core Abstractions**: Simple structs and interfaces for DAGs and Tasks.
- **DAG Definition**: Data-driven DAGs defined in YAML (avoids recompiling the binary for new workflows).
- **State Management**: Embedded SQLite (no need for external databases like Postgres/Redis).
- **Scheduler**: A Go ticker/goroutine that evaluates standard cron expressions.
- **Execution**: Native Go channels and an in-memory worker pool for concurrent task execution (no Celery/RabbitMQ).
- **Task Types**: Starting strictly with executed shell commands/scripts on the host machine.
