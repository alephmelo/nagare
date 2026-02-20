package models

import (
	"testing"
	"time"
)

func TestNewStoreAndInitSchema(t *testing.T) {
	// Use a shared in-memory db for testing so the connection pool shares the same db
	store, err := NewStore("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}
	defer store.Close()

	// Verify the tables exist by querying them
	rows, err := store.db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name='dag_runs'")
	if err != nil {
		t.Fatalf("failed to query sqlite_master: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Error("table dag_runs was not created")
	}

	rows2, err := store.db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name='task_instances'")
	if err != nil {
		t.Fatalf("failed to query sqlite_master: %v", err)
	}
	defer rows2.Close()

	if !rows2.Next() {
		t.Error("table task_instances was not created")
	}
}

func TestStoreQueries(t *testing.T) {
	store, err := NewStore("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}
	defer store.Close()

	run := DagRun{
		ID:        "run_1",
		DAGID:     "my_dag",
		Status:    RunRunning,
		ExecDate:  time.Now(),
		CreatedAt: time.Now(),
	}

	if err := store.CreateDagRun(&run); err != nil {
		t.Fatalf("failed to create dag run: %v", err)
	}

	task := TaskInstance{
		ID:        "task_1",
		RunID:     "run_1",
		TaskID:    "my_task",
		Status:    TaskQueued,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if err := store.CreateTaskInstance(&task); err != nil {
		t.Fatalf("failed to create task instance: %v", err)
	}

	queuedTasks, err := store.GetQueuedTasks()
	if err != nil {
		t.Fatalf("failed to get queued tasks: %v", err)
	}

	if len(queuedTasks) != 1 || queuedTasks[0].ID != "task_1" {
		t.Errorf("expected 1 queued task with ID 'task_1', got %+v", queuedTasks)
	}

	if err := store.UpdateTaskInstanceStatus("task_1", TaskRunning); err != nil {
		t.Fatalf("failed to update task status: %v", err)
	}

	queuedTasksEmpty, _ := store.GetQueuedTasks()
	if len(queuedTasksEmpty) != 0 {
		t.Errorf("expected 0 queued tasks, got %d", len(queuedTasksEmpty))
	}
}
