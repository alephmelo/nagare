package models

import (
	"fmt"
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

func TestGetDagRunsPaginationAndFiltering(t *testing.T) {
	// Use a new shared in-memory db specifically for this test to avoid state bleeding if running in parallel
	store, err := NewStore("file::memory:?cache=shared&mode=memory")
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}
	defer store.Close()

	// Insert 5 runs for dag_A
	for i := 0; i < 5; i++ {
		run := DagRun{
			ID:        fmt.Sprintf("run_A_%d", i),
			DAGID:     "dag_A",
			Status:    RunSuccess,
			ExecDate:  time.Now(),
			CreatedAt: time.Now().Add(time.Duration(-i) * time.Minute), // earlier ones created later to test ordering
		}
		if err := store.CreateDagRun(&run); err != nil {
			t.Fatalf("failed to create dag run: %v", err)
		}
	}

	// Insert 3 runs for dag_B
	for i := 0; i < 3; i++ {
		run := DagRun{
			ID:        fmt.Sprintf("run_B_%d", i),
			DAGID:     "dag_B",
			Status:    RunSuccess,
			ExecDate:  time.Now(),
			CreatedAt: time.Now().Add(time.Duration(-i) * time.Minute),
		}
		if err := store.CreateDagRun(&run); err != nil {
			t.Fatalf("failed to create dag run: %v", err)
		}
	}

	// Test GetDagRunsCount
	countA, err := store.GetDagRunsCount("dag_A")
	if err != nil || countA != 5 {
		t.Errorf("expected 5 runs for dag_A, got %d", countA)
	}

	countAll, err := store.GetDagRunsCount("")
	if err != nil || countAll != 8 {
		t.Errorf("expected 8 total runs, got %d", countAll)
	}

	// Test Pagination for All Runs
	// limit 3, offset 0 -> 3 runs
	runs, err := store.GetDagRuns(3, 0, "")
	if err != nil || len(runs) != 3 {
		t.Errorf("expected 3 runs with limit 3, got %d", len(runs))
	}

	// Test Pagination with Filtering
	// dag_A, limit 2, offset 2 -> should get 2 runs
	runsA, err := store.GetDagRuns(2, 2, "dag_A")
	if err != nil || len(runsA) != 2 {
		t.Errorf("expected 2 runs for dag_A with limit 2, got %d", len(runsA))
	}

	// dag_B, limit 5, offset 0 -> should get 3 runs
	runsB, err := store.GetDagRuns(5, 0, "dag_B")
	if err != nil || len(runsB) != 3 {
		t.Errorf("expected 3 runs for dag_B, got %d", len(runsB))
	}
}
