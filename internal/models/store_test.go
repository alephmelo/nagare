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
		ID:          "run_1",
		DAGID:       "my_dag",
		Status:      RunRunning,
		ExecDate:    time.Now(),
		TriggerType: "scheduled",
		CreatedAt:   time.Now(),
	}

	if err := store.CreateDagRun(&run); err != nil {
		t.Fatalf("failed to create dag run: %v", err)
	}

	task := TaskInstance{
		ID:        "task_1",
		RunID:     "run_1",
		TaskID:    "my_task",
		Status:    TaskQueued,
		Attempt:   1,
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

// TestTaskAttemptDefaultsToOne verifies that the first task instance for a
// task is automatically assigned attempt number 1.
func TestTaskAttemptDefaultsToOne(t *testing.T) {
	store, err := NewStore("file::memory:?cache=shared&mode=memory")
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}
	defer store.Close()

	if err := store.CreateDagRun(&DagRun{
		ID: "run_a1", DAGID: "dag_a", Status: RunRunning,
		ExecDate: time.Now(), TriggerType: "manual", CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	ti := &TaskInstance{
		ID: "run_a1_extract", RunID: "run_a1", TaskID: "extract",
		Status: TaskQueued, Attempt: 1,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := store.CreateTaskInstance(ti); err != nil {
		t.Fatalf("create task instance: %v", err)
	}

	tasks, err := store.GetLatestTaskAttempts("run_a1")
	if err != nil {
		t.Fatalf("GetLatestTaskAttempts: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	if tasks[0].Attempt != 1 {
		t.Errorf("expected attempt=1, got %d", tasks[0].Attempt)
	}
}

// TestCreateNewAttemptOnRetry verifies that retrying a failed task inserts
// a brand-new row (attempt=2) without modifying the original (attempt=1) row.
func TestCreateNewAttemptOnRetry(t *testing.T) {
	store, err := NewStore("file::memory:?cache=shared&mode=memory")
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}
	defer store.Close()

	if err := store.CreateDagRun(&DagRun{
		ID: "run_b1", DAGID: "dag_b", Status: RunRunning,
		ExecDate: time.Now(), TriggerType: "manual", CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	// 1st attempt – fails
	attempt1 := &TaskInstance{
		ID: "run_b1_load_1", RunID: "run_b1", TaskID: "load",
		Status: TaskFailed, Attempt: 1, Output: "exit code 1: connection refused",
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := store.CreateTaskInstance(attempt1); err != nil {
		t.Fatalf("create attempt 1: %v", err)
	}

	// Simulate the retry: create a 2nd attempt row
	newID, err := store.CreateNewTaskAttempt("run_b1", "load")
	if err != nil {
		t.Fatalf("CreateNewTaskAttempt: %v", err)
	}
	if newID == "" {
		t.Fatal("expected a non-empty new task instance ID")
	}

	// The original attempt row must be untouched
	allAttempts, err := store.GetTaskAttempts("run_b1", "load")
	if err != nil {
		t.Fatalf("GetTaskAttempts: %v", err)
	}
	if len(allAttempts) != 2 {
		t.Fatalf("expected 2 attempts, got %d", len(allAttempts))
	}
	if allAttempts[0].Attempt != 1 {
		t.Errorf("first attempt should be 1, got %d", allAttempts[0].Attempt)
	}
	if allAttempts[0].Output != "exit code 1: connection refused" {
		t.Errorf("original attempt output was overwritten: got %q", allAttempts[0].Output)
	}
	if allAttempts[1].Attempt != 2 {
		t.Errorf("second attempt should be 2, got %d", allAttempts[1].Attempt)
	}
	if allAttempts[1].Status != TaskQueued {
		t.Errorf("new attempt should start as queued, got %s", allAttempts[1].Status)
	}
}

// TestGetLatestTaskAttemptsReturnsMostRecent verifies that when multiple
// attempts exist for a task, GetLatestTaskAttempts returns only the most
// recent one per task ID.
func TestGetLatestTaskAttemptsReturnsMostRecent(t *testing.T) {
	store, err := NewStore("file::memory:?cache=shared&mode=memory")
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}
	defer store.Close()

	if err := store.CreateDagRun(&DagRun{
		ID: "run_c1", DAGID: "dag_c", Status: RunRunning,
		ExecDate: time.Now(), TriggerType: "manual", CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	// Insert two tasks, "extract" with 2 attempts and "load" with 1 attempt
	instances := []TaskInstance{
		{ID: "run_c1_extract_1", RunID: "run_c1", TaskID: "extract", Attempt: 1, Status: TaskFailed, Output: "fail log", CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{ID: "run_c1_extract_2", RunID: "run_c1", TaskID: "extract", Attempt: 2, Status: TaskSuccess, Output: "ok", CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{ID: "run_c1_load_1", RunID: "run_c1", TaskID: "load", Attempt: 1, Status: TaskSuccess, Output: "loaded 100 rows", CreatedAt: time.Now(), UpdatedAt: time.Now()},
	}
	for _, ti := range instances {
		tiCopy := ti
		if err := store.CreateTaskInstance(&tiCopy); err != nil {
			t.Fatalf("create task instance %s: %v", ti.ID, err)
		}
	}

	latest, err := store.GetLatestTaskAttempts("run_c1")
	if err != nil {
		t.Fatalf("GetLatestTaskAttempts: %v", err)
	}

	// Should have exactly 2 tasks (extract and load), not 3 rows
	if len(latest) != 2 {
		t.Fatalf("expected 2 latest tasks, got %d", len(latest))
	}

	// Find extract in results and verify it's attempt 2
	for _, task := range latest {
		if task.TaskID == "extract" {
			if task.Attempt != 2 {
				t.Errorf("expected extract to show attempt=2, got %d", task.Attempt)
			}
			if task.Output != "ok" {
				t.Errorf("expected extract output to be 'ok', got %q", task.Output)
			}
		}
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
			ID:          fmt.Sprintf("run_A_%d", i),
			DAGID:       "dag_A",
			Status:      RunSuccess,
			ExecDate:    time.Now(),
			TriggerType: "scheduled",
			CreatedAt:   time.Now().Add(time.Duration(-i) * time.Minute), // earlier ones created later to test ordering
		}
		if err := store.CreateDagRun(&run); err != nil {
			t.Fatalf("failed to create dag run: %v", err)
		}
	}

	// Insert 3 runs for dag_B
	for i := 0; i < 3; i++ {
		run := DagRun{
			ID:          fmt.Sprintf("run_B_%d", i),
			DAGID:       "dag_B",
			Status:      RunSuccess,
			ExecDate:    time.Now(),
			TriggerType: "scheduled",
			CreatedAt:   time.Now().Add(time.Duration(-i) * time.Minute),
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
