package worker

import (
	"context"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/models"
)

func TestWorkerPoolExecution(t *testing.T) {
	store, _ := models.NewStore("file::memory:?cache=shared")
	defer store.Close()

	dags := make(map[string]*models.DAGDef)
	dags["test_dag"] = &models.DAGDef{
		ID: "test_dag",
		Tasks: []models.TaskDef{
			{ID: "t1", Command: "echo hi"},
		},
	}

	pool := NewPool(store, dags, 1)

	// Inject a run and task instance directly
	_ = store.CreateDagRun(&models.DagRun{
		ID:     "run_1",
		DAGID:  "test_dag",
		Status: models.RunRunning,
	})

	_ = store.CreateTaskInstance(&models.TaskInstance{
		ID:     "task_1",
		RunID:  "run_1",
		TaskID: "t1",
		Status: models.TaskQueued,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)

	// Dispatch the task
	if err := pool.Dispatch(); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}

	// Give the worker a moment to execute
	time.Sleep(100 * time.Millisecond)

	// Verify the status changed to Success
	status, _ := store.GetTaskStatus("run_1", "t1")
	if status != models.TaskSuccess {
		t.Errorf("expected task status to be success, got %s", status)
	}
}
