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

	getDAG := func(id string) (*models.DAGDef, bool) {
		d, ok := dags[id]
		return d, ok
	}

	triggerDAG := func(id string, triggerType string) (*models.DagRun, error) {
		return &models.DagRun{ID: "dummy_run", DAGID: id}, nil
	}

	pool := NewPool(store, getDAG, triggerDAG, map[string]int{"default": 1})

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

func TestWorkerPoolTriggerDagExecution(t *testing.T) {
	store, _ := models.NewStore("file::memory:?cache=shared")
	defer store.Close()

	dags := make(map[string]*models.DAGDef)
	dags["test_dag"] = &models.DAGDef{
		ID: "test_dag",
		Tasks: []models.TaskDef{
			{ID: "t1", Type: "trigger_dag", DagID: "downstream_dag"},
		},
	}

	getDAG := func(id string) (*models.DAGDef, bool) {
		d, ok := dags[id]
		return d, ok
	}

	triggered := false
	triggerDAG := func(id string, triggerType string) (*models.DagRun, error) {
		if id == "downstream_dag" {
			triggered = true
		}
		return &models.DagRun{ID: "downstream_run_1", DAGID: id, TriggerType: triggerType}, nil
	}

	pool := NewPool(store, getDAG, triggerDAG, map[string]int{"default": 1})

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

	if err := pool.Dispatch(); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	status, _ := store.GetTaskStatus("run_1", "t1")
	if status != models.TaskSuccess {
		t.Errorf("expected task status to be success, got %s", status)
	}

	if !triggered {
		t.Errorf("expected downstream_dag to be triggered")
	}
}

func TestWorkerPoolExecutionEnv(t *testing.T) {
	store, _ := models.NewStore("file::memory:?cache=shared")
	defer store.Close()

	dags := make(map[string]*models.DAGDef)
	dags["test_dag"] = &models.DAGDef{
		ID: "test_dag",
		Tasks: []models.TaskDef{
			{
				ID:      "t1",
				Command: "bash -c 'if [ \"$TEST_ENV_VAR\" != \"secret_value\" ]; then exit 1; fi'",
				Env: map[string]string{
					"TEST_ENV_VAR": "secret_value",
				},
			},
		},
	}

	getDAG := func(id string) (*models.DAGDef, bool) {
		d, ok := dags[id]
		return d, ok
	}

	triggerDAG := func(id string, triggerType string) (*models.DagRun, error) {
		return &models.DagRun{ID: "dummy_run", DAGID: id}, nil
	}

	pool := NewPool(store, getDAG, triggerDAG, map[string]int{"default": 1})

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

	if err := pool.Dispatch(); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	status, _ := store.GetTaskStatus("run_1", "t1")
	if status != models.TaskSuccess {
		t.Errorf("expected task status to be success, got %s", status)
	}
}

func TestWorkerPoolExecutionTimeout(t *testing.T) {
	store, _ := models.NewStore("file::memory:?cache=shared")
	defer store.Close()

	dags := make(map[string]*models.DAGDef)
	dags["test_dag"] = &models.DAGDef{
		ID: "test_dag",
		Tasks: []models.TaskDef{
			{
				ID:             "t1",
				Command:        "sleep 2",
				TimeoutSeconds: 1,
			},
		},
	}

	getDAG := func(id string) (*models.DAGDef, bool) {
		d, ok := dags[id]
		return d, ok
	}

	triggerDAG := func(id string, triggerType string) (*models.DagRun, error) {
		return &models.DagRun{ID: "dummy_run", DAGID: id}, nil
	}

	pool := NewPool(store, getDAG, triggerDAG, map[string]int{"default": 1})

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

	if err := pool.Dispatch(); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}

	time.Sleep(1500 * time.Millisecond)

	status, _ := store.GetTaskStatus("run_1", "t1")
	if status != models.TaskFailed {
		t.Errorf("expected task status to be failed, got %s", status)
	}
}

func TestWorkerPoolMultipleQueues(t *testing.T) {
	store, _ := models.NewStore("file::memory:?cache=shared")
	defer store.Close()

	dags := make(map[string]*models.DAGDef)
	dags["test_dag"] = &models.DAGDef{
		ID: "test_dag",
		Tasks: []models.TaskDef{
			{ID: "t_default", Command: "echo hi"},
			{ID: "t_custom", Command: "echo hi", Pool: "custom_pool"},
			{ID: "t_missing", Command: "echo hi", Pool: "missing_pool"},
		},
	}

	getDAG := func(id string) (*models.DAGDef, bool) {
		d, ok := dags[id]
		return d, ok
	}

	triggerDAG := func(id string, triggerType string) (*models.DagRun, error) {
		return &models.DagRun{ID: "dummy_run", DAGID: id}, nil
	}

	pool := NewPool(store, getDAG, triggerDAG, map[string]int{
		"default":     1,
		"custom_pool": 1,
	})

	_ = store.CreateDagRun(&models.DagRun{
		ID:     "run_1",
		DAGID:  "test_dag",
		Status: models.RunRunning,
	})

	_ = store.CreateTaskInstance(&models.TaskInstance{
		ID:     "task_default",
		RunID:  "run_1",
		TaskID: "t_default",
		Status: models.TaskQueued,
	})

	_ = store.CreateTaskInstance(&models.TaskInstance{
		ID:     "task_custom",
		RunID:  "run_1",
		TaskID: "t_custom",
		Status: models.TaskQueued,
	})

	_ = store.CreateTaskInstance(&models.TaskInstance{
		ID:     "task_missing",
		RunID:  "run_1",
		TaskID: "t_missing",
		Status: models.TaskQueued,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)

	if err := pool.Dispatch(); err != nil {
		t.Fatalf("Dispatch failed: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	statusDef, _ := store.GetTaskStatus("run_1", "t_default")
	if statusDef != models.TaskSuccess {
		t.Errorf("expected t_default to be success, got %s", statusDef)
	}

	statusCust, _ := store.GetTaskStatus("run_1", "t_custom")
	if statusCust != models.TaskSuccess {
		t.Errorf("expected t_custom to be success, got %s", statusCust)
	}

	statusMiss, _ := store.GetTaskStatus("run_1", "t_missing")
	if statusMiss != models.TaskFailed {
		t.Errorf("expected t_missing to fail due to no pool, got %s", statusMiss)
	}
}
