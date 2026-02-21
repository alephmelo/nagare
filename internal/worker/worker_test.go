package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/logbroker"
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

	triggerDAG := func(id string, triggerType string, conf map[string]string) (*models.DagRun, error) {
		return &models.DagRun{ID: "dummy_run", DAGID: id}, nil
	}

	pool := NewPool(store, getDAG, triggerDAG, map[string]int{"default": 1}, logbroker.NewBroker())

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
	triggerDAG := func(id string, triggerType string, conf map[string]string) (*models.DagRun, error) {
		if id == "downstream_dag" {
			triggered = true
		}
		return &models.DagRun{ID: "downstream_run_1", DAGID: id, TriggerType: triggerType}, nil
	}

	pool := NewPool(store, getDAG, triggerDAG, map[string]int{"default": 1}, logbroker.NewBroker())

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
				Command: "bash -c 'if [ \"$TEST_ENV_VAR\" != \"secret_value\" ]; then exit 1; fi; if [ -z \"$NAGARE_EXECUTION_DATE\" ]; then exit 1; fi; if [ -z \"$NAGARE_SCHEDULED_TIME\" ]; then exit 1; fi'",
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

	triggerDAG := func(id string, triggerType string, conf map[string]string) (*models.DagRun, error) {
		return &models.DagRun{ID: "dummy_run", DAGID: id}, nil
	}

	pool := NewPool(store, getDAG, triggerDAG, map[string]int{"default": 1}, logbroker.NewBroker())

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

	triggerDAG := func(id string, triggerType string, conf map[string]string) (*models.DagRun, error) {
		return &models.DagRun{ID: "dummy_run", DAGID: id}, nil
	}

	pool := NewPool(store, getDAG, triggerDAG, map[string]int{"default": 1}, logbroker.NewBroker())

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

	triggerDAG := func(id string, triggerType string, conf map[string]string) (*models.DagRun, error) {
		return &models.DagRun{ID: "dummy_run", DAGID: id}, nil
	}

	pool := NewPool(store, getDAG, triggerDAG, map[string]int{
		"default":     1,
		"custom_pool": 1,
	}, logbroker.NewBroker())

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

// TestWorkerStreamsOutputToBroker verifies that lines are published to the
// broker in real time and that the final output is persisted to the DB.
func TestWorkerStreamsOutputToBroker(t *testing.T) {
	store, _ := models.NewStore("file::memory:?cache=shared")
	defer store.Close()

	dags := map[string]*models.DAGDef{
		"test_dag": {
			ID: "test_dag",
			Tasks: []models.TaskDef{
				{ID: "t1", Command: "echo line1; echo line2; echo line3"},
			},
		},
	}

	broker := logbroker.NewBroker()
	pool := NewPool(store, func(id string) (*models.DAGDef, bool) {
		d, ok := dags[id]
		return d, ok
	}, func(id, tt string, conf map[string]string) (*models.DagRun, error) {
		return &models.DagRun{ID: "dummy", DAGID: id}, nil
	}, map[string]int{"default": 1}, broker)

	_ = store.CreateDagRun(&models.DagRun{ID: "run_s1", DAGID: "test_dag", Status: models.RunRunning})
	_ = store.CreateTaskInstance(&models.TaskInstance{
		ID: "run_s1_t1", RunID: "run_s1", TaskID: "t1", Status: models.TaskQueued,
	})

	// Subscribe before dispatching so we don't miss any lines.
	ch, unsub := broker.Subscribe("run_s1_t1")
	defer unsub()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)

	if err := pool.Dispatch(); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	var received []string
	for line := range ch {
		received = append(received, line)
	}

	if len(received) != 3 {
		t.Fatalf("expected 3 streamed lines, got %d: %v", len(received), received)
	}

	// DB should contain the full concatenated output after completion.
	time.Sleep(50 * time.Millisecond)
	inst, _ := store.GetTaskInstance("run_s1_t1")
	if !strings.Contains(inst.Output, "line1") || !strings.Contains(inst.Output, "line3") {
		t.Errorf("expected DB output to contain all lines, got: %q", inst.Output)
	}
}

// TestWorkerKillPreservesPartialOutput verifies that killing a running task
// saves whatever output was produced before the kill.
func TestWorkerKillPreservesPartialOutput(t *testing.T) {
	store, _ := models.NewStore("file::memory:?cache=shared")
	defer store.Close()

	dags := map[string]*models.DAGDef{
		"test_dag": {
			ID: "test_dag",
			Tasks: []models.TaskDef{
				// Print a line, then sleep so we have time to kill it.
				{ID: "t1", Command: "echo partial_output; sleep 10"},
			},
		},
	}

	broker := logbroker.NewBroker()
	pool := NewPool(store, func(id string) (*models.DAGDef, bool) {
		d, ok := dags[id]
		return d, ok
	}, func(id, tt string, conf map[string]string) (*models.DagRun, error) {
		return &models.DagRun{ID: "dummy", DAGID: id}, nil
	}, map[string]int{"default": 1}, broker)

	_ = store.CreateDagRun(&models.DagRun{ID: "run_k1", DAGID: "test_dag", Status: models.RunRunning})
	_ = store.CreateTaskInstance(&models.TaskInstance{
		ID: "run_k1_t1", RunID: "run_k1", TaskID: "t1", Status: models.TaskQueued,
	})

	// Subscribe before dispatching to ensure we don't miss the first line.
	ch, unsub := broker.Subscribe("run_k1_t1")
	defer unsub()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)

	if err := pool.Dispatch(); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	// Wait until we receive the first line; this guarantees the process is running.
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first output line")
	}

	if err := pool.KillTask("run_k1_t1"); err != nil {
		t.Fatalf("KillTask: %v", err)
	}

	// Wait for the worker goroutine to finish the kill path and persist output.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		inst, err := store.GetTaskInstance("run_k1_t1")
		if err == nil && inst.Status == models.TaskCancelled && strings.Contains(inst.Output, "partial_output") {
			return // success
		}
		time.Sleep(50 * time.Millisecond)
	}

	inst, _ := store.GetTaskInstance("run_k1_t1")
	if inst.Status != models.TaskCancelled {
		t.Errorf("expected cancelled status, got %s", inst.Status)
	}
	if !strings.Contains(inst.Output, "partial_output") {
		t.Errorf("expected partial output to be saved after kill, got: %q", inst.Output)
	}
}
