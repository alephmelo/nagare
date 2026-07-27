package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/logbroker"
	"github.com/alephmelo/nagare/internal/models"
)

func TestClaimedAndEnqueuedCancellationPreventsCommandExecution(t *testing.T) {
	store, err := models.NewStore("file:claimed_enqueued_cancel?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	marker := filepath.Join(t.TempDir(), "executed")
	dag := &models.DAGDef{
		ID: "test_dag",
		Tasks: []models.TaskDef{{
			ID:      "task",
			Command: "touch " + marker,
		}},
	}
	pool := NewPool(
		store,
		func(id string) (*models.DAGDef, bool) { return dag, id == dag.ID },
		func(string, string, map[string]string) (*models.DagRun, error) {
			t.Fatal("command task unexpectedly used triggerDAG")
			return nil, nil
		},
		map[string]int{"default": 1},
		logbroker.NewBroker(),
	)
	createWorkerBehaviorAttempt(t, store, "attempt-command")

	// Dispatch without starting workers so cancellation lands after claim and
	// enqueue, but before running-process registration.
	if err := pool.Dispatch(); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if err := pool.KillTask("attempt-command"); err != nil {
		t.Fatalf("KillTask: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)
	time.Sleep(250 * time.Millisecond)
	cancel()
	pool.Stop()

	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("cancelled attempt executed command; marker stat error = %v", err)
	}
	assertWorkerBehaviorStatus(t, store, "attempt-command", models.TaskCancelled)
}

func TestClaimedAndEnqueuedCancellationPreventsTriggerDAG(t *testing.T) {
	store, err := models.NewStore("file:claimed_enqueued_trigger_cancel?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	triggered := make(chan struct{}, 1)
	dag := &models.DAGDef{
		ID: "test_dag",
		Tasks: []models.TaskDef{{
			ID:    "task",
			Type:  "trigger_dag",
			DagID: "downstream",
		}},
	}
	pool := NewPool(
		store,
		func(id string) (*models.DAGDef, bool) { return dag, id == dag.ID },
		func(id, triggerType string, conf map[string]string) (*models.DagRun, error) {
			triggered <- struct{}{}
			return &models.DagRun{ID: "downstream-run", DAGID: id}, nil
		},
		map[string]int{"default": 1},
		logbroker.NewBroker(),
	)
	createWorkerBehaviorAttempt(t, store, "attempt-trigger")

	if err := pool.Dispatch(); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if err := pool.KillTask("attempt-trigger"); err != nil {
		t.Fatalf("KillTask: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)
	defer func() {
		cancel()
		pool.Stop()
	}()
	select {
	case <-triggered:
		t.Fatal("cancelled attempt triggered downstream DAG")
	case <-time.After(250 * time.Millisecond):
	}

	assertWorkerBehaviorStatus(t, store, "attempt-trigger", models.TaskCancelled)
}

func TestFullLocalQueueLeavesUndispatchedAttemptQueued(t *testing.T) {
	store, err := models.NewStore("file:full_local_queue?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	dag := &models.DAGDef{
		ID: "test_dag",
		Tasks: []models.TaskDef{
			{ID: "first", Command: "true"},
			{ID: "second", Command: "true"},
		},
	}
	pool := NewPool(
		store,
		func(id string) (*models.DAGDef, bool) { return dag, id == dag.ID },
		func(string, string, map[string]string) (*models.DagRun, error) { return nil, nil },
		map[string]int{"default": 1},
		logbroker.NewBroker(),
	)
	createWorkerBehaviorAttemptForTask(t, store, "attempt-first", "first")
	createWorkerBehaviorAttemptForTask(t, store, "attempt-second", "second")

	if err := pool.Dispatch(); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	first, err := store.GetTaskInstance("attempt-first")
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.GetTaskInstance("attempt-second")
	if err != nil {
		t.Fatal(err)
	}
	statuses := make(map[models.TaskStatus]int)
	statuses[first.Status]++
	statuses[second.Status]++
	if statuses[models.TaskRunning] != 1 || statuses[models.TaskQueued] != 1 {
		t.Fatalf("full queue statuses = [%s, %s], want one running and one queued", first.Status, second.Status)
	}
}

func createWorkerBehaviorAttempt(t *testing.T, store *models.Store, id string) {
	t.Helper()
	createWorkerBehaviorAttemptForTask(t, store, id, "task")
}

func createWorkerBehaviorAttemptForTask(t *testing.T, store *models.Store, id, taskID string) {
	t.Helper()
	if err := store.CreateDagRun(&models.DagRun{
		ID: id + "-run", DAGID: "test_dag", Status: models.RunRunning,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTaskInstance(&models.TaskInstance{
		ID: id, RunID: id + "-run", TaskID: taskID, Status: models.TaskQueued, Attempt: 1,
	}); err != nil {
		t.Fatal(err)
	}
}

func assertWorkerBehaviorStatus(t *testing.T, store *models.Store, id string, want models.TaskStatus) {
	t.Helper()
	attempt, err := store.GetTaskInstance(id)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Status != want {
		t.Fatalf("attempt status = %s, want %s", attempt.Status, want)
	}
}
