package scheduler

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/logbroker"
	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/worker"
)

func TestCancelMapParentStopsRunningChildExecutorAndReplays(t *testing.T) {
	store := openControlStore(t, filepath.Join(t.TempDir(), "map-child-executor.db"))
	now := time.Date(2026, time.July, 26, 21, 0, 0, 0, time.UTC)
	createControlRun(t, store, "run-1", models.RunRunning, now)
	createControlAttempt(t, store, models.TaskInstance{
		ID: "map-parent", RunID: "run-1", TaskID: "map",
		Status: models.TaskRunning, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	})
	createControlAttempt(t, store, models.TaskInstance{
		ID: "map-child", RunID: "run-1", TaskID: "map[0]",
		Status: models.TaskQueued, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	})

	startedPath := filepath.Join(t.TempDir(), "started")
	finishedPath := filepath.Join(t.TempDir(), "finished")
	dag := &models.DAGDef{
		ID: "dag",
		Tasks: []models.TaskDef{{
			ID:      "map",
			Type:    "map",
			MapOver: "source",
			Command: fmt.Sprintf("touch %q; sleep 30; touch %q", startedPath, finishedPath),
		}},
	}
	getDAG := func(id string) (*models.DAGDef, bool) {
		return dag, id == dag.ID
	}
	pool := worker.NewPool(
		store,
		getDAG,
		func(string, string, map[string]string) (*models.DagRun, error) { return nil, nil },
		map[string]int{"default": 1},
		logbroker.NewBroker(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)
	defer func() {
		cancel()
		if err := pool.Stop(); err != nil {
			t.Errorf("stop worker pool: %v", err)
		}
	}()

	if err := pool.Dispatch(); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	waitForFile(t, startedPath)

	sched := NewScheduler(store)
	sched.dags["dag"] = dag
	if err := sched.CancelTask("run-1", "map", pool); err != nil {
		t.Fatalf("CancelTask(map parent): %v", err)
	}
	if err := sched.CancelTask("run-1", "map", pool); err != nil {
		t.Fatalf("replayed CancelTask(map parent): %v", err)
	}

	time.Sleep(150 * time.Millisecond)
	if _, err := os.Stat(finishedPath); !os.IsNotExist(err) {
		t.Fatalf("child executor reached post-cancellation marker: %v", err)
	}
	for _, attemptID := range []string{"map-parent", "map-child"} {
		attempt, err := store.GetTaskInstance(attemptID)
		if err != nil {
			t.Fatalf("GetTaskInstance(%s): %v", attemptID, err)
		}
		if attempt.Status != models.TaskCancelled {
			t.Fatalf("%s status = %q, want cancelled", attemptID, attempt.Status)
		}
	}
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat %s: %v", path, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}
