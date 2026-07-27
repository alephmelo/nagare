package main

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/scheduler"
	"github.com/alephmelo/nagare/internal/tasklifecycle"
)

func TestRecoverStartupStatePreservesMapSetupAndOrdinaryRetryPolicy(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "startup.db")
	dagsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dagsDir, "startup.yaml"), []byte(`
id: startup
schedule: workflow_dispatch
tasks:
  - id: source
    type: command
  - id: map
    type: map
    map_over: source
    command: echo {{item}}
  - id: ordinary
    type: command
    retries: 1
  - id: queued
    type: command
`), 0o600); err != nil {
		t.Fatalf("write DAG: %v", err)
	}

	initial, err := models.NewStore(dbPath)
	if err != nil {
		t.Fatalf("NewStore(initial): %v", err)
	}
	now := time.Date(2026, time.July, 26, 22, 0, 0, 0, time.UTC)
	if err := initial.CreateDagRun(&models.DagRun{
		ID: "run", DAGID: "startup", Status: models.RunRunning,
		ExecDate: now, TriggerType: "manual", CreatedAt: now,
	}); err != nil {
		t.Fatalf("CreateDagRun: %v", err)
	}
	item := "a"
	for _, attempt := range []models.TaskInstance{
		{ID: "source", RunID: "run", TaskID: "source", Status: models.TaskSuccess, Output: `["a","b"]`, Attempt: 1},
		{ID: "map", RunID: "run", TaskID: "map", Status: models.TaskPending, Attempt: 1},
		{ID: "run_map[0]", RunID: "run", TaskID: "map[0]", Status: models.TaskPending, ItemValue: &item, Attempt: 1},
		{ID: "ordinary", RunID: "run", TaskID: "ordinary", Status: models.TaskRunning, Attempt: 1},
		{ID: "queued", RunID: "run", TaskID: "queued", Status: models.TaskQueued, Attempt: 1},
	} {
		attempt.CreatedAt, attempt.UpdatedAt = now, now
		if err := initial.CreateTaskInstance(&attempt); err != nil {
			t.Fatalf("CreateTaskInstance(%s): %v", attempt.ID, err)
		}
	}
	startedAt := now.Add(time.Minute)
	if _, _, err := initial.EnsureMapSetup(models.MapSetup{
		ParentAttemptID: "map", UpstreamAttemptID: "source",
		UpstreamOutput: `["a","b"]`, StartedAt: startedAt,
	}); err != nil {
		t.Fatalf("EnsureMapSetup: %v", err)
	}
	if disposition, err := tasklifecycle.New(initial).StartSetup("map", startedAt); err != nil ||
		disposition != tasklifecycle.Applied {
		t.Fatalf("StartSetup = (%v, %v), want Applied", disposition, err)
	}
	if err := initial.Close(); err != nil {
		t.Fatalf("Close initial store: %v", err)
	}

	reopened, err := models.NewStore(dbPath)
	if err != nil {
		t.Fatalf("NewStore(reopened): %v", err)
	}
	defer reopened.Close()
	lifecycle := tasklifecycle.New(reopened)
	sched := scheduler.NewSchedulerWithLifecycle(reopened, lifecycle)
	if err := sched.LoadDAGs(dagsDir); err != nil {
		t.Fatalf("LoadDAGs: %v", err)
	}

	reconciled, err := recoverStartupState(reopened, lifecycle, sched)
	if err != nil {
		t.Fatalf("recoverStartupState: %v", err)
	}
	if reconciled != 1 {
		t.Fatalf("reconciled stale attempts = %d, want 1", reconciled)
	}
	ordinary, _ := reopened.GetTaskInstance("ordinary")
	if ordinary.Status != models.TaskUpForRetry {
		t.Fatalf("ordinary status = %s, want up_for_retry", ordinary.Status)
	}
	queued, _ := reopened.GetTaskInstance("queued")
	if queued.Status != models.TaskQueued {
		t.Fatalf("queued status = %s, want queued", queued.Status)
	}
	parent, _ := reopened.GetTaskInstance("map")
	if parent.Status != models.TaskRunning || parent.StartedAt == nil || !parent.StartedAt.Equal(startedAt) {
		t.Fatalf("map parent was not replayed in place: %+v", parent)
	}
	for index, wantItem := range []string{"a", "b"} {
		taskID := "map[" + strconv.Itoa(index) + "]"
		children, err := reopened.GetTaskAttempts("run", taskID)
		if err != nil || len(children) != 1 || children[0].Status != models.TaskQueued ||
			children[0].ItemValue == nil || *children[0].ItemValue != wantItem {
			t.Fatalf("child %s = %+v, %v", taskID, children, err)
		}
	}
	run, _ := reopened.GetDagRun("run")
	if run.Status != models.RunRunning {
		t.Fatalf("run status = %s, want running", run.Status)
	}
}

func TestRecoverStartupStateRetriesWorkerOwnedRunningMapParents(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "worker-owned-map.db")
	dagsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dagsDir, "startup.yaml"), []byte(`
id: startup
schedule: workflow_dispatch
tasks:
  - id: source
    type: command
  - id: map
    type: map
    map_over: source
    retries: 1
`), 0o600); err != nil {
		t.Fatalf("write DAG: %v", err)
	}
	initial, err := models.NewStore(dbPath)
	if err != nil {
		t.Fatalf("NewStore(initial): %v", err)
	}
	now := time.Date(2026, time.July, 26, 23, 0, 0, 0, time.UTC)
	lifecycle := tasklifecycle.New(initial)
	for _, runID := range []string{"no-binding", "mismatched-binding"} {
		if err := initial.CreateDagRun(&models.DagRun{
			ID: runID, DAGID: "startup", Status: models.RunRunning,
			ExecDate: now, TriggerType: "manual", CreatedAt: now,
		}); err != nil {
			t.Fatalf("CreateDagRun(%s): %v", runID, err)
		}
		attemptID := runID + "_map"
		if err := initial.CreateTaskInstance(&models.TaskInstance{
			ID: attemptID, RunID: runID, TaskID: "map",
			Status: models.TaskQueued, Attempt: 1, CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateTaskInstance(%s): %v", attemptID, err)
		}
		if runID == "mismatched-binding" {
			if _, _, err := initial.EnsureMapSetup(models.MapSetup{
				ParentAttemptID: attemptID, UpstreamAttemptID: runID + "_source",
				UpstreamOutput: `["stale"]`, StartedAt: now.Add(time.Minute),
			}); err != nil {
				t.Fatalf("EnsureMapSetup: %v", err)
			}
		}
		workerStartedAt := now.Add(2 * time.Minute)
		if disposition, err := lifecycle.Claim(attemptID, workerStartedAt); err != nil ||
			disposition != tasklifecycle.Applied {
			t.Fatalf("Claim(%s) = (%v, %v), want Applied", attemptID, disposition, err)
		}
	}
	if err := initial.Close(); err != nil {
		t.Fatalf("Close initial store: %v", err)
	}

	reopened, err := models.NewStore(dbPath)
	if err != nil {
		t.Fatalf("NewStore(reopened): %v", err)
	}
	defer reopened.Close()
	reopenedLifecycle := tasklifecycle.New(reopened)
	sched := scheduler.NewSchedulerWithLifecycle(reopened, reopenedLifecycle)
	if err := sched.LoadDAGs(dagsDir); err != nil {
		t.Fatalf("LoadDAGs: %v", err)
	}
	reconciled, err := recoverStartupState(reopened, reopenedLifecycle, sched)
	if err != nil {
		t.Fatalf("recoverStartupState: %v", err)
	}
	if reconciled != 2 {
		t.Fatalf("reconciled attempts = %d, want 2", reconciled)
	}
	for _, runID := range []string{"no-binding", "mismatched-binding"} {
		attempt, err := reopened.GetTaskInstance(runID + "_map")
		if err != nil {
			t.Fatalf("GetTaskInstance(%s): %v", runID, err)
		}
		if attempt.Status != models.TaskUpForRetry ||
			attempt.Output != "task interrupted by master restart" {
			t.Fatalf("%s attempt after recovery = %+v", runID, attempt)
		}
	}
}
