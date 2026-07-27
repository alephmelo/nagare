package models

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newRunProgressionStore(t *testing.T) *Store {
	t.Helper()
	store, err := NewStore(filepath.Join(t.TempDir(), "run-progression.db"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return store
}

func createRunProgressionRun(t *testing.T, store *Store, id string, status RunStatus, createdAt time.Time) {
	t.Helper()
	if err := store.CreateDagRun(&DagRun{
		ID:          id,
		DAGID:       "progression-test",
		Status:      status,
		ExecDate:    createdAt,
		TriggerType: "manual",
		CreatedAt:   createdAt,
	}); err != nil {
		t.Fatalf("CreateDagRun(%s): %v", id, err)
	}
}

func TestCompareAndSetDagRunStatusPreservesCompletionAcrossReplaysAndReopens(t *testing.T) {
	store := newRunProgressionStore(t)
	createdAt := time.Date(2026, time.July, 27, 8, 0, 0, 0, time.UTC)
	createRunProgressionRun(t, store, "run-status", RunRunning, createdAt)

	completedAt := createdAt.Add(time.Minute)
	applied, err := store.CompareAndSetDagRunStatus(
		"run-status", RunRunning, RunSuccess, completedAt,
	)
	if err != nil {
		t.Fatalf("complete run: %v", err)
	}
	if !applied {
		t.Fatal("complete run: CAS unexpectedly missed")
	}

	replayedAt := completedAt.Add(time.Hour)
	applied, err = store.CompareAndSetDagRunStatus(
		"run-status", RunSuccess, RunSuccess, replayedAt,
	)
	if err != nil {
		t.Fatalf("replay completion: %v", err)
	}
	if !applied {
		t.Fatal("replay completion: CAS unexpectedly missed")
	}
	run, err := store.GetDagRun("run-status")
	if err != nil {
		t.Fatalf("GetDagRun after replay: %v", err)
	}
	if run.CompletedAt == nil || !run.CompletedAt.Equal(completedAt) {
		t.Fatalf("completion replay changed completed_at: got %v, want %v", run.CompletedAt, completedAt)
	}

	terminalTransitionAt := replayedAt.Add(time.Hour)
	applied, err = store.CompareAndSetDagRunStatus(
		"run-status", RunSuccess, RunFailed, terminalTransitionAt,
	)
	if err != nil {
		t.Fatalf("change terminal status: %v", err)
	}
	if !applied {
		t.Fatal("change terminal status: CAS unexpectedly missed")
	}
	run, err = store.GetDagRun("run-status")
	if err != nil {
		t.Fatalf("GetDagRun after terminal transition: %v", err)
	}
	if run.CompletedAt == nil || !run.CompletedAt.Equal(completedAt) {
		t.Fatalf("terminal transition changed completed_at: got %v, want %v", run.CompletedAt, completedAt)
	}

	reopenedAt := terminalTransitionAt.Add(time.Hour)
	applied, err = store.CompareAndSetDagRunStatus(
		"run-status", RunFailed, RunRunning, reopenedAt,
	)
	if err != nil {
		t.Fatalf("reopen run: %v", err)
	}
	if !applied {
		t.Fatal("reopen run: CAS unexpectedly missed")
	}
	run, err = store.GetDagRun("run-status")
	if err != nil {
		t.Fatalf("GetDagRun after reopen: %v", err)
	}
	if run.CompletedAt != nil {
		t.Fatalf("reopen retained completed_at: %v", run.CompletedAt)
	}

	applied, err = store.CompareAndSetDagRunStatus(
		"run-status", RunSuccess, RunCancelled, reopenedAt.Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("stale transition: %v", err)
	}
	if applied {
		t.Fatal("stale transition unexpectedly applied")
	}
	run, err = store.GetDagRun("run-status")
	if err != nil {
		t.Fatalf("GetDagRun after CAS miss: %v", err)
	}
	if run.Status != RunRunning || run.CompletedAt != nil {
		t.Fatalf("CAS miss mutated run: %+v", run)
	}
}

func TestCompareAndSetDagRunStatusForTaskSnapshot(t *testing.T) {
	t.Run("unchanged snapshot applies", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 9, 0, 0, 0, time.UTC)
		createRunProgressionRun(t, store, "run-current", RunRunning, now)
		if err := store.CreateTaskInstance(&TaskInstance{
			ID:        "run-current_task",
			RunID:     "run-current",
			TaskID:    "task",
			Status:    TaskSuccess,
			Attempt:   1,
			CreatedAt: now,
			UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateTaskInstance: %v", err)
		}
		snapshot, err := store.GetLatestTaskAttempts("run-current")
		if err != nil {
			t.Fatalf("GetLatestTaskAttempts: %v", err)
		}

		applied, err := store.CompareAndSetDagRunStatusForTaskSnapshot(
			"run-current", RunRunning, RunSuccess, now.Add(time.Minute), snapshot,
		)
		if err != nil {
			t.Fatalf("guarded CAS: %v", err)
		}
		if !applied {
			t.Fatal("guarded CAS missed with an unchanged snapshot")
		}
	})

	t.Run("new latest attempt rejects stale snapshot", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 10, 0, 0, 0, time.UTC)
		createRunProgressionRun(t, store, "run-stale-attempt", RunRunning, now)
		if err := store.CreateTaskInstance(&TaskInstance{
			ID:        "run-stale-attempt_task",
			RunID:     "run-stale-attempt",
			TaskID:    "task",
			Status:    TaskSuccess,
			Attempt:   1,
			CreatedAt: now,
			UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateTaskInstance(attempt 1): %v", err)
		}
		snapshot, err := store.GetLatestTaskAttempts("run-stale-attempt")
		if err != nil {
			t.Fatalf("GetLatestTaskAttempts: %v", err)
		}
		if err := store.CreateTaskInstance(&TaskInstance{
			ID:        "run-stale-attempt_task_2",
			RunID:     "run-stale-attempt",
			TaskID:    "task",
			Status:    TaskQueued,
			Attempt:   2,
			CreatedAt: now.Add(time.Minute),
			UpdatedAt: now.Add(time.Minute),
		}); err != nil {
			t.Fatalf("CreateTaskInstance(attempt 2): %v", err)
		}

		applied, err := store.CompareAndSetDagRunStatusForTaskSnapshot(
			"run-stale-attempt", RunRunning, RunSuccess, now.Add(2*time.Minute), snapshot,
		)
		if err != nil {
			t.Fatalf("guarded CAS: %v", err)
		}
		if applied {
			t.Fatal("guarded CAS applied after the latest attempt changed")
		}
		run, err := store.GetDagRun("run-stale-attempt")
		if err != nil {
			t.Fatalf("GetDagRun: %v", err)
		}
		if run.Status != RunRunning || run.CompletedAt != nil {
			t.Fatalf("stale snapshot mutated run: %+v", run)
		}
	})

	t.Run("task state change rejects stale snapshot", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 11, 0, 0, 0, time.UTC)
		createRunProgressionRun(t, store, "run-stale-state", RunRunning, now)
		if err := store.CreateTaskInstance(&TaskInstance{
			ID:        "run-stale-state_task",
			RunID:     "run-stale-state",
			TaskID:    "task",
			Status:    TaskRunning,
			Attempt:   1,
			CreatedAt: now,
			UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateTaskInstance: %v", err)
		}
		snapshot, err := store.GetLatestTaskAttempts("run-stale-state")
		if err != nil {
			t.Fatalf("GetLatestTaskAttempts: %v", err)
		}
		if err := store.UpdateTaskInstanceStatus("run-stale-state_task", TaskSuccess); err != nil {
			t.Fatalf("UpdateTaskInstanceStatus: %v", err)
		}

		applied, err := store.CompareAndSetDagRunStatusForTaskSnapshot(
			"run-stale-state", RunRunning, RunSuccess, now.Add(time.Minute), snapshot,
		)
		if err != nil {
			t.Fatalf("guarded CAS: %v", err)
		}
		if applied {
			t.Fatal("guarded CAS applied after task state changed")
		}
	})
}

func TestEnsureTaskInstanceRejectsDifferentIdentityForLogicalAttempt(t *testing.T) {
	store := newRunProgressionStore(t)
	now := time.Date(2026, time.July, 27, 12, 0, 0, 0, time.UTC)
	createRunProgressionRun(t, store, "run-ensure", RunRunning, now)
	instance := TaskInstance{
		ID:        "run-ensure_task",
		RunID:     "run-ensure",
		TaskID:    "task",
		Status:    TaskQueued,
		Attempt:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	created, err := store.EnsureTaskInstance(&instance)
	if err != nil {
		t.Fatalf("first EnsureTaskInstance: %v", err)
	}
	if !created {
		t.Fatal("first EnsureTaskInstance did not create the row")
	}
	if err := store.UpdateTaskInstanceStatus(instance.ID, TaskRunning); err != nil {
		t.Fatalf("progress task: %v", err)
	}
	created, err = store.EnsureTaskInstance(&instance)
	if err != nil {
		t.Fatalf("same identity replay after progress: %v", err)
	}
	if created {
		t.Fatal("same identity replay reported a new row")
	}

	conflict := instance
	conflict.ID = "different-instance-id"
	created, err = store.EnsureTaskInstance(&conflict)
	if err == nil {
		t.Fatal("different identity for logical attempt was accepted")
	}
	if created {
		t.Fatal("different identity conflict reported a new row")
	}
	if !strings.Contains(err.Error(), "different-instance-id") ||
		!strings.Contains(err.Error(), instance.ID) {
		t.Fatalf("identity conflict error lacks both identities: %v", err)
	}
}

func TestGetMetricsTimeSeriesIncludesSameDateTimestampWithOffset(t *testing.T) {
	store := newRunProgressionStore(t)
	cest := time.FixedZone("CEST", 2*60*60)
	createdAt := time.Date(2026, time.July, 27, 0, 1, 0, 0, cest)
	createRunProgressionRun(t, store, "run-metrics", RunRunning, createdAt)
	if err := store.CreateTaskInstance(&TaskInstance{
		ID:        "run-metrics_task",
		RunID:     "run-metrics",
		TaskID:    "task",
		Status:    TaskSuccess,
		Attempt:   1,
		CreatedAt: createdAt,
		UpdatedAt: createdAt,
	}); err != nil {
		t.Fatalf("CreateTaskInstance: %v", err)
	}
	if err := store.InsertTaskMetrics(&TaskMetrics{
		TaskInstanceID: "run-metrics_task",
		RunID:          "run-metrics",
		DAGID:          "progression-test",
		TaskID:         "task",
		DurationMs:     42,
		ExitCode:       0,
		ExecutorType:   "local",
		CreatedAt:      createdAt,
	}); err != nil {
		t.Fatalf("InsertTaskMetrics: %v", err)
	}

	points, err := store.GetMetricsTimeSeries(
		"progression-test", createdAt.Add(-time.Minute), 10,
	)
	if err != nil {
		t.Fatalf("GetMetricsTimeSeries: %v", err)
	}
	if len(points) != 1 {
		t.Fatalf("GetMetricsTimeSeries returned %d points, want 1", len(points))
	}
	if points[0].RunID != "run-metrics" || points[0].DurationMs != 42 {
		t.Fatalf("unexpected metric point: %+v", points[0])
	}
}
