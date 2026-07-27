package scheduler

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/tasklifecycle"
)

func TestRetryTaskReplayRepairsRunStatusWithoutAnotherSuccessor(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "retry-repair.db")
	store := openControlStore(t, dbPath)
	now := time.Date(2026, time.July, 26, 21, 0, 0, 0, time.UTC)
	createControlRun(t, store, "run-1", models.RunFailed, now)
	createControlAttempt(t, store, models.TaskInstance{
		ID: "failed", RunID: "run-1", TaskID: "task",
		Status: models.TaskFailed, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	})
	delegate := tasklifecycle.New(store)
	sched := NewScheduler(store)
	sched.dags["dag"] = &models.DAGDef{
		ID: "dag", Tasks: []models.TaskDef{{ID: "task", Type: "command"}},
	}
	sched.lifecycle = &controlLifecycleFault{
		delegate: delegate,
		afterRetry: func() {
			if err := store.Close(); err != nil {
				t.Errorf("Close after retry: %v", err)
			}
		},
	}

	if err := sched.RetryTask("run-1", "task"); err == nil {
		t.Fatal("first RetryTask unexpectedly succeeded after closing persistence")
	}

	reopened := openControlStore(t, dbPath)
	recovery := NewScheduler(reopened)
	recovery.dags["dag"] = &models.DAGDef{
		ID: "dag", Tasks: []models.TaskDef{{ID: "task", Type: "command"}},
	}
	if err := recovery.RetryTask("run-1", "task"); err != nil {
		t.Fatalf("replayed RetryTask: %v", err)
	}
	run, err := reopened.GetDagRun("run-1")
	if err != nil {
		t.Fatalf("GetDagRun: %v", err)
	}
	if run.Status != models.RunRunning {
		t.Fatalf("run status = %q, want running", run.Status)
	}
	attempts, err := reopened.GetTaskAttempts("run-1", "task")
	if err != nil {
		t.Fatalf("GetTaskAttempts: %v", err)
	}
	if len(attempts) != 2 || attempts[1].Status != models.TaskQueued {
		t.Fatalf("attempts = %#v, want exactly one queued successor", attempts)
	}
}

func TestRetryTaskRejectsOrdinaryInitialPendingAndQueued(t *testing.T) {
	for _, status := range []models.TaskStatus{models.TaskPending, models.TaskQueued} {
		t.Run(string(status), func(t *testing.T) {
			store := openControlStore(t, filepath.Join(t.TempDir(), "initial.db"))
			now := time.Date(2026, time.July, 26, 21, 0, 0, 0, time.UTC)
			createControlRun(t, store, "run-1", models.RunRunning, now)
			createControlAttempt(t, store, models.TaskInstance{
				ID: "initial", RunID: "run-1", TaskID: "task",
				Status: status, Attempt: 1, CreatedAt: now, UpdatedAt: now,
			})

			if err := NewScheduler(store).RetryTask("run-1", "task"); err == nil {
				t.Fatalf("RetryTask accepted ordinary initial %s attempt", status)
			}
			attempts, err := store.GetTaskAttempts("run-1", "task")
			if err != nil {
				t.Fatalf("GetTaskAttempts: %v", err)
			}
			if len(attempts) != 1 || attempts[0].Status != status {
				t.Fatalf("attempts = %#v, want unchanged initial attempt", attempts)
			}
		})
	}

	t.Run("map parent pending", func(t *testing.T) {
		store := openControlStore(t, filepath.Join(t.TempDir(), "initial-map.db"))
		now := time.Date(2026, time.July, 26, 21, 0, 0, 0, time.UTC)
		createControlRun(t, store, "run-1", models.RunRunning, now)
		createControlAttempt(t, store, models.TaskInstance{
			ID: "map", RunID: "run-1", TaskID: "map",
			Status: models.TaskPending, Attempt: 1, CreatedAt: now, UpdatedAt: now,
		})
		sched := NewScheduler(store)
		sched.dags["dag"] = &models.DAGDef{
			ID: "dag", Tasks: []models.TaskDef{{ID: "map", Type: "map", MapOver: "source"}},
		}

		if err := sched.RetryTask("run-1", "map"); err == nil {
			t.Fatal("RetryTask accepted ordinary initial pending map parent")
		}
		attempts, err := store.GetTaskAttempts("run-1", "map")
		if err != nil {
			t.Fatalf("GetTaskAttempts: %v", err)
		}
		if len(attempts) != 1 {
			t.Fatalf("attempt count = %d, want 1", len(attempts))
		}
	})
}

func TestRetryTaskAutomaticallyDoesNotRetryLaterCancellation(t *testing.T) {
	store := openControlStore(t, filepath.Join(t.TempDir(), "automatic-cancel.db"))
	now := time.Date(2026, time.July, 26, 21, 0, 0, 0, time.UTC)
	createControlRun(t, store, "run-1", models.RunRunning, now)
	expected := models.TaskInstance{
		ID: "retryable", RunID: "run-1", TaskID: "task",
		Status: models.TaskUpForRetry, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	}
	createControlAttempt(t, store, expected)
	lifecycle := tasklifecycle.New(store)
	if got, err := lifecycle.CancelCurrent("run-1", "task", now.Add(time.Second)); err != nil || got != tasklifecycle.Applied {
		t.Fatalf("CancelCurrent = (%v, %v), want (Applied, nil)", got, err)
	}

	if err := NewScheduler(store).RetryTaskAutomatically(expected, 0, now.Add(2*time.Second)); err != nil {
		t.Fatalf("RetryTaskAutomatically after cancellation: %v", err)
	}
	attempts, err := store.GetTaskAttempts("run-1", "task")
	if err != nil {
		t.Fatalf("GetTaskAttempts: %v", err)
	}
	if len(attempts) != 1 || attempts[0].Status != models.TaskCancelled {
		t.Fatalf("attempts = %#v, want one cancelled attempt", attempts)
	}
}

func TestRetryTaskAutomaticallyUsesExactSnapshotAndDueTime(t *testing.T) {
	store := openControlStore(t, filepath.Join(t.TempDir(), "automatic-due.db"))
	observedAt := time.Date(2026, time.July, 26, 21, 0, 0, 0, time.UTC)
	delay := 5 * time.Minute
	dueAt := observedAt.Add(delay)
	createControlRun(t, store, "run-1", models.RunRunning, observedAt)
	expected := models.TaskInstance{
		ID: "retryable", RunID: "run-1", TaskID: "task",
		Status: models.TaskUpForRetry, Attempt: 1, CreatedAt: observedAt, UpdatedAt: observedAt,
	}
	createControlAttempt(t, store, expected)
	sched := NewScheduler(store)
	sched.dags["dag"] = &models.DAGDef{
		ID: "dag", Tasks: []models.TaskDef{{ID: "task", Type: "command"}},
	}

	if err := sched.RetryTaskAutomatically(expected, delay, dueAt.Add(-time.Nanosecond)); err != nil {
		t.Fatalf("RetryTaskAutomatically before due time: %v", err)
	}
	stale := expected
	stale.UpdatedAt = stale.UpdatedAt.Add(-time.Nanosecond)
	if err := sched.RetryTaskAutomatically(stale, delay, dueAt); err != nil {
		t.Fatalf("RetryTaskAutomatically with stale snapshot: %v", err)
	}
	attempts, err := store.GetTaskAttempts("run-1", "task")
	if err != nil {
		t.Fatalf("GetTaskAttempts before due retry: %v", err)
	}
	if len(attempts) != 1 {
		t.Fatalf("attempt count before exact due retry = %d, want 1", len(attempts))
	}

	if err := sched.RetryTaskAutomatically(expected, delay, dueAt); err != nil {
		t.Fatalf("RetryTaskAutomatically at due time: %v", err)
	}
	attempts, err = store.GetTaskAttempts("run-1", "task")
	if err != nil {
		t.Fatalf("GetTaskAttempts after due retry: %v", err)
	}
	if len(attempts) != 2 {
		t.Fatalf("attempt count after exact due retry = %d, want 2", len(attempts))
	}
	successor := attempts[1]
	if successor.Status != models.TaskQueued || !successor.CreatedAt.Equal(dueAt) || !successor.UpdatedAt.Equal(dueAt) {
		t.Fatalf("successor = %#v, want queued at exact due time %s", successor, dueAt)
	}
}

func TestRetryTaskAutomaticallySerializesWithCancellation(t *testing.T) {
	for iteration := 0; iteration < 20; iteration++ {
		store := openControlStore(t, filepath.Join(t.TempDir(), "automatic-race.db"))
		now := time.Date(2026, time.July, 26, 21, 0, 0, 0, time.UTC)
		runID := "run-race"
		createControlRun(t, store, runID, models.RunRunning, now)
		expected := models.TaskInstance{
			ID: "retryable", RunID: runID, TaskID: "task",
			Status: models.TaskUpForRetry, Attempt: 1, CreatedAt: now, UpdatedAt: now,
		}
		createControlAttempt(t, store, expected)
		sched := NewScheduler(store)
		sched.dags["dag"] = &models.DAGDef{
			ID: "dag", Tasks: []models.TaskDef{{ID: "task", Type: "command"}},
		}

		start := make(chan struct{})
		errs := make(chan error, 2)
		go func() {
			<-start
			errs <- sched.RetryTaskAutomatically(expected, 0, now.Add(time.Second))
		}()
		go func() {
			<-start
			errs <- sched.CancelTask(runID, "task", nil)
		}()
		close(start)
		for range 2 {
			if err := <-errs; err != nil {
				t.Fatalf("iteration %d control race: %v", iteration, err)
			}
		}

		attempts, err := store.GetTaskAttempts(runID, "task")
		if err != nil {
			t.Fatalf("GetTaskAttempts: %v", err)
		}
		latest := attempts[len(attempts)-1]
		if latest.Status != models.TaskCancelled {
			t.Fatalf("iteration %d latest = %#v, want cancelled", iteration, latest)
		}
	}
}

func TestRetryTaskAutomaticallyPropagatesLifecycleFailure(t *testing.T) {
	store := openControlStore(t, filepath.Join(t.TempDir(), "automatic-error.db"))
	now := time.Date(2026, time.July, 26, 21, 0, 0, 0, time.UTC)
	createControlRun(t, store, "run-1", models.RunRunning, now)
	expected := models.TaskInstance{
		ID: "retryable", RunID: "run-1", TaskID: "task",
		Status: models.TaskUpForRetry, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	}
	createControlAttempt(t, store, expected)
	retryErr := errors.New("retry persistence failed")
	sched := NewScheduler(store)
	sched.dags["dag"] = &models.DAGDef{
		ID: "dag", Tasks: []models.TaskDef{{ID: "task", Type: "command"}},
	}
	sched.lifecycle = &controlLifecycleFault{
		delegate: tasklifecycle.New(store),
		retryErr: retryErr,
	}

	if err := sched.RetryTaskAutomatically(expected, 0, now.Add(time.Second)); !errors.Is(err, retryErr) {
		t.Fatalf("RetryTaskAutomatically error = %v, want retry failure", err)
	}
}

func TestEvaluateCadenceAutomaticRetryDoesNotOverrideCancellation(t *testing.T) {
	store := openControlStore(t, filepath.Join(t.TempDir(), "cadence-cancel.db"))
	observedAt := time.Now().UTC().Add(-time.Minute)
	createControlRun(t, store, "run-1", models.RunRunning, observedAt)
	expected := models.TaskInstance{
		ID: "retryable", RunID: "run-1", TaskID: "task",
		Status: models.TaskUpForRetry, Attempt: 1, CreatedAt: observedAt, UpdatedAt: observedAt,
	}
	createControlAttempt(t, store, expected)

	delegate := tasklifecycle.New(store)
	var cancellationErr error
	sched := NewScheduler(store)
	sched.dags["dag"] = &models.DAGDef{
		ID: "dag",
		Tasks: []models.TaskDef{{
			ID: "task", Retries: 1, RetryDelaySeconds: 0,
		}},
	}
	sched.lifecycle = &controlLifecycleFault{
		delegate: delegate,
		beforeRetry: func() {
			_, cancellationErr = delegate.CancelCurrentAttempt(
				expected.RunID,
				expected.TaskID,
				expected.UpdatedAt.Add(time.Second),
			)
		},
	}

	if err := sched.evaluateRunCompletions(); err != nil {
		t.Fatalf("evaluateRunCompletions: %v", err)
	}
	if cancellationErr != nil {
		t.Fatalf("injected cancellation: %v", cancellationErr)
	}
	attempts, err := store.GetTaskAttempts(expected.RunID, expected.TaskID)
	if err != nil {
		t.Fatalf("GetTaskAttempts: %v", err)
	}
	if len(attempts) != 1 || attempts[0].Status != models.TaskCancelled {
		t.Fatalf("attempts = %#v, want cadence to preserve exact cancellation", attempts)
	}
}

func TestEvaluateRunDoesNotOverwriteKillFromStaleActiveSnapshot(t *testing.T) {
	store := openControlStore(t, filepath.Join(t.TempDir(), "evaluate-kill.db"))
	now := time.Date(2026, time.July, 26, 21, 0, 0, 0, time.UTC)
	createControlRun(t, store, "run-1", models.RunRunning, now)
	createControlAttempt(t, store, models.TaskInstance{
		ID: "successful", RunID: "run-1", TaskID: "task",
		Status: models.TaskSuccess, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	})
	sched := NewScheduler(store)
	sched.dags["dag"] = &models.DAGDef{
		ID: "dag", Tasks: []models.TaskDef{{ID: "task"}},
	}
	active, err := store.GetActiveDagRuns()
	if err != nil || len(active) != 1 {
		t.Fatalf("GetActiveDagRuns = (%#v, %v), want one stale observation", active, err)
	}

	killDone := make(chan error, 1)
	go func() {
		killDone <- sched.KillDagRun("run-1", nil)
	}()
	if err := <-killDone; err != nil {
		t.Fatalf("KillDagRun: %v", err)
	}

	sched.orchestrate.Lock()
	retries, evaluateErr := sched.evaluateRunLocked(active[0], now.Add(time.Second))
	sched.orchestrate.Unlock()
	if evaluateErr != nil {
		t.Fatalf("evaluate stale active observation: %v", evaluateErr)
	}
	if len(retries) != 0 {
		t.Fatalf("stale evaluation retries = %#v, want none", retries)
	}
	run, err := store.GetDagRun("run-1")
	if err != nil {
		t.Fatalf("GetDagRun: %v", err)
	}
	if run.Status != models.RunCancelled {
		t.Fatalf("run status = %q, want cancellation to remain authoritative", run.Status)
	}
}

func TestCancelTaskReplayRetriesExactLocalStop(t *testing.T) {
	store := openControlStore(t, filepath.Join(t.TempDir(), "cancel-replay.db"))
	now := time.Date(2026, time.July, 26, 21, 0, 0, 0, time.UTC)
	createControlRun(t, store, "run-1", models.RunRunning, now)
	createControlAttempt(t, store, models.TaskInstance{
		ID: "exact-attempt", RunID: "run-1", TaskID: "task",
		Status: models.TaskRunning, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	})
	stopErr := errors.New("local stop failed")
	pool := &scriptedControlPool{errs: []error{stopErr, nil}}
	sched := NewScheduler(store)

	if err := sched.CancelTask("run-1", "task", pool); !errors.Is(err, stopErr) {
		t.Fatalf("first CancelTask error = %v, want local stop failure", err)
	}
	if err := sched.CancelTask("run-1", "task", pool); err != nil {
		t.Fatalf("replayed CancelTask: %v", err)
	}
	if len(pool.ids) != 2 || pool.ids[0] != "exact-attempt" || pool.ids[1] != "exact-attempt" {
		t.Fatalf("KillTask IDs = %v, want exact attempt twice", pool.ids)
	}
	attempts, err := store.GetTaskAttempts("run-1", "task")
	if err != nil {
		t.Fatalf("GetTaskAttempts: %v", err)
	}
	if len(attempts) != 1 || attempts[0].Status != models.TaskCancelled {
		t.Fatalf("attempts = %#v, want one cancelled exact attempt", attempts)
	}
}

func TestCancelMapParentCascadesToCurrentGenerationChildren(t *testing.T) {
	store := openControlStore(t, filepath.Join(t.TempDir(), "cancel-map-parent.db"))
	now := time.Date(2026, time.July, 26, 21, 0, 0, 0, time.UTC)
	createControlRun(t, store, "run-1", models.RunRunning, now)
	createControlAttempt(t, store, models.TaskInstance{
		ID: "map-parent", RunID: "run-1", TaskID: "map",
		Status: models.TaskRunning, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	})
	createControlAttempt(t, store, models.TaskInstance{
		ID: "map-child", RunID: "run-1", TaskID: "map[0]",
		Status: models.TaskRunning, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	})
	sched := NewScheduler(store)
	sched.dags["dag"] = &models.DAGDef{
		ID: "dag",
		Tasks: []models.TaskDef{{
			ID: "map", Type: "map", MapOver: "source",
		}},
	}
	childStopErr := errors.New("child executor stop failed")
	pool := &scriptedControlPool{errs: []error{childStopErr, nil, nil, nil}}

	if err := sched.CancelTask("run-1", "map", pool); !errors.Is(err, childStopErr) {
		t.Fatalf("CancelTask(map parent) error = %v, want child stop failure", err)
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
	if err := sched.CancelTask("run-1", "map", pool); err != nil {
		t.Fatalf("replayed CancelTask(map parent): %v", err)
	}
	wantStops := []string{"map-child", "map-parent", "map-child", "map-parent"}
	if len(pool.ids) != len(wantStops) {
		t.Fatalf("KillTask IDs = %v, want %v", pool.ids, wantStops)
	}
	for i := range wantStops {
		if pool.ids[i] != wantStops[i] {
			t.Fatalf("KillTask IDs = %v, want %v", pool.ids, wantStops)
		}
	}
}

func TestKillDagRunContinuesAfterLocalStopFailures(t *testing.T) {
	store := openControlStore(t, filepath.Join(t.TempDir(), "kill-local-errors.db"))
	now := time.Date(2026, time.July, 26, 21, 0, 0, 0, time.UTC)
	createControlRun(t, store, "run-1", models.RunRunning, now)
	for _, taskID := range []string{"first", "second"} {
		createControlAttempt(t, store, models.TaskInstance{
			ID: taskID + "-attempt", RunID: "run-1", TaskID: taskID,
			Status: models.TaskRunning, Attempt: 1, CreatedAt: now, UpdatedAt: now,
		})
	}
	firstErr := errors.New("first local stop failed")
	secondErr := errors.New("second local stop failed")
	pool := &scriptedControlPool{errs: []error{firstErr, secondErr}}

	err := NewScheduler(store).KillDagRun("run-1", pool)
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("KillDagRun error = %v, want both local stop failures", err)
	}
	if len(pool.ids) != 2 {
		t.Fatalf("KillTask calls = %v, want both attempts", pool.ids)
	}
	run, err := store.GetDagRun("run-1")
	if err != nil {
		t.Fatalf("GetDagRun: %v", err)
	}
	if run.Status != models.RunCancelled {
		t.Fatalf("run status = %q, want cancelled after durable cancellations", run.Status)
	}
	for _, taskID := range []string{"first", "second"} {
		attempts, err := store.GetTaskAttempts("run-1", taskID)
		if err != nil {
			t.Fatalf("GetTaskAttempts(%s): %v", taskID, err)
		}
		if len(attempts) != 1 || attempts[0].Status != models.TaskCancelled {
			t.Fatalf("%s attempts = %#v, want preserved cancelled attempt", taskID, attempts)
		}
	}
}

func TestKillDagRunCancelsPersistedMapChildrenWithoutUsableDAG(t *testing.T) {
	for _, test := range []struct {
		name       string
		installDAG bool
	}{
		{name: "missing DAG"},
		{name: "invalid DAG", installDAG: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := openControlStore(t, filepath.Join(t.TempDir(), "kill-map.db"))
			now := time.Date(2026, time.July, 26, 21, 0, 0, 0, time.UTC)
			createControlRun(t, store, "run-1", models.RunRunning, now)
			for _, attempt := range []models.TaskInstance{
				{
					ID: "map-parent", RunID: "run-1", TaskID: "map",
					Status: models.TaskRunning, Attempt: 1, CreatedAt: now, UpdatedAt: now,
				},
				{
					ID: "map-child", RunID: "run-1", TaskID: "map[0]",
					Status: models.TaskRunning, Attempt: 1, CreatedAt: now, UpdatedAt: now,
				},
			} {
				createControlAttempt(t, store, attempt)
			}

			sched := NewScheduler(store)
			if test.installDAG {
				sched.dags["dag"] = &models.DAGDef{
					ID:    "dag",
					Tasks: []models.TaskDef{{ID: "unrelated"}},
				}
			}
			if err := sched.KillDagRun("run-1", nil); err != nil {
				t.Fatalf("KillDagRun: %v", err)
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
			run, err := store.GetDagRun("run-1")
			if err != nil {
				t.Fatalf("GetDagRun: %v", err)
			}
			if run.Status != models.RunCancelled {
				t.Fatalf("run status = %q, want cancelled", run.Status)
			}
		})
	}
}

func TestKillDagRunReplayRetriesCancelledAttemptLocalStop(t *testing.T) {
	store := openControlStore(t, filepath.Join(t.TempDir(), "kill-replay-stop.db"))
	now := time.Date(2026, time.July, 26, 21, 0, 0, 0, time.UTC)
	createControlRun(t, store, "run-1", models.RunRunning, now)
	createControlAttempt(t, store, models.TaskInstance{
		ID: "exact-attempt", RunID: "run-1", TaskID: "task",
		Status: models.TaskRunning, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	})
	stopErr := errors.New("first local stop failed")
	pool := &scriptedControlPool{errs: []error{stopErr, nil}}
	sched := NewScheduler(store)

	if err := sched.KillDagRun("run-1", pool); !errors.Is(err, stopErr) {
		t.Fatalf("first KillDagRun error = %v, want local stop failure", err)
	}
	if err := sched.KillDagRun("run-1", pool); err != nil {
		t.Fatalf("replayed KillDagRun: %v", err)
	}
	if len(pool.ids) != 2 || pool.ids[0] != "exact-attempt" || pool.ids[1] != "exact-attempt" {
		t.Fatalf("KillTask IDs = %v, want exact cancelled attempt twice", pool.ids)
	}
	attempts, err := store.GetTaskAttempts("run-1", "task")
	if err != nil {
		t.Fatalf("GetTaskAttempts: %v", err)
	}
	if len(attempts) != 1 || attempts[0].Status != models.TaskCancelled {
		t.Fatalf("attempts = %#v, want one preserved cancelled attempt", attempts)
	}
}

func TestKillDagRunAggregatesDurableCancellationErrorsAndKeepsRunRunning(t *testing.T) {
	store := openControlStore(t, filepath.Join(t.TempDir(), "kill-durable-errors.db"))
	now := time.Date(2026, time.July, 26, 21, 0, 0, 0, time.UTC)
	createControlRun(t, store, "run-1", models.RunRunning, now)
	for _, taskID := range []string{"first", "second", "third"} {
		createControlAttempt(t, store, models.TaskInstance{
			ID: taskID + "-attempt", RunID: "run-1", TaskID: taskID,
			Status: models.TaskRunning, Attempt: 1, CreatedAt: now, UpdatedAt: now,
		})
	}
	firstErr := errors.New("first cancellation failed")
	secondErr := errors.New("second cancellation failed")
	faults := &controlLifecycleFault{
		delegate: tasklifecycle.New(store),
		exactCancelErrs: map[string]error{
			"first-attempt":  firstErr,
			"second-attempt": secondErr,
		},
	}
	sched := NewScheduler(store)
	sched.lifecycle = faults

	err := sched.KillDagRun("run-1", &scriptedControlPool{})
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("KillDagRun error = %v, want both cancellation failures", err)
	}
	if len(faults.exactCancelCalls) != 3 {
		t.Fatalf("CancelAttempt calls = %v, want every eligible task", faults.exactCancelCalls)
	}
	run, err := store.GetDagRun("run-1")
	if err != nil {
		t.Fatalf("GetDagRun: %v", err)
	}
	if run.Status != models.RunRunning {
		t.Fatalf("run status = %q, want running after incomplete durable cancellation", run.Status)
	}
}

func TestKillDagRunCancelsAttemptsMaterializedDuringEnumeration(t *testing.T) {
	store := openControlStore(t, filepath.Join(t.TempDir(), "kill-fixed-point.db"))
	now := time.Date(2026, time.July, 26, 21, 0, 0, 0, time.UTC)
	createControlRun(t, store, "run-1", models.RunRunning, now)
	createControlAttempt(t, store, models.TaskInstance{
		ID: "parent", RunID: "run-1", TaskID: "map",
		Status: models.TaskRunning, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	})
	item := "late"
	lateChild := models.TaskInstance{
		ID: "late-child", RunID: "run-1", TaskID: "map[0]",
		Status: models.TaskPending, ItemValue: &item, Attempt: 1,
		CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
	}
	faults := &controlLifecycleFault{delegate: tasklifecycle.New(store)}
	faults.afterExactCancel = func(attemptID string) {
		if attemptID != "parent" {
			return
		}
		faults.afterExactCancel = nil
		createControlAttempt(t, store, lateChild)
	}
	sched := NewScheduler(store)
	sched.lifecycle = faults

	if err := sched.KillDagRun("run-1", nil); err != nil {
		t.Fatalf("KillDagRun: %v", err)
	}
	reloaded, err := store.GetTaskInstance(lateChild.ID)
	if err != nil || reloaded.Status != models.TaskCancelled {
		t.Fatalf("late child = %#v, %v, want cancelled", reloaded, err)
	}
	run, err := store.GetDagRun("run-1")
	if err != nil || run.Status != models.RunCancelled {
		t.Fatalf("run = %#v, %v, want cancelled", run, err)
	}
	if len(faults.exactCancelCalls) != 2 {
		t.Fatalf("CancelAttempt calls = %v, want parent and late child", faults.exactCancelCalls)
	}
}

type scriptedControlPool struct {
	mu   sync.Mutex
	errs []error
	ids  []string
}

func (p *scriptedControlPool) KillTask(attemptID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ids = append(p.ids, attemptID)
	if len(p.errs) == 0 {
		return nil
	}
	err := p.errs[0]
	p.errs = p.errs[1:]
	return err
}

type controlLifecycleFault struct {
	delegate         attemptLifecycle
	beforeRetry      func()
	afterRetry       func()
	retryErr         error
	promoteErr       error
	startSetup       func() (tasklifecycle.Disposition, error)
	cancelErrs       map[string]error
	cancelCalls      []string
	exactCancelErrs  map[string]error
	exactCancelCalls []string
	afterExactCancel func(string)
}

func (l *controlLifecycleFault) PromoteGuarded(
	target models.TaskInstance,
	status models.RunStatus,
	tasks []models.TaskInstance,
	at time.Time,
) (tasklifecycle.Disposition, error) {
	if l.promoteErr != nil {
		return 0, l.promoteErr
	}
	return l.delegate.PromoteGuarded(target, status, tasks, at)
}

func (l *controlLifecycleFault) StartSetupGuarded(
	target models.TaskInstance,
	status models.RunStatus,
	tasks []models.TaskInstance,
	setup models.MapSetup,
) (tasklifecycle.Disposition, error) {
	return l.delegate.StartSetupGuarded(target, status, tasks, setup)
}

func (l *controlLifecycleFault) StartSetup(
	id string,
	at time.Time,
) (tasklifecycle.Disposition, error) {
	if l.startSetup != nil {
		start := l.startSetup
		l.startSetup = nil
		return start()
	}
	return l.delegate.StartSetup(id, at)
}

func (l *controlLifecycleFault) Complete(input tasklifecycle.Completion) (tasklifecycle.Disposition, error) {
	return l.delegate.Complete(input)
}

func (l *controlLifecycleFault) CancelAttempt(id string, at time.Time) (tasklifecycle.Disposition, error) {
	l.exactCancelCalls = append(l.exactCancelCalls, id)
	if err := l.exactCancelErrs[id]; err != nil {
		return 0, err
	}
	disposition, err := l.delegate.CancelAttempt(id, at)
	if err == nil && l.afterExactCancel != nil {
		l.afterExactCancel(id)
	}
	return disposition, err
}

func (l *controlLifecycleFault) CancelCurrentAttempt(runID, taskID string, at time.Time) (tasklifecycle.CurrentCancellation, error) {
	l.cancelCalls = append(l.cancelCalls, taskID)
	if err := l.cancelErrs[taskID]; err != nil {
		return tasklifecycle.CurrentCancellation{}, err
	}
	return l.delegate.CancelCurrentAttempt(runID, taskID, at)
}

func (l *controlLifecycleFault) RetryCurrent(runID, taskID string, at time.Time) (tasklifecycle.Disposition, error) {
	if l.retryErr != nil {
		return 0, l.retryErr
	}
	if l.beforeRetry != nil {
		before := l.beforeRetry
		l.beforeRetry = nil
		before()
	}
	disposition, err := l.delegate.RetryCurrent(runID, taskID, at)
	if l.afterRetry != nil {
		after := l.afterRetry
		l.afterRetry = nil
		after()
	}
	return disposition, err
}

func (l *controlLifecycleFault) RetryCurrentPending(runID, taskID string, at time.Time) (tasklifecycle.Disposition, error) {
	if l.retryErr != nil {
		return 0, l.retryErr
	}
	if l.beforeRetry != nil {
		before := l.beforeRetry
		l.beforeRetry = nil
		before()
	}
	disposition, err := l.delegate.RetryCurrentPending(runID, taskID, at)
	if l.afterRetry != nil {
		after := l.afterRetry
		l.afterRetry = nil
		after()
	}
	return disposition, err
}

func openControlStore(t *testing.T, path string) *models.Store {
	t.Helper()
	store, err := models.NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func createControlRun(t *testing.T, store *models.Store, runID string, status models.RunStatus, at time.Time) {
	t.Helper()
	if err := store.CreateDagRun(&models.DagRun{
		ID: runID, DAGID: "dag", Status: status,
		ExecDate: at, TriggerType: "manual", CreatedAt: at,
	}); err != nil {
		t.Fatalf("CreateDagRun: %v", err)
	}
}

func createControlAttempt(t *testing.T, store *models.Store, attempt models.TaskInstance) {
	t.Helper()
	if err := store.CreateTaskInstance(&attempt); err != nil {
		t.Fatalf("CreateTaskInstance(%s): %v", attempt.ID, err)
	}
}
