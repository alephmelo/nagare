package tasklifecycle_test

import (
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/tasklifecycle"
)

func TestStartSetupAtomicallyOwnsPendingOrQueuedAttempt(t *testing.T) {
	for _, status := range []models.TaskStatus{models.TaskPending, models.TaskQueued} {
		t.Run(string(status), func(t *testing.T) {
			store := newStore(t)
			createAttempt(t, store, status, 1, "preserved")

			got, err := tasklifecycle.New(store).StartSetup("attempt-1", changedAt)
			if err != nil {
				t.Fatalf("StartSetup: %v", err)
			}
			if got != tasklifecycle.Applied {
				t.Fatalf("StartSetup = %v, want Applied", got)
			}
			attempt := getAttempt(t, store)
			if attempt.Status != models.TaskRunning {
				t.Errorf("status = %q, want running", attempt.Status)
			}
			if attempt.Output != "preserved" {
				t.Errorf("output = %q, want preserved", attempt.Output)
			}
			if attempt.StartedAt == nil || !attempt.StartedAt.Equal(changedAt) {
				t.Errorf("started_at = %v, want %v", attempt.StartedAt, changedAt)
			}
			if !attempt.UpdatedAt.Equal(changedAt) {
				t.Errorf("updated_at = %v, want %v", attempt.UpdatedAt, changedAt)
			}
		})
	}
}

func TestStartSetupReplayDoesNotRewriteOwnership(t *testing.T) {
	store := newStore(t)
	createAttempt(t, store, models.TaskPending, 1, "")
	lifecycle := tasklifecycle.New(store)

	if got, err := lifecycle.StartSetup("attempt-1", changedAt); err != nil || got != tasklifecycle.Applied {
		t.Fatalf("first StartSetup = (%v, %v), want (Applied, nil)", got, err)
	}
	started := snapshot(t, store)

	got, err := lifecycle.StartSetup("attempt-1", changedAt.Add(time.Hour))
	if err != nil {
		t.Fatalf("replayed StartSetup: %v", err)
	}
	if got != tasklifecycle.AlreadyApplied {
		t.Fatalf("replayed StartSetup = %v, want AlreadyApplied", got)
	}
	assertSnapshot(t, store, started)
}

func TestStartSetupRejectsInvalidAndMissingAttempts(t *testing.T) {
	for _, status := range []models.TaskStatus{
		models.TaskSuccess,
		models.TaskFailed,
		models.TaskUpForRetry,
		models.TaskCancelled,
	} {
		t.Run(string(status), func(t *testing.T) {
			store := newStore(t)
			createAttempt(t, store, status, 1, "authoritative")
			before := snapshot(t, store)

			_, err := tasklifecycle.New(store).StartSetup("attempt-1", changedAt)
			var invalid *tasklifecycle.InvalidTransitionError
			if !errors.As(err, &invalid) {
				t.Fatalf("StartSetup error = %T %v, want InvalidTransitionError", err, err)
			}
			assertSnapshot(t, store, before)
		})
	}

	store := newStore(t)
	_, err := tasklifecycle.New(store).StartSetup("missing", changedAt)
	var missing *tasklifecycle.MissingAttemptError
	if !errors.As(err, &missing) {
		t.Fatalf("missing StartSetup error = %T %v, want MissingAttemptError", err, err)
	}
}

func TestConcurrentStartSetupAuthorizesOneOwner(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "setup-race.db")
	first := openStoreAt(t, dbPath)
	second := openStoreAt(t, dbPath)
	insertLogicalAttempt(t, first, "attempt", models.TaskPending, 1, "", createdAt)

	start := make(chan struct{})
	results := make(chan lifecycleResult, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for i, lifecycle := range []*tasklifecycle.Lifecycle{tasklifecycle.New(first), tasklifecycle.New(second)} {
		startedAt := changedAt.Add(time.Duration(i) * time.Second)
		go func(lifecycle *tasklifecycle.Lifecycle, startedAt time.Time) {
			ready.Done()
			<-start
			disposition, err := lifecycle.StartSetup("attempt", startedAt)
			results <- lifecycleResult{disposition, err}
		}(lifecycle, startedAt)
	}
	ready.Wait()
	close(start)

	applied := 0
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent StartSetup: %v", result.err)
		}
		if result.disposition == tasklifecycle.Applied {
			applied++
		}
	}
	if applied != 1 {
		t.Fatalf("Applied owners = %d, want 1", applied)
	}
	attempt, err := first.GetTaskInstance("attempt")
	if err != nil {
		t.Fatalf("GetTaskInstance: %v", err)
	}
	if attempt.Status != models.TaskRunning {
		t.Fatalf("status = %q, want running", attempt.Status)
	}
}

func TestStartSetupVersusWorkerClaimAuthorizesOneOwner(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "setup-claim-race.db")
	setupStore := openStoreAt(t, dbPath)
	workerStore := openStoreAt(t, dbPath)
	insertLogicalAttempt(t, setupStore, "attempt", models.TaskQueued, 1, "", createdAt)

	start := make(chan struct{})
	results := make(chan lifecycleResult, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	go func() {
		ready.Done()
		<-start
		disposition, err := tasklifecycle.New(setupStore).StartSetup("attempt", changedAt)
		results <- lifecycleResult{disposition, err}
	}()
	go func() {
		ready.Done()
		<-start
		disposition, err := tasklifecycle.New(workerStore).Claim("attempt", changedAt.Add(time.Second))
		results <- lifecycleResult{disposition, err}
	}()
	ready.Wait()
	close(start)

	applied := 0
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("setup/claim race: %v", result.err)
		}
		if result.disposition == tasklifecycle.Applied {
			applied++
		}
	}
	if applied != 1 {
		t.Fatalf("Applied owners = %d, want 1", applied)
	}
}

func TestRetryCurrentPendingCreatesIdempotentPendingSuccessor(t *testing.T) {
	store := newStore(t)
	item := "mapped item"
	if err := store.CreateTaskInstance(&models.TaskInstance{
		ID: "predecessor", RunID: "run-1", TaskID: "task-1",
		Status: models.TaskFailed, Output: "failure", ItemValue: &item, Attempt: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}); err != nil {
		t.Fatalf("CreateTaskInstance: %v", err)
	}
	lifecycle := tasklifecycle.New(store)

	first, err := lifecycle.RetryCurrentPending("run-1", "task-1", changedAt)
	if err != nil {
		t.Fatalf("first RetryCurrentPending: %v", err)
	}
	if first != tasklifecycle.Applied {
		t.Fatalf("first RetryCurrentPending = %v, want Applied", first)
	}
	history := attempts(t, store)
	if len(history) != 2 {
		t.Fatalf("attempt count = %d, want 2", len(history))
	}
	successor := history[1]
	if successor.Status != models.TaskPending || successor.Attempt != 2 {
		t.Fatalf("successor = %#v, want pending attempt 2", successor)
	}
	if successor.ItemValue == nil || *successor.ItemValue != item {
		t.Fatalf("successor item = %v, want %q", successor.ItemValue, item)
	}

	replay, err := lifecycle.RetryCurrentPending("run-1", "task-1", changedAt.Add(time.Hour))
	if err != nil {
		t.Fatalf("replayed RetryCurrentPending: %v", err)
	}
	if replay != tasklifecycle.AlreadyApplied {
		t.Fatalf("replayed RetryCurrentPending = %v, want AlreadyApplied", replay)
	}
	if after := attempts(t, store); !reflect.DeepEqual(after, history) {
		t.Errorf("replayed pending retry mutated history\n got: %#v\nwant: %#v", after, history)
	}
}

func TestRetryCurrentPendingPreservesCancellationOrdering(t *testing.T) {
	store := newStore(t)
	insertLogicalAttempt(t, store, "current", models.TaskUpForRetry, 1, "failure", createdAt)
	lifecycle := tasklifecycle.New(store)

	if got, err := lifecycle.CancelCurrent("run-1", "task-1", changedAt); err != nil || got != tasklifecycle.Applied {
		t.Fatalf("CancelCurrent = (%v, %v), want (Applied, nil)", got, err)
	}
	got, err := lifecycle.RetryCurrentPending("run-1", "task-1", changedAt)
	if err != nil {
		t.Fatalf("same-instant RetryCurrentPending: %v", err)
	}
	if got != tasklifecycle.AlreadyApplied {
		t.Fatalf("same-instant RetryCurrentPending = %v, want AlreadyApplied", got)
	}
	if history := attempts(t, store); len(history) != 1 || history[0].Status != models.TaskCancelled {
		t.Fatalf("same-instant history = %#v, want one cancelled attempt", history)
	}

	got, err = lifecycle.RetryCurrentPending("run-1", "task-1", changedAt.Add(time.Nanosecond))
	if err != nil {
		t.Fatalf("later RetryCurrentPending: %v", err)
	}
	if got != tasklifecycle.Applied {
		t.Fatalf("later RetryCurrentPending = %v, want Applied", got)
	}
	history := attempts(t, store)
	if len(history) != 2 || history[1].Status != models.TaskPending {
		t.Fatalf("later retry history = %#v, want pending successor", history)
	}
}

func TestConcurrentRetryCurrentPendingInsertsOneSuccessor(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pending-retry-race.db")
	first := openStoreAt(t, dbPath)
	second := openStoreAt(t, dbPath)
	insertLogicalAttempt(t, first, "failed", models.TaskFailed, 1, "failure", createdAt)

	start := make(chan struct{})
	results := make(chan lifecycleResult, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for _, lifecycle := range []*tasklifecycle.Lifecycle{tasklifecycle.New(first), tasklifecycle.New(second)} {
		go func(lifecycle *tasklifecycle.Lifecycle) {
			ready.Done()
			<-start
			disposition, err := lifecycle.RetryCurrentPending("run-1", "task-1", changedAt)
			results <- lifecycleResult{disposition, err}
		}(lifecycle)
	}
	ready.Wait()
	close(start)

	applied := 0
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent RetryCurrentPending: %v", result.err)
		}
		if result.disposition == tasklifecycle.Applied {
			applied++
		}
	}
	if applied != 1 {
		t.Fatalf("Applied pending retries = %d, want 1", applied)
	}
	history := attempts(t, first)
	if len(history) != 2 || history[1].Status != models.TaskPending {
		t.Fatalf("history = %#v, want one pending successor", history)
	}
}

func TestRetryCurrentPendingRejectsQueuedRunningAndMissing(t *testing.T) {
	for _, status := range []models.TaskStatus{models.TaskQueued, models.TaskRunning} {
		t.Run(string(status), func(t *testing.T) {
			store := newStore(t)
			insertLogicalAttempt(t, store, "current", status, 1, "preserved", createdAt)
			before := attempts(t, store)

			_, err := tasklifecycle.New(store).RetryCurrentPending("run-1", "task-1", changedAt)
			var invalid *tasklifecycle.InvalidTransitionError
			if !errors.As(err, &invalid) {
				t.Fatalf("RetryCurrentPending error = %T %v, want InvalidTransitionError", err, err)
			}
			if after := attempts(t, store); !reflect.DeepEqual(after, before) {
				t.Errorf("invalid pending retry mutated history\n got: %#v\nwant: %#v", after, before)
			}
		})
	}

	store := newStore(t)
	_, err := tasklifecycle.New(store).RetryCurrentPending("missing-run", "missing-task", changedAt)
	var missing *tasklifecycle.MissingAttemptError
	if !errors.As(err, &missing) {
		t.Fatalf("missing RetryCurrentPending error = %T %v, want MissingAttemptError", err, err)
	}
}

func TestRetryCurrentVariantsDoNotReplayDifferentSuccessorKinds(t *testing.T) {
	t.Run("queued after pending", func(t *testing.T) {
		store := newStore(t)
		insertLogicalAttempt(t, store, "failed", models.TaskFailed, 1, "failure", createdAt)
		lifecycle := tasklifecycle.New(store)
		if _, err := lifecycle.RetryCurrentPending("run-1", "task-1", changedAt); err != nil {
			t.Fatalf("RetryCurrentPending: %v", err)
		}
		before := attempts(t, store)

		_, err := lifecycle.RetryCurrent("run-1", "task-1", changedAt.Add(time.Second))
		var invalid *tasklifecycle.InvalidTransitionError
		if !errors.As(err, &invalid) {
			t.Fatalf("RetryCurrent after pending successor error = %T %v, want InvalidTransitionError", err, err)
		}
		if after := attempts(t, store); !reflect.DeepEqual(after, before) {
			t.Errorf("cross-variant retry mutated history\n got: %#v\nwant: %#v", after, before)
		}
	})

	t.Run("pending after queued", func(t *testing.T) {
		store := newStore(t)
		insertLogicalAttempt(t, store, "failed", models.TaskFailed, 1, "failure", createdAt)
		lifecycle := tasklifecycle.New(store)
		if _, err := lifecycle.RetryCurrent("run-1", "task-1", changedAt); err != nil {
			t.Fatalf("RetryCurrent: %v", err)
		}
		before := attempts(t, store)

		_, err := lifecycle.RetryCurrentPending("run-1", "task-1", changedAt.Add(time.Second))
		var invalid *tasklifecycle.InvalidTransitionError
		if !errors.As(err, &invalid) {
			t.Fatalf("RetryCurrentPending after queued successor error = %T %v, want InvalidTransitionError", err, err)
		}
		if after := attempts(t, store); !reflect.DeepEqual(after, before) {
			t.Errorf("cross-variant retry mutated history\n got: %#v\nwant: %#v", after, before)
		}
	})
}
