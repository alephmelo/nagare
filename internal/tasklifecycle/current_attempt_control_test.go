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

func TestCancelCurrentChangesOnlyLatestEligibleAttempt(t *testing.T) {
	for _, status := range []models.TaskStatus{
		models.TaskPending,
		models.TaskQueued,
		models.TaskRunning,
		models.TaskUpForRetry,
	} {
		t.Run(string(status), func(t *testing.T) {
			store := newStore(t)
			insertLogicalAttempt(t, store, "old", models.TaskFailed, 1, "historical output", createdAt)
			insertLogicalAttempt(t, store, "current", status, 2, "current output", createdAt.Add(time.Second))
			before := attempts(t, store)

			got, err := tasklifecycle.New(store).CancelCurrent("run-1", "task-1", changedAt)
			if err != nil {
				t.Fatalf("CancelCurrent: %v", err)
			}
			if got != tasklifecycle.Applied {
				t.Fatalf("CancelCurrent = %v, want Applied", got)
			}

			after := attempts(t, store)
			if !reflect.DeepEqual(after[0], before[0]) {
				t.Errorf("historical attempt changed\n got: %#v\nwant: %#v", after[0], before[0])
			}
			wantCurrent := before[1]
			wantCurrent.Status = models.TaskCancelled
			wantCurrent.UpdatedAt = changedAt
			if !reflect.DeepEqual(after[1], wantCurrent) {
				t.Errorf("current attempt\n got: %#v\nwant: %#v", after[1], wantCurrent)
			}
		})
	}
}

func TestCancelCurrentReplayIsPersistenceIdempotent(t *testing.T) {
	store := newStore(t)
	insertLogicalAttempt(t, store, "old", models.TaskSuccess, 1, "old result", createdAt)
	insertLogicalAttempt(t, store, "current", models.TaskRunning, 2, "partial output", createdAt.Add(time.Second))
	lifecycle := tasklifecycle.New(store)

	if got, err := lifecycle.CancelCurrent("run-1", "task-1", changedAt); err != nil || got != tasklifecycle.Applied {
		t.Fatalf("first CancelCurrent = (%v, %v), want (Applied, nil)", got, err)
	}
	cancelled := attempts(t, store)

	got, err := lifecycle.CancelCurrent("run-1", "task-1", changedAt.Add(time.Hour))
	if err != nil {
		t.Fatalf("replayed CancelCurrent: %v", err)
	}
	if got != tasklifecycle.AlreadyApplied {
		t.Fatalf("replayed CancelCurrent = %v, want AlreadyApplied", got)
	}
	if after := attempts(t, store); !reflect.DeepEqual(after, cancelled) {
		t.Errorf("replayed cancellation rewrote history\n got: %#v\nwant: %#v", after, cancelled)
	}
}

func TestCancelCurrentRejectsIneligibleCurrentAttemptWithoutMutation(t *testing.T) {
	for _, status := range []models.TaskStatus{models.TaskFailed, models.TaskSuccess} {
		t.Run(string(status), func(t *testing.T) {
			store := newStore(t)
			insertLogicalAttempt(t, store, "current", status, 1, "authoritative output", createdAt)
			before := attempts(t, store)

			_, err := tasklifecycle.New(store).CancelCurrent("run-1", "task-1", changedAt)
			var invalid *tasklifecycle.InvalidTransitionError
			if !errors.As(err, &invalid) {
				t.Fatalf("CancelCurrent error = %T %v, want InvalidTransitionError", err, err)
			}
			if after := attempts(t, store); !reflect.DeepEqual(after, before) {
				t.Errorf("invalid cancellation mutated attempts\n got: %#v\nwant: %#v", after, before)
			}
		})
	}
}

func TestRetryCurrentEligibilityAndPredecessorPreservation(t *testing.T) {
	for _, status := range []models.TaskStatus{
		models.TaskPending,
		models.TaskCancelled,
		models.TaskUpForRetry,
		models.TaskFailed,
		models.TaskSuccess,
	} {
		t.Run(string(status), func(t *testing.T) {
			store := newStore(t)
			startedAt := createdAt.Add(-time.Minute)
			item := "matrix-item"
			if err := store.CreateTaskInstance(&models.TaskInstance{
				ID: "current", RunID: "run-1", TaskID: "task-1", Status: status,
				Output: "preserve me", ItemValue: &item, Attempt: 3,
				CreatedAt: createdAt, UpdatedAt: createdAt, StartedAt: &startedAt,
			}); err != nil {
				t.Fatalf("CreateTaskInstance: %v", err)
			}
			predecessor := attempts(t, store)[0]

			got, err := tasklifecycle.New(store).RetryCurrent("run-1", "task-1", changedAt)
			if err != nil {
				t.Fatalf("RetryCurrent: %v", err)
			}
			if got != tasklifecycle.Applied {
				t.Fatalf("RetryCurrent = %v, want Applied", got)
			}

			history := attempts(t, store)
			if len(history) != 2 {
				t.Fatalf("attempt count = %d, want 2", len(history))
			}
			if !reflect.DeepEqual(history[0], predecessor) {
				t.Errorf("retry changed predecessor\n got: %#v\nwant: %#v", history[0], predecessor)
			}
			successor := history[1]
			if successor.Attempt != 4 || successor.Status != models.TaskQueued {
				t.Errorf("successor = %#v, want attempt 4 queued", successor)
			}
			if !successor.CreatedAt.Equal(changedAt) || !successor.UpdatedAt.Equal(changedAt) {
				t.Errorf("successor timestamps = (%v, %v), want %v", successor.CreatedAt, successor.UpdatedAt, changedAt)
			}
		})
	}
}

func TestRetryCurrentRejectsQueuedAndRunningWithoutMutation(t *testing.T) {
	for _, status := range []models.TaskStatus{models.TaskQueued, models.TaskRunning} {
		t.Run(string(status), func(t *testing.T) {
			store := newStore(t)
			insertLogicalAttempt(t, store, "current", status, 2, "untouched", createdAt)
			before := attempts(t, store)

			_, err := tasklifecycle.New(store).RetryCurrent("run-1", "task-1", changedAt)
			var invalid *tasklifecycle.InvalidTransitionError
			if !errors.As(err, &invalid) {
				t.Fatalf("RetryCurrent error = %T %v, want InvalidTransitionError", err, err)
			}
			if after := attempts(t, store); !reflect.DeepEqual(after, before) {
				t.Errorf("invalid retry mutated attempts\n got: %#v\nwant: %#v", after, before)
			}
		})
	}
}

func TestRetryCurrentReplayInsertsAtMostOneSuccessor(t *testing.T) {
	store := newStore(t)
	insertLogicalAttempt(t, store, "failed", models.TaskFailed, 1, "failure", createdAt)
	lifecycle := tasklifecycle.New(store)

	first, err := lifecycle.RetryCurrent("run-1", "task-1", changedAt)
	if err != nil || first != tasklifecycle.Applied {
		t.Fatalf("first RetryCurrent = (%v, %v), want (Applied, nil)", first, err)
	}
	second, err := lifecycle.RetryCurrent("run-1", "task-1", changedAt.Add(time.Hour))
	if err != nil {
		t.Fatalf("replayed RetryCurrent: %v", err)
	}
	if second != tasklifecycle.AlreadyApplied {
		t.Fatalf("replayed RetryCurrent = %v, want AlreadyApplied", second)
	}
	if history := attempts(t, store); len(history) != 2 {
		t.Fatalf("replayed retry produced %d attempts, want 2", len(history))
	}
}

func TestConcurrentRetryCurrentInsertsOneSuccessorAcrossStoreConnections(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "retry-race.db")
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
			disposition, err := lifecycle.RetryCurrent("run-1", "task-1", changedAt)
			results <- lifecycleResult{disposition, err}
		}(lifecycle)
	}
	ready.Wait()
	close(start)

	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent RetryCurrent: %v", result.err)
		}
	}
	if history := attempts(t, first); len(history) != 2 {
		t.Fatalf("concurrent retries produced %d attempts, want 2", len(history))
	}
}

func TestRetryVersusCancelSerializesAcrossStoreConnections(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "retry-cancel-race.db")
	retryStore := openStoreAt(t, dbPath)
	cancelStore := openStoreAt(t, dbPath)
	insertLogicalAttempt(t, retryStore, "pending", models.TaskPending, 1, "preserve", createdAt)

	start := make(chan struct{})
	results := make(chan lifecycleResult, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	go func() {
		ready.Done()
		<-start
		got, err := tasklifecycle.New(retryStore).RetryCurrent("run-1", "task-1", changedAt)
		results <- lifecycleResult{got, err}
	}()
	go func() {
		ready.Done()
		<-start
		got, err := tasklifecycle.New(cancelStore).CancelCurrent("run-1", "task-1", changedAt)
		results <- lifecycleResult{got, err}
	}()
	ready.Wait()
	close(start)

	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("retry/cancel race: %v", result.err)
		}
	}
	history := attempts(t, retryStore)
	current := history[len(history)-1]
	if current.Status == models.TaskQueued || current.Status == models.TaskRunning {
		t.Fatalf("cancellation won but current successor remains %q: %#v", current.Status, history)
	}
}

type lifecycleResult struct {
	disposition tasklifecycle.Disposition
	err         error
}

func openStoreAt(t *testing.T, path string) *models.Store {
	t.Helper()
	store, err := models.NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	ensureTestRun(t, store)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return store
}

func insertLogicalAttempt(
	t *testing.T,
	store *models.Store,
	id string,
	status models.TaskStatus,
	attempt int,
	output string,
	at time.Time,
) {
	t.Helper()
	if err := store.CreateTaskInstance(&models.TaskInstance{
		ID: id, RunID: "run-1", TaskID: "task-1", Status: status,
		Output: output, Attempt: attempt, CreatedAt: at, UpdatedAt: at,
	}); err != nil {
		t.Fatalf("CreateTaskInstance: %v", err)
	}
}

func attempts(t *testing.T, store *models.Store) []models.TaskInstance {
	t.Helper()
	got, err := store.GetTaskAttempts("run-1", "task-1")
	if err != nil {
		t.Fatalf("GetTaskAttempts: %v", err)
	}
	return got
}
