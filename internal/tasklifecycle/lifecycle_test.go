package tasklifecycle_test

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/tasklifecycle"
)

var (
	createdAt = time.Date(2026, time.July, 26, 8, 0, 0, 0, time.UTC)
	changedAt = createdAt.Add(time.Minute)
)

func TestClaimSequentialReplayAuthorizesDispatchExactlyOnce(t *testing.T) {
	store := newStore(t)
	createAttempt(t, store, models.TaskQueued, 1, "")
	lifecycle := tasklifecycle.New(store)

	first, err := lifecycle.Claim("attempt-1", changedAt)
	if err != nil {
		t.Fatalf("first Claim: %v", err)
	}
	second, err := lifecycle.Claim("attempt-1", changedAt.Add(time.Minute))
	if err != nil {
		t.Fatalf("replayed Claim: %v", err)
	}

	if first != tasklifecycle.Applied {
		t.Fatalf("first Claim = %v, want Applied", first)
	}
	if second != tasklifecycle.AlreadyApplied {
		t.Fatalf("replayed Claim = %v, want AlreadyApplied", second)
	}
	dispatches := 0
	for _, disposition := range []tasklifecycle.Disposition{first, second} {
		if disposition == tasklifecycle.Applied {
			dispatches++
		}
	}
	if dispatches != 1 {
		t.Fatalf("Applied authorized %d dispatches, want exactly one", dispatches)
	}

	got := getAttempt(t, store)
	if got.Status != models.TaskRunning {
		t.Errorf("status = %q, want running", got.Status)
	}
	if got.StartedAt == nil || !got.StartedAt.Equal(changedAt) {
		t.Errorf("started_at = %v, want %v", got.StartedAt, changedAt)
	}
	if !got.UpdatedAt.Equal(changedAt) {
		t.Errorf("updated_at = %v, want %v", got.UpdatedAt, changedAt)
	}
}

func TestPromoteIsConditionalAndPreservesTerminalState(t *testing.T) {
	t.Run("pending is promoted", func(t *testing.T) {
		store := newStore(t)
		createAttempt(t, store, models.TaskPending, 1, "")

		got, err := tasklifecycle.New(store).Promote("attempt-1", changedAt)
		if err != nil {
			t.Fatalf("Promote: %v", err)
		}
		if got != tasklifecycle.Applied {
			t.Fatalf("Promote = %v, want Applied", got)
		}
		attempt := getAttempt(t, store)
		if attempt.Status != models.TaskQueued || !attempt.UpdatedAt.Equal(changedAt) {
			t.Fatalf("promoted attempt = %+v", attempt)
		}
	})

	for _, terminal := range []models.TaskStatus{
		models.TaskCancelled, models.TaskSuccess, models.TaskFailed,
	} {
		t.Run(string(terminal)+" is not overwritten", func(t *testing.T) {
			store := newStore(t)
			createAttempt(t, store, terminal, 1, "authoritative output")
			before := snapshot(t, store)

			_, err := tasklifecycle.New(store).Promote("attempt-1", changedAt)
			var invalid *tasklifecycle.InvalidTransitionError
			if !errors.As(err, &invalid) {
				t.Fatalf("Promote error = %T %v, want InvalidTransitionError", err, err)
			}
			assertSnapshot(t, store, before)
		})
	}
}

func TestCompleteDerivesPersistedOutcome(t *testing.T) {
	tests := []struct {
		name       string
		input      tasklifecycle.Completion
		attempt    int
		wantStatus models.TaskStatus
	}{
		{
			name:    "success",
			input:   tasklifecycle.Completion{Succeeded: true, Output: "ok"},
			attempt: 1, wantStatus: models.TaskSuccess,
		},
		{
			name:    "ordinary failure with retries remaining",
			input:   tasklifecycle.Completion{Output: "exit 1", Retries: 2},
			attempt: 1, wantStatus: models.TaskUpForRetry,
		},
		{
			name:    "ordinary failure with retries exhausted",
			input:   tasklifecycle.Completion{Output: "exit 1", Retries: 2},
			attempt: 3, wantStatus: models.TaskFailed,
		},
		{
			name:    "timeout is terminal even with retries remaining",
			input:   tasklifecycle.Completion{Output: "deadline exceeded", TimedOut: true, Retries: 5},
			attempt: 1, wantStatus: models.TaskFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newStore(t)
			createAttempt(t, store, models.TaskRunning, tt.attempt, "")
			input := tt.input
			input.AttemptID = "attempt-1"
			input.CompletedAt = changedAt

			got, err := tasklifecycle.New(store).Complete(input)
			if err != nil {
				t.Fatalf("Complete: %v", err)
			}
			if got != tasklifecycle.Applied {
				t.Fatalf("Complete = %v, want Applied", got)
			}
			attempt := getAttempt(t, store)
			if attempt.Status != tt.wantStatus {
				t.Errorf("status = %q, want %q", attempt.Status, tt.wantStatus)
			}
			if attempt.Output != input.Output {
				t.Errorf("output = %q, want %q", attempt.Output, input.Output)
			}
			if !attempt.UpdatedAt.Equal(changedAt) {
				t.Errorf("updated_at = %v, want %v", attempt.UpdatedAt, changedAt)
			}
		})
	}
}

func TestCompleteTerminalReplayAndConflict(t *testing.T) {
	store := newStore(t)
	createAttempt(t, store, models.TaskRunning, 1, "")
	lifecycle := tasklifecycle.New(store)
	completion := tasklifecycle.Completion{
		AttemptID: "attempt-1", Succeeded: true, Output: "result", CompletedAt: changedAt,
	}

	if got, err := lifecycle.Complete(completion); err != nil || got != tasklifecycle.Applied {
		t.Fatalf("first Complete = (%v, %v), want (Applied, nil)", got, err)
	}
	persisted := snapshot(t, store)

	replay := completion
	replay.CompletedAt = changedAt.Add(time.Hour)
	if got, err := lifecycle.Complete(replay); err != nil || got != tasklifecycle.AlreadyApplied {
		t.Fatalf("replayed Complete = (%v, %v), want (AlreadyApplied, nil)", got, err)
	}
	assertSnapshot(t, store, persisted)

	conflicting := completion
	conflicting.Succeeded = false
	conflicting.Output = "late failure"
	_, err := lifecycle.Complete(conflicting)
	var conflict *tasklifecycle.ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("conflicting Complete error = %T %v, want ConflictError", err, err)
	}
	assertSnapshot(t, store, persisted)
}

func TestCompletePreservesAuthoritativeCancellation(t *testing.T) {
	store := newStore(t)
	createAttempt(t, store, models.TaskCancelled, 1, "cancelled by user")
	before := snapshot(t, store)

	got, err := tasklifecycle.New(store).Complete(tasklifecycle.Completion{
		AttemptID: "attempt-1", Output: "process killed", Retries: 3, CompletedAt: changedAt,
	})
	if err != nil {
		t.Fatalf("Complete after cancellation: %v", err)
	}
	if got != tasklifecycle.AlreadyApplied {
		t.Fatalf("Complete after cancellation = %v, want AlreadyApplied", got)
	}
	assertSnapshot(t, store, before)
}

func TestInvalidAndMissingTransitionsDoNotMutateAttempts(t *testing.T) {
	store := newStore(t)
	createAttempt(t, store, models.TaskPending, 2, "untouched")
	before := snapshot(t, store)
	lifecycle := tasklifecycle.New(store)

	_, err := lifecycle.Claim("attempt-1", changedAt)
	var invalid *tasklifecycle.InvalidTransitionError
	if !errors.As(err, &invalid) {
		t.Fatalf("Claim error = %T %v, want InvalidTransitionError", err, err)
	}
	assertSnapshot(t, store, before)

	_, err = lifecycle.Claim("missing", changedAt)
	var missing *tasklifecycle.MissingAttemptError
	if !errors.As(err, &missing) {
		t.Fatalf("missing Claim error = %T %v, want MissingAttemptError", err, err)
	}
	assertSnapshot(t, store, before)
}

type persistedSnapshot struct {
	attempt models.TaskInstance
	history []models.TaskInstance
}

func newStore(t *testing.T) *models.Store {
	t.Helper()
	store, err := models.NewStore(filepath.Join(t.TempDir(), "lifecycle.db"))
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

func createAttempt(t *testing.T, store *models.Store, status models.TaskStatus, attempt int, output string) {
	t.Helper()
	err := store.CreateTaskInstance(&models.TaskInstance{
		ID: "attempt-1", RunID: "run-1", TaskID: "task-1",
		Status: status, Output: output, Attempt: attempt,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	})
	if err != nil {
		t.Fatalf("CreateTaskInstance: %v", err)
	}
}

func getAttempt(t *testing.T, store *models.Store) models.TaskInstance {
	t.Helper()
	attempt, err := store.GetTaskInstance("attempt-1")
	if err != nil {
		t.Fatalf("GetTaskInstance: %v", err)
	}
	return *attempt
}

func snapshot(t *testing.T, store *models.Store) persistedSnapshot {
	t.Helper()
	attempt := getAttempt(t, store)
	history, err := store.GetTaskAttempts(attempt.RunID, attempt.TaskID)
	if err != nil {
		t.Fatalf("GetTaskAttempts: %v", err)
	}
	return persistedSnapshot{attempt: attempt, history: history}
}

func assertSnapshot(t *testing.T, store *models.Store, want persistedSnapshot) {
	t.Helper()
	got := snapshot(t, store)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("persisted attempt changed\n got: %#v\nwant: %#v", got, want)
	}
}
