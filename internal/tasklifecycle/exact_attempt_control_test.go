package tasklifecycle_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/tasklifecycle"
)

func TestCancelAttemptChangesOnlyTheNamedAttempt(t *testing.T) {
	store := newStore(t)
	insertLogicalAttempt(t, store, "stale", models.TaskRunning, 1, "stale output", createdAt)
	insertLogicalAttempt(t, store, "successor", models.TaskQueued, 2, "successor output", createdAt.Add(time.Second))
	before := attempts(t, store)

	got, err := tasklifecycle.New(store).CancelAttempt("stale", changedAt)
	if err != nil {
		t.Fatalf("CancelAttempt: %v", err)
	}
	if got != tasklifecycle.Applied {
		t.Fatalf("CancelAttempt = %v, want Applied", got)
	}

	after := attempts(t, store)
	wantStale := before[0]
	wantStale.Status = models.TaskCancelled
	wantStale.UpdatedAt = changedAt
	if !reflect.DeepEqual(after[0], wantStale) {
		t.Errorf("named attempt\n got: %#v\nwant: %#v", after[0], wantStale)
	}
	if !reflect.DeepEqual(after[1], before[1]) {
		t.Errorf("newer successor changed\n got: %#v\nwant: %#v", after[1], before[1])
	}
}

func TestCancelAttemptEligibleStates(t *testing.T) {
	for _, status := range []models.TaskStatus{
		models.TaskPending,
		models.TaskQueued,
		models.TaskRunning,
		models.TaskUpForRetry,
	} {
		t.Run(string(status), func(t *testing.T) {
			store := newStore(t)
			createAttempt(t, store, status, 1, "preserved output")

			got, err := tasklifecycle.New(store).CancelAttempt("attempt-1", changedAt)
			if err != nil {
				t.Fatalf("CancelAttempt: %v", err)
			}
			if got != tasklifecycle.Applied {
				t.Fatalf("CancelAttempt = %v, want Applied", got)
			}
			attempt := getAttempt(t, store)
			if attempt.Status != models.TaskCancelled {
				t.Errorf("status = %q, want cancelled", attempt.Status)
			}
			if attempt.Output != "preserved output" {
				t.Errorf("output = %q, want preserved output", attempt.Output)
			}
			if !attempt.UpdatedAt.Equal(changedAt) {
				t.Errorf("updated_at = %v, want %v", attempt.UpdatedAt, changedAt)
			}
		})
	}
}

func TestCancelAttemptReplayDoesNotRewriteCancellation(t *testing.T) {
	store := newStore(t)
	createAttempt(t, store, models.TaskRunning, 1, "partial output")
	lifecycle := tasklifecycle.New(store)

	if got, err := lifecycle.CancelAttempt("attempt-1", changedAt); err != nil || got != tasklifecycle.Applied {
		t.Fatalf("first CancelAttempt = (%v, %v), want (Applied, nil)", got, err)
	}
	cancelled := snapshot(t, store)

	got, err := lifecycle.CancelAttempt("attempt-1", changedAt.Add(time.Hour))
	if err != nil {
		t.Fatalf("replayed CancelAttempt: %v", err)
	}
	if got != tasklifecycle.AlreadyApplied {
		t.Fatalf("replayed CancelAttempt = %v, want AlreadyApplied", got)
	}
	assertSnapshot(t, store, cancelled)
}

func TestCancelAttemptRejectsConflictingTerminalState(t *testing.T) {
	for _, status := range []models.TaskStatus{models.TaskFailed, models.TaskSuccess} {
		t.Run(string(status), func(t *testing.T) {
			store := newStore(t)
			createAttempt(t, store, status, 1, "authoritative output")
			before := snapshot(t, store)

			_, err := tasklifecycle.New(store).CancelAttempt("attempt-1", changedAt)
			var invalid *tasklifecycle.InvalidTransitionError
			if !errors.As(err, &invalid) {
				t.Fatalf("CancelAttempt error = %T %v, want InvalidTransitionError", err, err)
			}
			assertSnapshot(t, store, before)
		})
	}
}

func TestCancelAttemptMissingDoesNotMutateOtherAttempts(t *testing.T) {
	store := newStore(t)
	createAttempt(t, store, models.TaskRunning, 1, "untouched")
	before := snapshot(t, store)

	_, err := tasklifecycle.New(store).CancelAttempt("missing", changedAt)
	var missing *tasklifecycle.MissingAttemptError
	if !errors.As(err, &missing) {
		t.Fatalf("CancelAttempt error = %T %v, want MissingAttemptError", err, err)
	}
	assertSnapshot(t, store, before)
}
