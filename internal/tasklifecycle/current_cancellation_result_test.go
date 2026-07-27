package tasklifecycle_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/tasklifecycle"
)

func TestCancelCurrentAttemptReturnsSelectedAttemptID(t *testing.T) {
	store := newStore(t)
	insertLogicalAttempt(t, store, "historical", models.TaskFailed, 1, "old output", createdAt)
	insertLogicalAttempt(t, store, "selected", models.TaskRunning, 2, "partial output", createdAt.Add(time.Second))

	got, err := tasklifecycle.New(store).CancelCurrentAttempt("run-1", "task-1", changedAt)
	if err != nil {
		t.Fatalf("CancelCurrentAttempt: %v", err)
	}
	want := tasklifecycle.CurrentCancellation{
		Disposition: tasklifecycle.Applied,
		AttemptID:   "selected",
	}
	if got != want {
		t.Fatalf("CancelCurrentAttempt = %#v, want %#v", got, want)
	}
}

func TestCancelCurrentAttemptReplayReturnsAuthoritativeAttemptID(t *testing.T) {
	store := newStore(t)
	insertLogicalAttempt(t, store, "current", models.TaskRunning, 1, "partial output", createdAt)
	lifecycle := tasklifecycle.New(store)

	first, err := lifecycle.CancelCurrentAttempt("run-1", "task-1", changedAt)
	if err != nil {
		t.Fatalf("first CancelCurrentAttempt: %v", err)
	}
	cancelled := attempts(t, store)

	replay, err := lifecycle.CancelCurrentAttempt("run-1", "task-1", changedAt.Add(time.Hour))
	if err != nil {
		t.Fatalf("replayed CancelCurrentAttempt: %v", err)
	}
	if first.Disposition != tasklifecycle.Applied || first.AttemptID != "current" {
		t.Fatalf("first cancellation = %#v, want applied current", first)
	}
	if replay.Disposition != tasklifecycle.AlreadyApplied || replay.AttemptID != "current" {
		t.Fatalf("replayed cancellation = %#v, want already-applied current", replay)
	}
	if after := attempts(t, store); !reflect.DeepEqual(after, cancelled) {
		t.Errorf("replayed cancellation rewrote history\n got: %#v\nwant: %#v", after, cancelled)
	}
}

func TestCancelCurrentAttemptSelectsTransactionalRetrySuccessor(t *testing.T) {
	store := newStore(t)
	insertLogicalAttempt(t, store, "predecessor", models.TaskFailed, 1, "failure", createdAt)
	lifecycle := tasklifecycle.New(store)

	if disposition, err := lifecycle.RetryCurrent("run-1", "task-1", changedAt); err != nil {
		t.Fatalf("RetryCurrent: %v", err)
	} else if disposition != tasklifecycle.Applied {
		t.Fatalf("RetryCurrent = %v, want Applied", disposition)
	}
	before := attempts(t, store)
	if len(before) != 2 {
		t.Fatalf("attempt count = %d, want 2", len(before))
	}
	successorID := before[1].ID

	got, err := lifecycle.CancelCurrentAttempt("run-1", "task-1", changedAt.Add(time.Second))
	if err != nil {
		t.Fatalf("CancelCurrentAttempt: %v", err)
	}
	if got.Disposition != tasklifecycle.Applied || got.AttemptID != successorID {
		t.Fatalf("CancelCurrentAttempt = %#v, want applied successor %q", got, successorID)
	}
	after := attempts(t, store)
	if !reflect.DeepEqual(after[0], before[0]) {
		t.Errorf("predecessor changed\n got: %#v\nwant: %#v", after[0], before[0])
	}
	if after[1].Status != models.TaskCancelled {
		t.Fatalf("successor status = %q, want cancelled", after[1].Status)
	}
}

func TestCancelCurrentAttemptInvalidAndMissingReturnNoAuthorization(t *testing.T) {
	t.Run("invalid", func(t *testing.T) {
		store := newStore(t)
		insertLogicalAttempt(t, store, "terminal", models.TaskSuccess, 1, "result", createdAt)
		before := attempts(t, store)

		got, err := tasklifecycle.New(store).CancelCurrentAttempt("run-1", "task-1", changedAt)
		var invalid *tasklifecycle.InvalidTransitionError
		if !errors.As(err, &invalid) {
			t.Fatalf("CancelCurrentAttempt error = %T %v, want InvalidTransitionError", err, err)
		}
		if got != (tasklifecycle.CurrentCancellation{}) {
			t.Fatalf("invalid cancellation result = %#v, want zero value", got)
		}
		if after := attempts(t, store); !reflect.DeepEqual(after, before) {
			t.Errorf("invalid cancellation mutated attempts\n got: %#v\nwant: %#v", after, before)
		}
	})

	t.Run("missing", func(t *testing.T) {
		store := newStore(t)

		got, err := tasklifecycle.New(store).CancelCurrentAttempt("missing-run", "missing-task", changedAt)
		var missing *tasklifecycle.MissingAttemptError
		if !errors.As(err, &missing) {
			t.Fatalf("CancelCurrentAttempt error = %T %v, want MissingAttemptError", err, err)
		}
		if got != (tasklifecycle.CurrentCancellation{}) {
			t.Fatalf("missing cancellation result = %#v, want zero value", got)
		}
	})
}
