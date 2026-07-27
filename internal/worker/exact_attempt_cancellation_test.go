package worker

import (
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/tasklifecycle"
)

func TestCancelAttemptDelegatesByExactAttemptID(t *testing.T) {
	lifecycle := &cancellationLifecycleSpy{}
	pool := &Pool{lifecycle: lifecycle}
	cancelledAt := time.Date(2026, time.July, 26, 19, 0, 0, 0, time.UTC)

	got, err := pool.cancelAttempt(models.TaskInstance{
		ID: "stale-attempt", RunID: "run-1", TaskID: "task-1",
	}, cancelledAt)
	if err != nil {
		t.Fatalf("cancelAttempt: %v", err)
	}
	if got != tasklifecycle.Applied {
		t.Fatalf("cancelAttempt = %v, want Applied", got)
	}
	if lifecycle.attemptID != "stale-attempt" {
		t.Errorf("CancelAttempt ID = %q, want stale-attempt", lifecycle.attemptID)
	}
	if !lifecycle.cancelledAt.Equal(cancelledAt) {
		t.Errorf("CancelAttempt timestamp = %v, want %v", lifecycle.cancelledAt, cancelledAt)
	}
	if lifecycle.cancelCurrentCalls != 0 {
		t.Fatalf("CancelCurrent called %d times, want 0", lifecycle.cancelCurrentCalls)
	}
}

type cancellationLifecycleSpy struct {
	attemptID          string
	cancelledAt        time.Time
	cancelCurrentCalls int
}

func (*cancellationLifecycleSpy) Claim(string, time.Time) (tasklifecycle.Disposition, error) {
	panic("unexpected Claim")
}

func (*cancellationLifecycleSpy) Complete(tasklifecycle.Completion) (tasklifecycle.Disposition, error) {
	panic("unexpected Complete")
}

func (s *cancellationLifecycleSpy) CancelAttempt(attemptID string, cancelledAt time.Time) (tasklifecycle.Disposition, error) {
	s.attemptID = attemptID
	s.cancelledAt = cancelledAt
	return tasklifecycle.Applied, nil
}

func (s *cancellationLifecycleSpy) CancelCurrent(string, string, time.Time) (tasklifecycle.Disposition, error) {
	s.cancelCurrentCalls++
	return tasklifecycle.Applied, nil
}
