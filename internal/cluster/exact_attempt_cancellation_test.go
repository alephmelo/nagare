package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/tasklifecycle"
)

func TestCompleteRemoteCancelledDelegatesByExactAttemptID(t *testing.T) {
	lifecycle := &remoteCancellationLifecycleSpy{}
	coordinator := &Coordinator{
		lifecycle: lifecycle,
		assignments: map[string]*remoteAssignment{
			"stale-attempt": {
				workerID: "worker-1",
				runID:    "run-1",
				taskID:   "task-1",
			},
		},
		workers: map[string]*WorkerInfo{
			"worker-1": {ActiveTasks: 1},
		},
	}
	cancelledAt := time.Date(2026, time.July, 26, 20, 0, 0, 0, time.UTC)

	completion, err := coordinator.completeRemote(TaskResult{
		TaskInstanceID: "stale-attempt",
		WorkerID:       "worker-1",
		Status:         "cancelled",
	}, cancelledAt)
	if err != nil {
		t.Fatalf("completeRemote: %v", err)
	}
	if completion.disposition != tasklifecycle.Applied {
		t.Fatalf("disposition = %v, want Applied", completion.disposition)
	}
	if lifecycle.attemptID != "stale-attempt" {
		t.Errorf("CancelAttempt ID = %q, want stale-attempt", lifecycle.attemptID)
	}
	if !lifecycle.cancelledAt.Equal(cancelledAt) {
		t.Errorf("CancelAttempt timestamp = %v, want %v", lifecycle.cancelledAt, cancelledAt)
	}
}

func TestCompleteRemoteCancelledDoesNotCancelSuccessorAttempt(t *testing.T) {
	store, err := models.NewStore(fmt.Sprintf(
		"file:%s?mode=memory&cache=private&_busy_timeout=5000",
		t.Name(),
	))
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	if err := store.CreateDagRun(&models.DagRun{
		ID: "run-1", DAGID: "dag-1", Status: models.RunRunning,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}
	for _, attempt := range []models.TaskInstance{
		{
			ID: "stale-attempt", RunID: "run-1", TaskID: "task-1",
			Status: models.TaskRunning, Attempt: 1,
		},
		{
			ID: "successor-attempt", RunID: "run-1", TaskID: "task-1",
			Status: models.TaskRunning, Attempt: 2,
		},
	} {
		if err := store.CreateTaskInstance(&attempt); err != nil {
			t.Fatalf("create %s: %v", attempt.ID, err)
		}
	}

	coordinator := NewCoordinator(store, nil, 30*time.Second, "")
	coordinator.assignments["stale-attempt"] = &remoteAssignment{
		workerID: "worker-1",
		runID:    "run-1",
		taskID:   "task-1",
	}
	coordinator.workers["worker-1"] = &WorkerInfo{ActiveTasks: 1}

	if _, err := coordinator.completeRemote(TaskResult{
		TaskInstanceID: "stale-attempt",
		WorkerID:       "worker-1",
		Status:         "cancelled",
	}, time.Now()); err != nil {
		t.Fatalf("completeRemote: %v", err)
	}

	stale, err := store.GetTaskInstance("stale-attempt")
	if err != nil {
		t.Fatalf("get stale attempt: %v", err)
	}
	if stale.Status != models.TaskCancelled {
		t.Errorf("stale attempt status = %q, want %q", stale.Status, models.TaskCancelled)
	}
	successor, err := store.GetTaskInstance("successor-attempt")
	if err != nil {
		t.Fatalf("get successor attempt: %v", err)
	}
	if successor.Status != models.TaskRunning {
		t.Errorf("successor status = %q, want %q", successor.Status, models.TaskRunning)
	}
}

type remoteCancellationLifecycleSpy struct {
	attemptID   string
	cancelledAt time.Time
}

func (*remoteCancellationLifecycleSpy) Claim(string, time.Time) (tasklifecycle.Disposition, error) {
	panic("unexpected Claim")
}

func (*remoteCancellationLifecycleSpy) Complete(tasklifecycle.Completion) (tasklifecycle.Disposition, error) {
	panic("unexpected Complete")
}

func (s *remoteCancellationLifecycleSpy) CancelAttempt(attemptID string, cancelledAt time.Time) (tasklifecycle.Disposition, error) {
	s.attemptID = attemptID
	s.cancelledAt = cancelledAt
	return tasklifecycle.Applied, nil
}
