package cluster

import (
	"fmt"
	"time"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/tasklifecycle"
	"github.com/alephmelo/nagare/internal/worker"
)

type remoteAssignment struct {
	workerID string
	runID    string
	taskID   string
	retries  int
	released bool
}

type remoteCompletion struct {
	disposition tasklifecycle.Disposition
	cleanup     bool
}

func (c *Coordinator) claimRemote(workerID string, ti models.TaskInstance, assignment *worker.TaskAssignment, claimedAt time.Time) (tasklifecycle.Disposition, error) {
	disposition, err := c.lifecycle.Claim(ti.ID, claimedAt)
	if err != nil || disposition != tasklifecycle.Applied {
		return disposition, err
	}

	c.mu.Lock()
	c.assignments[ti.ID] = &remoteAssignment{
		workerID: workerID,
		runID:    ti.RunID,
		taskID:   ti.TaskID,
		retries:  assignment.Retries,
	}
	if registered, ok := c.workers[workerID]; ok {
		registered.ActiveTasks++
	}
	c.mu.Unlock()
	return disposition, nil
}

func (c *Coordinator) completeRemote(result TaskResult, completedAt time.Time) (remoteCompletion, error) {
	c.mu.RLock()
	ownership := c.assignments[result.TaskInstanceID]
	c.mu.RUnlock()

	// Preserve the historical local-only handler behavior for callers that do
	// not identify a remote worker. A claimed remote result must always prove
	// ownership.
	if ownership == nil {
		if result.WorkerID != "" {
			return remoteCompletion{}, fmt.Errorf("task attempt is not owned by worker")
		}
		attempt, err := c.store.GetTaskInstance(result.TaskInstanceID)
		if err != nil {
			return remoteCompletion{}, err
		}
		ownership = &remoteAssignment{runID: attempt.RunID, taskID: attempt.TaskID}
	} else if result.WorkerID != ownership.workerID {
		return remoteCompletion{}, fmt.Errorf("task attempt is owned by another worker")
	}

	var (
		disposition tasklifecycle.Disposition
		err         error
	)
	switch result.Status {
	case "success":
		disposition, err = c.lifecycle.Complete(tasklifecycle.Completion{
			AttemptID: result.TaskInstanceID, Succeeded: true, Output: result.Output,
			Retries: ownership.retries, CompletedAt: completedAt,
		})
	case "failed":
		disposition, err = c.lifecycle.Complete(tasklifecycle.Completion{
			AttemptID: result.TaskInstanceID, Output: result.Output, TimedOut: result.TimedOut,
			Retries: ownership.retries, CompletedAt: completedAt,
		})
	case "cancelled":
		disposition, err = c.lifecycle.CancelAttempt(result.TaskInstanceID, completedAt)
	case "up_for_retry":
		// Complete owns persisted completion validity. An explicit retry outcome
		// is intentionally retryable independently of the automatic retry policy.
		disposition, err = c.lifecycle.Complete(tasklifecycle.Completion{
			AttemptID: result.TaskInstanceID, Output: result.Output,
			Retries: int(^uint(0) >> 1), CompletedAt: completedAt,
		})
	default:
		return remoteCompletion{}, fmt.Errorf("unknown status")
	}
	if err != nil {
		return remoteCompletion{}, err
	}

	c.mu.Lock()
	owned := c.assignments[result.TaskInstanceID]
	if owned != nil && !owned.released {
		if registered, ok := c.workers[owned.workerID]; ok && registered.ActiveTasks > 0 {
			registered.ActiveTasks--
		}
		owned.released = true
	}
	c.mu.Unlock()
	return remoteCompletion{disposition: disposition, cleanup: true}, nil
}
