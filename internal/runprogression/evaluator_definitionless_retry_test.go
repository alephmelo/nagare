package runprogression_test

import (
	"reflect"
	"testing"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/runprogression"
)

func TestEvaluateDefinitionlessManualRetryRequiresCurrentDurableSuccessor(t *testing.T) {
	tests := []struct {
		name       string
		runStatus  models.RunStatus
		allow      bool
		attempts   []runprogression.AttemptSnapshot
		wantStatus *models.RunStatus
	}{
		{
			name:      "failed run reopens for queued retry",
			runStatus: models.RunFailed,
			allow:     true,
			attempts: []runprogression.AttemptSnapshot{
				definitionlessAttempt("task-1", "task", 1, models.TaskFailed),
				definitionlessAttempt("task-2", "task", 2, models.TaskQueued),
			},
			wantStatus: definitionlessRunStatus(models.RunRunning),
		},
		{
			name:      "cancelled run reopens for pending retry",
			runStatus: models.RunCancelled,
			allow:     true,
			attempts: []runprogression.AttemptSnapshot{
				definitionlessAttempt("task-1", "task", 1, models.TaskCancelled),
				definitionlessAttempt("task-2", "task", 2, models.TaskPending),
			},
			wantStatus: definitionlessRunStatus(models.RunRunning),
		},
		{
			name:      "successful run reopens for running retry",
			runStatus: models.RunSuccess,
			allow:     true,
			attempts: []runprogression.AttemptSnapshot{
				definitionlessAttempt("task-1", "task", 1, models.TaskSuccess),
				definitionlessAttempt("task-2", "task", 2, models.TaskRunning),
			},
			wantStatus: definitionlessRunStatus(models.RunRunning),
		},
		{
			name:      "automatic reconciliation cannot reopen",
			runStatus: models.RunFailed,
			attempts: []runprogression.AttemptSnapshot{
				definitionlessAttempt("task-2", "task", 2, models.TaskQueued),
			},
		},
		{
			name:      "authorization without successor cannot reopen",
			runStatus: models.RunFailed,
			allow:     true,
			attempts: []runprogression.AttemptSnapshot{
				definitionlessAttempt("task-1", "task", 1, models.TaskQueued),
			},
		},
		{
			name:      "older queued successor cannot reopen after newer terminal attempt",
			runStatus: models.RunCancelled,
			allow:     true,
			attempts: []runprogression.AttemptSnapshot{
				definitionlessAttempt("task-2", "task", 2, models.TaskQueued),
				definitionlessAttempt("task-3", "task", 3, models.TaskCancelled),
			},
		},
		{
			name:      "retryable marker is not a durable successor",
			runStatus: models.RunFailed,
			allow:     true,
			attempts: []runprogression.AttemptSnapshot{
				definitionlessAttempt("task-2", "task", 2, models.TaskUpForRetry),
			},
		},
		{
			name:      "running run does not emit redundant transition",
			runStatus: models.RunRunning,
			allow:     true,
			attempts: []runprogression.AttemptSnapshot{
				definitionlessAttempt("task-2", "task", 2, models.TaskRunning),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := runprogression.Input{
				RunStatus:            tt.runStatus,
				AllowCancelledReopen: tt.allow,
				Attempts:             tt.attempts,
			}

			first := runprogression.Evaluate(input)
			second := runprogression.Evaluate(input)

			if !reflect.DeepEqual(first.DesiredRunStatus, tt.wantStatus) {
				t.Fatalf("desired run status = %v, want %v", first.DesiredRunStatus, tt.wantStatus)
			}
			if len(first.PromoteAttemptIDs) != 0 {
				t.Fatalf("definitionless plan promoted attempts: %v", first.PromoteAttemptIDs)
			}
			if !reflect.DeepEqual(second, first) {
				t.Fatalf("replayed evaluation changed plan:\nfirst:  %#v\nsecond: %#v", first, second)
			}
		})
	}
}

func definitionlessAttempt(
	id string,
	taskID string,
	number int,
	status models.TaskStatus,
) runprogression.AttemptSnapshot {
	return runprogression.AttemptSnapshot{
		ID:      id,
		TaskID:  taskID,
		Attempt: number,
		Status:  status,
	}
}

func definitionlessRunStatus(status models.RunStatus) *models.RunStatus {
	return &status
}
