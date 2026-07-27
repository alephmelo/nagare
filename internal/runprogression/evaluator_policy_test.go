package runprogression_test

import (
	"reflect"
	"testing"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/runprogression"
)

func TestEvaluateSeparatesChainInitializationFromIndependentRoots(t *testing.T) {
	plan := runprogression.Evaluate(runprogression.Input{
		RunStatus: models.RunRunning,
		Definitions: []runprogression.TaskDefinition{
			{ID: "chain-root"},
			{ID: "downstream", DependsOn: []string{"chain-root"}},
			{ID: "independent"},
		},
		Attempts: []runprogression.AttemptSnapshot{
			{ID: "chain-root-1", TaskID: "chain-root", Attempt: 1, Status: models.TaskPending},
			{ID: "downstream-1", TaskID: "downstream", Attempt: 1, Status: models.TaskPending},
			{ID: "independent-1", TaskID: "independent", Attempt: 1, Status: models.TaskPending},
		},
	})

	if want := []string{"independent-1"}; !reflect.DeepEqual(plan.PromoteAttemptIDs, want) {
		t.Fatalf("promotions = %v, want %v", plan.PromoteAttemptIDs, want)
	}
}

func TestEvaluateTerminalPlansNeverPromote(t *testing.T) {
	tests := []struct {
		name       string
		blocker    models.TaskStatus
		wantStatus models.RunStatus
	}{
		{name: "failure", blocker: models.TaskFailed, wantStatus: models.RunFailed},
		{name: "cancellation", blocker: models.TaskCancelled, wantStatus: models.RunCancelled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := runprogression.Evaluate(runprogression.Input{
				RunStatus: models.RunRunning,
				Definitions: []runprogression.TaskDefinition{
					{ID: "ready"},
					{ID: "terminal"},
				},
				Attempts: []runprogression.AttemptSnapshot{
					{ID: "ready-1", TaskID: "ready", Attempt: 1, Status: models.TaskPending},
					{ID: "terminal-1", TaskID: "terminal", Attempt: 1, Status: tt.blocker},
				},
			})

			if len(plan.PromoteAttemptIDs) != 0 {
				t.Fatalf("terminal plan promoted attempts: %v", plan.PromoteAttemptIDs)
			}
			if plan.DesiredRunStatus == nil || *plan.DesiredRunStatus != tt.wantStatus {
				t.Fatalf("desired run status = %v, want %s", plan.DesiredRunStatus, tt.wantStatus)
			}
		})
	}
}

func TestEvaluateCancelledRunRemainsTerminal(t *testing.T) {
	plan := runprogression.Evaluate(runprogression.Input{
		RunStatus:   models.RunCancelled,
		Definitions: []runprogression.TaskDefinition{{ID: "task"}},
		Attempts: []runprogression.AttemptSnapshot{
			{ID: "task-1", TaskID: "task", Attempt: 1, Status: models.TaskPending},
		},
	})

	if plan.DesiredRunStatus != nil {
		t.Fatalf("cancelled run produced transition to %s", *plan.DesiredRunStatus)
	}
	if len(plan.PromoteAttemptIDs) != 0 {
		t.Fatalf("cancelled run promoted attempts: %v", plan.PromoteAttemptIDs)
	}
}

func TestEvaluateExplicitRetryMayReopenCancelledRunAfterDurableSuccessor(t *testing.T) {
	tests := []struct {
		name       string
		allow      bool
		attempts   []runprogression.AttemptSnapshot
		wantStatus *models.RunStatus
	}{
		{
			name:  "default policy keeps cancellation terminal",
			allow: false,
			attempts: []runprogression.AttemptSnapshot{
				{ID: "task-2", TaskID: "task", Attempt: 2, Status: models.TaskQueued},
			},
		},
		{
			name:  "authorization alone cannot reopen",
			allow: true,
			attempts: []runprogression.AttemptSnapshot{
				{ID: "task-1", TaskID: "task", Attempt: 1, Status: models.TaskCancelled},
			},
		},
		{
			name:  "obsolete successor cannot reopen",
			allow: true,
			attempts: []runprogression.AttemptSnapshot{
				{ID: "task-2", TaskID: "task", Attempt: 2, Status: models.TaskQueued},
				{ID: "task-3", TaskID: "task", Attempt: 3, Status: models.TaskCancelled},
			},
		},
		{
			name:  "durable retry successor reopens",
			allow: true,
			attempts: []runprogression.AttemptSnapshot{
				{ID: "task-2", TaskID: "task", Attempt: 2, Status: models.TaskQueued},
			},
			wantStatus: runStatus(models.RunRunning),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := runprogression.Evaluate(runprogression.Input{
				RunStatus:            models.RunCancelled,
				AllowCancelledReopen: tt.allow,
				Definitions:          []runprogression.TaskDefinition{{ID: "task"}},
				Attempts:             tt.attempts,
			})

			if !reflect.DeepEqual(plan.DesiredRunStatus, tt.wantStatus) {
				t.Fatalf("desired run status = %v, want %v", plan.DesiredRunStatus, tt.wantStatus)
			}
			if len(plan.PromoteAttemptIDs) != 0 {
				t.Fatalf("cancelled retry plan promoted attempts: %v", plan.PromoteAttemptIDs)
			}
		})
	}
}

func runStatus(status models.RunStatus) *models.RunStatus {
	return &status
}
