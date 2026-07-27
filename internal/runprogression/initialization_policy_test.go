package runprogression_test

import (
	"reflect"
	"testing"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/runprogression"
)

func TestEvaluateInitializationPromotesPendingChainRoots(t *testing.T) {
	plan := runprogression.Evaluate(runprogression.Input{
		RunStatus:               models.RunRunning,
		AllowChainRootPromotion: true,
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

	want := []string{"chain-root-1", "independent-1"}
	if !reflect.DeepEqual(plan.PromoteAttemptIDs, want) {
		t.Fatalf("promotions = %v, want initialization roots %v", plan.PromoteAttemptIDs, want)
	}
	if plan.DesiredRunStatus != nil {
		t.Fatalf("initialization produced run transition to %s", *plan.DesiredRunStatus)
	}
}

func TestEvaluateInitializationPromotesEveryFanInRootButNotJoin(t *testing.T) {
	plan := runprogression.Evaluate(runprogression.Input{
		RunStatus:               models.RunRunning,
		AllowChainRootPromotion: true,
		Definitions: []runprogression.TaskDefinition{
			{ID: "left"},
			{ID: "right"},
			{ID: "join", DependsOn: []string{"left", "right"}},
		},
		Attempts: []runprogression.AttemptSnapshot{
			{ID: "right-1", TaskID: "right", Attempt: 1, Status: models.TaskPending},
			{ID: "join-1", TaskID: "join", Attempt: 1, Status: models.TaskPending},
			{ID: "left-1", TaskID: "left", Attempt: 1, Status: models.TaskPending},
		},
	})

	want := []string{"left-1", "right-1"}
	if !reflect.DeepEqual(plan.PromoteAttemptIDs, want) {
		t.Fatalf("promotions = %v, want sorted fan-in roots %v", plan.PromoteAttemptIDs, want)
	}
}

func TestEvaluateInitializationRemainsTerminalFirst(t *testing.T) {
	for _, test := range []struct {
		name       string
		status     models.TaskStatus
		wantStatus models.RunStatus
	}{
		{name: "failure", status: models.TaskFailed, wantStatus: models.RunFailed},
		{name: "cancellation", status: models.TaskCancelled, wantStatus: models.RunCancelled},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := runprogression.Evaluate(runprogression.Input{
				RunStatus:               models.RunRunning,
				AllowChainRootPromotion: true,
				Definitions: []runprogression.TaskDefinition{
					{ID: "chain-root"},
					{ID: "downstream", DependsOn: []string{"chain-root"}},
					{ID: "terminal"},
				},
				Attempts: []runprogression.AttemptSnapshot{
					{ID: "chain-root-1", TaskID: "chain-root", Attempt: 1, Status: models.TaskPending},
					{ID: "downstream-1", TaskID: "downstream", Attempt: 1, Status: models.TaskPending},
					{ID: "terminal-1", TaskID: "terminal", Attempt: 1, Status: test.status},
				},
			})

			if len(plan.PromoteAttemptIDs) != 0 {
				t.Fatalf("terminal initialization promoted attempts: %v", plan.PromoteAttemptIDs)
			}
			if plan.DesiredRunStatus == nil || *plan.DesiredRunStatus != test.wantStatus {
				t.Fatalf("desired run status = %v, want %s", plan.DesiredRunStatus, test.wantStatus)
			}
		})
	}
}

func TestEvaluateDefaultPolicyStillSuppressesChainRoots(t *testing.T) {
	input := runprogression.Input{
		RunStatus: models.RunRunning,
		Definitions: []runprogression.TaskDefinition{
			{ID: "scheduler-owned-root"},
			{ID: "downstream", DependsOn: []string{"scheduler-owned-root"}},
			{ID: "independent"},
		},
		Attempts: []runprogression.AttemptSnapshot{
			{
				ID: "scheduler-owned-root-1", TaskID: "scheduler-owned-root",
				Attempt: 1, Status: models.TaskPending,
			},
			{ID: "downstream-1", TaskID: "downstream", Attempt: 1, Status: models.TaskPending},
			{ID: "independent-1", TaskID: "independent", Attempt: 1, Status: models.TaskPending},
		},
	}

	plan := runprogression.Evaluate(input)
	want := []string{"independent-1"}
	if !reflect.DeepEqual(plan.PromoteAttemptIDs, want) {
		t.Fatalf("default promotions = %v, want %v", plan.PromoteAttemptIDs, want)
	}
}
