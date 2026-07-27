package runprogression_test

import (
	"reflect"
	"testing"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/runprogression"
)

func TestEvaluatePromotesOnlyWhenEveryRequiredPredecessorSucceeded(t *testing.T) {
	definitions := []runprogression.TaskDefinition{
		{ID: "left"},
		{ID: "right"},
		{ID: "join", DependsOn: []string{"left", "right"}},
	}

	tests := []struct {
		name     string
		attempts []runprogression.AttemptSnapshot
		want     []string
	}{
		{
			name: "all predecessors succeeded",
			attempts: []runprogression.AttemptSnapshot{
				attempt("left-1", "left", 1, models.TaskSuccess),
				attempt("right-1", "right", 1, models.TaskSuccess),
				attempt("join-1", "join", 1, models.TaskPending),
			},
			want: []string{"join-1"},
		},
		{
			name: "missing predecessor snapshot",
			attempts: []runprogression.AttemptSnapshot{
				attempt("left-1", "left", 1, models.TaskSuccess),
				attempt("join-1", "join", 1, models.TaskPending),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := runprogression.Evaluate(runprogression.Input{
				RunStatus:   models.RunRunning,
				Definitions: definitions,
				Attempts:    tt.attempts,
			})
			if !reflect.DeepEqual(plan.PromoteAttemptIDs, tt.want) {
				t.Fatalf("promotions = %v, want %v", plan.PromoteAttemptIDs, tt.want)
			}
		})
	}
}

func TestEvaluateEveryNonSuccessPredecessorBlocksPromotion(t *testing.T) {
	blocking := []models.TaskStatus{
		models.TaskPending,
		models.TaskQueued,
		models.TaskRunning,
		models.TaskUpForRetry,
		models.TaskFailed,
		models.TaskCancelled,
	}

	for _, status := range blocking {
		t.Run(string(status), func(t *testing.T) {
			plan := runprogression.Evaluate(runprogression.Input{
				RunStatus: models.RunRunning,
				Definitions: []runprogression.TaskDefinition{
					{ID: "predecessor"},
					{ID: "downstream", DependsOn: []string{"predecessor"}},
				},
				Attempts: []runprogression.AttemptSnapshot{
					attempt("predecessor-1", "predecessor", 1, status),
					attempt("downstream-1", "downstream", 1, models.TaskPending),
				},
			})
			if len(plan.PromoteAttemptIDs) != 0 {
				t.Fatalf("status %q promoted downstream attempts: %v", status, plan.PromoteAttemptIDs)
			}
		})
	}
}

func TestEvaluateUsesLatestLogicalAttemptAndEmitsCanonicalReplaySafePlan(t *testing.T) {
	input := runprogression.Input{
		RunStatus: models.RunRunning,
		Definitions: []runprogression.TaskDefinition{
			{ID: "root"},
			{ID: "dependent", DependsOn: []string{"root"}},
			{ID: "other"},
		},
		Attempts: []runprogression.AttemptSnapshot{
			attempt("dependent-1", "dependent", 1, models.TaskPending),
			attempt("root-2", "root", 2, models.TaskSuccess),
			attempt("other-1", "other", 1, models.TaskPending),
			attempt("root-1", "root", 1, models.TaskFailed),
			// A replayed snapshot must not duplicate a logical promotion.
			attempt("dependent-1", "dependent", 1, models.TaskPending),
		},
	}

	first := runprogression.Evaluate(input)
	second := runprogression.Evaluate(input)
	want := []string{"dependent-1", "other-1"}
	if !reflect.DeepEqual(first.PromoteAttemptIDs, want) {
		t.Fatalf("promotions = %v, want sorted/deduplicated %v", first.PromoteAttemptIDs, want)
	}
	if !reflect.DeepEqual(second, first) {
		t.Fatalf("replayed evaluation changed plan:\nfirst:  %#v\nsecond: %#v", first, second)
	}
}

func TestEvaluateDoesNotPromoteAnOlderPendingAttempt(t *testing.T) {
	plan := runprogression.Evaluate(runprogression.Input{
		RunStatus:   models.RunRunning,
		Definitions: []runprogression.TaskDefinition{{ID: "task"}},
		Attempts: []runprogression.AttemptSnapshot{
			attempt("task-1", "task", 1, models.TaskPending),
			attempt("task-2", "task", 2, models.TaskQueued),
		},
	})
	if len(plan.PromoteAttemptIDs) != 0 {
		t.Fatalf("promoted a non-current attempt: %v", plan.PromoteAttemptIDs)
	}
}

func TestEvaluateDerivesTerminalRunStatusFromAuthoritativeTasks(t *testing.T) {
	status := func(value models.RunStatus) *models.RunStatus { return &value }
	definitions := []runprogression.TaskDefinition{{ID: "one"}, {ID: "two"}}

	tests := []struct {
		name       string
		runStatus  models.RunStatus
		attempts   []runprogression.AttemptSnapshot
		wantStatus *models.RunStatus
	}{
		{
			name:      "all successful",
			runStatus: models.RunRunning,
			attempts: []runprogression.AttemptSnapshot{
				attempt("one-1", "one", 1, models.TaskSuccess),
				attempt("two-1", "two", 1, models.TaskSuccess),
			},
			wantStatus: status(models.RunSuccess),
		},
		{
			name:      "retryable failure remains nonterminal",
			runStatus: models.RunRunning,
			attempts: []runprogression.AttemptSnapshot{
				attempt("one-1", "one", 1, models.TaskUpForRetry),
				attempt("two-1", "two", 1, models.TaskPending),
			},
		},
		{
			name:      "exhausted failure fails run",
			runStatus: models.RunRunning,
			attempts: []runprogression.AttemptSnapshot{
				attempt("one-1", "one", 1, models.TaskFailed),
				attempt("two-1", "two", 1, models.TaskPending),
			},
			wantStatus: status(models.RunFailed),
		},
		{
			name:      "authoritative cancellation wins over failure",
			runStatus: models.RunRunning,
			attempts: []runprogression.AttemptSnapshot{
				attempt("one-1", "one", 1, models.TaskFailed),
				attempt("two-1", "two", 1, models.TaskCancelled),
			},
			wantStatus: status(models.RunCancelled),
		},
		{
			name:      "partial snapshot cannot succeed",
			runStatus: models.RunRunning,
			attempts: []runprogression.AttemptSnapshot{
				attempt("one-1", "one", 1, models.TaskSuccess),
			},
		},
		{
			name:      "already terminal status emits no redundant transition",
			runStatus: models.RunSuccess,
			attempts: []runprogression.AttemptSnapshot{
				attempt("one-1", "one", 1, models.TaskSuccess),
				attempt("two-1", "two", 1, models.TaskSuccess),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := runprogression.Evaluate(runprogression.Input{
				RunStatus:   tt.runStatus,
				Definitions: definitions,
				Attempts:    tt.attempts,
			})
			if !reflect.DeepEqual(plan.DesiredRunStatus, tt.wantStatus) {
				t.Fatalf("desired run status = %v, want %v", plan.DesiredRunStatus, tt.wantStatus)
			}
		})
	}
}

func TestEvaluateIgnoresMappedChildrenAsRunAuthorities(t *testing.T) {
	plan := runprogression.Evaluate(runprogression.Input{
		RunStatus: models.RunRunning,
		Definitions: []runprogression.TaskDefinition{
			{ID: "map"},
			{ID: "after", DependsOn: []string{"map"}},
		},
		Attempts: []runprogression.AttemptSnapshot{
			attempt("map-1", "map", 1, models.TaskSuccess),
			attempt("map-child-0", "map[0]", 1, models.TaskCancelled),
			attempt("map-child-1", "map[1]", 1, models.TaskFailed),
			attempt("after-1", "after", 1, models.TaskPending),
		},
	})

	if !reflect.DeepEqual(plan.PromoteAttemptIDs, []string{"after-1"}) {
		t.Fatalf("mapped children affected downstream promotion: %v", plan.PromoteAttemptIDs)
	}
	if plan.DesiredRunStatus != nil {
		t.Fatalf("mapped child produced run transition: %v", *plan.DesiredRunStatus)
	}
}

func attempt(id, taskID string, number int, status models.TaskStatus) runprogression.AttemptSnapshot {
	return runprogression.AttemptSnapshot{
		ID:      id,
		TaskID:  taskID,
		Attempt: number,
		Status:  status,
	}
}
