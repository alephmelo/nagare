// Package runprogression contains the run-level dependency and terminal-state
// policy. It is deliberately pure: callers load snapshots and apply the plan.
package runprogression

import (
	"sort"

	"github.com/alephmelo/nagare/internal/models"
)

type TaskDefinition struct {
	ID        string
	DependsOn []string
}

type AttemptSnapshot struct {
	ID      string
	TaskID  string
	Attempt int
	Status  models.TaskStatus
}

type Input struct {
	RunStatus models.RunStatus
	// AllowCancelledReopen is reserved for an explicit manual retry. It only
	// takes effect when Attempts proves that a durable current retry successor
	// exists; ordinary and automatic reconciliation must leave it false.
	AllowCancelledReopen bool
	Definitions          []TaskDefinition
	Attempts             []AttemptSnapshot
}

type Plan struct {
	PromoteAttemptIDs []string
	DesiredRunStatus  *models.RunStatus
}

// Evaluate returns a canonical, replay-safe progression plan.
func Evaluate(input Input) Plan {
	definitions := make(map[string]TaskDefinition, len(input.Definitions))
	for _, definition := range input.Definitions {
		definitions[definition.ID] = definition
	}

	latest := make(map[string]AttemptSnapshot, len(definitions))
	for _, attempt := range input.Attempts {
		if _, authoritative := definitions[attempt.TaskID]; !authoritative {
			continue
		}
		current, exists := latest[attempt.TaskID]
		if !exists || attempt.Attempt > current.Attempt ||
			(attempt.Attempt == current.Attempt && attempt.ID < current.ID) {
			latest[attempt.TaskID] = attempt
		}
	}

	plan := Plan{}
	if len(definitions) > 0 {
		desiredRunStatus, terminal := deriveRunStatus(
			input.RunStatus,
			input.AllowCancelledReopen,
			definitions,
			latest,
		)
		plan.DesiredRunStatus = desiredRunStatus
		if terminal {
			return plan
		}
	}

	requiredPredecessors := make(map[string]struct{})
	for _, definition := range definitions {
		for _, predecessorID := range definition.DependsOn {
			requiredPredecessors[predecessorID] = struct{}{}
		}
	}

	promotions := make(map[string]struct{})
	for taskID, definition := range definitions {
		attempt, exists := latest[taskID]
		if !exists || attempt.Status != models.TaskPending {
			continue
		}
		// Roots that feed a dependency chain are queued by run materialization,
		// not dependency progression. Keeping that initialization boundary
		// explicit prevents a blocked downstream check from also reporting its
		// pending prerequisite as newly runnable. Independent roots remain valid
		// progression candidates.
		if len(definition.DependsOn) == 0 {
			if _, initializesChain := requiredPredecessors[taskID]; initializesChain {
				continue
			}
		}
		ready := true
		for _, predecessorID := range definition.DependsOn {
			predecessor, exists := latest[predecessorID]
			if !exists || predecessor.Status != models.TaskSuccess {
				ready = false
				break
			}
		}
		if ready {
			promotions[attempt.ID] = struct{}{}
		}
	}

	for attemptID := range promotions {
		plan.PromoteAttemptIDs = append(plan.PromoteAttemptIDs, attemptID)
	}
	sort.Strings(plan.PromoteAttemptIDs)
	return plan
}

func deriveRunStatus(
	current models.RunStatus,
	allowCancelledReopen bool,
	definitions map[string]TaskDefinition,
	latest map[string]AttemptSnapshot,
) (*models.RunStatus, bool) {
	if current == models.RunCancelled &&
		(!allowCancelledReopen || !hasDurableRetrySuccessor(latest)) {
		return nil, true
	}

	allSuccess := true
	anyFailed := false
	anyCancelled := false
	for taskID := range definitions {
		attempt, exists := latest[taskID]
		if !exists {
			allSuccess = false
			continue
		}
		switch attempt.Status {
		case models.TaskSuccess:
		case models.TaskFailed:
			allSuccess = false
			anyFailed = true
		case models.TaskCancelled:
			allSuccess = false
			anyCancelled = true
		default:
			allSuccess = false
		}
	}

	var desired models.RunStatus
	switch {
	case anyCancelled:
		desired = models.RunCancelled
	case anyFailed:
		desired = models.RunFailed
	case allSuccess:
		desired = models.RunSuccess
	case current != models.RunRunning:
		desired = models.RunRunning
	default:
		return nil, false
	}

	terminal := desired != models.RunRunning
	if desired == current {
		return nil, terminal
	}
	return &desired, terminal
}

func hasDurableRetrySuccessor(latest map[string]AttemptSnapshot) bool {
	for _, attempt := range latest {
		if attempt.Attempt <= 1 {
			continue
		}
		switch attempt.Status {
		case models.TaskPending, models.TaskQueued, models.TaskRunning:
			return true
		}
	}
	return false
}
