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
	RunStatus   models.RunStatus
	Definitions []TaskDefinition
	Attempts    []AttemptSnapshot
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

	promotions := make(map[string]struct{})
	for taskID, definition := range definitions {
		attempt, exists := latest[taskID]
		if !exists || attempt.Status != models.TaskPending {
			continue
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

	plan := Plan{}
	for attemptID := range promotions {
		plan.PromoteAttemptIDs = append(plan.PromoteAttemptIDs, attemptID)
	}
	sort.Strings(plan.PromoteAttemptIDs)

	if len(definitions) == 0 {
		return plan
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
	case input.RunStatus != models.RunRunning && input.RunStatus != models.RunCancelled:
		desired = models.RunRunning
	default:
		if input.RunStatus == models.RunCancelled {
			plan.PromoteAttemptIDs = nil
		}
		return plan
	}
	if desired != input.RunStatus {
		plan.DesiredRunStatus = &desired
	}
	if desired == models.RunFailed || desired == models.RunCancelled || desired == models.RunSuccess {
		plan.PromoteAttemptIDs = nil
	}
	return plan
}
