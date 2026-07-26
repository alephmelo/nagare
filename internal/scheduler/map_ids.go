package scheduler

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/alephmelo/nagare/internal/models"
)

// splitMapChildID recognizes only the scheduler's public map-child shape.
// Brackets in ordinary task IDs remain ordinary data.
func splitMapChildID(taskID string) (parentID string, index int, ok bool) {
	if !strings.HasSuffix(taskID, "]") {
		return "", 0, false
	}
	open := strings.LastIndexByte(taskID, '[')
	if open <= 0 || open == len(taskID)-2 {
		return "", 0, false
	}
	index, err := strconv.Atoi(taskID[open+1 : len(taskID)-1])
	if err != nil || index < 0 {
		return "", 0, false
	}
	return taskID[:open], index, true
}

// splitInternalMapGeneration recognizes the exact reserved storage suffix.
// Generation one deliberately has no suffix for backwards compatibility.
func splitInternalMapGeneration(taskID string) (publicID string, generation int, ok bool) {
	suffix := strings.LastIndex(taskID, "@g")
	if suffix < 0 || suffix+2 == len(taskID) {
		return "", 0, false
	}
	generation, err := strconv.Atoi(taskID[suffix+2:])
	if err != nil || generation < 2 {
		return "", 0, false
	}
	publicID = taskID[:suffix]
	if _, _, child := splitMapChildID(publicID); !child {
		return "", 0, false
	}
	return publicID, generation, true
}

// PublicTaskID removes scheduler-only map-generation storage metadata.
func PublicTaskID(taskID string) string {
	if publicID, _, ok := splitInternalMapGeneration(taskID); ok {
		return publicID
	}
	return taskID
}

func generationTaskID(publicTaskID string, generation int) string {
	if generation <= 1 {
		return publicTaskID
	}
	return fmt.Sprintf("%s@g%d", publicTaskID, generation)
}

func belongsToMapGeneration(taskID, parentID string, generation int) bool {
	publicID := PublicTaskID(taskID)
	childParent, _, ok := splitMapChildID(publicID)
	return ok && childParent == parentID && taskID == generationTaskID(publicID, generation)
}

// ResolveTaskID maps a public map-child identity to the current parent
// generation without interpreting bracketed or @g-suffixed ordinary IDs.
func (s *Scheduler) ResolveTaskID(runID, taskID string) (string, error) {
	publicID := PublicTaskID(taskID)
	parentID, _, isChild := splitMapChildID(publicID)
	if !isChild {
		return taskID, nil
	}

	run, err := s.store.GetDagRun(runID)
	if err != nil {
		return "", err
	}
	s.mu.RLock()
	dag := s.dags[run.DAGID]
	s.mu.RUnlock()
	if dag == nil {
		return "", fmt.Errorf("DAG %s not found for run %s", run.DAGID, runID)
	}

	// An exact DAG task definition always wins over scheduler interpretation.
	if dag.FindTask(publicID) != nil {
		return taskID, nil
	}
	parentDef := dag.FindTask(parentID)
	if parentDef == nil || parentDef.Type != "map" {
		return taskID, nil
	}

	parents, err := s.store.GetTaskAttempts(runID, parentID)
	if err != nil {
		return "", err
	}
	if len(parents) == 0 {
		return "", fmt.Errorf("map parent %s not found in run %s", parentID, runID)
	}
	return generationTaskID(publicID, parents[len(parents)-1].Attempt), nil
}

// ProjectTaskInstance returns a copy with scheduler-only storage identities
// translated back to their stable public forms.
func ProjectTaskInstance(task models.TaskInstance) models.TaskInstance {
	publicID := PublicTaskID(task.TaskID)
	if publicID == task.TaskID {
		return task
	}
	storagePrefix := task.RunID + "_" + task.TaskID
	if strings.HasPrefix(task.ID, storagePrefix) {
		task.ID = task.RunID + "_" + publicID + strings.TrimPrefix(task.ID, storagePrefix)
	}
	task.TaskID = publicID
	return task
}

// ResolveTaskInstanceID maps a projected current-attempt identity back to its
// persisted identity. Raw storage IDs remain accepted for compatibility.
func (s *Scheduler) ResolveTaskInstanceID(runID, instanceID string) (string, error) {
	tasks, err := s.store.GetLatestTaskAttempts(runID)
	if err != nil {
		return "", err
	}
	for _, task := range tasks {
		publicID := PublicTaskID(task.TaskID)
		resolvedID, resolveErr := s.ResolveTaskID(runID, publicID)
		if resolveErr != nil || resolvedID != task.TaskID {
			continue
		}
		if ProjectTaskInstance(task).ID == instanceID {
			return task.ID, nil
		}
	}
	if task, err := s.store.GetTaskInstance(instanceID); err == nil {
		if task.RunID != runID {
			return "", sql.ErrNoRows
		}
		return instanceID, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	return "", sql.ErrNoRows
}
