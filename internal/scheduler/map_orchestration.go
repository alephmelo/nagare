package scheduler

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/tasklifecycle"
)

func (s *Scheduler) promoteAndRecover() error {
	s.orchestrate.Lock()
	defer s.orchestrate.Unlock()

	pending, err := s.store.GetTasksByStatus(models.TaskPending)
	if err != nil {
		return err
	}

	var mapParents []models.TaskInstance
	seenMapParents := make(map[string]bool)
	for _, task := range pending {
		run, dag, err := s.runAndDAG(task.RunID)
		if err != nil {
			log.Printf("Failed to resolve DAG for pending task %s: %v", task.ID, err)
			if failErr := s.failUnresolvablePendingLocked(task); failErr != nil {
				return errors.Join(err, failErr)
			}
			continue
		}
		if run.Status != models.RunRunning {
			continue
		}

		taskDef := dag.FindTask(task.TaskID)
		if taskDef == nil {
			if s.isStoredMapChild(dag, task.TaskID) {
				// Reconciliation promotes map children only after it has proved
				// that the complete expected set exists.
				continue
			}
			if err := s.failUnresolvablePendingLocked(task); err != nil {
				return err
			}
			continue
		}
		if taskDef.Type == "map" {
			if !s.mapSetupCanResume(task, taskDef) {
				continue
			}
			mapParents = append(mapParents, task)
			seenMapParents[task.ID] = true
			continue
		}
		if !s.dependenciesSucceeded(task.RunID, taskDef) {
			continue
		}

		log.Printf("Promoting task %s to queued", task.ID)
		if _, err := s.lifecycle.Promote(task.ID, time.Now().UTC()); err != nil {
			return err
		}
	}

	// A crash may leave a map parent queued before setup ownership, or running
	// after setup ownership. Enumerate both so either phase can be replayed.
	for _, status := range []models.TaskStatus{models.TaskQueued, models.TaskRunning} {
		tasks, err := s.store.GetTasksByStatus(status)
		if err != nil {
			return err
		}
		for _, task := range tasks {
			if seenMapParents[task.ID] {
				continue
			}
			run, dag, err := s.runAndDAG(task.RunID)
			if err != nil || run.Status != models.RunRunning {
				continue
			}
			taskDef := dag.FindTask(task.TaskID)
			if taskDef == nil || taskDef.Type != "map" || taskDef.ID != task.TaskID {
				continue
			}
			if status != models.TaskRunning && !s.mapSetupCanResume(task, taskDef) {
				continue
			}
			mapParents = append(mapParents, task)
			seenMapParents[task.ID] = true
		}
	}

	sort.Slice(mapParents, func(i, j int) bool {
		if mapParents[i].CreatedAt.Equal(mapParents[j].CreatedAt) {
			return mapParents[i].ID < mapParents[j].ID
		}
		return mapParents[i].CreatedAt.Before(mapParents[j].CreatedAt)
	})
	for _, parent := range mapParents {
		_, dag, err := s.runAndDAG(parent.RunID)
		if err != nil {
			return err
		}
		taskDef := dag.FindTask(parent.TaskID)
		if taskDef == nil || taskDef.Type != "map" {
			continue
		}
		if err := s.reconcileMapParentLocked(parent, taskDef); err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) runAndDAG(runID string) (*models.DagRun, *models.DAGDef, error) {
	run, err := s.store.GetDagRun(runID)
	if err != nil {
		return nil, nil, err
	}
	s.mu.RLock()
	dag := s.dags[run.DAGID]
	s.mu.RUnlock()
	if dag == nil {
		return run, nil, fmt.Errorf("DAG %s not found in memory", run.DAGID)
	}
	return run, dag, nil
}

func (s *Scheduler) failUnresolvablePendingLocked(task models.TaskInstance) error {
	disposition, err := s.lifecycle.StartSetup(task.ID, time.Now().UTC())
	if err != nil {
		return err
	}
	if disposition != tasklifecycle.Applied {
		return nil
	}
	_, err = s.lifecycle.Complete(tasklifecycle.Completion{
		AttemptID: task.ID, CompletedAt: time.Now().UTC(),
	})
	return err
}

func (s *Scheduler) dependenciesSucceeded(runID string, taskDef *models.TaskDef) bool {
	for _, dependency := range taskDef.DependsOn {
		status, err := s.store.GetTaskStatus(runID, dependency)
		if err != nil || status != models.TaskSuccess {
			return false
		}
	}
	return true
}

func (s *Scheduler) mapSetupCanResume(task models.TaskInstance, taskDef *models.TaskDef) bool {
	if _, err := s.store.GetMapSetup(task.ID); err == nil {
		return true
	} else if !errors.Is(err, sql.ErrNoRows) {
		// Let reconciliation surface the storage failure.
		return true
	}
	return s.dependenciesSucceeded(task.RunID, taskDef)
}

func (s *Scheduler) isStoredMapChild(dag *models.DAGDef, taskID string) bool {
	publicID := PublicTaskID(taskID)
	parentID, _, ok := splitMapChildID(publicID)
	if !ok || dag.FindTask(publicID) != nil {
		return false
	}
	taskDef := dag.FindTask(parentID)
	return taskDef != nil && taskDef.Type == "map"
}

func (s *Scheduler) reconcileMapParentLocked(parent models.TaskInstance, taskDef *models.TaskDef) error {
	attempts, err := s.store.GetTaskAttempts(parent.RunID, parent.TaskID)
	if err != nil {
		return err
	}
	if len(attempts) == 0 || attempts[len(attempts)-1].ID != parent.ID {
		return nil
	}
	parent = attempts[len(attempts)-1]

	setup, err := s.ensureMapSetupLocked(parent, taskDef)
	if err != nil {
		if _, claimErr := s.lifecycle.StartSetup(parent.ID, time.Now().UTC()); claimErr != nil {
			return errors.Join(err, claimErr)
		}
		reloaded, reloadErr := s.store.GetTaskInstance(parent.ID)
		if reloadErr == nil {
			parent = *reloaded
		}
		if parent.Status == models.TaskCancelled {
			return nil
		}
		return s.failMapParentLocked(parent, taskDef, err)
	}
	disposition, err := s.lifecycle.StartSetup(parent.ID, setup.StartedAt)
	if err != nil {
		reloaded, reloadErr := s.store.GetTaskInstance(parent.ID)
		if reloadErr == nil && reloaded.Status == models.TaskCancelled {
			return nil
		}
		return errors.Join(err, reloadErr)
	}
	if disposition != tasklifecycle.Applied && disposition != tasklifecycle.AlreadyApplied {
		return fmt.Errorf("start map setup %s returned unknown disposition %d", parent.ID, disposition)
	}

	parent, err = s.reloadOwnedMapSetup(parent.ID, setup)
	if err != nil {
		return s.failMapParentLocked(parent, taskDef, err)
	}
	items, err := decodeMapItems(setup.UpstreamOutput)
	if err != nil {
		return s.failMapParentLocked(parent, taskDef, err)
	}
	if err := s.validateBoundUpstream(parent.RunID, taskDef, setup); err != nil {
		return s.failMapParentLocked(parent, taskDef, err)
	}
	if err := s.reconcileMapChildrenLocked(parent, items); err != nil {
		return s.failMapParentLocked(parent, taskDef, err)
	}

	if len(items) == 0 {
		disposition, err := s.lifecycle.Complete(tasklifecycle.Completion{
			AttemptID: parent.ID, Succeeded: true, CompletedAt: time.Now().UTC(),
		})
		if err != nil {
			return err
		}
		if disposition == tasklifecycle.Applied {
			s.closeMapStream(parent.ID)
		}
	}
	return nil
}

func (s *Scheduler) ensureMapSetupLocked(parent models.TaskInstance, taskDef *models.TaskDef) (models.MapSetup, error) {
	if setup, err := s.store.GetMapSetup(parent.ID); err == nil {
		return *setup, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return models.MapSetup{}, err
	}

	var upstream models.TaskInstance
	attempts, err := s.store.GetTaskAttempts(parent.RunID, taskDef.MapOver)
	if err != nil {
		return models.MapSetup{}, fmt.Errorf("load map source %s: %w", taskDef.MapOver, err)
	}
	if len(attempts) == 0 {
		return models.MapSetup{}, fmt.Errorf("map source %s has no attempts", taskDef.MapOver)
	}
	upstream = attempts[len(attempts)-1]
	if upstream.Status != models.TaskSuccess {
		return models.MapSetup{}, fmt.Errorf("map source %s is %s, not success", upstream.ID, upstream.Status)
	}

	setup := models.MapSetup{
		ParentAttemptID:   parent.ID,
		UpstreamAttemptID: upstream.ID,
		UpstreamOutput:    upstream.Output,
		StartedAt:         time.Now().UTC(),
	}
	persisted, _, err := s.store.EnsureMapSetup(setup)
	if err != nil {
		return models.MapSetup{}, err
	}
	return persisted, nil
}

func (s *Scheduler) reloadOwnedMapSetup(parentID string, setup models.MapSetup) (models.TaskInstance, error) {
	parent, err := s.store.GetTaskInstance(parentID)
	if err != nil {
		return models.TaskInstance{}, err
	}
	if parent.Status != models.TaskRunning {
		return *parent, fmt.Errorf("map parent %s is %s after setup claim", parent.ID, parent.Status)
	}
	if parent.StartedAt == nil || !parent.StartedAt.Equal(setup.StartedAt) {
		return *parent, fmt.Errorf("map parent %s is running without scheduler setup ownership", parent.ID)
	}
	return *parent, nil
}

func decodeMapItems(output string) ([]string, error) {
	var items []string
	if err := json.Unmarshal([]byte(output), &items); err != nil {
		return nil, fmt.Errorf("failed to parse map_over JSON array: %w; output was: %s", err, output)
	}
	if items == nil {
		return nil, fmt.Errorf("map_over output must be a JSON array, got %s", output)
	}
	return items, nil
}

func (s *Scheduler) validateBoundUpstream(runID string, taskDef *models.TaskDef, setup models.MapSetup) error {
	upstream, err := s.store.GetTaskInstance(setup.UpstreamAttemptID)
	if err != nil {
		return fmt.Errorf("reload bound map source %s: %w", setup.UpstreamAttemptID, err)
	}
	if upstream.RunID != runID || upstream.TaskID != taskDef.MapOver {
		return fmt.Errorf("map setup source %s does not belong to %s/%s", upstream.ID, runID, taskDef.MapOver)
	}
	if upstream.Status != models.TaskSuccess {
		return fmt.Errorf("bound map source %s changed to %s", upstream.ID, upstream.Status)
	}
	if upstream.Output != setup.UpstreamOutput {
		return fmt.Errorf("bound map source %s output changed after setup", upstream.ID)
	}
	return nil
}

func expectedMapChildren(parent models.TaskInstance, items []string) map[string]string {
	expected := make(map[string]string, len(items))
	for index, item := range items {
		publicID := fmt.Sprintf("%s[%d]", parent.TaskID, index)
		expected[generationTaskID(publicID, parent.Attempt)] = item
	}
	return expected
}

func (s *Scheduler) reconcileMapChildrenLocked(parent models.TaskInstance, items []string) error {
	expected := expectedMapChildren(parent, items)
	tasks, err := s.store.GetLatestTaskAttempts(parent.RunID)
	if err != nil {
		return err
	}
	persisted := make(map[string]models.TaskInstance, len(expected))
	for _, task := range tasks {
		if !belongsToMapGeneration(task.TaskID, parent.TaskID, parent.Attempt) {
			continue
		}
		item, ok := expected[task.TaskID]
		if !ok {
			return fmt.Errorf("unexpected map child %s for parent %s generation %d", task.TaskID, parent.TaskID, parent.Attempt)
		}
		if task.ItemValue == nil || *task.ItemValue != item {
			return fmt.Errorf("map child %s has conflicting item value", task.TaskID)
		}
		if task.ID != mapChildAttemptID(parent.RunID, task.TaskID, task.Attempt) {
			return fmt.Errorf("map child %s has conflicting attempt identity %s", task.TaskID, task.ID)
		}
		persisted[task.TaskID] = task
	}

	now := time.Now().UTC()
	for index, item := range items {
		taskID := generationTaskID(fmt.Sprintf("%s[%d]", parent.TaskID, index), parent.Attempt)
		if _, exists := persisted[taskID]; exists {
			continue
		}
		itemValue := item
		child := &models.TaskInstance{
			ID:        mapChildAttemptID(parent.RunID, taskID, 1),
			RunID:     parent.RunID,
			TaskID:    taskID,
			Status:    models.TaskPending,
			ItemValue: &itemValue,
			Attempt:   1,
			CreatedAt: now,
			UpdatedAt: now,
		}
		if err := s.store.CreateTaskInstance(child); err != nil {
			return fmt.Errorf("create mapped instance %s: %w", child.ID, err)
		}
		persisted[taskID] = *child
	}
	if len(persisted) != len(expected) {
		return fmt.Errorf("map parent %s expected %d children, found %d", parent.ID, len(expected), len(persisted))
	}

	taskIDs := make([]string, 0, len(expected))
	for taskID := range expected {
		taskIDs = append(taskIDs, taskID)
	}
	sort.Strings(taskIDs)
	for _, taskID := range taskIDs {
		child := persisted[taskID]
		if child.Status != models.TaskPending {
			continue
		}
		if _, err := s.lifecycle.Promote(child.ID, time.Now().UTC()); err != nil {
			return fmt.Errorf("queue mapped instance %s: %w", child.ID, err)
		}
	}
	return nil
}

func mapChildAttemptID(runID, taskID string, attempt int) string {
	if attempt <= 1 {
		return runID + "_" + taskID
	}
	return fmt.Sprintf("%s_%s_%d", runID, taskID, attempt)
}

func (s *Scheduler) failMapParentLocked(parent models.TaskInstance, taskDef *models.TaskDef, setupErr error) error {
	cancelErr := s.cancelMapChildrenLocked(parent.RunID, parent.TaskID, parent.Attempt)
	_, completeErr := s.lifecycle.Complete(tasklifecycle.Completion{
		AttemptID:   parent.ID,
		Output:      setupErr.Error(),
		Retries:     taskDef.Retries,
		CompletedAt: time.Now().UTC(),
	})
	return errors.Join(setupErr, cancelErr, completeErr)
}

// cancelMapChildrenLocked cancels every cancellable latest child in one exact
// map generation. Callers must hold orchestrate.
func (s *Scheduler) cancelMapChildrenLocked(runID, parentID string, generation int) error {
	tasks, err := s.store.GetLatestTaskAttempts(runID)
	if err != nil {
		return err
	}
	var cancelErrs []error
	for _, task := range tasks {
		if !belongsToMapGeneration(task.TaskID, parentID, generation) {
			continue
		}
		switch task.Status {
		case models.TaskPending, models.TaskQueued, models.TaskRunning, models.TaskUpForRetry:
			if _, err := s.lifecycle.CancelAttempt(task.ID, time.Now().UTC()); err != nil {
				cancelErrs = append(cancelErrs, fmt.Errorf("cancel mapped instance %s: %w", task.ID, err))
			}
		}
	}
	return errors.Join(cancelErrs...)
}

func (s *Scheduler) evaluateRuns() error {
	runs, err := s.store.GetActiveDagRuns()
	if err != nil {
		return err
	}
	var evaluationErrs []error
	for _, run := range runs {
		s.orchestrate.Lock()
		s.mu.RLock()
		dag := s.dags[run.DAGID]
		s.mu.RUnlock()
		if dag == nil {
			s.orchestrate.Unlock()
			log.Printf("DAG %s not found in memory. Marking run %s as failed", run.DAGID, run.ID)
			if err := s.store.UpdateDagRunStatus(run.ID, models.RunFailed); err != nil {
				evaluationErrs = append(evaluationErrs, err)
			}
			continue
		}

		if err := s.aggregateMapParentsLocked(run.ID, dag); err != nil {
			s.orchestrate.Unlock()
			evaluationErrs = append(evaluationErrs, fmt.Errorf("aggregate maps for run %s: %w", run.ID, err))
			continue
		}
		tasks, err := s.store.GetLatestTaskAttempts(run.ID)
		if err != nil {
			s.orchestrate.Unlock()
			evaluationErrs = append(evaluationErrs, err)
			continue
		}
		tasks = currentGenerationTasks(dag, tasks)

		allSuccess := len(tasks) > 0
		anyFailed := false
		var retryTaskIDs []string
		now := time.Now().UTC()
		for _, task := range tasks {
			taskDef := taskDefinitionForStoredID(dag, task.TaskID)
			switch task.Status {
			case models.TaskUpForRetry:
				allSuccess = false
				if taskDef != nil {
					delay := time.Duration(taskDef.RetryDelaySeconds) * time.Second
					if !now.Before(task.UpdatedAt.Add(delay)) {
						retryTaskIDs = append(retryTaskIDs, task.TaskID)
					}
				}
			case models.TaskFailed:
				anyFailed = true
			case models.TaskSuccess:
			default:
				allSuccess = false
			}
		}
		if anyFailed {
			log.Printf("Marking run %s as failed", run.ID)
			if err := s.store.UpdateDagRunStatus(run.ID, models.RunFailed); err != nil {
				evaluationErrs = append(evaluationErrs, err)
			}
		} else if allSuccess {
			log.Printf("Marking run %s as success", run.ID)
			if err := s.store.UpdateDagRunStatus(run.ID, models.RunSuccess); err != nil {
				evaluationErrs = append(evaluationErrs, err)
			}
		}
		s.orchestrate.Unlock()

		for _, taskID := range retryTaskIDs {
			if err := s.RetryTask(run.ID, taskID); err != nil {
				evaluationErrs = append(evaluationErrs, fmt.Errorf("retry task %s: %w", taskID, err))
			}
		}
	}
	return errors.Join(evaluationErrs...)
}

func (s *Scheduler) aggregateMapParentsLocked(runID string, dag *models.DAGDef) error {
	tasks, err := s.store.GetLatestTaskAttempts(runID)
	if err != nil {
		return err
	}
	for _, parent := range tasks {
		taskDef := dag.FindTask(parent.TaskID)
		if taskDef == nil || taskDef.Type != "map" || taskDef.ID != parent.TaskID || parent.Status != models.TaskRunning {
			continue
		}
		setup, err := s.store.GetMapSetup(parent.ID)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		if parent.StartedAt == nil || !parent.StartedAt.Equal(setup.StartedAt) {
			return s.failMapParentLocked(parent, taskDef, fmt.Errorf("map parent %s has invalid setup ownership", parent.ID))
		}
		items, err := decodeMapItems(setup.UpstreamOutput)
		if err != nil {
			return s.failMapParentLocked(parent, taskDef, err)
		}
		expected := expectedMapChildren(parent, items)
		children := make(map[string]models.TaskInstance, len(expected))
		for _, task := range tasks {
			if !belongsToMapGeneration(task.TaskID, parent.TaskID, parent.Attempt) {
				continue
			}
			if _, ok := expected[task.TaskID]; !ok {
				return s.failMapParentLocked(parent, taskDef, fmt.Errorf("unexpected map child %s", task.TaskID))
			}
			children[task.TaskID] = task
		}
		if len(children) != len(expected) {
			continue
		}

		allSuccess := true
		anyFailed := false
		for taskID, item := range expected {
			child := children[taskID]
			if child.ItemValue == nil || *child.ItemValue != item {
				return s.failMapParentLocked(parent, taskDef, fmt.Errorf("map child %s has conflicting item value", taskID))
			}
			if child.ID != mapChildAttemptID(parent.RunID, child.TaskID, child.Attempt) {
				return s.failMapParentLocked(parent, taskDef, fmt.Errorf("map child %s has conflicting attempt identity %s", taskID, child.ID))
			}
			switch child.Status {
			case models.TaskSuccess:
			case models.TaskFailed, models.TaskCancelled:
				anyFailed = true
			default:
				allSuccess = false
			}
		}
		if !anyFailed && !allSuccess {
			continue
		}
		disposition, err := s.lifecycle.Complete(tasklifecycle.Completion{
			AttemptID:   parent.ID,
			Succeeded:   allSuccess && !anyFailed,
			Output:      mapCompletionOutput(anyFailed),
			Retries:     taskDef.Retries,
			CompletedAt: time.Now().UTC(),
		})
		if err != nil {
			return err
		}
		if disposition == tasklifecycle.Applied {
			s.closeMapStream(parent.ID)
		}
	}
	return nil
}

func mapCompletionOutput(failed bool) string {
	if failed {
		return "one or more mapped children failed or were cancelled"
	}
	return ""
}

func (s *Scheduler) closeMapStream(attemptID string) {
	if s.broker != nil {
		s.broker.Close(attemptID)
		s.broker.Cleanup(attemptID)
	}
}

func currentGenerationTasks(dag *models.DAGDef, tasks []models.TaskInstance) []models.TaskInstance {
	parentGenerations := make(map[string]int)
	for _, task := range tasks {
		taskDef := dag.FindTask(task.TaskID)
		if taskDef != nil && taskDef.Type == "map" && taskDef.ID == task.TaskID {
			parentGenerations[task.TaskID] = task.Attempt
		}
	}
	current := make([]models.TaskInstance, 0, len(tasks))
	for _, task := range tasks {
		if dag.FindTask(task.TaskID) != nil {
			current = append(current, task)
			continue
		}
		publicID := PublicTaskID(task.TaskID)
		parentID, _, isChild := splitMapChildID(publicID)
		taskDef := dag.FindTask(parentID)
		if !isChild || taskDef == nil || taskDef.Type != "map" {
			current = append(current, task)
			continue
		}
		generation, ok := parentGenerations[parentID]
		if ok && belongsToMapGeneration(task.TaskID, parentID, generation) {
			current = append(current, task)
		}
	}
	return current
}

func taskDefinitionForStoredID(dag *models.DAGDef, taskID string) *models.TaskDef {
	if taskDef := dag.FindTask(taskID); taskDef != nil {
		return taskDef
	}
	publicID := PublicTaskID(taskID)
	parentID, _, ok := splitMapChildID(publicID)
	if !ok {
		return nil
	}
	taskDef := dag.FindTask(parentID)
	if taskDef == nil || taskDef.Type != "map" {
		return nil
	}
	return taskDef
}
