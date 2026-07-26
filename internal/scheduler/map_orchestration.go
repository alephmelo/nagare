package scheduler

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/runprogression"
	"github.com/alephmelo/nagare/internal/tasklifecycle"
)

var (
	errMapSetupNotOwned      = errors.New("map setup is not scheduler-owned")
	errGuardedActionRejected = errors.New("guarded progression action rejected")
)

type progressionIntent uint8

const (
	progressionObserved progressionIntent = iota
	progressionInitialization
	progressionRecovery
	progressionAutomaticRetry
	progressionManualRetry
	progressionCancellation
)

func (s *Scheduler) promoteAndRecover() error {
	s.orchestrate.Lock()
	defer s.orchestrate.Unlock()

	pending, err := s.store.GetTasksByStatus(models.TaskPending)
	if err != nil {
		return err
	}

	runSet := make(map[string]struct{})
	for _, task := range pending {
		runSet[task.RunID] = struct{}{}
	}

	runIDs := make([]string, 0, len(runSet))
	for runID := range runSet {
		runIDs = append(runIDs, runID)
	}
	sort.Strings(runIDs)
	for _, runID := range runIDs {
		run, err := s.store.GetDagRun(runID)
		if err != nil {
			return err
		}
		if run.Status != models.RunRunning {
			continue
		}
		tasks, err := s.store.GetLatestTaskAttempts(runID)
		if err != nil {
			return err
		}
		s.mu.RLock()
		dag := s.dags[run.DAGID]
		s.mu.RUnlock()
		if dag == nil {
			continue
		}
		if err := dag.Validate(); err != nil {
			if terminalErr := s.terminalizeUnresolvableRunLocked(run.ID, time.Now().UTC()); terminalErr != nil {
				return errors.Join(err, terminalErr)
			}
			continue
		}
		invalidDefinition, err := s.quarantineUndefinedPendingTasksLocked(dag, tasks)
		if err != nil {
			return err
		}
		if invalidDefinition {
			if err := s.terminalizeUnresolvableRunLocked(run.ID, time.Now().UTC()); err != nil {
				return err
			}
			continue
		}
		if err := s.applyProgressionIntentLocked(
			run, dag, tasks, time.Now().UTC(), progressionRecovery,
		); err != nil {
			return err
		}
	}

	// Narrow recovery for a crash after durable setup exists. Pending parents
	// are handled only by the evaluator above; queued legacy and running
	// parents may resume solely from their exact persisted binding.
	for _, status := range []models.TaskStatus{models.TaskQueued, models.TaskRunning} {
		tasks, err := s.store.GetTasksByStatus(status)
		if err != nil {
			return err
		}
		for _, task := range tasks {
			run, dag, err := s.runAndDAG(task.RunID)
			if err != nil || run.Status != models.RunRunning {
				continue
			}
			taskDef := dag.FindTask(task.TaskID)
			if taskDef == nil || taskDef.Type != "map" || taskDef.ID != task.TaskID {
				continue
			}
			resumable, err := s.mapSetupCanResume(task)
			if err != nil {
				return err
			}
			if !resumable {
				continue
			}
			if err := s.reconcileMapParentLocked(task, taskDef); err != nil {
				return err
			}
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

func (s *Scheduler) mapSetupCanResume(task models.TaskInstance) (bool, error) {
	setup, err := s.store.GetMapSetup(task.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if task.Status == models.TaskRunning {
		return task.StartedAt != nil && task.StartedAt.Equal(setup.StartedAt), nil
	}
	return task.Status == models.TaskQueued, nil
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

func (s *Scheduler) failInvalidDefinitionTaskLocked(task models.TaskInstance) error {
	now := time.Now().UTC()
	var disposition tasklifecycle.Disposition
	for stale := 0; stale < 4; stale++ {
		var err error
		disposition, err = s.lifecycle.StartSetup(task.ID, now)
		if err != nil {
			current, reloadErr := s.store.GetTaskInstance(task.ID)
			run, runErr := s.store.GetDagRun(task.RunID)
			if reloadErr == nil && runErr == nil &&
				(current.Status == models.TaskCancelled || run.Status != models.RunRunning) {
				return nil
			}
			return errors.Join(
				fmt.Errorf("quarantine undefined task %s: %w", task.ID, err),
				reloadErr,
				runErr,
			)
		}
		switch disposition {
		case tasklifecycle.Applied, tasklifecycle.AlreadyApplied:
			stale = 4
		case tasklifecycle.Stale:
			current, reloadErr := s.store.GetTaskInstance(task.ID)
			run, runErr := s.store.GetDagRun(task.RunID)
			if reloadErr == nil && runErr == nil &&
				(current.Status == models.TaskCancelled || run.Status != models.RunRunning) {
				return nil
			}
			if reloadErr != nil || runErr != nil {
				return errors.Join(reloadErr, runErr)
			}
			continue
		case tasklifecycle.Invalid:
			return fmt.Errorf(
				"quarantine undefined task %s returned invalid disposition",
				task.ID,
			)
		default:
			return fmt.Errorf(
				"quarantine undefined task %s returned unknown disposition %d",
				task.ID, disposition,
			)
		}
	}
	if disposition != tasklifecycle.Applied && disposition != tasklifecycle.AlreadyApplied {
		return fmt.Errorf("quarantine undefined task %s did not converge", task.ID)
	}
	disposition, err := s.lifecycle.Complete(tasklifecycle.Completion{
		AttemptID:   task.ID,
		Output:      "persisted task has no exact loaded DAG definition",
		CompletedAt: now,
	})
	if err != nil {
		current, reloadErr := s.store.GetTaskInstance(task.ID)
		run, runErr := s.store.GetDagRun(task.RunID)
		if reloadErr == nil && runErr == nil &&
			(current.Status == models.TaskCancelled || run.Status != models.RunRunning) {
			return nil
		}
		return fmt.Errorf("fail undefined task %s: %w", task.ID, err)
	}
	if disposition != tasklifecycle.Applied && disposition != tasklifecycle.AlreadyApplied {
		return fmt.Errorf(
			"fail undefined task %s returned disposition %d",
			task.ID, disposition,
		)
	}
	return nil
}

func (s *Scheduler) quarantineUndefinedPendingTasksLocked(
	dag *models.DAGDef,
	tasks []models.TaskInstance,
) (bool, error) {
	quarantined := false
	for _, task := range tasks {
		if task.Status != models.TaskPending || dag.FindTask(task.TaskID) != nil ||
			s.isStoredMapChild(dag, task.TaskID) {
			continue
		}
		if err := s.failInvalidDefinitionTaskLocked(task); err != nil {
			return false, err
		}
		quarantined = true
	}
	return quarantined, nil
}

func (s *Scheduler) reconcileMapParentLocked(parent models.TaskInstance, taskDef *models.TaskDef) error {
	setup, err := s.store.GetMapSetup(parent.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	owned := parent.Status == models.TaskRunning
	for stale := 0; stale < 4; stale++ {
		run, err := s.store.GetDagRun(parent.RunID)
		if err != nil {
			return err
		}
		tasks, err := s.store.GetLatestTaskAttempts(parent.RunID)
		if err != nil {
			return err
		}
		current, ok := exactAttempt(tasks, parent.ID)
		if !ok || current.TaskID != parent.TaskID || current.Attempt != parent.Attempt {
			return fmt.Errorf("map parent %s is no longer the current exact attempt", parent.ID)
		}
		parent = current
		if parent.Status == models.TaskRunning {
			owned = true
			break
		}
		disposition, err := s.lifecycle.StartSetupGuarded(parent, run.Status, tasks, *setup)
		if err != nil {
			return fmt.Errorf("resume map setup %s: %w", parent.ID, err)
		}
		switch disposition {
		case tasklifecycle.Applied, tasklifecycle.AlreadyApplied:
			owned = true
			stale = 4
		case tasklifecycle.Stale:
			continue
		case tasklifecycle.Invalid:
			return fmt.Errorf(
				"%w: resume map setup %s was invalid",
				errGuardedActionRejected, parent.ID,
			)
		default:
			return fmt.Errorf("resume map setup %s returned unknown disposition %d", parent.ID, disposition)
		}
	}
	if !owned {
		return fmt.Errorf("resume map setup %s did not converge after stale snapshots", parent.ID)
	}
	return s.reconcileOwnedMapParentLocked(parent, taskDef, *setup)
}

func (s *Scheduler) reconcileOwnedMapParentLocked(
	parent models.TaskInstance,
	taskDef *models.TaskDef,
	setup models.MapSetup,
) error {
	var err error
	parent, err = s.reloadOwnedMapSetup(parent.ID, setup)
	if errors.Is(err, errMapSetupNotOwned) {
		return nil
	}
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
		if errors.Is(err, errGuardedActionRejected) {
			return err
		}
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

func (s *Scheduler) mapSetupForPromotionLocked(
	parent models.TaskInstance,
	taskDef *models.TaskDef,
	observedTasks []models.TaskInstance,
	now time.Time,
) (models.MapSetup, error) {
	var upstream models.TaskInstance
	found := false
	for _, attempt := range observedTasks {
		if attempt.TaskID != taskDef.MapOver {
			continue
		}
		if !found || attempt.Attempt > upstream.Attempt ||
			(attempt.Attempt == upstream.Attempt && attempt.ID < upstream.ID) {
			upstream = attempt
			found = true
		}
	}
	if !found {
		return models.MapSetup{}, fmt.Errorf("map source %s has no current attempt", taskDef.MapOver)
	}
	if upstream.Status != models.TaskSuccess {
		return models.MapSetup{}, fmt.Errorf("map source %s is %s, not success", upstream.ID, upstream.Status)
	}

	setup := models.MapSetup{
		ParentAttemptID:   parent.ID,
		UpstreamAttemptID: upstream.ID,
		UpstreamOutput:    upstream.Output,
		StartedAt:         now.UTC(),
	}
	persisted, err := s.store.GetMapSetup(parent.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return setup, nil
	}
	if err != nil {
		return models.MapSetup{}, fmt.Errorf("load map setup %s: %w", parent.ID, err)
	}
	if persisted.ParentAttemptID != setup.ParentAttemptID ||
		persisted.UpstreamAttemptID != setup.UpstreamAttemptID ||
		persisted.UpstreamOutput != setup.UpstreamOutput {
		return models.MapSetup{}, fmt.Errorf(
			"map setup %s conflicts with the evaluator-observed upstream attempt",
			parent.ID,
		)
	}
	return *persisted, nil
}

func exactAttempt(tasks []models.TaskInstance, attemptID string) (models.TaskInstance, bool) {
	for _, task := range tasks {
		if task.ID == attemptID {
			return task, true
		}
	}
	return models.TaskInstance{}, false
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
		return *parent, fmt.Errorf("%w: map parent %s has a different ownership watermark", errMapSetupNotOwned, parent.ID)
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
	}

	now := time.Now().UTC()
	for index, item := range items {
		taskID := generationTaskID(fmt.Sprintf("%s[%d]", parent.TaskID, index), parent.Attempt)
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
		if _, err := s.store.EnsureTaskInstance(child); err != nil {
			return fmt.Errorf("ensure mapped instance %s: %w", child.ID, err)
		}
	}

	taskIDs := make([]string, 0, len(expected))
	for taskID := range expected {
		taskIDs = append(taskIDs, taskID)
	}
	sort.Strings(taskIDs)
	for _, taskID := range taskIDs {
		if err := s.promoteMapChildLocked(
			parent,
			taskID,
			expected[taskID],
			time.Now().UTC(),
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) promoteMapChildLocked(
	parent models.TaskInstance,
	taskID, item string,
	now time.Time,
) error {
	const maxSnapshotMisses = 8
	initialID := mapChildAttemptID(parent.RunID, taskID, 1)
	for stale := 0; stale < maxSnapshotMisses; stale++ {
		run, err := s.store.GetDagRun(parent.RunID)
		if err != nil {
			return fmt.Errorf("reload run for mapped instance %s: %w", initialID, err)
		}
		tasks, err := s.store.GetLatestTaskAttempts(parent.RunID)
		if err != nil {
			return fmt.Errorf("reload snapshot for mapped instance %s: %w", initialID, err)
		}
		var child models.TaskInstance
		ok := false
		for _, task := range tasks {
			if task.TaskID == taskID {
				child, ok = task, true
				break
			}
		}
		if !ok {
			return fmt.Errorf("mapped instance %s disappeared after ensure", initialID)
		}
		if child.RunID != parent.RunID ||
			child.ID != mapChildAttemptID(parent.RunID, taskID, child.Attempt) ||
			child.ItemValue == nil || *child.ItemValue != item {
			return fmt.Errorf("mapped instance %s has conflicting identity or item binding", child.ID)
		}
		switch child.Status {
		case models.TaskRunning, models.TaskSuccess, models.TaskFailed,
			models.TaskCancelled, models.TaskUpForRetry:
			return nil
		case models.TaskPending, models.TaskQueued:
		default:
			return fmt.Errorf("mapped instance %s has unknown status %s", child.ID, child.Status)
		}

		disposition, err := s.lifecycle.PromoteGuarded(child, run.Status, tasks, now)
		if err != nil {
			return fmt.Errorf("queue mapped instance %s: %w", child.ID, err)
		}
		switch disposition {
		case tasklifecycle.Applied, tasklifecycle.AlreadyApplied:
			return nil
		case tasklifecycle.Stale:
			continue
		case tasklifecycle.Invalid:
			return fmt.Errorf(
				"%w: queue mapped instance %s was invalid",
				errGuardedActionRejected, child.ID,
			)
		default:
			return fmt.Errorf("queue mapped instance %s returned unknown disposition %d", child.ID, disposition)
		}
	}
	return fmt.Errorf(
		"%w: queue mapped instance %s did not converge after %d stale snapshots",
		errGuardedActionRejected, initialID, maxSnapshotMisses,
	)
}

func mapChildAttemptID(runID, taskID string, attempt int) string {
	if attempt <= 1 {
		return runID + "_" + taskID
	}
	return fmt.Sprintf("%s_%s_%d", runID, taskID, attempt)
}

func (s *Scheduler) failMapParentLocked(parent models.TaskInstance, taskDef *models.TaskDef, setupErr error) error {
	cancelErr, stopErr := s.cancelMapChildrenLocked(parent.RunID, parent.TaskID, parent.Attempt, nil)
	_, completeErr := s.lifecycle.Complete(tasklifecycle.Completion{
		AttemptID:   parent.ID,
		Output:      setupErr.Error(),
		Retries:     taskDef.Retries,
		CompletedAt: time.Now().UTC(),
	})
	return errors.Join(setupErr, cancelErr, stopErr, completeErr)
}

// cancelMapChildrenLocked durably cancels every latest child in one exact map
// generation, then requests an exact local stop. Callers must hold orchestrate.
func (s *Scheduler) cancelMapChildrenLocked(runID, parentID string, generation int, pool interface {
	KillTask(string) error
}) (durableErr, followupErr error) {
	tasks, err := s.store.GetLatestTaskAttempts(runID)
	if err != nil {
		return err, nil
	}
	var durableErrs []error
	var followupErrs []error
	for _, task := range tasks {
		if !belongsToMapGeneration(task.TaskID, parentID, generation) {
			continue
		}
		switch task.Status {
		case models.TaskPending, models.TaskQueued, models.TaskRunning, models.TaskUpForRetry, models.TaskCancelled:
			disposition, err := s.lifecycle.CancelAttempt(task.ID, time.Now().UTC())
			if err != nil {
				durableErrs = append(durableErrs, fmt.Errorf("cancel mapped instance %s: %w", task.ID, err))
				continue
			}
			if disposition != tasklifecycle.Applied && disposition != tasklifecycle.AlreadyApplied {
				durableErrs = append(durableErrs,
					fmt.Errorf("cancel mapped instance %s returned unknown disposition %d", task.ID, disposition))
				continue
			}
			if pool != nil {
				if err := pool.KillTask(task.ID); err != nil {
					followupErrs = append(followupErrs, fmt.Errorf("stop mapped instance %s: %w", task.ID, err))
				}
			}
		}
	}
	return errors.Join(durableErrs...), errors.Join(followupErrs...)
}

type automaticRetryCandidate struct {
	expected models.TaskInstance
	delay    time.Duration
	dueAt    time.Time
}

func (s *Scheduler) evaluateRuns() error {
	runs, err := s.store.GetActiveDagRuns()
	if err != nil {
		return err
	}
	var evaluationErrs []error
	for _, observedRun := range runs {
		s.orchestrate.Lock()
		retries, evaluationErr := s.evaluateRunLocked(observedRun, time.Now().UTC())
		s.orchestrate.Unlock()
		if evaluationErr != nil {
			evaluationErrs = append(evaluationErrs, evaluationErr)
		}

		for _, retry := range retries {
			if err := s.RetryTaskAutomatically(retry.expected, retry.delay, retry.dueAt); err != nil {
				evaluationErrs = append(evaluationErrs,
					fmt.Errorf("automatically retry attempt %s: %w", retry.expected.ID, err))
			}
		}
	}
	return errors.Join(evaluationErrs...)
}

// evaluateRunLocked revalidates an active-run observation and evaluates its
// latest task attempts as one serialized control-plane decision. Callers must
// hold orchestrate.
func (s *Scheduler) evaluateRunLocked(observedRun models.DagRun, now time.Time) ([]automaticRetryCandidate, error) {
	run, err := s.store.GetDagRun(observedRun.ID)
	if err != nil {
		return nil, fmt.Errorf("reload run %s for evaluation: %w", observedRun.ID, err)
	}
	if run.Status != models.RunRunning {
		return nil, nil
	}

	s.mu.RLock()
	dag := s.dags[run.DAGID]
	s.mu.RUnlock()
	if dag == nil {
		err := s.terminalizeUnresolvableRunLocked(run.ID, now)
		return nil, errors.Join(
			fmt.Errorf("DAG %s not found in memory for run %s", run.DAGID, run.ID),
			err,
		)
	}
	if validationErr := dag.Validate(); validationErr != nil {
		err := s.terminalizeUnresolvableRunLocked(run.ID, now)
		return nil, errors.Join(
			fmt.Errorf("DAG %s is invalid for run %s: %w", run.DAGID, run.ID, validationErr),
			err,
		)
	}

	tasks, err := s.store.GetLatestTaskAttempts(run.ID)
	if err != nil {
		return nil, fmt.Errorf("load latest tasks for run %s: %w", run.ID, err)
	}
	quarantined, err := s.quarantineUndefinedPendingTasksLocked(dag, tasks)
	if err != nil {
		return nil, fmt.Errorf("quarantine undefined tasks for run %s: %w", run.ID, err)
	}
	if quarantined {
		return nil, s.terminalizeUnresolvableRunLocked(run.ID, now)
	}
	if err := s.aggregateMapParentsLocked(run.ID, dag); err != nil {
		return nil, fmt.Errorf("aggregate maps for run %s: %w", run.ID, err)
	}
	tasks, err = s.store.GetLatestTaskAttempts(run.ID)
	if err != nil {
		return nil, fmt.Errorf("load latest tasks for run %s: %w", run.ID, err)
	}
	projectedTasks := currentGenerationTasks(dag, tasks)

	var retries []automaticRetryCandidate
	for _, task := range projectedTasks {
		taskDef := taskDefinitionForStoredID(dag, task.TaskID)
		if task.Status == models.TaskUpForRetry {
			if taskDef != nil {
				delay := time.Duration(taskDef.RetryDelaySeconds) * time.Second
				dueAt := task.UpdatedAt.Add(delay)
				if !now.Before(dueAt) {
					retries = append(retries, automaticRetryCandidate{
						expected: task,
						delay:    delay,
						dueAt:    dueAt,
					})
				}
			}
		}
	}
	if err := s.applyProgressionLocked(run, dag, tasks, now); err != nil {
		return retries, err
	}
	return retries, nil
}

func (s *Scheduler) terminalizeUnresolvableRunLocked(runID string, now time.Time) error {
	const maxSnapshotMisses = 8
	for stale := 0; stale < maxSnapshotMisses; stale++ {
		run, err := s.store.GetDagRun(runID)
		if err != nil {
			return fmt.Errorf("reload unresolvable run %s: %w", runID, err)
		}
		tasks, err := s.store.GetLatestTaskAttempts(runID)
		if err != nil {
			return fmt.Errorf("load unresolvable run %s task snapshot: %w", runID, err)
		}
		desired := models.RunFailed
		for _, task := range tasks {
			if task.Status == models.TaskCancelled {
				desired = models.RunCancelled
				break
			}
		}
		if run.Status == desired {
			return nil
		}
		if run.Status != models.RunRunning && desired != models.RunCancelled {
			// Never replace an independently chosen terminal result with the
			// missing-definition fallback.
			return nil
		}
		applied, err := s.store.CompareAndSetDagRunStatusForTaskSnapshot(
			run.ID, run.Status, desired, now, tasks,
		)
		if err != nil {
			return fmt.Errorf("terminalize unresolvable run %s: %w", runID, err)
		}
		if applied {
			return nil
		}
	}
	return fmt.Errorf(
		"terminalize unresolvable run %s did not converge after %d stale snapshots",
		runID, maxSnapshotMisses,
	)
}

func progressionInput(run *models.DagRun, dag *models.DAGDef, tasks []models.TaskInstance) runprogression.Input {
	input := runprogression.Input{RunStatus: run.Status}
	if dag != nil {
		for _, definition := range dag.Tasks {
			input.Definitions = append(input.Definitions, runprogression.TaskDefinition{
				ID: definition.ID, DependsOn: definition.DependsOn,
			})
		}
	}
	for _, task := range tasks {
		input.Attempts = append(input.Attempts, runprogression.AttemptSnapshot{
			ID: task.ID, TaskID: task.TaskID, Attempt: task.Attempt, Status: task.Status,
		})
	}
	return input
}

func (s *Scheduler) applyProgressionLocked(run *models.DagRun, dag *models.DAGDef, tasks []models.TaskInstance, now time.Time) error {
	return s.applyProgressionIntentLocked(run, dag, tasks, now, progressionObserved)
}

func (s *Scheduler) applyProgressionIntentLocked(
	run *models.DagRun,
	dag *models.DAGDef,
	observedTasks []models.TaskInstance,
	now time.Time,
	intent progressionIntent,
) error {
	const (
		maxSnapshotMisses      = 4
		maxReconciliationSteps = 4096
	)
	snapshotMisses := 0
	for step := 0; step < maxReconciliationSteps; step++ {
		input := progressionInput(run, dag, observedTasks)
		input.AllowCancelledReopen = intent == progressionManualRetry
		input.AllowChainRootPromotion =
			intent == progressionInitialization || intent == progressionRecovery
		plan := runprogression.Evaluate(input)
		desired := plan.DesiredRunStatus
		if intent == progressionCancellation {
			cancelled := models.RunCancelled
			desired = &cancelled
		}
		if desired != nil {
			if *desired == run.Status {
				return nil
			}
			applied, err := s.store.CompareAndSetDagRunStatusForTaskSnapshot(
				run.ID, run.Status, *desired, now, observedTasks,
			)
			if err != nil {
				return fmt.Errorf("transition run %s progression: %w", run.ID, err)
			}
			if !applied {
				snapshotMisses++
				if snapshotMisses >= maxSnapshotMisses {
					return fmt.Errorf(
						"run %s progression did not converge after %d stale snapshots",
						run.ID, snapshotMisses,
					)
				}
				var reloadErr error
				run, dag, observedTasks, reloadErr = s.reloadProgressionSnapshotLocked(run.ID)
				if reloadErr != nil {
					return reloadErr
				}
				continue
			}
			run.Status = *desired
			snapshotMisses = 0
			if *desired != models.RunRunning {
				return nil
			}
			var reloadErr error
			run, dag, observedTasks, reloadErr = s.reloadProgressionSnapshotLocked(run.ID)
			if reloadErr != nil {
				return reloadErr
			}
			continue
		}

		if len(plan.PromoteAttemptIDs) == 0 || intent == progressionCancellation {
			return nil
		}
		attemptID := plan.PromoteAttemptIDs[0]
		target, ok := exactAttempt(observedTasks, attemptID)
		if !ok {
			return fmt.Errorf("progression selected missing attempt %s", attemptID)
		}
		taskDef := dag.FindTask(target.TaskID)
		if taskDef == nil {
			return fmt.Errorf("progression selected attempt %s without an exact definition", attemptID)
		}

		var disposition tasklifecycle.Disposition
		var err error
		if taskDef.Type == "map" {
			setup, setupErr := s.mapSetupForPromotionLocked(target, taskDef, observedTasks, now)
			if setupErr != nil {
				return fmt.Errorf("derive map setup for %s: %w", target.ID, setupErr)
			}
			disposition, err = s.lifecycle.StartSetupGuarded(
				target, run.Status, observedTasks, setup,
			)
			if err == nil &&
				(disposition == tasklifecycle.Applied || disposition == tasklifecycle.AlreadyApplied) {
				if err := s.reconcileOwnedMapParentLocked(target, taskDef, setup); err != nil {
					return err
				}
			}
		} else {
			disposition, err = s.lifecycle.PromoteGuarded(
				target, run.Status, observedTasks, now,
			)
		}
		if err != nil {
			return fmt.Errorf("apply progression action for attempt %s: %w", attemptID, err)
		}
		switch disposition {
		case tasklifecycle.Applied, tasklifecycle.AlreadyApplied:
			snapshotMisses = 0
		case tasklifecycle.Stale:
			snapshotMisses++
			if snapshotMisses >= maxSnapshotMisses {
				return fmt.Errorf(
					"run %s progression did not converge after %d stale snapshots",
					run.ID, snapshotMisses,
				)
			}
		case tasklifecycle.Invalid:
			return fmt.Errorf(
				"%w: attempt %s",
				errGuardedActionRejected, attemptID,
			)
		default:
			return fmt.Errorf(
				"progression action for attempt %s returned unknown disposition %d",
				attemptID, disposition,
			)
		}
		var reloadErr error
		run, dag, observedTasks, reloadErr = s.reloadProgressionSnapshotLocked(run.ID)
		if reloadErr != nil {
			return reloadErr
		}
	}
	return fmt.Errorf(
		"run %s progression exceeded %d reconciliation steps",
		run.ID, maxReconciliationSteps,
	)
}

func (s *Scheduler) reloadProgressionSnapshotLocked(
	runID string,
) (*models.DagRun, *models.DAGDef, []models.TaskInstance, error) {
	run, err := s.store.GetDagRun(runID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("reload run %s progression: %w", runID, err)
	}
	tasks, err := s.store.GetLatestTaskAttempts(runID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("reload run %s task snapshot: %w", runID, err)
	}
	s.mu.RLock()
	dag := s.dags[run.DAGID]
	s.mu.RUnlock()
	return run, dag, tasks, nil
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
			continue
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
		if anyFailed {
			durableErr, followupErr := s.cancelMapChildrenLocked(parent.RunID, parent.TaskID, parent.Attempt, nil)
			if err := errors.Join(durableErr, followupErr); err != nil {
				return fmt.Errorf("cancel active siblings for failed map parent %s: %w", parent.ID, err)
			}
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
