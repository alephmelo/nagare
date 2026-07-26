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
	"github.com/alephmelo/nagare/internal/runprogression"
	"github.com/alephmelo/nagare/internal/tasklifecycle"
)

var errMapSetupNotOwned = errors.New("map setup is not scheduler-owned")

type progressionIntent uint8

const (
	progressionObserved progressionIntent = iota
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

	var mapParents []models.TaskInstance
	seenMapParents := make(map[string]bool)
	ordinaryRunIDs := make(map[string]struct{})
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
		ordinaryRunIDs[task.RunID] = struct{}{}
	}

	runIDs := make([]string, 0, len(ordinaryRunIDs))
	for runID := range ordinaryRunIDs {
		runIDs = append(runIDs, runID)
	}
	sort.Strings(runIDs)
	for _, runID := range runIDs {
		run, dag, err := s.runAndDAG(runID)
		if err != nil || run.Status != models.RunRunning {
			continue
		}
		tasks, err := s.store.GetLatestTaskAttempts(runID)
		if err != nil {
			return err
		}
		if err := s.applyProgressionLocked(run, dag, tasks, time.Now().UTC()); err != nil {
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
	run, dag, err := s.runAndDAG(runID)
	if err != nil {
		return false
	}
	tasks, err := s.store.GetLatestTaskAttempts(runID)
	if err != nil {
		return false
	}
	for index := range tasks {
		if tasks[index].TaskID == taskDef.ID {
			tasks[index].Status = models.TaskPending
		}
	}
	plan := runprogression.Evaluate(progressionInput(run, dag, tasks))
	for _, attemptID := range plan.PromoteAttemptIDs {
		for _, task := range tasks {
			if task.TaskID == taskDef.ID && task.ID == attemptID {
				return true
			}
		}
	}
	return false
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
	if errors.Is(err, errMapSetupNotOwned) {
		return nil
	}
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
	if parent.Status == models.TaskRunning {
		// A worker won the queued-attempt claim before the scheduler durably
		// bound setup. It is not safe to infer a source or reconcile children.
		return models.MapSetup{}, errMapSetupNotOwned
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
		// A transient registry miss is not authority for a terminal write. The
		// next cadence will reconcile after definitions are loaded.
		log.Printf("DAG %s not found in memory; deferring run %s evaluation", run.DAGID, run.ID)
		return nil, nil
	}

	if err := s.aggregateMapParentsLocked(run.ID, dag); err != nil {
		return nil, fmt.Errorf("aggregate maps for run %s: %w", run.ID, err)
	}
	tasks, err := s.store.GetLatestTaskAttempts(run.ID)
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

func progressionInput(run *models.DagRun, dag *models.DAGDef, tasks []models.TaskInstance) runprogression.Input {
	input := runprogression.Input{RunStatus: run.Status}
	if dag != nil {
		materialized := make(map[string]struct{}, len(tasks))
		for _, task := range tasks {
			if dag.FindTask(task.TaskID) != nil {
				materialized[task.TaskID] = struct{}{}
			}
		}
		for _, definition := range dag.Tasks {
			if _, exists := materialized[definition.ID]; !exists {
				continue
			}
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
		projectedTasks := progressionTasks(dag, observedTasks)
		input := progressionInput(run, dag, projectedTasks)
		input.AllowCancelledReopen = intent == progressionManualRetry
		plan := runprogression.Evaluate(input)
		promotions := progressionPromotions(dag, projectedTasks, plan, intent)
		desired := progressionDesiredStatus(input, plan, intent)

		// A same-status guarded write makes a promotion-only plan prove that
		// its complete task snapshot is still current. Desired status is always
		// guarded before promotion because promotion mutates that snapshot.
		if desired == nil && len(promotions) == 0 {
			return nil
		}
		guardedStatus := run.Status
		if desired != nil {
			guardedStatus = *desired
		}
		applied, err := s.store.CompareAndSetDagRunStatusForTaskSnapshot(
			run.ID, run.Status, guardedStatus, now, observedTasks,
		)
		if err != nil {
			return fmt.Errorf("guard run %s progression: %w", run.ID, err)
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
		snapshotMisses = 0
		if desired != nil {
			run.Status = *desired
		}
		if len(promotions) == 0 {
			return nil
		}

		// Apply exactly one promotion. It changes the guarded snapshot, so the
		// next candidate must be derived from a fresh snapshot and plan.
		if _, err := s.lifecycle.Promote(promotions[0], now); err != nil {
			var invalid *tasklifecycle.InvalidTransitionError
			if !errors.As(err, &invalid) {
				return fmt.Errorf("promote attempt %s: %w", promotions[0], err)
			}
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

func progressionTasks(dag *models.DAGDef, observedTasks []models.TaskInstance) []models.TaskInstance {
	if dag == nil {
		return observedTasks
	}
	return currentGenerationTasks(dag, observedTasks)
}

func progressionPromotions(
	dag *models.DAGDef,
	tasks []models.TaskInstance,
	plan runprogression.Plan,
	intent progressionIntent,
) []string {
	if dag == nil || intent == progressionCancellation {
		return nil
	}
	attemptsByID := make(map[string]models.TaskInstance, len(tasks))
	for _, task := range tasks {
		attemptsByID[task.ID] = task
	}
	promotions := make([]string, 0, len(plan.PromoteAttemptIDs))
	for _, attemptID := range plan.PromoteAttemptIDs {
		task, exists := attemptsByID[attemptID]
		if !exists {
			continue
		}
		taskDef := dag.FindTask(task.TaskID)
		if taskDef != nil && taskDef.Type == "map" && taskDef.ID == task.TaskID {
			// A map parent remains pending until reconcileMapParentLocked has
			// persisted setup ownership. Queued means worker-claimable.
			continue
		}
		promotions = append(promotions, attemptID)
	}
	return promotions
}

func progressionDesiredStatus(
	input runprogression.Input,
	plan runprogression.Plan,
	intent progressionIntent,
) *models.RunStatus {
	if intent == progressionCancellation {
		status := models.RunCancelled
		return &status
	}
	if intent == progressionManualRetry &&
		len(input.Definitions) == 0 &&
		hasDurableRetrySuccessor(input.Attempts) {
		// Missing definitions cannot derive a run state, but the exact current
		// snapshot can still prove that explicit retry materialized a successor.
		status := models.RunRunning
		return &status
	}
	return plan.DesiredRunStatus
}

func hasDurableRetrySuccessor(attempts []runprogression.AttemptSnapshot) bool {
	for _, attempt := range attempts {
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
