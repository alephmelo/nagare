package scheduler

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/alephmelo/nagare/internal/logbroker"
	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/tasklifecycle"
	"github.com/robfig/cron/v3"
)

// Scheduler manages the ingestion of DAGs and the scheduling of runs
type Scheduler struct {
	store       *models.Store
	broker      *logbroker.Broker
	mu          sync.RWMutex
	dags        map[string]*models.DAGDef
	lastExec    map[string]time.Time
	dagErrors   map[string]string
	pausedDAGs  map[string]bool // dag IDs that are paused; protected by mu
	lifecycle   attemptLifecycle
	orchestrate sync.Mutex
}

type attemptLifecycle interface {
	Promote(string, time.Time) (tasklifecycle.Disposition, error)
	StartSetup(string, time.Time) (tasklifecycle.Disposition, error)
	Complete(tasklifecycle.Completion) (tasklifecycle.Disposition, error)
	CancelAttempt(string, time.Time) (tasklifecycle.Disposition, error)
	CancelCurrentAttempt(string, string, time.Time) (tasklifecycle.CurrentCancellation, error)
	RetryCurrent(string, string, time.Time) (tasklifecycle.Disposition, error)
	RetryCurrentPending(string, string, time.Time) (tasklifecycle.Disposition, error)
}

// SetBroker attaches a log broker so the scheduler can close map task streams
// when the parent task transitions to a terminal state.
func (s *Scheduler) SetBroker(b *logbroker.Broker) {
	s.broker = b
}

// NewScheduler creates a new scheduler instance
func NewScheduler(store *models.Store) *Scheduler {
	return NewSchedulerWithLifecycle(store, tasklifecycle.New(store))
}

// NewSchedulerWithLifecycle creates a scheduler using the authoritative
// lifecycle shared by all production attempt orchestrators.
func NewSchedulerWithLifecycle(store *models.Store, lifecycle *tasklifecycle.Lifecycle) *Scheduler {
	s := &Scheduler{
		store:      store,
		lifecycle:  lifecycle,
		dags:       make(map[string]*models.DAGDef),
		lastExec:   make(map[string]time.Time),
		dagErrors:  make(map[string]string),
		pausedDAGs: make(map[string]bool),
	}

	// Load persisted paused states so they survive restarts.
	if paused, err := store.GetPausedDAGs(); err == nil {
		s.pausedDAGs = paused
	} else {
		log.Printf("Warning: could not load paused DAG states: %v", err)
	}

	return s
}

// GetDAGs returns the loaded DAG definitions
func (s *Scheduler) GetDAGs() map[string]*models.DAGDef {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dags
}

// IsDAGPaused reports whether the given DAG is currently paused.
func (s *Scheduler) IsDAGPaused(dagID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.pausedDAGs[dagID]
}

// PauseDAG pauses a DAG so its cron schedule no longer fires.
// Manual triggers are still allowed while a DAG is paused.
func (s *Scheduler) PauseDAG(dagID string) error {
	s.mu.Lock()
	if _, ok := s.dags[dagID]; !ok {
		s.mu.Unlock()
		return fmt.Errorf("DAG %s not found", dagID)
	}
	s.pausedDAGs[dagID] = true
	s.mu.Unlock()

	return s.store.SetDAGPaused(dagID, true)
}

// ActivateDAG resumes a previously paused DAG.
func (s *Scheduler) ActivateDAG(dagID string) error {
	s.mu.Lock()
	if _, ok := s.dags[dagID]; !ok {
		s.mu.Unlock()
		return fmt.Errorf("DAG %s not found", dagID)
	}
	delete(s.pausedDAGs, dagID)
	s.mu.Unlock()

	return s.store.SetDAGPaused(dagID, false)
}

// GetDAGErrors returns the map of file paths to validation errors
func (s *Scheduler) GetDAGErrors() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dagErrors
}

// LoadDAGs parses YAML files from a directory and loads them into memory
func (s *Scheduler) LoadDAGs(dirPath string) error {
	log.Printf("Loading DAGs from %s", dirPath)

	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return err
	}

	// Reset dag errors on each load to clear resolved issues
	newErrors := make(map[string]string)
	newDags := make(map[string]*models.DAGDef)

	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".yaml" {
			filePath := filepath.Join(dirPath, entry.Name())
			content, err := os.ReadFile(filePath)
			if err != nil {
				errMsg := fmt.Sprintf("Failed to read: %v", err)
				log.Printf("%s: %s", filePath, errMsg)
				newErrors[entry.Name()] = errMsg
				continue
			}

			dag, err := models.ParseDAG(content)
			if err != nil {
				errMsg := fmt.Sprintf("Failed to parse YAML: %v", err)
				log.Printf("%s: %s", filePath, errMsg)
				newErrors[entry.Name()] = errMsg
				continue
			}

			if err := dag.Validate(); err != nil {
				errMsg := fmt.Sprintf("Validation failed: %v", err)
				log.Printf("%s: %s", filePath, errMsg)
				newErrors[entry.Name()] = errMsg
				continue
			}

			// Check for duplicate DAG IDs across different files
			if _, exists := newDags[dag.ID]; exists {
				errMsg := fmt.Sprintf("Conflict: DAG ID '%s' is already defined by another loaded file.", dag.ID)
				log.Printf("%s: %s", filePath, errMsg)
				newErrors[entry.Name()] = errMsg
				continue
			}

			newDags[dag.ID] = dag
			log.Printf("Loaded DAG: %s", dag.ID)
		}
	}

	// Safely swap the maps
	s.mu.Lock()
	defer s.mu.Unlock()

	s.dags = newDags
	s.dagErrors = newErrors

	// Initialize lastExec for new DAGs to now so they wait for their first natural cron interval.
	// Setting this to the past caused all scheduled DAGs to fire simultaneously on boot,
	// saturating the worker pool and freezing the process on startup.
	for id := range s.dags {
		if _, exists := s.lastExec[id]; !exists {
			s.lastExec[id] = time.Now()
		}
	}

	return nil
}

// Tick evaluates schedules and triggers new runs if necessary
func (s *Scheduler) Tick() error {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	now := time.Now()

	s.mu.Lock()
	for _, dag := range s.dags {
		if dag.Schedule == "" || dag.Schedule == "workflow_dispatch" {
			continue // Skip cron evaluation for manual DAGs
		}

		if s.pausedDAGs[dag.ID] {
			continue // Skip scheduling for paused DAGs
		}

		sched, err := parser.Parse(dag.Schedule)
		if err != nil {
			log.Printf("Invalid cron schedule for DAG %s: %v", dag.ID, err)
			continue
		}

		lastRunTime := s.lastExec[dag.ID]
		nextRunTime := sched.Next(lastRunTime)

		// If it's time to run
		if now.After(nextRunTime) || now.Equal(nextRunTime) {
			log.Printf("Cron Triggering DAG %s", dag.ID)

			if dag.Catchup != nil && *dag.Catchup {
				// Catchup: create a run for every missed interval
				currRunTime := nextRunTime
				for now.After(currRunTime) || now.Equal(currRunTime) {
					_, err := s.createRun(dag, "scheduled", currRunTime, nil)
					if err != nil {
						log.Printf("Cron failed to trigger %s at %v: %v", dag.ID, currRunTime, err)
					}
					s.lastExec[dag.ID] = currRunTime
					currRunTime = sched.Next(currRunTime)
				}
			} else {
				// No catchup: trigger single run, advance lastExec to now
				_, err := s.createRun(dag, "scheduled", now, nil)
				if err != nil {
					log.Printf("Cron failed to trigger %s: %v", dag.ID, err)
				}
				s.lastExec[dag.ID] = now
			}
		}
	}
	s.mu.Unlock()

	// Now promote any pending tasks whose dependencies are met
	if err := s.PromotePendingTasks(); err != nil {
		log.Printf("Error promoting pending tasks: %v", err)
	}

	return s.evaluateRunCompletions()
}

func (s *Scheduler) evaluateRunCompletions() error {
	return s.evaluateRuns()
}

// PromotePendingTasks finds pending tasks and queues them if parents are successful
func (s *Scheduler) PromotePendingTasks() error {
	return s.promoteAndRecover()
}

// TriggerDAG forcefully instantiates a new run of a DAG manually bypassing cron
func (s *Scheduler) TriggerDAG(dagID string, triggerType string, conf map[string]string) (*models.DagRun, error) {
	s.mu.RLock()
	dag, exists := s.dags[dagID]
	s.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("DAG %s not found in memory map", dagID)
	}

	return s.createRun(dag, triggerType, time.Now(), conf)
}

func (s *Scheduler) createRun(dag *models.DAGDef, triggerType string, execDate time.Time, conf map[string]string) (*models.DagRun, error) {
	now := time.Now()
	run := &models.DagRun{
		ID:          fmt.Sprintf("%s_%d", dag.ID, now.UnixNano()),
		DAGID:       dag.ID,
		Status:      models.RunRunning,
		ExecDate:    execDate,
		TriggerType: triggerType,
		Conf:        conf,
		CreatedAt:   now,
	}

	if err := s.store.CreateDagRun(run); err != nil {
		return nil, fmt.Errorf("failed to create DagRun for %s: %v", dag.ID, err)
	}

	for _, tDef := range dag.Tasks {
		status := models.TaskPending
		if len(tDef.DependsOn) == 0 {
			status = models.TaskQueued
		}

		ti := &models.TaskInstance{
			ID:        fmt.Sprintf("%s_%s", run.ID, tDef.ID),
			RunID:     run.ID,
			TaskID:    tDef.ID,
			Status:    status,
			CreatedAt: now,
			UpdatedAt: now,
		}
		if err := s.store.CreateTaskInstance(ti); err != nil {
			log.Printf("Failed to map TaskInstance %s: %v", ti.ID, err)
		}
	}

	return run, nil
}

// RetryTask creates a new attempt for a failed/succeeded task rather than
// overwriting the existing row, preserving the full attempt history.
func (s *Scheduler) RetryTask(runID, taskID string) error {
	s.orchestrate.Lock()
	defer s.orchestrate.Unlock()

	resolvedTaskID, err := s.ResolveTaskID(runID, taskID)
	if err != nil {
		return err
	}
	taskID = resolvedTaskID

	attempts, err := s.store.GetTaskAttempts(runID, taskID)
	if err != nil {
		return fmt.Errorf("task %s not found in run %s: %w", taskID, runID, err)
	}
	if len(attempts) == 0 {
		return fmt.Errorf("task %s not found in run %s", taskID, runID)
	}
	current := attempts[len(attempts)-1]
	if (current.Status == models.TaskPending || current.Status == models.TaskQueued) &&
		(len(attempts) == 1 || current.Attempt <= 1) {
		return fmt.Errorf("cannot retry task %s from its initial %s attempt", taskID, current.Status)
	}

	pendingSuccessor, err := s.retryUsesPendingSuccessor(runID, taskID)
	if err != nil {
		return fmt.Errorf("resolve retry kind for task %s: %w", taskID, err)
	}
	disposition, err := s.retryCurrentLocked(runID, taskID, pendingSuccessor, time.Now())
	if err != nil {
		return fmt.Errorf("failed creating new attempt for task %s: %w", taskID, err)
	}
	if disposition == tasklifecycle.AlreadyApplied {
		valid, reloadErr := s.hasRetrySuccessor(runID, taskID, pendingSuccessor)
		if reloadErr != nil {
			return fmt.Errorf("failed reloading retry for task %s: %w", taskID, reloadErr)
		}
		if !valid {
			status, statusErr := s.store.GetTaskStatus(runID, taskID)
			if statusErr != nil {
				return fmt.Errorf("failed reloading retry for task %s: %w", taskID, statusErr)
			}
			return fmt.Errorf("retry for task %s has authoritative status %s", taskID, status)
		}
	} else if disposition != tasklifecycle.Applied {
		return fmt.Errorf("retry for task %s returned unknown disposition %d", taskID, disposition)
	}

	// Also run on replay: the successor may have committed before a previous
	// status update failed.
	if err := s.store.UpdateDagRunStatus(runID, models.RunRunning); err != nil {
		return fmt.Errorf("failed resetting dag run %s to running: %w", runID, err)
	}

	log.Printf("Staged retry for task %s on run %s", taskID, runID)
	return nil
}

// RetryTaskAutomatically retries only the exact up-for-retry observation that
// produced the scheduling decision. It intentionally uses the observation's
// due time as the lifecycle serialization time, so a later cancellation wins
// even if it races this method from another scheduler process.
func (s *Scheduler) RetryTaskAutomatically(expected models.TaskInstance, retryDelay time.Duration, now time.Time) error {
	s.orchestrate.Lock()
	defer s.orchestrate.Unlock()

	persisted, err := s.store.GetTaskInstance(expected.ID)
	if err != nil {
		return fmt.Errorf("reload automatic retry attempt %s: %w", expected.ID, err)
	}
	if !sameAutomaticRetryObservation(persisted, expected) {
		return nil
	}

	dueAt := expected.UpdatedAt.Add(retryDelay)
	if now.Before(dueAt) {
		return nil
	}

	attempts, err := s.store.GetTaskAttempts(expected.RunID, expected.TaskID)
	if err != nil {
		return fmt.Errorf("reload automatic retry history for task %s: %w", expected.TaskID, err)
	}
	if len(attempts) == 0 {
		return fmt.Errorf("automatic retry task %s has no attempts", expected.TaskID)
	}
	latest := attempts[len(attempts)-1]
	if latest.ID != expected.ID {
		// The observation is stale. In particular, never reinterpret a later
		// cancelled attempt as a fresh explicit retry.
		return nil
	}

	pendingSuccessor, err := s.retryUsesPendingSuccessor(expected.RunID, expected.TaskID)
	if err != nil {
		return fmt.Errorf("resolve automatic retry kind for task %s: %w", expected.TaskID, err)
	}
	disposition, err := s.retryCurrentLocked(expected.RunID, expected.TaskID, pendingSuccessor, dueAt)
	if err != nil {
		return fmt.Errorf("automatically retry task %s: %w", expected.TaskID, err)
	}
	if disposition == tasklifecycle.AlreadyApplied {
		valid, reloadErr := s.hasRetrySuccessor(expected.RunID, expected.TaskID, pendingSuccessor)
		if reloadErr != nil {
			return fmt.Errorf("reload automatic retry successor for task %s: %w", expected.TaskID, reloadErr)
		}
		if !valid {
			// A concurrent cancellation is an authoritative, successful no-op
			// for this stale automatic scheduling decision.
			return nil
		}
	} else if disposition != tasklifecycle.Applied {
		return fmt.Errorf("automatic retry for task %s returned unknown disposition %d", expected.TaskID, disposition)
	}

	if err := s.store.UpdateDagRunStatus(expected.RunID, models.RunRunning); err != nil {
		return fmt.Errorf("failed resetting dag run %s to running: %w", expected.RunID, err)
	}
	return nil
}

func sameAutomaticRetryObservation(persisted *models.TaskInstance, expected models.TaskInstance) bool {
	return persisted.ID == expected.ID &&
		persisted.RunID == expected.RunID &&
		persisted.TaskID == expected.TaskID &&
		persisted.Status == models.TaskUpForRetry &&
		expected.Status == models.TaskUpForRetry &&
		persisted.Attempt == expected.Attempt &&
		persisted.UpdatedAt.Equal(expected.UpdatedAt)
}

func (s *Scheduler) retryUsesPendingSuccessor(runID, taskID string) (bool, error) {
	run, err := s.store.GetDagRun(runID)
	if err != nil {
		return false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	dag := s.dags[run.DAGID]
	if dag == nil {
		return false, nil
	}
	def := dag.FindTask(models.BaseTaskID(taskID))
	return def != nil && def.Type == "map" && taskID == models.BaseTaskID(taskID), nil
}

func (s *Scheduler) retryCurrentLocked(
	runID, taskID string,
	pendingSuccessor bool,
	at time.Time,
) (tasklifecycle.Disposition, error) {
	if pendingSuccessor {
		return s.lifecycle.RetryCurrentPending(runID, taskID, at)
	}
	return s.lifecycle.RetryCurrent(runID, taskID, at)
}

func (s *Scheduler) hasRetrySuccessor(runID, taskID string, pendingSuccessor bool) (bool, error) {
	attempts, err := s.store.GetTaskAttempts(runID, taskID)
	if err != nil {
		return false, err
	}
	if len(attempts) < 2 {
		return false, nil
	}
	want := models.TaskQueued
	if pendingSuccessor {
		want = models.TaskPending
	}
	latest := attempts[len(attempts)-1]
	return latest.Attempt > 1 && latest.Status == want, nil
}

// CancelTask resolves a public logical task to its exact current persisted
// attempt. Replays retry the same exact local stop after durable authorization.
func (s *Scheduler) CancelTask(runID, taskID string, pool interface {
	KillTask(string) error
}) error {
	s.orchestrate.Lock()
	defer s.orchestrate.Unlock()
	return s.cancelTaskLocked(runID, taskID, pool)
}

func (s *Scheduler) cancelTaskLocked(runID, taskID string, pool interface {
	KillTask(string) error
}) error {
	durableErr, followupErr := s.cancelTaskLockedDetailed(runID, taskID, pool)
	return errors.Join(durableErr, followupErr)
}

func (s *Scheduler) cancelTaskLockedDetailed(runID, taskID string, pool interface {
	KillTask(string) error
}) (durableErr, followupErr error) {
	resolvedTaskID, err := s.ResolveTaskID(runID, taskID)
	if err != nil {
		return fmt.Errorf("resolve task %s: %w", taskID, err), nil
	}
	taskID = resolvedTaskID
	result, err := s.lifecycle.CancelCurrentAttempt(runID, taskID, time.Now())
	if err != nil {
		return fmt.Errorf("durably cancel task %s: %w", taskID, err), nil
	}
	if result.Disposition != tasklifecycle.Applied && result.Disposition != tasklifecycle.AlreadyApplied {
		return fmt.Errorf("cancel task %s returned unknown disposition %d", taskID, result.Disposition), nil
	}

	var durableErrs []error
	var followupErrs []error
	attempt, readErr := s.store.GetTaskInstance(result.AttemptID)
	if readErr != nil {
		followupErrs = append(followupErrs,
			fmt.Errorf("reload cancelled attempt %s: %w", result.AttemptID, readErr))
	} else if attempt.Status != models.TaskCancelled {
		followupErrs = append(followupErrs,
			fmt.Errorf("cancelled attempt %s has authoritative status %s", result.AttemptID, attempt.Status))
	} else {
		isMapParent, mapErr := s.retryUsesPendingSuccessor(runID, taskID)
		if mapErr != nil {
			durableErrs = append(durableErrs,
				fmt.Errorf("resolve mapped children for cancelled attempt %s: %w", result.AttemptID, mapErr))
		} else if isMapParent {
			if cascadeErr := s.cancelMapChildrenLocked(runID, taskID, attempt.Attempt); cascadeErr != nil {
				durableErrs = append(durableErrs,
					fmt.Errorf("cancel mapped children for attempt %s: %w", result.AttemptID, cascadeErr))
			}
		}
	}
	if pool != nil {
		if stopErr := pool.KillTask(result.AttemptID); stopErr != nil {
			followupErrs = append(followupErrs,
				fmt.Errorf("stop cancelled attempt %s: %w", result.AttemptID, stopErr))
		}
	}
	return errors.Join(durableErrs...), errors.Join(followupErrs...)
}

// KillDagRun cancels only the latest current attempt of every logical task.
func (s *Scheduler) KillDagRun(runID string, pool interface {
	KillTask(string) error
}) error {
	s.orchestrate.Lock()
	defer s.orchestrate.Unlock()
	tasks, err := s.store.GetTaskInstancesByRun(runID)
	if err != nil {
		return err
	}

	var durableErrs []error
	var followupErrs []error
	for _, ti := range tasks {
		publicID := PublicTaskID(ti.TaskID)
		resolvedID, resolveErr := s.ResolveTaskID(runID, publicID)
		if resolveErr != nil {
			durableErrs = append(durableErrs, fmt.Errorf("resolve task %s: %w", publicID, resolveErr))
			continue
		}
		if resolvedID != ti.TaskID {
			continue
		}
		switch ti.Status {
		case models.TaskPending, models.TaskQueued, models.TaskRunning, models.TaskUpForRetry, models.TaskCancelled:
			durableErr, followupErr := s.cancelTaskLockedDetailed(runID, publicID, pool)
			if durableErr != nil {
				durableErrs = append(durableErrs, fmt.Errorf("cancel task %s: %w", publicID, durableErr))
			}
			if followupErr != nil {
				followupErrs = append(followupErrs, fmt.Errorf("finish cancelling task %s: %w", publicID, followupErr))
			}
		}
	}

	// Local process cleanup can be retried independently. The run is durably
	// cancelled once all eligible task lifecycle writes have succeeded.
	if len(durableErrs) == 0 {
		if err := s.store.UpdateDagRunStatus(runID, models.RunCancelled); err != nil {
			durableErrs = append(durableErrs, fmt.Errorf("mark dag run %s cancelled: %w", runID, err))
		}
	}
	return errors.Join(errors.Join(durableErrs...), errors.Join(followupErrs...))
}
