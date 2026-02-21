package scheduler

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/robfig/cron/v3"
)

// Scheduler manages the ingestion of DAGs and the scheduling of runs
type Scheduler struct {
	store     *models.Store
	mu        sync.RWMutex
	dags      map[string]*models.DAGDef
	lastExec  map[string]time.Time
	dagErrors map[string]string
}

// NewScheduler creates a new scheduler instance
func NewScheduler(store *models.Store) *Scheduler {
	return &Scheduler{
		store:     store,
		dags:      make(map[string]*models.DAGDef),
		lastExec:  make(map[string]time.Time),
		dagErrors: make(map[string]string),
	}
}

// GetDAGs returns the loaded DAG definitions
func (s *Scheduler) GetDAGs() map[string]*models.DAGDef {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dags
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

	// Initialize lastExec for new DAGs to 1 minute ago so they trigger immediately on boot for testing
	for id := range s.dags {
		if _, exists := s.lastExec[id]; !exists {
			s.lastExec[id] = time.Now().Add(-1 * time.Minute)
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
					_, err := s.createRun(dag, "scheduled", currRunTime)
					if err != nil {
						log.Printf("Cron failed to trigger %s at %v: %v", dag.ID, currRunTime, err)
					}
					s.lastExec[dag.ID] = currRunTime
					currRunTime = sched.Next(currRunTime)
				}
			} else {
				// No catchup: trigger single run, advance lastExec to now
				_, err := s.createRun(dag, "scheduled", now)
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
	// Let's get all running DAGs
	rows, err := s.store.GetActiveDagRuns()
	if err != nil {
		return err
	}

	for _, r := range rows {
		tasks, err := s.store.GetTaskInstancesByRun(r.ID)
		if err != nil {
			continue
		}

		s.mu.RLock()
		dag, ok := s.dags[r.DAGID]
		s.mu.RUnlock()

		if !ok {
			log.Printf("DAG %s not found in memory. Marking run %s as failed", r.DAGID, r.ID)
			s.store.UpdateDagRunStatus(r.ID, models.RunFailed)
			continue
		}

		// Pre-evaluate map meta-tasks
		for k, ti := range tasks {
			var taskDef *models.TaskDef
			baseTaskID := ti.TaskID
			if idx := strings.Index(baseTaskID, "["); idx != -1 {
				baseTaskID = baseTaskID[:idx]
			}
			for i := range dag.Tasks {
				if dag.Tasks[i].ID == baseTaskID {
					taskDef = &dag.Tasks[i]
					break
				}
			}

			if taskDef != nil && taskDef.Type == "map" && ti.TaskID == baseTaskID {
				if ti.Status == models.TaskRunning {
					hasChildren := false
					allChildrenSuccess := true
					anyChildFailed := false

					for _, childTi := range tasks {
						// precise matcher: "task["
						if strings.HasPrefix(childTi.TaskID, baseTaskID+"[") {
							hasChildren = true
							if childTi.Status == models.TaskFailed {
								anyChildFailed = true
							} else if childTi.Status != models.TaskSuccess {
								allChildrenSuccess = false
							}
						}
					}

					if hasChildren {
						if anyChildFailed {
							s.store.UpdateTaskInstanceStatus(ti.ID, models.TaskFailed)
							tasks[k].Status = models.TaskFailed
						} else if allChildrenSuccess {
							s.store.UpdateTaskInstanceStatus(ti.ID, models.TaskSuccess)
							tasks[k].Status = models.TaskSuccess
						}
					}
				}
			}
		}

		allSuccess := true
		anyFailed := false

		for _, ti := range tasks {
			if ti.Status == models.TaskUpForRetry {
				allSuccess = false
				var taskDef *models.TaskDef
				baseTaskID := ti.TaskID
				if idx := strings.Index(baseTaskID, "["); idx != -1 {
					baseTaskID = baseTaskID[:idx]
				}
				for i := range dag.Tasks {
					if dag.Tasks[i].ID == baseTaskID {
						taskDef = &dag.Tasks[i]
						break
					}
				}

				if taskDef != nil {
					delay := time.Duration(taskDef.RetryDelaySeconds) * time.Second
					if time.Now().After(ti.UpdatedAt.Add(delay)) || time.Now().Equal(ti.UpdatedAt.Add(delay)) {
						log.Printf("Task %s is up for retry. Delay passed. Queuing retry (%d/%d).", ti.TaskID, ti.Attempt, taskDef.Retries)
						_ = s.RetryTask(r.ID, ti.TaskID)
					}
				}
			} else if ti.Status == models.TaskFailed {
				anyFailed = true
				break
			} else if ti.Status != models.TaskSuccess {
				allSuccess = false
			}
		}

		if anyFailed {
			log.Printf("Marking run %s as failed", r.ID)
			s.store.UpdateDagRunStatus(r.ID, models.RunFailed)
		} else if allSuccess && len(tasks) > 0 {
			log.Printf("Marking run %s as success", r.ID)
			s.store.UpdateDagRunStatus(r.ID, models.RunSuccess)
		}
	}

	return nil
}

// PromotePendingTasks finds pending tasks and queues them if parents are successful
func (s *Scheduler) PromotePendingTasks() error {
	pending, err := s.store.GetTasksByStatus(models.TaskPending)
	if err != nil {
		return err
	}

	for _, ti := range pending {
		run, err := s.store.GetDagRun(ti.RunID)
		if err != nil {
			log.Printf("Failed to get DAG run %s: %v", ti.RunID, err)
			continue
		}

		s.mu.RLock()
		dag, ok := s.dags[run.DAGID]
		if !ok {
			s.mu.RUnlock()
			log.Printf("DAG %s not found in memory. Marking pending task %s as failed.", run.DAGID, ti.ID)
			s.store.UpdateTaskInstanceStatus(ti.ID, models.TaskFailed)
			continue
		}

		// Find the task definition
		var taskDef *models.TaskDef
		for _, t := range dag.Tasks {
			if t.ID == ti.TaskID {
				taskDef = &t
				break
			}
		}
		s.mu.RUnlock()

		if taskDef == nil {
			log.Printf("Task %s not found in DAG %s", ti.TaskID, dag.ID)
			continue
		}

		// Check if all dependencies are success
		allSuccess := true
		for _, depTaskID := range taskDef.DependsOn {
			depStatus, err := s.store.GetTaskStatus(ti.RunID, depTaskID)
			if err != nil || depStatus != models.TaskSuccess {
				allSuccess = false
				break
			}
		}

		if allSuccess {
			if taskDef.Type == "map" && taskDef.MapOver != "" {
				mapTaskInst, err := s.store.GetTaskAttempts(ti.RunID, taskDef.MapOver)
				if err != nil || len(mapTaskInst) == 0 {
					log.Printf("MapOver task %s not found for run %s", taskDef.MapOver, ti.RunID)
					s.store.UpdateTaskInstanceStatus(ti.ID, models.TaskFailed)
					continue
				}

				lastAttempt := mapTaskInst[len(mapTaskInst)-1]
				var items []string
				if err := json.Unmarshal([]byte(lastAttempt.Output), &items); err != nil {
					log.Printf("Failed to unmarshal output for map task %s: %v", ti.ID, err)
					s.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskFailed, fmt.Sprintf("failed to parse map_over json array: %v\nOutput was: %s", err, lastAttempt.Output))
					continue
				}

				s.store.UpdateTaskInstanceStatus(ti.ID, models.TaskRunning)

				now := time.Now()
				for i, item := range items {
					mappedTaskID := fmt.Sprintf("%s[%d]", ti.TaskID, i)
					mappedInstID := fmt.Sprintf("%s_%s", ti.RunID, mappedTaskID)
					itemVal := item
					childTi := &models.TaskInstance{
						ID:        mappedInstID,
						RunID:     ti.RunID,
						TaskID:    mappedTaskID,
						Status:    models.TaskQueued,
						ItemValue: &itemVal,
						Attempt:   1,
						CreatedAt: now,
						UpdatedAt: now,
					}
					if err := s.store.CreateTaskInstance(childTi); err != nil {
						log.Printf("Failed to create mapped instance %s: %v", mappedInstID, err)
					}
				}

				if len(items) == 0 {
					s.store.UpdateTaskInstanceStatus(ti.ID, models.TaskSuccess)
				}
			} else {
				log.Printf("Promoting task %s to queued", ti.ID)
				s.store.UpdateTaskInstanceStatus(ti.ID, models.TaskQueued)
			}
		}
	}
	return nil
}

// TriggerDAG forcefully instantiates a new run of a DAG manually bypassing cron
func (s *Scheduler) TriggerDAG(dagID string, triggerType string) (*models.DagRun, error) {
	s.mu.RLock()
	dag, exists := s.dags[dagID]
	s.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("DAG %s not found in memory map", dagID)
	}

	return s.createRun(dag, triggerType, time.Now())
}

func (s *Scheduler) createRun(dag *models.DAGDef, triggerType string, execDate time.Time) (*models.DagRun, error) {
	now := time.Now()
	run := &models.DagRun{
		ID:          fmt.Sprintf("%s_%d", dag.ID, now.UnixNano()),
		DAGID:       dag.ID,
		Status:      models.RunRunning,
		ExecDate:    execDate,
		TriggerType: triggerType,
		CreatedAt:   now,
	}

	if err := s.store.CreateDagRun(run); err != nil {
		return nil, fmt.Errorf("Failed to create DagRun for %s: %v", dag.ID, err)
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
	taskStatus, err := s.store.GetTaskStatus(runID, taskID)
	if err != nil {
		return fmt.Errorf("task %s not found in run %s: %w", taskID, runID, err)
	}

	if taskStatus == models.TaskRunning || taskStatus == models.TaskQueued {
		return fmt.Errorf("cannot retry task %s that is currently %s", taskID, taskStatus)
	}

	// Insert a brand-new attempt row (increments attempt counter)
	newID, err := s.store.CreateNewTaskAttempt(runID, taskID)
	if err != nil {
		return fmt.Errorf("failed creating new attempt for task %s: %w", taskID, err)
	}

	// Flip the parent DagRun back to running so the worker picks it up
	err = s.store.UpdateDagRunStatus(runID, models.RunRunning)
	if err != nil {
		return fmt.Errorf("failed resetting dag run %s to running: %w", runID, err)
	}

	log.Printf("Staged retry attempt %s for task %s on run %s", newID, taskID, runID)
	return nil
}

// KillDagRun terminates all active tasks for a run and marks it as failed
func (s *Scheduler) KillDagRun(runID string, pool interface {
	KillTask(string) error
}) error {
	tasks, err := s.store.GetTaskInstancesByRun(runID)
	if err != nil {
		return err
	}

	for _, ti := range tasks {
		if ti.Status == models.TaskRunning || ti.Status == models.TaskQueued {
			if err := pool.KillTask(ti.ID); err != nil {
				log.Printf("Failed to kill task %s: %v", ti.ID, err)
			}
		} else if ti.Status == models.TaskPending {
			s.store.UpdateTaskInstanceStatus(ti.ID, models.TaskCancelled)
		}
	}

	return s.store.UpdateDagRunStatus(runID, models.RunCancelled)
}
