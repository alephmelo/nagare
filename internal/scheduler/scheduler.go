package scheduler

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
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
		sched, err := parser.Parse(dag.Schedule)
		if err != nil {
			log.Printf("Invalid cron schedule for DAG %s: %v", dag.ID, err)
			continue
		}

		lastRunTime := s.lastExec[dag.ID]
		nextRunTime := sched.Next(lastRunTime)

		// If it's time to run
		if now.After(nextRunTime) || now.Equal(nextRunTime) {
			log.Printf("Triggering DAG %s", dag.ID)

			run := &models.DagRun{
				ID:        fmt.Sprintf("%s_%d", dag.ID, now.UnixNano()),
				DAGID:     dag.ID,
				Status:    models.RunRunning,
				ExecDate:  now,
				CreatedAt: now,
			}

			if err := s.store.CreateDagRun(run); err != nil {
				log.Printf("Failed to create DagRun for %s: %v", dag.ID, err)
				continue
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
					log.Printf("Failed to create TaskInstance %s: %v", ti.ID, err)
				}
			}

			s.lastExec[dag.ID] = now
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

		allSuccess := true
		anyFailed := false

		for _, ti := range tasks {
			if ti.Status == models.TaskFailed {
				anyFailed = true
				break
			}
			if ti.Status != models.TaskSuccess {
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
			log.Printf("Promoting task %s to queued", ti.ID)
			s.store.UpdateTaskInstanceStatus(ti.ID, models.TaskQueued)
		}
	}
	return nil
}
