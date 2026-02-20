package scheduler

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/robfig/cron/v3"
)

// Scheduler manages the ingestion of DAGs and the scheduling of runs
type Scheduler struct {
	store    *models.Store
	dags     map[string]*models.DAGDef
	lastExec map[string]time.Time
}

// NewScheduler creates a new scheduler instance
func NewScheduler(store *models.Store) *Scheduler {
	return &Scheduler{
		store:    store,
		dags:     make(map[string]*models.DAGDef),
		lastExec: make(map[string]time.Time),
	}
}

// GetDAGs returns the loaded DAG definitions
func (s *Scheduler) GetDAGs() map[string]*models.DAGDef {
	return s.dags
}

// LoadDAGs parses YAML files from a directory and loads them into memory
func (s *Scheduler) LoadDAGs(dirPath string) error {
	log.Printf("Loading DAGs from %s", dirPath)

	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".yaml" {
			filePath := filepath.Join(dirPath, entry.Name())
			content, err := os.ReadFile(filePath)
			if err != nil {
				log.Printf("Failed to read %s: %v", filePath, err)
				continue
			}

			dag, err := models.ParseDAG(content)
			if err != nil {
				log.Printf("Failed to parse %s as DAG: %v", filePath, err)
				continue
			}

			s.dags[dag.ID] = dag
			// Initialize lastExec to 1 minute ago so it triggers immediately on boot for testing
			if _, exists := s.lastExec[dag.ID]; !exists {
				s.lastExec[dag.ID] = time.Now().Add(-1 * time.Minute)
			}
			log.Printf("Loaded DAG: %s", dag.ID)
		}
	}

	return nil
}

// Tick evaluates schedules and triggers new runs if necessary
func (s *Scheduler) Tick() error {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	now := time.Now()

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

		dag, ok := s.dags[run.DAGID]
		if !ok {
			log.Printf("DAG %s not found in memory", run.DAGID)
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
