package worker

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"

	"github.com/alephmelo/nagare/internal/models"
)

// Pool manages a group of worker goroutines
type Pool struct {
	store      *models.Store
	getDAG     func(string) (*models.DAGDef, bool)
	triggerDAG func(string, string) (*models.DagRun, error)
	taskQueue  chan models.TaskInstance
	workerSize int
	wg         sync.WaitGroup
	running    map[string]*exec.Cmd
	rmu        sync.RWMutex
}

// NewPool initializes a new worker pool
func NewPool(store *models.Store, getDAG func(string) (*models.DAGDef, bool), triggerDAG func(string, string) (*models.DagRun, error), size int) *Pool {
	return &Pool{
		store:      store,
		getDAG:     getDAG,
		triggerDAG: triggerDAG,
		taskQueue:  make(chan models.TaskInstance, 100),
		workerSize: size,
		running:    make(map[string]*exec.Cmd),
	}
}

// Start boots up the worker goroutines
func (p *Pool) Start(ctx context.Context) {
	for i := 0; i < p.workerSize; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
}

// Dispatch polls the database for queued tasks and sends them to the workers
func (p *Pool) Dispatch() error {
	queued, err := p.store.GetQueuedTasks()
	if err != nil {
		return err
	}

	for _, ti := range queued {
		// Mark task as running so it isn't picked up multiple times
		if err := p.store.UpdateTaskInstanceStatus(ti.ID, models.TaskRunning); err != nil {
			log.Printf("Failed to mark task %s as running: %v", ti.ID, err)
			continue
		}

		// Send to channel
		select {
		case p.taskQueue <- ti:
			log.Printf("Dispatched task %s", ti.ID)
		default:
			// If queue is full, rollback to queued
			p.store.UpdateTaskInstanceStatus(ti.ID, models.TaskQueued)
			log.Printf("Queue full, rolled back task %s", ti.ID)
		}
	}
	return nil
}

// worker listens on the queue and executes shell commands
func (p *Pool) worker(ctx context.Context, id int) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d shutting down", id)
			return
		case ti := <-p.taskQueue:
			p.executeTask(ti, id)
		}
	}
}

func (p *Pool) executeTask(ti models.TaskInstance, workerID int) {
	log.Printf("Worker %d starting task %s", workerID, ti.ID)

	run, err := p.store.GetDagRun(ti.RunID)
	if err != nil {
		p.store.UpdateTaskInstanceStatus(ti.ID, models.TaskFailed)
		return
	}

	dag, ok := p.getDAG(run.DAGID)
	if !ok {
		p.store.UpdateTaskInstanceStatus(ti.ID, models.TaskFailed)
		return
	}

	var taskDef *models.TaskDef
	for i := range dag.Tasks {
		if dag.Tasks[i].ID == ti.TaskID {
			taskDef = &dag.Tasks[i]
			break
		}
	}

	if taskDef == nil {
		p.store.UpdateTaskInstanceStatus(ti.ID, models.TaskFailed)
		return
	}

	if taskDef.Type == "trigger_dag" {
		triggeredRun, err := p.triggerDAG(taskDef.DagID, "triggered")
		if err != nil {
			output := fmt.Sprintf("Failed to trigger DAG %s: %v", taskDef.DagID, err)
			log.Printf("Worker %d: Task %s FAILED: %s", workerID, ti.ID, output)
			p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskFailed, output)
		} else {
			output := fmt.Sprintf("Successfully triggered DAG run: %s", triggeredRun.ID)
			log.Printf("Worker %d: Task %s SUCCESS\nOutput: %s", workerID, ti.ID, output)
			p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskSuccess, output)
		}
		return
	}

	cmdStr := taskDef.Command
	if cmdStr == "" {
		p.store.UpdateTaskInstanceStatus(ti.ID, models.TaskFailed)
		return
	}

	// Execute the command natively
	cmd := exec.Command("sh", "-c", cmdStr)

	if len(taskDef.Env) > 0 {
		cmd.Env = os.Environ()
		for k, v := range taskDef.Env {
			cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
		}
	}

	p.rmu.Lock()
	p.running[ti.ID] = cmd
	p.rmu.Unlock()

	defer func() {
		p.rmu.Lock()
		delete(p.running, ti.ID)
		p.rmu.Unlock()
	}()

	output, err := cmd.CombinedOutput()

	if err != nil {
		log.Printf("Worker %d: Task %s FAILED: %v\nOutput: %s", workerID, ti.ID, err, string(output))

		// Check if it was cancelled/killed externally before marking as failed
		latest, getErr := p.store.GetTaskInstance(ti.ID)
		if getErr == nil && latest.Status == models.TaskCancelled {
			log.Printf("Worker %d: Task %s was already marked as CANCELLED, skipping failure update", workerID, ti.ID)
			return
		}

		if ti.Attempt <= taskDef.Retries {
			log.Printf("Worker %d: Task %s FAILED but has retries remaining (%d/%d). Output: %s", workerID, ti.ID, ti.Attempt, taskDef.Retries, string(output))
			p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskUpForRetry, string(output))
		} else {
			p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskFailed, string(output))
		}
	} else {
		log.Printf("Worker %d: Task %s SUCCESS\nOutput: %s", workerID, ti.ID, string(output))
		p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskSuccess, string(output))
	}
}

// KillTask terminates a running task process
func (p *Pool) KillTask(taskInstanceID string) error {
	p.rmu.RLock()
	cmd, ok := p.running[taskInstanceID]
	p.rmu.RUnlock()

	if !ok {
		// If not in memory, it might be and already finished or in queue.
		// We still mark it as cancelled in DB to be sure.
		return p.store.UpdateTaskInstanceStatus(taskInstanceID, models.TaskCancelled)
	}

	if cmd != nil && cmd.Process != nil {
		if err := cmd.Process.Kill(); err != nil {
			return err
		}
	}

	return p.store.UpdateTaskInstanceStatus(taskInstanceID, models.TaskCancelled)
}

// Stop gracefully shuts down the pool
func (p *Pool) Stop() {
	p.wg.Wait()
}
