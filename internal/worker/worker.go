package worker

import (
	"context"
	"log"
	"os/exec"
	"sync"

	"github.com/alephmelo/nagare/internal/models"
)

// Pool manages a group of worker goroutines
type Pool struct {
	store      *models.Store
	getDAG     func(string) (*models.DAGDef, bool)
	taskQueue  chan models.TaskInstance
	workerSize int
	wg         sync.WaitGroup
}

// NewPool initializes a new worker pool
func NewPool(store *models.Store, getDAG func(string) (*models.DAGDef, bool), size int) *Pool {
	return &Pool{
		store:      store,
		getDAG:     getDAG,
		taskQueue:  make(chan models.TaskInstance, 100),
		workerSize: size,
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

	var cmdStr string
	for _, t := range dag.Tasks {
		if t.ID == ti.TaskID {
			cmdStr = t.Command
			break
		}
	}

	if cmdStr == "" {
		p.store.UpdateTaskInstanceStatus(ti.ID, models.TaskFailed)
		return
	}

	// Execute the command natively
	cmd := exec.Command("sh", "-c", cmdStr)
	output, err := cmd.CombinedOutput()

	if err != nil {
		log.Printf("Worker %d: Task %s FAILED: %v\nOutput: %s", workerID, ti.ID, err, string(output))
		p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskFailed, string(output))
	} else {
		log.Printf("Worker %d: Task %s SUCCESS\nOutput: %s", workerID, ti.ID, string(output))
		p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskSuccess, string(output))
	}
}

// Stop gracefully shuts down the pool
func (p *Pool) Stop() {
	p.wg.Wait()
}
