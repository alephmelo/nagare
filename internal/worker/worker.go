package worker

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/alephmelo/nagare/internal/models"
)

// Pool manages groups of worker goroutines across different task queues
type Pool struct {
	store       *models.Store
	getDAG      func(string) (*models.DAGDef, bool)
	triggerDAG  func(string, string) (*models.DagRun, error)
	taskQueues  map[string]chan models.TaskInstance
	workerSizes map[string]int
	wg          sync.WaitGroup
	running     map[string]*exec.Cmd
	rmu         sync.RWMutex
}

// NewPool initializes a new worker pool manager
func NewPool(store *models.Store, getDAG func(string) (*models.DAGDef, bool), triggerDAG func(string, string) (*models.DagRun, error), sizes map[string]int) *Pool {
	queues := make(map[string]chan models.TaskInstance)
	for name := range sizes {
		queues[name] = make(chan models.TaskInstance, 100)
	}

	return &Pool{
		store:       store,
		getDAG:      getDAG,
		triggerDAG:  triggerDAG,
		taskQueues:  queues,
		workerSizes: sizes,
		running:     make(map[string]*exec.Cmd),
	}
}

// Start boots up the worker goroutines for all configured pools
func (p *Pool) Start(ctx context.Context) {
	workerID := 0
	for poolName, size := range p.workerSizes {
		for i := 0; i < size; i++ {
			p.wg.Add(1)
			go p.worker(ctx, workerID, poolName)
			workerID++
		}
	}
}

// Dispatch polls the database for queued tasks and sends them to the appropriate workers
func (p *Pool) Dispatch() error {
	queued, err := p.store.GetQueuedTasks()
	if err != nil {
		return err
	}

	for _, ti := range queued {
		// Look up the task's pool
		run, err := p.store.GetDagRun(ti.RunID)
		if err != nil {
			p.store.UpdateTaskInstanceStatus(ti.ID, models.TaskFailed)
			continue
		}

		dag, ok := p.getDAG(run.DAGID)
		if !ok {
			p.store.UpdateTaskInstanceStatus(ti.ID, models.TaskFailed)
			continue
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
			continue
		}

		poolName := taskDef.Pool
		if poolName == "" {
			poolName = "default"
		}

		queue, exists := p.taskQueues[poolName]
		if !exists {
			errMsg := fmt.Sprintf("Target pool '%s' does not exist", poolName)
			log.Printf("Worker Pool: Task %s FAILED: %s", ti.ID, errMsg)
			p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskFailed, errMsg)
			continue
		}

		// Mark task as running so it isn't picked up multiple times
		if err := p.store.UpdateTaskInstanceStatus(ti.ID, models.TaskRunning); err != nil {
			log.Printf("Failed to mark task %s as running: %v", ti.ID, err)
			continue
		}

		// Send to channel
		select {
		case queue <- ti:
			log.Printf("Dispatched task %s to pool %s", ti.ID, poolName)
		default:
			// If queue is full, rollback to queued
			p.store.UpdateTaskInstanceStatus(ti.ID, models.TaskQueued)
			log.Printf("Queue full for pool %s, rolled back task %s", poolName, ti.ID)
		}
	}
	return nil
}

// worker listens on its pool queue and executes shell commands
func (p *Pool) worker(ctx context.Context, id int, poolName string) {
	defer p.wg.Done()
	queue := p.taskQueues[poolName]
	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d (pool: %s) shutting down", id, poolName)
			return
		case ti := <-queue:
			p.executeTask(ctx, ti, id, poolName)
		}
	}
}

func (p *Pool) executeTask(ctx context.Context, ti models.TaskInstance, workerID int, poolName string) {
	log.Printf("Worker %d (pool: %s) starting task %s", workerID, poolName, ti.ID)

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

	var cmdCtx context.Context
	var cancel context.CancelFunc

	if taskDef.TimeoutSeconds > 0 {
		cmdCtx, cancel = context.WithTimeout(ctx, time.Duration(taskDef.TimeoutSeconds)*time.Second)
	} else {
		cmdCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	// Execute the command natively
	cmd := exec.CommandContext(cmdCtx, "sh", "-c", cmdStr)

	cmd.Env = os.Environ()
	execDateStr := run.ExecDate.Format(time.RFC3339)
	cmd.Env = append(cmd.Env, fmt.Sprintf("NAGARE_EXECUTION_DATE=%s", execDateStr))
	cmd.Env = append(cmd.Env, fmt.Sprintf("NAGARE_SCHEDULED_TIME=%s", execDateStr))

	if len(taskDef.Env) > 0 {
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
		// Check if the context timed out
		if cmdCtx.Err() == context.DeadlineExceeded {
			errMsg := fmt.Sprintf("Task timed out after %d seconds\nOutput: %s", taskDef.TimeoutSeconds, string(output))
			log.Printf("Worker %d: Task %s TIMEOUT: %s", workerID, ti.ID, errMsg)
			p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskFailed, errMsg)
			return
		}

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
