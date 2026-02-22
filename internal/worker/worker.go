package worker

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/alephmelo/nagare/internal/logbroker"
	"github.com/alephmelo/nagare/internal/models"
)

// runningTask holds a cancel function that kills the running execution,
// regardless of whether it is a local process or a Docker container.
type runningTask struct {
	cancel func()
}

// Pool manages groups of worker goroutines across different task queues
type Pool struct {
	store       *models.Store
	getDAG      func(string) (*models.DAGDef, bool)
	triggerDAG  func(string, string, map[string]string) (*models.DagRun, error)
	broker      *logbroker.Broker
	taskQueues  map[string]chan models.TaskInstance
	workerSizes map[string]int
	wg          sync.WaitGroup
	running     map[string]*runningTask
	rmu         sync.RWMutex
}

// NewPool initializes a new worker pool manager
func NewPool(store *models.Store, getDAG func(string) (*models.DAGDef, bool), triggerDAG func(string, string, map[string]string) (*models.DagRun, error), sizes map[string]int, broker *logbroker.Broker) *Pool {
	queues := make(map[string]chan models.TaskInstance)
	for name := range sizes {
		queues[name] = make(chan models.TaskInstance, 100)
	}

	return &Pool{
		store:       store,
		getDAG:      getDAG,
		triggerDAG:  triggerDAG,
		broker:      broker,
		taskQueues:  queues,
		workerSizes: sizes,
		running:     make(map[string]*runningTask),
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

		select {
		case queue <- ti:
			log.Printf("Dispatched task %s to pool %s", ti.ID, poolName)
		default:
			// Queue full — roll back to queued so the next Dispatch picks it up.
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

	// Handle trigger_dag type tasks before building a full assignment.
	baseTaskID := ti.TaskID
	if idx := strings.Index(baseTaskID, "["); idx != -1 {
		baseTaskID = baseTaskID[:idx]
	}
	for i := range dag.Tasks {
		if dag.Tasks[i].ID == baseTaskID && dag.Tasks[i].Type == "trigger_dag" {
			taskDef := &dag.Tasks[i]
			triggeredRun, err := p.triggerDAG(taskDef.DagID, "triggered", nil)
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
	}

	// Resolve command, env, timeout via shared helper.
	assignment, err := PrepareTaskAssignment(run, ti, dag)
	if err != nil {
		p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskFailed, err.Error())
		return
	}

	if assignment.Command == "" {
		p.store.UpdateTaskInstanceStatus(ti.ID, models.TaskFailed)
		return
	}

	// Register a slot in the running map; onCancel fills in the cancel func after
	// the executor has started the underlying process or container.
	p.rmu.Lock()
	p.running[ti.ID] = &runningTask{}
	p.rmu.Unlock()

	defer func() {
		p.rmu.Lock()
		delete(p.running, ti.ID)
		p.rmu.Unlock()
	}()

	exec := NewExecutor(assignment)

	// Record when actual execution begins.
	startedAt := time.Now()
	p.store.SetTaskStartedAt(ti.ID, startedAt)

	result, runErr := exec.Run(ctx, assignment,
		func(line string) {
			p.broker.Publish(ti.ID, line)
		},
		func(cancelFn func()) {
			p.rmu.Lock()
			p.running[ti.ID] = &runningTask{cancel: cancelFn}
			p.rmu.Unlock()
		},
	)

	log.Printf("executeTask %s: RunCommand done, err=%v", ti.ID, runErr)

	// Persist resource metrics regardless of success/failure.
	p.persistMetrics(ti, run.DAGID, result)

	if runErr != nil {
		if result.TimedOut {
			errMsg := fmt.Sprintf("Task timed out after %d seconds\nOutput: %s", assignment.TimeoutSecs, result.Output)
			log.Printf("Worker %d: Task %s TIMEOUT: %s", workerID, ti.ID, errMsg)
			p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskFailed, errMsg)
			p.broker.Close(ti.ID)
			p.broker.Cleanup(ti.ID)
			return
		}

		log.Printf("Worker %d: Task %s FAILED: %v\nOutput: %s", workerID, ti.ID, runErr, result.Output)

		// Check if it was cancelled/killed externally before marking as failed.
		latest, getErr := p.store.GetTaskInstance(ti.ID)
		if getErr == nil && latest.Status == models.TaskCancelled {
			log.Printf("Worker %d: Task %s was already marked as CANCELLED, skipping failure update", workerID, ti.ID)
			p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskCancelled, result.Output)
			p.broker.Close(ti.ID)
			p.broker.Cleanup(ti.ID)
			return
		}

		if ti.Attempt <= assignment.Retries {
			log.Printf("Worker %d: Task %s FAILED but has retries remaining (%d/%d). Output: %s", workerID, ti.ID, ti.Attempt, assignment.Retries, result.Output)
			p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskUpForRetry, result.Output)
		} else {
			p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskFailed, result.Output)
		}
	} else {
		log.Printf("Worker %d: Task %s SUCCESS\nOutput: %s", workerID, ti.ID, result.Output)
		p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskSuccess, result.Output)
	}

	p.broker.Close(ti.ID)
	p.broker.Cleanup(ti.ID)
}

// persistMetrics saves a TaskMetrics row for a completed task execution.
// Errors are logged but do not affect task status.
func (p *Pool) persistMetrics(ti models.TaskInstance, dagID string, result RunResult) {
	m := &models.TaskMetrics{
		TaskInstanceID:  ti.ID,
		RunID:           ti.RunID,
		DAGID:           dagID,
		TaskID:          ti.TaskID,
		DurationMs:      result.DurationMs,
		CpuUserMs:       result.CpuUserMs,
		CpuSystemMs:     result.CpuSystemMs,
		PeakMemoryBytes: result.PeakMemoryBytes,
		ExitCode:        result.ExitCode,
		ExecutorType:    result.ExecutorType,
		CreatedAt:       time.Now(),
	}
	if err := p.store.InsertTaskMetrics(m); err != nil {
		log.Printf("Warning: failed to persist metrics for task %s: %v", ti.ID, err)
	}
}

// KillTask terminates a running task process or container
func (p *Pool) KillTask(taskInstanceID string) error {
	p.rmu.RLock()
	rt, ok := p.running[taskInstanceID]
	p.rmu.RUnlock()

	if !ok {
		// If not in memory, it might already be finished or queued.
		// Still mark it cancelled in the DB to be safe.
		return p.store.UpdateTaskInstanceStatus(taskInstanceID, models.TaskCancelled)
	}

	if rt.cancel != nil {
		rt.cancel()
	}

	return p.store.UpdateTaskInstanceStatus(taskInstanceID, models.TaskCancelled)
}

// Stop gracefully shuts down the pool
func (p *Pool) Stop() {
	p.wg.Wait()
}
