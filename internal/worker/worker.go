package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/alephmelo/nagare/internal/logbroker"
	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/tasklifecycle"
)

type attemptLifecycle interface {
	Claim(string, time.Time) (tasklifecycle.Disposition, error)
	Complete(tasklifecycle.Completion) (tasklifecycle.Disposition, error)
	CancelAttempt(string, time.Time) (tasklifecycle.Disposition, error)
}

type localAttempt struct {
	ti        models.TaskInstance
	admission chan struct{}

	// gate totally orders cancellation with starting an executor or admitting a
	// trigger. It also protects the two cancellation callbacks.
	gate        sync.Mutex
	cancelled   bool
	ctxCancel   context.CancelFunc
	execCancel  func()
	output      strings.Builder
	outputSaved bool
}

type queuedAttempt struct {
	ti      models.TaskInstance
	control *localAttempt
}

const (
	lifecyclePersistenceAttempts = 6
	lifecyclePersistenceBackoff  = 10 * time.Millisecond
)

// Pool manages groups of worker goroutines across different task queues.
type Pool struct {
	store       *models.Store
	lifecycle   attemptLifecycle
	getDAG      func(string) (*models.DAGDef, bool)
	triggerDAG  func(string, string, map[string]string) (*models.DagRun, error)
	broker      *logbroker.Broker
	taskQueues  map[string]chan queuedAttempt
	admissions  map[string]chan struct{}
	workerSizes map[string]int
	wg          sync.WaitGroup

	rmu      sync.Mutex
	attempts map[string]*localAttempt
	// asyncErr contains errors not yet observed by Dispatch. stopErr retains
	// every asynchronous error so shutdown always has a deterministic error
	// boundary even when Dispatch previously observed the same failures.
	asyncErr error
	stopErr  error
}

// NewPool initializes a new worker pool manager.
func NewPool(store *models.Store, getDAG func(string) (*models.DAGDef, bool), triggerDAG func(string, string, map[string]string) (*models.DagRun, error), sizes map[string]int, broker *logbroker.Broker) *Pool {
	return NewPoolWithLifecycle(store, tasklifecycle.New(store), getDAG, triggerDAG, sizes, broker)
}

// NewPoolWithLifecycle initializes a worker pool using the provided shared
// task-attempt lifecycle.
func NewPoolWithLifecycle(store *models.Store, lifecycle *tasklifecycle.Lifecycle, getDAG func(string) (*models.DAGDef, bool), triggerDAG func(string, string, map[string]string) (*models.DagRun, error), sizes map[string]int, broker *logbroker.Broker) *Pool {
	queues := make(map[string]chan queuedAttempt)
	admissions := make(map[string]chan struct{})
	for name, size := range sizes {
		if size < 1 {
			size = 1
		}
		queues[name] = make(chan queuedAttempt, size)
		admissions[name] = make(chan struct{}, size)
	}
	return &Pool{
		store: store, lifecycle: lifecycle, getDAG: getDAG,
		triggerDAG: triggerDAG, broker: broker, taskQueues: queues,
		admissions: admissions, workerSizes: sizes, attempts: make(map[string]*localAttempt),
	}
}

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

// Dispatch reserves local capacity before making the authoritative claim.
func (p *Pool) Dispatch() error {
	p.rmu.Lock()
	priorErr := p.asyncErr
	p.asyncErr = nil
	p.rmu.Unlock()
	if priorErr != nil {
		return priorErr
	}
	queued, err := p.store.GetQueuedTasks()
	if err != nil {
		return err
	}
	for _, ti := range queued {
		run, err := p.store.GetDagRun(ti.RunID)
		if err != nil {
			if err := p.claimAndFail(ti, err.Error()); err != nil {
				return err
			}
			continue
		}
		dag, ok := p.getDAG(run.DAGID)
		if !ok {
			if err := p.claimAndFail(ti, fmt.Sprintf("DAG %q not found", run.DAGID)); err != nil {
				return err
			}
			continue
		}
		taskDef := dag.FindTask(models.BaseTaskID(ti.TaskID))
		if taskDef == nil {
			if err := p.claimAndFail(ti, fmt.Sprintf("task definition %q not found in DAG %q", ti.TaskID, dag.ID)); err != nil {
				return err
			}
			continue
		}
		poolName := taskDef.Pool
		if poolName == "" {
			poolName = "default"
		}
		admission, exists := p.admissions[poolName]
		if !exists {
			if err := p.claimAndFail(ti, fmt.Sprintf("Target pool '%s' does not exist", poolName)); err != nil {
				return err
			}
			continue
		}
		select {
		case admission <- struct{}{}:
		default:
			continue
		}
		// Serialize the authoritative claim with publishing its local control.
		// KillTask uses the same lock, so it can never observe a claimed attempt
		// before the cancellation/execution gate exists.
		p.rmu.Lock()
		disposition, err := p.lifecycle.Claim(ti.ID, time.Now())
		if err != nil {
			p.rmu.Unlock()
			<-admission
			return err
		}
		if disposition != tasklifecycle.Applied {
			p.rmu.Unlock()
			<-admission
			continue
		}
		control := &localAttempt{ti: ti, admission: admission}
		p.attempts[ti.ID] = control
		p.rmu.Unlock()
		// The admission token makes this send non-blocking by construction.
		p.taskQueues[poolName] <- queuedAttempt{ti: ti, control: control}
		log.Printf("Dispatched task %s to pool %s", ti.ID, poolName)
	}
	return nil
}

func (p *Pool) claimAndFail(ti models.TaskInstance, output string) error {
	disposition, err := p.lifecycle.Claim(ti.ID, time.Now())
	if err != nil || disposition != tasklifecycle.Applied {
		return err
	}
	defer p.cleanupBroker(ti.ID)
	_, err = p.completeLifecycle(tasklifecycle.Completion{
		AttemptID: ti.ID, Output: output, CompletedAt: time.Now(),
	})
	return err
}

func (p *Pool) worker(ctx context.Context, id int, poolName string) {
	defer p.wg.Done()
	queue := p.taskQueues[poolName]
	for {
		select {
		case work := <-queue:
			p.executeTask(ctx, work, id, poolName)
		case <-ctx.Done():
			p.drainUndispatched(queue)
			log.Printf("Worker %d (pool: %s) shutting down", id, poolName)
			return
		}
	}
}

// drainUndispatched persists first. A failed cancellation deliberately retains
// both the queued work and admission token and is surfaced by the next Dispatch.
func (p *Pool) drainUndispatched(queue chan queuedAttempt) {
	for {
		select {
		case work := <-queue:
			work.control.gate.Lock()
			disposition, err := p.cancelAttemptDuringShutdown(work.ti, time.Now())
			if err != nil {
				work.control.gate.Unlock()
				p.cleanupBroker(work.ti.ID)
				queue <- work
				p.recordError(fmt.Errorf("cancel task attempt %q during shutdown: %w", work.ti.ID, err))
				return
			}
			if disposition == tasklifecycle.Applied || disposition == tasklifecycle.AlreadyApplied {
				work.control.cancelled = true
			}
			work.control.gate.Unlock()
			p.cleanupBroker(work.ti.ID)
			p.finishLocal(work.control)
		default:
			return
		}
	}
}

func (p *Pool) executeTask(ctx context.Context, work queuedAttempt, workerID int, poolName string) {
	ti, control := work.ti, work.control
	releaseLocal := true
	defer func() {
		if releaseLocal {
			p.finishLocal(control)
		}
	}()
	defer p.cleanupBroker(ti.ID)

	run, err := p.store.GetDagRun(ti.RunID)
	if err != nil {
		p.completeAttempt(control, ti, "", RunResult{Output: err.Error()}, false, 0, workerID)
		return
	}
	dag, ok := p.getDAG(run.DAGID)
	if !ok {
		p.completeAttempt(control, ti, run.DAGID, RunResult{Output: fmt.Sprintf("DAG %q not found", run.DAGID)}, false, 0, workerID)
		return
	}
	taskDef := dag.FindTask(models.BaseTaskID(ti.TaskID))

	control.gate.Lock()
	if ctx.Err() != nil {
		disposition, cancelErr := p.cancelAttemptDuringShutdown(ti, time.Now())
		if cancelErr != nil {
			// Keep the attempt and admission token owned until process exit:
			// releasing either would make a still-running persisted row look
			// available after shutdown cancellation failed.
			releaseLocal = false
			control.gate.Unlock()
			p.recordError(fmt.Errorf("cancel task attempt %q during shutdown: %w", ti.ID, cancelErr))
			return
		}
		if disposition == tasklifecycle.Applied || disposition == tasklifecycle.AlreadyApplied {
			control.cancelled = true
		}
		control.gate.Unlock()
		return
	}
	if control.cancelled {
		control.gate.Unlock()
		return
	}
	if taskDef != nil && taskDef.Type == "trigger_dag" {
		// Keep the gate across the irreversible side effect. KillTask either
		// commits first (and prevents it), or commits after this admission.
		triggeredRun, triggerErr := p.triggerDAG(taskDef.DagID, "triggered", nil)
		control.gate.Unlock()
		result := RunResult{}
		if triggerErr != nil {
			result.Output = fmt.Sprintf("Failed to trigger DAG %s: %v", taskDef.DagID, triggerErr)
			p.completeAttempt(control, ti, run.DAGID, result, false, taskDef.Retries, workerID)
		} else {
			result.Output = fmt.Sprintf("Successfully triggered DAG run: %s", triggeredRun.ID)
			p.completeAttempt(control, ti, run.DAGID, result, true, taskDef.Retries, workerID)
		}
		return
	}
	execCtx, cancel := context.WithCancel(ctx)
	control.ctxCancel = cancel
	control.gate.Unlock()
	defer cancel()

	assignment, err := PrepareTaskAssignment(run, ti, dag)
	if err != nil {
		retries := 0
		if taskDef != nil {
			retries = taskDef.Retries
		}
		p.completeAttempt(control, ti, run.DAGID, RunResult{Output: err.Error()}, false, retries, workerID)
		return
	}
	if assignment.Command == "" {
		p.completeAttempt(control, ti, run.DAGID, RunResult{}, false, assignment.Retries, workerID)
		return
	}
	result, runErr := NewExecutor(assignment).Run(execCtx, assignment,
		func(line string) {
			p.broker.Publish(ti.ID, line)
			control.gate.Lock()
			control.output.WriteString(line + "\n")
			control.gate.Unlock()
		},
		func(cancelFn func()) {
			control.gate.Lock()
			control.execCancel = cancelFn
			cancelled := control.cancelled
			control.gate.Unlock()
			if cancelled {
				cancelFn()
			}
		})
	if runErr != nil && strings.TrimSpace(result.Output) == "" {
		result.Output = runErr.Error()
	}
	if result.TimedOut {
		result.Output = fmt.Sprintf("Task timed out after %d seconds\nOutput: %s", assignment.TimeoutSecs, result.Output)
	}
	p.completeAttempt(control, ti, run.DAGID, result, runErr == nil, assignment.Retries, workerID)
}

func (p *Pool) completeAttempt(control *localAttempt, ti models.TaskInstance, dagID string, result RunResult, succeeded bool, retries int, workerID int) {
	control.gate.Lock()
	defer control.gate.Unlock()
	p.completeWithRetries(ti, dagID, result, succeeded, retries, workerID)
}

func (p *Pool) completeWithRetries(ti models.TaskInstance, dagID string, result RunResult, succeeded bool, retries int, workerID int) {
	disposition, err := p.completeLifecycle(tasklifecycle.Completion{
		AttemptID: ti.ID, Succeeded: succeeded, Output: result.Output,
		TimedOut: result.TimedOut, Retries: retries, CompletedAt: time.Now(),
	})
	if err != nil {
		p.recordError(err)
		return
	}
	if disposition != tasklifecycle.Applied {
		return
	}
	p.persistMetrics(ti, dagID, result)
	if succeeded {
		log.Printf("Worker %d: Task %s SUCCESS\nOutput: %s", workerID, ti.ID, result.Output)
	} else if result.TimedOut {
		log.Printf("Worker %d: Task %s TIMEOUT: %s", workerID, ti.ID, result.Output)
	} else {
		log.Printf("Worker %d: Task %s FAILED\nOutput: %s", workerID, ti.ID, result.Output)
	}
}

func (p *Pool) persistMetrics(ti models.TaskInstance, dagID string, result RunResult) {
	m := &models.TaskMetrics{
		TaskInstanceID: ti.ID, RunID: ti.RunID, DAGID: dagID, TaskID: ti.TaskID,
		DurationMs: result.DurationMs, CpuUserMs: result.CpuUserMs,
		CpuSystemMs: result.CpuSystemMs, PeakMemoryBytes: result.PeakMemoryBytes,
		ExitCode: result.ExitCode, ExecutorType: result.ExecutorType, CreatedAt: time.Now(),
	}
	if err := p.store.InsertTaskMetrics(m); err != nil {
		log.Printf("Warning: failed to persist metrics for task %s: %v", ti.ID, err)
	}
}

func (p *Pool) KillTask(taskInstanceID string) error {
	ti, err := p.store.GetTaskInstance(taskInstanceID)
	if err != nil {
		return err
	}
	p.rmu.Lock()
	control := p.attempts[taskInstanceID]
	p.rmu.Unlock()
	if control == nil {
		_, err := p.cancelAttempt(*ti, time.Now())
		p.cleanupBroker(taskInstanceID)
		return err
	}
	control.gate.Lock()
	disposition, err := p.cancelAttempt(*ti, time.Now())
	if err != nil {
		control.gate.Unlock()
		p.cleanupBroker(taskInstanceID)
		return err
	}
	if disposition == tasklifecycle.Applied || disposition == tasklifecycle.AlreadyApplied {
		control.cancelled = true
		if control.ctxCancel != nil {
			control.ctxCancel()
		}
		if control.execCancel != nil {
			control.execCancel()
		}
		// Cancellation is authoritative, while output is a non-terminal stream.
		// Persist the captured prefix only after cancellation is durable.
		if output := control.output.String(); output != "" && !control.outputSaved {
			if appendErr := p.store.AppendTaskOutput(taskInstanceID, output); appendErr != nil {
				control.gate.Unlock()
				p.cleanupBroker(taskInstanceID)
				return appendErr
			}
			control.outputSaved = true
		}
	}
	control.gate.Unlock()
	p.cleanupBroker(taskInstanceID)
	return nil
}

// cancelAttempt preserves the worker API's exact-attempt contract at the
// lifecycle boundary.
func (p *Pool) cancelAttempt(ti models.TaskInstance, cancelledAt time.Time) (tasklifecycle.Disposition, error) {
	return p.lifecycle.CancelAttempt(ti.ID, cancelledAt)
}

func (p *Pool) cancelAttemptDuringShutdown(ti models.TaskInstance, cancelledAt time.Time) (tasklifecycle.Disposition, error) {
	var disposition tasklifecycle.Disposition
	var err error
	for attempt := 0; attempt < lifecyclePersistenceAttempts; attempt++ {
		disposition, err = p.cancelAttempt(ti, cancelledAt)
		if err == nil || !isTransientPersistenceError(err) {
			return disposition, err
		}
		if attempt+1 < lifecyclePersistenceAttempts {
			time.Sleep(lifecyclePersistenceBackoff)
		}
	}
	return disposition, err
}

func isTransientPersistenceError(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "locked") || strings.Contains(message, "busy")
}

func (p *Pool) finishLocal(control *localAttempt) {
	p.rmu.Lock()
	delete(p.attempts, control.ti.ID)
	p.rmu.Unlock()
	<-control.admission
}

func (p *Pool) cleanupBroker(id string) {
	p.broker.Close(id)
	p.broker.Cleanup(id)
}

func (p *Pool) recordError(err error) {
	if err == nil {
		return
	}
	p.rmu.Lock()
	p.asyncErr = errors.Join(p.asyncErr, err)
	p.stopErr = errors.Join(p.stopErr, err)
	p.rmu.Unlock()
}

// SQLite can transiently report table locking when two local pools complete at
// once. Retry only that transient condition; the final persistence error is
// still returned to the synchronous caller or retained in asyncErr.
func (p *Pool) completeLifecycle(input tasklifecycle.Completion) (tasklifecycle.Disposition, error) {
	var disposition tasklifecycle.Disposition
	var err error
	for i := 0; i < 6; i++ {
		disposition, err = p.lifecycle.Complete(input)
		if err == nil || !strings.Contains(strings.ToLower(err.Error()), "locked") {
			return disposition, err
		}
		time.Sleep(10 * time.Millisecond)
	}
	return disposition, err
}

// Stop waits for all local workers and reports every asynchronous adapter
// error, including errors already observed by a later Dispatch call.
func (p *Pool) Stop() error {
	p.wg.Wait()
	p.rmu.Lock()
	defer p.rmu.Unlock()
	return p.stopErr
}
