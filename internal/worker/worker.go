package worker

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/alephmelo/nagare/internal/logbroker"
	"github.com/alephmelo/nagare/internal/models"
)

// runningTask holds a running command and its output pipe reader so KillTask
// can close the pipe (unblocking the scanner goroutine) when the process group
// is killed but the scanner is still waiting for data.
type runningTask struct {
	cmd *exec.Cmd
	pr  *os.File
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
		return
	}

	if taskDef.Type == "trigger_dag" {
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

	cmdStr := taskDef.Command
	if ti.ItemValue != nil {
		cmdStr = strings.ReplaceAll(cmdStr, "{{item}}", *ti.ItemValue)
	}

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

	cmd := exec.CommandContext(cmdCtx, "sh", "-c", cmdStr)

	// Place the child in its own process group so that killing the group also
	// terminates any subprocesses spawned by the shell command.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	cmd.Env = os.Environ()
	execDateStr := run.ExecDate.Format(time.RFC3339)
	cmd.Env = append(cmd.Env, fmt.Sprintf("NAGARE_EXECUTION_DATE=%s", execDateStr))
	cmd.Env = append(cmd.Env, fmt.Sprintf("NAGARE_SCHEDULED_TIME=%s", execDateStr))

	if run.Conf != nil {
		for k, v := range run.Conf {
			cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
		}
	}

	if len(taskDef.Env) > 0 {
		for k, v := range taskDef.Env {
			cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
		}
	}

	// Use an os.Pipe so both stdout and stderr go to a single *os.File.
	// Because cmd.Stdout is an *os.File, exec does NOT start an internal copy
	// goroutine; cmd.Wait() returns as soon as the process exits, regardless
	// of whether the reader is still draining data.
	pr, pw, pipeErr := os.Pipe()
	if pipeErr != nil {
		p.broker.Close(ti.ID)
		p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskFailed, pipeErr.Error())
		return
	}
	cmd.Stdout = pw
	cmd.Stderr = pw

	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		p.broker.Close(ti.ID)
		p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskFailed, err.Error())
		return
	}

	// Close the write end in the parent process — the child has its own
	// inherited copy.  This ensures pr reaches EOF once the child (and all
	// its subprocesses) close their copy of pw.
	pw.Close()

	// Register the running process only after Start() so cmd.Process is fully initialized.
	p.rmu.Lock()
	p.running[ti.ID] = &runningTask{cmd: cmd, pr: pr}
	p.rmu.Unlock()

	defer func() {
		p.rmu.Lock()
		delete(p.running, ti.ID)
		p.rmu.Unlock()
	}()

	// Stream output line-by-line, publishing to broker and accumulating for DB.
	// scanDone signals that the scanner goroutine has finished.
	var buf strings.Builder
	scanDone := make(chan struct{})
	go func() {
		defer close(scanDone)
		scanner := bufio.NewScanner(pr)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // support lines up to 1 MB
		for scanner.Scan() {
			line := scanner.Text()
			buf.WriteString(line + "\n")
			p.broker.Publish(ti.ID, line)
		}
	}()

	runErr := cmd.Wait()
	log.Printf("executeTask %s: cmd.Wait() done, err=%v", ti.ID, runErr)
	pr.Close() // signal EOF to the scanner goroutine
	<-scanDone // wait for all lines to be processed before reading buf

	output := buf.String()

	p.broker.Close(ti.ID)

	if runErr != nil {
		if cmdCtx.Err() == context.DeadlineExceeded {
			errMsg := fmt.Sprintf("Task timed out after %d seconds\nOutput: %s", taskDef.TimeoutSeconds, output)
			log.Printf("Worker %d: Task %s TIMEOUT: %s", workerID, ti.ID, errMsg)
			p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskFailed, errMsg)
			p.broker.Cleanup(ti.ID)
			return
		}

		log.Printf("Worker %d: Task %s FAILED: %v\nOutput: %s", workerID, ti.ID, runErr, output)

		// Check if it was cancelled/killed externally before marking as failed
		latest, getErr := p.store.GetTaskInstance(ti.ID)
		if getErr == nil && latest.Status == models.TaskCancelled {
			log.Printf("Worker %d: Task %s was already marked as CANCELLED, skipping failure update", workerID, ti.ID)
			// Still save partial output so it isn't lost on kill
			p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskCancelled, output)
			p.broker.Cleanup(ti.ID)
			return
		}

		if ti.Attempt <= taskDef.Retries {
			log.Printf("Worker %d: Task %s FAILED but has retries remaining (%d/%d). Output: %s", workerID, ti.ID, ti.Attempt, taskDef.Retries, output)
			p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskUpForRetry, output)
		} else {
			p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskFailed, output)
		}
	} else {
		log.Printf("Worker %d: Task %s SUCCESS\nOutput: %s", workerID, ti.ID, output)
		p.store.UpdateTaskInstanceStatusAndOutput(ti.ID, models.TaskSuccess, output)
	}

	p.broker.Cleanup(ti.ID)
}

// KillTask terminates a running task process
func (p *Pool) KillTask(taskInstanceID string) error {
	p.rmu.RLock()
	rt, ok := p.running[taskInstanceID]
	p.rmu.RUnlock()

	if !ok {
		// If not in memory, it might be already finished or in queue.
		// We still mark it as cancelled in DB to be sure.
		return p.store.UpdateTaskInstanceStatus(taskInstanceID, models.TaskCancelled)
	}

	if rt.cmd != nil && rt.cmd.Process != nil {
		// With Setpgid=true the child process group id equals its pid.
		// Passing a negative value to Kill targets the entire process group,
		// which also terminates any subprocesses spawned by the shell command.
		pgid := rt.cmd.Process.Pid
		if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil {
			// Fall back to killing just the process if group kill fails.
			rt.cmd.Process.Kill()
		}
	}
	// Close the pipe reader so the scanner goroutine unblocks even if some
	// file descriptor leak keeps the write end alive.
	if rt.pr != nil {
		rt.pr.Close()
	}

	return p.store.UpdateTaskInstanceStatus(taskInstanceID, models.TaskCancelled)
}

// Stop gracefully shuts down the pool
func (p *Pool) Stop() {
	p.wg.Wait()
}
