package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/alephmelo/nagare/internal/worker"
)

// RemoteWorkerConfig holds all configuration for a worker node.
type RemoteWorkerConfig struct {
	MasterAddr          string        // e.g. "http://192.168.1.10:8080"
	WorkerID            string        // Unique ID (UUID) generated at startup
	Pools               []string      // Pool names this worker serves
	Hostname            string        // OS hostname (filled automatically if empty)
	MaxTasks            int           // Concurrent task slots
	Token               string        // Shared secret (empty = no auth)
	PollInterval        time.Duration // How often to ask master for work
	HeartbeatInterval   time.Duration // How often to send heartbeat
	CancelCheckInterval time.Duration // How often running tasks check for cancel
}

// RemoteWorker is the worker-side agent. It registers with, polls, and reports
// results back to a Nagare master node over HTTP.
type RemoteWorker struct {
	cfg         RemoteWorkerConfig
	httpClient  *http.Client
	activeMu    sync.RWMutex
	activeTasks map[string]context.CancelFunc // taskInstanceID → cancel func
}

// NewRemoteWorker creates a RemoteWorker. Call Register() before Run() or PollOnce().
func NewRemoteWorker(cfg RemoteWorkerConfig) *RemoteWorker {
	if cfg.Hostname == "" {
		cfg.Hostname, _ = os.Hostname()
	}
	if cfg.MaxTasks <= 0 {
		cfg.MaxTasks = 4
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 10 * time.Second
	}
	if cfg.CancelCheckInterval <= 0 {
		cfg.CancelCheckInterval = 5 * time.Second
	}

	return &RemoteWorker{
		cfg:         cfg,
		httpClient:  &http.Client{Timeout: 30 * time.Second},
		activeTasks: make(map[string]context.CancelFunc),
	}
}

// Register sends a WorkerRegistration to the master. Must be called before Run.
func (rw *RemoteWorker) Register() error {
	reg := WorkerRegistration{
		WorkerID: rw.cfg.WorkerID,
		Pools:    rw.cfg.Pools,
		Hostname: rw.cfg.Hostname,
		MaxTasks: rw.cfg.MaxTasks,
	}
	resp, err := rw.post("/api/workers/register", reg)
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("register: master returned %d", resp.StatusCode)
	}
	log.Printf("RemoteWorker %s: registered with master %s (pools=%v)", rw.cfg.WorkerID, rw.cfg.MasterAddr, rw.cfg.Pools)
	return nil
}

// Run starts the heartbeat loop and the poll loop. It blocks until ctx is cancelled.
func (rw *RemoteWorker) Run(ctx context.Context) {
	go rw.heartbeatLoop(ctx)
	rw.pollLoop(ctx)
}

// PollOnce polls the master once and, if a task is returned, executes it
// asynchronously. Returns nil even when there is no work (204 response).
func (rw *RemoteWorker) PollOnce() error {
	req := PollRequest{WorkerID: rw.cfg.WorkerID, Pools: rw.cfg.Pools}
	resp, err := rw.post("/api/workers/poll", req)
	if err != nil {
		return fmt.Errorf("poll: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil // no work
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("poll: unexpected status %d", resp.StatusCode)
	}

	var assignment TaskAssignmentDTO
	if err := json.NewDecoder(resp.Body).Decode(&assignment); err != nil {
		return fmt.Errorf("poll: decode assignment: %w", err)
	}

	go rw.executeAssignment(context.Background(), assignment)
	return nil
}

// ----- internal --------------------------------------------------------------

func (rw *RemoteWorker) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(rw.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := rw.PollOnce(); err != nil {
				log.Printf("RemoteWorker %s: poll error: %v", rw.cfg.WorkerID, err)
			}
		}
	}
}

func (rw *RemoteWorker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(rw.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reg := WorkerRegistration{WorkerID: rw.cfg.WorkerID}
			resp, err := rw.post("/api/workers/heartbeat", reg)
			if err != nil {
				log.Printf("RemoteWorker %s: heartbeat error: %v", rw.cfg.WorkerID, err)
				continue
			}
			resp.Body.Close()
		}
	}
}

// executeAssignment runs a TaskAssignmentDTO locally and reports the result.
func (rw *RemoteWorker) executeAssignment(ctx context.Context, a TaskAssignmentDTO) {
	log.Printf("RemoteWorker %s: executing task %s", rw.cfg.WorkerID, a.TaskInstanceID)

	taskCtx, cancel := context.WithCancel(ctx)
	rw.activeMu.Lock()
	rw.activeTasks[a.TaskInstanceID] = cancel
	rw.activeMu.Unlock()

	defer func() {
		cancel()
		rw.activeMu.Lock()
		delete(rw.activeTasks, a.TaskInstanceID)
		rw.activeMu.Unlock()
	}()

	// Start a cancel-check goroutine that polls the master for external cancellation.
	go rw.cancelCheckLoop(taskCtx, cancel, a.TaskInstanceID)

	// Log buffer: collect lines and flush to master periodically.
	var logBuf []string
	var logMu sync.Mutex

	flushLogs := func() {
		logMu.Lock()
		if len(logBuf) == 0 {
			logMu.Unlock()
			return
		}
		batch := make([]string, len(logBuf))
		copy(batch, logBuf)
		logBuf = logBuf[:0]
		logMu.Unlock()

		payload := LogBatch{TaskInstanceID: a.TaskInstanceID, Lines: batch}
		resp, err := rw.post("/api/workers/log", payload)
		if err != nil {
			log.Printf("RemoteWorker %s: log flush error: %v", rw.cfg.WorkerID, err)
			return
		}
		resp.Body.Close()
	}

	// Log flusher: periodically send buffered lines to master while the task runs.
	logFlushStop := make(chan struct{})
	logDone := make(chan struct{})
	go func() {
		defer close(logDone)
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				flushLogs()
			case <-logFlushStop:
				return
			}
		}
	}()

	// Convert the DTO to a TaskAssignment so we can use the executor abstraction.
	taskAssignment := &worker.TaskAssignment{
		TaskInstanceID: a.TaskInstanceID,
		RunID:          a.RunID,
		Command:        a.Command,
		Env:            a.Env,
		TimeoutSecs:    a.TimeoutSecs,
		Retries:        a.Retries,
		Attempt:        a.Attempt,
		Image:          a.Image,
		Workdir:        a.Workdir,
		Volumes:        a.Volumes,
		Resources:      a.Resources,
	}

	exec := worker.NewExecutor(taskAssignment)

	result, runErr := exec.Run(
		taskCtx,
		taskAssignment,
		func(line string) {
			logMu.Lock()
			logBuf = append(logBuf, line)
			logMu.Unlock()
		},
		nil, // cancel registration not needed; context cancel stops execution
	)

	close(logFlushStop)
	<-logDone
	flushLogs() // final flush

	// Determine reported status.
	reportStatus := "success"
	if runErr != nil {
		// Check if it was an external cancel.
		inst, checkErr := rw.getCancelStatus(a.TaskInstanceID)
		if checkErr == nil && inst {
			reportStatus = "cancelled"
		} else {
			// Covers both timeout and general failure; TimedOut flag in TaskResult
			// communicates the distinction to the master.
			reportStatus = "failed"
		}
	}

	taskResult := TaskResult{
		TaskInstanceID:  a.TaskInstanceID,
		Status:          reportStatus,
		Output:          result.Output,
		TimedOut:        result.TimedOut,
		DurationMs:      result.DurationMs,
		CpuUserMs:       result.CpuUserMs,
		CpuSystemMs:     result.CpuSystemMs,
		PeakMemoryBytes: result.PeakMemoryBytes,
		ExitCode:        result.ExitCode,
		ExecutorType:    result.ExecutorType,
	}

	resp, err := rw.post("/api/workers/result", taskResult)
	if err != nil {
		log.Printf("RemoteWorker %s: result post error: %v", rw.cfg.WorkerID, err)
		return
	}
	resp.Body.Close()
	log.Printf("RemoteWorker %s: task %s completed with status %s", rw.cfg.WorkerID, a.TaskInstanceID, reportStatus)
}

// cancelCheckLoop polls the master periodically to detect external task cancellation.
func (rw *RemoteWorker) cancelCheckLoop(ctx context.Context, cancel context.CancelFunc, taskInstanceID string) {
	ticker := time.NewTicker(rw.cfg.CancelCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cancelled, err := rw.getCancelStatus(taskInstanceID)
			if err != nil {
				continue
			}
			if cancelled {
				log.Printf("RemoteWorker %s: task %s cancelled by master", rw.cfg.WorkerID, taskInstanceID)
				cancel()
				return
			}
		}
	}
}

// getCancelStatus calls GET /api/workers/tasks/{id}/cancel and returns whether it's cancelled.
func (rw *RemoteWorker) getCancelStatus(taskInstanceID string) (bool, error) {
	url := rw.cfg.MasterAddr + "/api/workers/tasks/" + taskInstanceID + "/cancel"
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}
	if rw.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+rw.cfg.Token)
	}
	resp, err := rw.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	var check CancelCheck
	if err := json.NewDecoder(resp.Body).Decode(&check); err != nil {
		return false, err
	}
	return check.Cancel, nil
}

// post is a convenience helper that JSON-encodes body and POSTs to the master.
func (rw *RemoteWorker) post(path string, body interface{}) (*http.Response, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, rw.cfg.MasterAddr+path, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if rw.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+rw.cfg.Token)
	}
	return rw.httpClient.Do(req)
}
