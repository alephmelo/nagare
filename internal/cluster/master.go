package cluster

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/alephmelo/nagare/internal/autoscaler"
	"github.com/alephmelo/nagare/internal/logbroker"
	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/worker"
)

// Coordinator manages remote worker connections on the master node.
// It handles worker registration, task polling, result reporting,
// and log forwarding.
type Coordinator struct {
	store         *models.Store
	getDAG        func(string) (*models.DAGDef, bool)
	broker        *logbroker.Broker
	token         string        // shared secret; empty = no auth required
	workerTimeout time.Duration // marks workers offline after this idle period
	mu            sync.RWMutex
	workers       map[string]*WorkerInfo // keyed by WorkerID

	// autoscaler is optional; when set, the coordinator notifies it on
	// worker registration and stale-worker expiry for cloud-managed workers.
	as *autoscaler.Autoscaler
}

// NewCoordinator creates a Coordinator. getDAG may be nil if only the HTTP
// handler is needed without task dispatch (e.g. in tests that don't poll).
func NewCoordinator(store *models.Store, getDAG func(string) (*models.DAGDef, bool), workerTimeout time.Duration, token string) *Coordinator {
	return &Coordinator{
		store:         store,
		getDAG:        getDAG,
		token:         token,
		workerTimeout: workerTimeout,
		workers:       make(map[string]*WorkerInfo),
	}
}

// SetBroker attaches a log broker so forwarded log lines are streamed to the UI.
func (c *Coordinator) SetBroker(b *logbroker.Broker) {
	c.broker = b
}

// SetAutoscaler wires up an autoscaler so the coordinator can notify it when
// cloud workers come online or go stale.  Call this before starting the server.
func (c *Coordinator) SetAutoscaler(a *autoscaler.Autoscaler) {
	c.as = a
}

// WorkerActiveTasks returns the number of tasks currently executing on the
// named worker.  Returns 0 when the worker is unknown or offline.
// This implements autoscaler.StatsSource.
func (c *Coordinator) WorkerActiveTasks(workerID string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if w, ok := c.workers[workerID]; ok {
		return w.ActiveTasks
	}
	return 0
}

// PoolStats returns a per-pool utilisation snapshot.  It counts queued tasks
// from the store and cross-references the registered online workers.
// This implements autoscaler.StatsSource.
func (c *Coordinator) PoolStats() map[string]autoscaler.PoolStats {
	queued, err := c.store.GetQueuedTasks()
	if err != nil {
		log.Printf("Coordinator.PoolStats: failed to query queued tasks: %v", err)
		queued = nil
	}

	// Count queued tasks per pool by walking the task instances.
	// We use task_id pool assignment via getDAG.  Tasks without a pool go to "default".
	queuedByPool := make(map[string]int)
	for _, ti := range queued {
		pool := "default"
		// Resolve pool from DAG definition if available.
		if c.getDAG != nil {
			if run, err := c.store.GetDagRun(ti.RunID); err == nil {
				if dag, ok := c.getDAG(run.DAGID); ok {
					baseTaskID := ti.TaskID
					if idx := strings.Index(baseTaskID, "["); idx != -1 {
						baseTaskID = baseTaskID[:idx]
					}
					for _, t := range dag.Tasks {
						if t.ID == baseTaskID && t.Pool != "" {
							pool = t.Pool
							break
						}
					}
				}
			}
		}
		queuedByPool[pool]++
	}

	// Count online workers per pool.
	c.mu.RLock()
	workersByPool := make(map[string]int)
	cloudByPool := make(map[string]int)
	for _, w := range c.workers {
		if w.Status != "online" {
			continue
		}
		for _, p := range w.Pools {
			workersByPool[p]++
			if w.IsCloudManaged {
				cloudByPool[p]++
			}
		}
	}
	c.mu.RUnlock()

	// Merge all known pools.
	allPools := make(map[string]struct{})
	for p := range queuedByPool {
		allPools[p] = struct{}{}
	}
	for p := range workersByPool {
		allPools[p] = struct{}{}
	}

	stats := make(map[string]autoscaler.PoolStats, len(allPools))
	for pool := range allPools {
		stats[pool] = autoscaler.PoolStats{
			Pool:          pool,
			QueuedTasks:   queuedByPool[pool],
			ActiveWorkers: workersByPool[pool],
			CloudWorkers:  cloudByPool[pool],
		}
	}

	// Always include the "default" pool even when empty, so the autoscaler
	// has something to evaluate.
	if _, ok := stats["default"]; !ok {
		stats["default"] = autoscaler.PoolStats{Pool: "default"}
	}

	return stats
}

// Register upserts a worker registration (idempotent on re-register/heartbeat).
func (c *Coordinator) Register(reg WorkerRegistration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	existing, ok := c.workers[reg.WorkerID]
	if ok {
		// Update last-seen and pool list on re-register.
		existing.LastSeen = time.Now()
		existing.Status = "online"
		if len(reg.Pools) > 0 {
			existing.Pools = reg.Pools
		}
		if reg.MaxTasks > 0 {
			existing.MaxTasks = reg.MaxTasks
		}
		if reg.Hostname != "" {
			existing.Hostname = reg.Hostname
		}
		return
	}

	wi := &WorkerInfo{
		WorkerID: reg.WorkerID,
		Pools:    reg.Pools,
		Hostname: reg.Hostname,
		MaxTasks: reg.MaxTasks,
		LastSeen: time.Now(),
		Status:   "online",
	}

	// Check if this is an autoscaler-provisioned worker.
	if c.as != nil {
		if instanceID, matched := c.as.TryClaimWorker(reg.WorkerID, reg.Pools); matched {
			wi.IsCloudManaged = true
			wi.CloudInstanceID = instanceID
			log.Printf("Cluster: cloud worker %s registered (instance=%s, pools=%v)", reg.WorkerID, instanceID, reg.Pools)
		}
	}

	c.workers[reg.WorkerID] = wi
	if !wi.IsCloudManaged {
		log.Printf("Cluster: worker %s registered (hostname=%s, pools=%v)", reg.WorkerID, reg.Hostname, reg.Pools)
	}
}

// ListWorkers returns a snapshot of all known workers.
func (c *Coordinator) ListWorkers() []WorkerInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make([]WorkerInfo, 0, len(c.workers))
	for _, w := range c.workers {
		out = append(out, *w)
	}
	return out
}

// ExpireStaleWorkers marks workers as offline if they haven't sent a heartbeat
// within the configured workerTimeout. Intended to be called on a schedule.
// Cloud-managed workers that go stale are also reported to the autoscaler so
// the underlying cloud resource can be terminated.
func (c *Coordinator) ExpireStaleWorkers() {
	c.mu.Lock()
	defer c.mu.Unlock()

	cutoff := time.Now().Add(-c.workerTimeout)
	for _, w := range c.workers {
		if w.Status == "online" && w.LastSeen.Before(cutoff) {
			w.Status = "offline"
			log.Printf("Cluster: worker %s marked offline (last seen %v ago)", w.WorkerID, time.Since(w.LastSeen))

			// Notify the autoscaler so it can terminate the cloud resource.
			if c.as != nil && w.IsCloudManaged && w.CloudInstanceID != "" {
				c.as.NotifyWorkerOffline(w.CloudInstanceID)
			}
		}
	}
}

// Handler returns an http.Handler that serves all /api/workers/* endpoints.
// It wraps every route with the token auth middleware when a token is configured.
func (c *Coordinator) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/workers/register", c.authMiddleware(c.handleRegister))
	mux.HandleFunc("/api/workers/heartbeat", c.authMiddleware(c.handleHeartbeat))
	mux.HandleFunc("/api/workers/poll", c.authMiddleware(c.handlePoll))
	mux.HandleFunc("/api/workers/result", c.authMiddleware(c.handleResult))
	mux.HandleFunc("/api/workers/log", c.authMiddleware(c.handleLog))
	mux.HandleFunc("/api/workers/tasks/", c.authMiddleware(c.handleTaskCancel))
	mux.HandleFunc("/api/workers", c.authMiddleware(c.handleListWorkers))

	return mux
}

// ----- auth middleware -------------------------------------------------------

func (c *Coordinator) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if c.token == "" {
			next(w, r)
			return
		}
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") || strings.TrimPrefix(auth, "Bearer ") != c.token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

// ----- HTTP handlers ---------------------------------------------------------

func (c *Coordinator) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var reg WorkerRegistration
	if err := json.NewDecoder(r.Body).Decode(&reg); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	c.Register(reg)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (c *Coordinator) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var reg WorkerRegistration
	if err := json.NewDecoder(r.Body).Decode(&reg); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	if w2, ok := c.workers[reg.WorkerID]; ok {
		w2.LastSeen = time.Now()
		w2.Status = "online"
	}
	c.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handlePoll is the hot path: a worker asks for a queued task matching its pools.
// The coordinator atomically claims the task (marks it running) and returns the
// full TaskAssignmentDTO. Returns 204 when there is no matching work.
func (c *Coordinator) handlePoll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req PollRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if c.getDAG == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	poolSet := make(map[string]bool, len(req.Pools))
	for _, p := range req.Pools {
		poolSet[p] = true
	}

	// Enforce MaxTasks: check how many tasks this worker is currently running.
	c.mu.RLock()
	wi, workerKnown := c.workers[req.WorkerID]
	var activeTasks, maxTasks int
	if workerKnown {
		activeTasks = wi.ActiveTasks
		maxTasks = wi.MaxTasks
	}
	c.mu.RUnlock()

	if workerKnown && maxTasks > 0 && activeTasks >= maxTasks {
		// Worker is at capacity — tell it to come back later.
		w.WriteHeader(http.StatusNoContent)
		return
	}

	queued, err := c.store.GetQueuedTasks()
	if err != nil {
		http.Error(w, "store error", http.StatusInternalServerError)
		return
	}

	for _, ti := range queued {
		run, err := c.store.GetDagRun(ti.RunID)
		if err != nil {
			continue
		}
		dag, ok := c.getDAG(run.DAGID)
		if !ok {
			continue
		}

		assignment, err := worker.PrepareTaskAssignment(run, ti, dag)
		if err != nil {
			continue
		}

		// Determine the task's pool.
		taskPool := "default"
		baseTaskID := ti.TaskID
		if idx := strings.Index(baseTaskID, "["); idx != -1 {
			baseTaskID = baseTaskID[:idx]
		}
		for i := range dag.Tasks {
			if dag.Tasks[i].ID == baseTaskID {
				if dag.Tasks[i].Pool != "" {
					taskPool = dag.Tasks[i].Pool
				}
				break
			}
		}

		if !poolSet[taskPool] {
			continue
		}

		// Claim the task by marking it running before returning.
		if err := c.store.UpdateTaskInstanceStatus(ti.ID, models.TaskRunning); err != nil {
			continue
		}

		// Increment the active-task counter for this worker.
		c.mu.Lock()
		if ww, ok := c.workers[req.WorkerID]; ok {
			ww.ActiveTasks++
		}
		c.mu.Unlock()

		// Record the dispatch time as the task's started_at so metrics have a
		// valid start timestamp even when the worker reports them later.
		c.store.SetTaskStartedAt(ti.ID, time.Now())

		dto := TaskAssignmentDTO{
			TaskInstanceID: assignment.TaskInstanceID,
			RunID:          assignment.RunID,
			Command:        assignment.Command,
			Env:            assignment.Env,
			TimeoutSecs:    assignment.TimeoutSecs,
			Retries:        assignment.Retries,
			Attempt:        assignment.Attempt,
			Image:          assignment.Image,
			Workdir:        assignment.Workdir,
			Volumes:        assignment.Volumes,
			Resources:      assignment.Resources,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(dto)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (c *Coordinator) handleResult(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var res TaskResult
	if err := json.NewDecoder(r.Body).Decode(&res); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}

	var status models.TaskStatus
	switch res.Status {
	case "success":
		status = models.TaskSuccess
	case "failed":
		status = models.TaskFailed
	case "cancelled":
		status = models.TaskCancelled
	case "up_for_retry":
		status = models.TaskUpForRetry
	default:
		http.Error(w, "unknown status", http.StatusBadRequest)
		return
	}

	if err := c.store.UpdateTaskInstanceStatusAndOutput(res.TaskInstanceID, status, res.Output); err != nil {
		http.Error(w, "store error", http.StatusInternalServerError)
		return
	}

	// Decrement the active-task counter for the worker that reported this result.
	if res.WorkerID != "" {
		c.mu.Lock()
		if ww, ok := c.workers[res.WorkerID]; ok && ww.ActiveTasks > 0 {
			ww.ActiveTasks--
		}
		c.mu.Unlock()
	}

	// Persist resource metrics reported by the remote worker.
	// We look up the task instance to get dag_id / task_id which the worker
	// does not carry in its result payload.
	if res.DurationMs > 0 || res.PeakMemoryBytes > 0 {
		if ti, err := c.store.GetTaskInstance(res.TaskInstanceID); err == nil {
			run, runErr := c.store.GetDagRun(ti.RunID)
			dagID := ""
			if runErr == nil {
				dagID = run.DAGID
			}
			m := &models.TaskMetrics{
				TaskInstanceID:  res.TaskInstanceID,
				RunID:           ti.RunID,
				DAGID:           dagID,
				TaskID:          ti.TaskID,
				DurationMs:      res.DurationMs,
				CpuUserMs:       res.CpuUserMs,
				CpuSystemMs:     res.CpuSystemMs,
				PeakMemoryBytes: res.PeakMemoryBytes,
				ExitCode:        res.ExitCode,
				ExecutorType:    res.ExecutorType,
				CreatedAt:       time.Now(),
			}
			if err := c.store.InsertTaskMetrics(m); err != nil {
				log.Printf("Cluster: warning: failed to persist metrics for task %s: %v", res.TaskInstanceID, err)
			}
		}
	}

	// Close the broker entry so SSE subscribers receive EOF.
	if c.broker != nil {
		c.broker.Close(res.TaskInstanceID)
		c.broker.Cleanup(res.TaskInstanceID)
	}

	log.Printf("Cluster: task %s reported %s by remote worker", res.TaskInstanceID, res.Status)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (c *Coordinator) handleLog(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var batch LogBatch
	if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}

	if c.broker != nil {
		for _, line := range batch.Lines {
			c.broker.Publish(batch.TaskInstanceID, line)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleTaskCancel handles GET /api/workers/tasks/{id}/cancel.
// Workers call this periodically to check whether the master has cancelled them.
func (c *Coordinator) handleTaskCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Path: /api/workers/tasks/{id}/cancel
	parts := strings.Split(strings.TrimSuffix(r.URL.Path, "/"), "/")
	if len(parts) < 5 {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	taskInstanceID := parts[len(parts)-2] // .../{id}/cancel

	inst, err := c.store.GetTaskInstance(taskInstanceID)
	if err != nil {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(CancelCheck{Cancel: inst.Status == models.TaskCancelled})
}

func (c *Coordinator) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	workers := c.ListWorkers()
	if workers == nil {
		workers = []WorkerInfo{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(workers)
}
