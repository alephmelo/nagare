package autoscaler

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/alephmelo/nagare/internal/config"
	"github.com/alephmelo/nagare/internal/models"
)

// StatusSnapshot is a point-in-time view of the autoscaler's state, suitable
// for serialising to the /api/autoscaler/status API endpoint.
type StatusSnapshot struct {
	Enabled         bool                                  `json:"enabled"`
	Provider        string                                `json:"provider"`
	CloudWorkers    int                                   `json:"cloud_workers"`
	MaxCloudWorkers int                                   `json:"max_cloud_workers"`
	Pools           map[string]PoolStats                  `json:"pools"`
	Instances       []WorkerInstance                      `json:"instances"`
	PerDAGOverrides map[string]models.DAGAutoscalerConfig `json:"per_dag_overrides,omitempty"`
}

// InstanceStore is the persistence interface the autoscaler uses to durably
// record cloud instance lifecycle events.  A SQLite-backed implementation is
// provided by models.Store; a fake is used in tests.
type InstanceStore interface {
	// SaveInstance persists a new WorkerInstance row.
	SaveInstance(inst WorkerInstance) error
	// UpdateInstanceStatus updates the status column for an existing row.
	UpdateInstanceStatus(id string, status InstanceStatus) error
	// UpdateInstanceWorkerID stores the nagare worker ID that the spawned
	// process registered with after booting.
	UpdateInstanceWorkerID(id, workerID string) error
	// ListActiveInstances returns all instances that are not yet terminated.
	// Used on master startup to reconcile orphaned cloud resources.
	ListActiveInstances() ([]WorkerInstance, error)
	// TerminateInstance marks an instance as terminated.
	TerminateInstance(id string, terminatedAt time.Time) error
}

// StatsSource provides current queue-depth and worker-utilisation data.
// The Coordinator implements this interface.
type StatsSource interface {
	// PoolStats returns a map of pool name → current utilisation snapshot.
	PoolStats() map[string]PoolStats
}

// Autoscaler monitors worker pool queue depths and automatically provisions
// (scales up) or terminates (scales down) cloud workers in response to load.
//
// Scale-up logic (per pool, per tick):
//  1. QueuedTasks > effective ScaleUpThreshold (global or per-DAG minimum)
//  2. Total cloud instances < effective MaxCloudWorkers (global or per-DAG maximum)
//  3. Time since last scale-up for this pool > CooldownSecs
//
// Scale-down logic (per cloud instance, per tick):
//  1. Instance is in InstanceRunning state
//  2. The pool it serves has 0 queued tasks
//  3. The instance has been idle for at least ScaleDownIdleMins
//
// The Autoscaler never terminates locally-configured workers — only instances
// it has provisioned itself (tracked in a.instances).
type Autoscaler struct {
	cfg           config.AutoscalerConfig
	provider      CloudProvider
	statsSource   StatsSource
	instanceStore InstanceStore

	// activeDAGs returns the current set of loaded DAG definitions. The
	// autoscaler reads their Autoscaler overrides on every tick to apply
	// per-DAG scale-up thresholds and max-worker caps using
	// "most permissive wins" semantics. A nil func is treated as no overrides.
	activeDAGs func() map[string]*models.DAGDef

	mu          sync.RWMutex
	instances   map[string]*WorkerInstance // keyed by Nagare instance ID
	lastScaleUp map[string]time.Time       // pool name → last scale-up time
	idleSince   map[string]time.Time       // instance ID → first time seen idle
}

// New creates an Autoscaler.  Call Run to start the background tick loop.
//
// provider must not be nil when cfg.Enabled is true.
// statsSource must always be non-nil.
// instanceStore must always be non-nil.
// activeDAGs may be nil (treated as no per-DAG overrides).
func New(cfg config.AutoscalerConfig, provider CloudProvider, statsSource StatsSource, instanceStore InstanceStore, activeDAGs func() map[string]*models.DAGDef) *Autoscaler {
	return &Autoscaler{
		cfg:           cfg,
		provider:      provider,
		statsSource:   statsSource,
		instanceStore: instanceStore,
		activeDAGs:    activeDAGs,
		instances:     make(map[string]*WorkerInstance),
		lastScaleUp:   make(map[string]time.Time),
		idleSince:     make(map[string]time.Time),
	}
}

// NewProvider is a factory that returns the CloudProvider implementation
// corresponding to cfg.Provider.  Returns an error when the provider name is
// unsupported or when the underlying client cannot be initialised.
func NewProvider(cfg config.AutoscalerConfig) (CloudProvider, error) {
	switch cfg.Provider {
	case "docker":
		return NewDockerProvider(cfg.Docker)
	case "aws":
		return NewAWSProvider(cfg.AWS)
	default:
		return nil, fmt.Errorf("autoscaler: unknown provider %q (supported: docker, aws)", cfg.Provider)
	}
}

// Run starts the autoscaler background loop.  It ticks every tickInterval and
// blocks until ctx is cancelled.  masterAddr and token are passed through to
// SpinUpRequest so the spawned workers know how to register.
//
// Call Run in a goroutine from main.go.
func (a *Autoscaler) Run(ctx context.Context, tickInterval time.Duration, masterAddr, token string) {
	if !a.cfg.Enabled {
		log.Println("Autoscaler: disabled — no cloud workers will be provisioned")
		return
	}

	// Reconcile any instances left over from a previous master run.
	a.reconcile(ctx)

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	log.Printf("Autoscaler: started (provider=%s, max=%d, threshold=%d, cooldown=%ds)",
		a.cfg.Provider, a.cfg.MaxCloudWorkers, a.cfg.ScaleUpThreshold, a.cfg.CooldownSecs)

	for {
		select {
		case <-ctx.Done():
			log.Println("Autoscaler: stopping")
			return
		case <-ticker.C:
			a.tick(ctx, masterAddr, token)
		}
	}
}

// tick is the core scaling decision loop.  It is exported for testing but
// callers should normally use Run.
func (a *Autoscaler) tick(ctx context.Context, masterAddr, token string) {
	if !a.cfg.Enabled {
		return
	}

	poolStats := a.statsSource.PoolStats()

	a.evaluateScaleUp(ctx, poolStats, masterAddr, token)
	a.evaluateScaleDown(ctx, poolStats)
}

// effectiveScaleParams returns the scale-up threshold and max-cloud-workers
// cap to use for a given tick, merging global config with any per-DAG
// overrides. Zero values in a DAGAutoscalerConfig mean "not set / use global".
//
// Resolution rules ("most permissive wins among overrides"):
//   - threshold: min(global, min of all non-zero per-DAG thresholds)
//     → the most eager DAG lowers the bar for everyone
//   - maxWorkers: min(global, max of all non-zero per-DAG maxWorkers)
//     → per-DAG overrides can only constrain below the global cap;
//     among multiple per-DAG values the largest (most permissive) wins
func (a *Autoscaler) effectiveScaleParams() (threshold, maxWorkers int) {
	threshold = a.cfg.ScaleUpThreshold
	maxWorkers = a.cfg.MaxCloudWorkers

	if a.activeDAGs == nil {
		return
	}

	bestThreshold := threshold // lowest non-zero DAG threshold seen
	bestMax := 0               // highest non-zero DAG max seen (0 = none set)

	for _, dag := range a.activeDAGs() {
		if dag.Autoscaler == nil {
			continue
		}
		if t := dag.Autoscaler.ScaleUpThreshold; t > 0 && t < bestThreshold {
			bestThreshold = t
		}
		if m := dag.Autoscaler.MaxCloudWorkers; m > 0 && m > bestMax {
			bestMax = m
		}
	}

	threshold = bestThreshold
	if bestMax > 0 {
		// Clamp to global ceiling.
		if bestMax < maxWorkers {
			maxWorkers = bestMax
		}
	}
	return
}

// evaluateScaleUp spins up a new worker for any pool whose queue depth exceeds
// the threshold, subject to the global cap and per-pool cooldown.
func (a *Autoscaler) evaluateScaleUp(ctx context.Context, poolStats map[string]PoolStats, masterAddr, token string) {
	threshold, maxWorkers := a.effectiveScaleParams()

	a.mu.RLock()
	cloudCount := len(a.instances)
	a.mu.RUnlock()

	for pool, stats := range poolStats {
		if stats.QueuedTasks <= threshold {
			continue
		}

		if cloudCount >= maxWorkers {
			log.Printf("Autoscaler: pool %s needs workers but cloud cap (%d) reached", pool, maxWorkers)
			continue
		}

		// Per-pool cooldown check.
		a.mu.RLock()
		lastUp, hasLastUp := a.lastScaleUp[pool]
		a.mu.RUnlock()
		if hasLastUp && time.Since(lastUp) < time.Duration(a.cfg.CooldownSecs)*time.Second {
			log.Printf("Autoscaler: pool %s in cooldown (%.0fs remaining)",
				pool, (time.Duration(a.cfg.CooldownSecs)*time.Second - time.Since(lastUp)).Seconds())
			continue
		}

		instanceID := generateInstanceID(a.provider.Name())
		req := SpinUpRequest{
			InstanceID: instanceID,
			Pools:      []string{pool},
			MasterAddr: masterAddr,
			Token:      token,
			NeedsGPU:   stats.NeedsGPU,
		}

		log.Printf("Autoscaler: scaling up pool %s (queued=%d, threshold=%d) → instance %s",
			pool, stats.QueuedTasks, threshold, instanceID)

		inst, err := a.provider.SpinUp(ctx, req)
		if err != nil {
			log.Printf("Autoscaler: SpinUp failed for pool %s: %v", pool, err)
			continue
		}

		// Track the new instance.
		a.mu.Lock()
		a.instances[inst.ID] = &inst
		a.lastScaleUp[pool] = time.Now()
		cloudCount++ // update local count
		a.mu.Unlock()

		// Persist to the database for crash recovery.
		if err := a.instanceStore.SaveInstance(inst); err != nil {
			log.Printf("Autoscaler: warning: failed to persist instance %s: %v", inst.ID, err)
		}
	}
}

// evaluateScaleDown terminates cloud workers that have been idle long enough.
func (a *Autoscaler) evaluateScaleDown(ctx context.Context, poolStats map[string]PoolStats) {
	idleThreshold := time.Duration(a.cfg.ScaleDownIdleMins) * time.Minute

	a.mu.Lock()
	defer a.mu.Unlock()

	for id, inst := range a.instances {
		if inst.Status != InstanceRunning {
			// Don't terminate instances that are still booting.
			continue
		}

		// Check if any of the pools this worker serves have queued work.
		workerHasWork := false
		for _, pool := range inst.Pools {
			if s, ok := poolStats[pool]; ok && s.QueuedTasks > 0 {
				workerHasWork = true
				break
			}
		}

		if workerHasWork {
			// Worker is needed — reset idle timer.
			delete(a.idleSince, id)
			continue
		}

		// Worker has no queued work.
		if _, seen := a.idleSince[id]; !seen {
			a.idleSince[id] = time.Now()
		}

		if time.Since(a.idleSince[id]) < idleThreshold {
			continue
		}

		log.Printf("Autoscaler: scaling down idle instance %s (provider=%s, idle=%s)",
			id, inst.ProviderID, time.Since(a.idleSince[id]).Round(time.Second))

		if err := a.provider.SpinDown(ctx, inst.ProviderID); err != nil {
			log.Printf("Autoscaler: SpinDown failed for instance %s: %v", id, err)
			continue
		}

		now := time.Now()
		if err := a.instanceStore.TerminateInstance(id, now); err != nil {
			log.Printf("Autoscaler: warning: failed to mark instance %s terminated: %v", id, err)
		}

		delete(a.instances, id)
		delete(a.idleSince, id)
	}
}

// TryClaimWorker attempts to correlate a newly-registered remote worker with
// one of the autoscaler's provisioning instances by matching pool membership.
// It returns the matched instance ID and true on success, or ("", false) when
// no provisioning instance for the given pools is found.
//
// The first matching provisioning instance is returned (FIFO order is not
// guaranteed for concurrent registrations).  This is a best-effort heuristic
// that works correctly in the common case where workers register sequentially.
//
// If a match is found the instance's status is immediately updated to Running
// and its WorkerID is set.
func (a *Autoscaler) TryClaimWorker(nagareWorkerID string, pools []string) (instanceID string, ok bool) {
	if !a.cfg.Enabled {
		return "", false
	}

	poolSet := make(map[string]struct{}, len(pools))
	for _, p := range pools {
		poolSet[p] = struct{}{}
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	for id, inst := range a.instances {
		if inst.Status != InstanceProvisioning {
			continue
		}
		// Check if any of the instance's pools overlap with the worker's pools.
		for _, p := range inst.Pools {
			if _, match := poolSet[p]; match {
				inst.Status = InstanceRunning
				inst.WorkerID = nagareWorkerID
				if err := a.instanceStore.UpdateInstanceStatus(id, InstanceRunning); err != nil {
					log.Printf("Autoscaler: warning: failed to update status for %s: %v", id, err)
				}
				if err := a.instanceStore.UpdateInstanceWorkerID(id, nagareWorkerID); err != nil {
					log.Printf("Autoscaler: warning: failed to update worker ID for %s: %v", id, err)
				}
				return id, true
			}
		}
	}
	return "", false
}

// InstanceIDForWorker looks up the autoscaler instance ID for a given nagare
// worker ID.  Returns ("", false) if the worker is not cloud-managed.
func (a *Autoscaler) InstanceIDForWorker(nagareWorkerID string) (string, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for id, inst := range a.instances {
		if inst.WorkerID == nagareWorkerID {
			return id, true
		}
	}
	return "", false
}

// NotifyWorkerOnline is called by the Coordinator when a remote worker with
// the given nagareWorkerID successfully registers.  The autoscaler uses the
// workerID to correlate the remote worker with the cloud instance it
// provisioned.
func (a *Autoscaler) NotifyWorkerOnline(instanceID, nagareWorkerID string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	inst, ok := a.instances[instanceID]
	if !ok {
		return
	}

	inst.Status = InstanceRunning
	inst.WorkerID = nagareWorkerID

	if err := a.instanceStore.UpdateInstanceStatus(instanceID, InstanceRunning); err != nil {
		log.Printf("Autoscaler: warning: failed to update status for %s: %v", instanceID, err)
	}
	if err := a.instanceStore.UpdateInstanceWorkerID(instanceID, nagareWorkerID); err != nil {
		log.Printf("Autoscaler: warning: failed to update worker ID for %s: %v", instanceID, err)
	}
}

// NotifyWorkerOffline is called by the Coordinator's stale-worker expiry
// logic when a cloud-provisioned worker stops sending heartbeats.  The
// autoscaler terminates the underlying cloud resource so it does not leak.
func (a *Autoscaler) NotifyWorkerOffline(instanceID string) {
	a.mu.Lock()
	inst, ok := a.instances[instanceID]
	if !ok {
		a.mu.Unlock()
		return
	}
	providerID := inst.ProviderID
	delete(a.instances, instanceID)
	delete(a.idleSince, instanceID)
	a.mu.Unlock()

	log.Printf("Autoscaler: cloud worker %s went offline — terminating provider resource %s", instanceID, providerID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := a.provider.SpinDown(ctx, providerID); err != nil {
		log.Printf("Autoscaler: SpinDown on offline worker failed: %v", err)
	}
	if err := a.instanceStore.TerminateInstance(instanceID, time.Now()); err != nil {
		log.Printf("Autoscaler: warning: failed to mark offline instance %s terminated: %v", instanceID, err)
	}
}

// Status returns a point-in-time snapshot of the autoscaler's state.
// Safe to call concurrently.
func (a *Autoscaler) Status() StatusSnapshot {
	a.mu.RLock()
	defer a.mu.RUnlock()

	instances := make([]WorkerInstance, 0, len(a.instances))
	for _, inst := range a.instances {
		instances = append(instances, *inst)
	}

	providerName := a.cfg.Provider
	if a.provider != nil {
		providerName = a.provider.Name()
	}

	// Collect per-DAG overrides from active DAGs that have an autoscaler block.
	var perDAGOverrides map[string]models.DAGAutoscalerConfig
	if a.activeDAGs != nil {
		for id, dag := range a.activeDAGs() {
			if dag.Autoscaler == nil {
				continue
			}
			if perDAGOverrides == nil {
				perDAGOverrides = make(map[string]models.DAGAutoscalerConfig)
			}
			perDAGOverrides[id] = *dag.Autoscaler
		}
	}

	return StatusSnapshot{
		Enabled:         a.cfg.Enabled,
		Provider:        providerName,
		CloudWorkers:    len(a.instances),
		MaxCloudWorkers: a.cfg.MaxCloudWorkers,
		Pools:           a.statsSource.PoolStats(),
		Instances:       instances,
		PerDAGOverrides: perDAGOverrides,
	}
}

// reconcile queries the cloud provider for instances that were created during
// a previous master run and re-populates the in-memory map.  Called once on
// startup.
func (a *Autoscaler) reconcile(ctx context.Context) {
	// First try the persistent store.
	stored, err := a.instanceStore.ListActiveInstances()
	if err != nil {
		log.Printf("Autoscaler: reconcile: failed to load instances from store: %v", err)
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	for _, inst := range stored {
		cp := inst
		a.instances[inst.ID] = &cp
	}

	if len(stored) > 0 {
		log.Printf("Autoscaler: reconciled %d instances from previous run", len(stored))
	}
}

// generateInstanceID returns a short unique identifier for a new cloud
// instance.  Format: "<provider>-<6-char-hex-timestamp>".
func generateInstanceID(provider string) string {
	// Use nanosecond timestamp encoded as hex for a collision-resistant suffix
	// without importing crypto/rand (keeps it fast and testable).
	suffix := fmt.Sprintf("%06x", time.Now().UnixNano()&0xFFFFFF)
	return fmt.Sprintf("%s-%s", provider, suffix)
}
