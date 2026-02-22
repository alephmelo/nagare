package autoscaler

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/config"
	"github.com/alephmelo/nagare/internal/models"
)

// ---- Fake provider used across autoscaler engine tests ----

type fakeProvider struct {
	mu          sync.Mutex
	spunUp      []WorkerInstance
	spunDown    []string // provider IDs
	listed      []WorkerInstance
	spinUpErr   error
	spinDownErr error
	listErr     error
}

func (f *fakeProvider) Name() string { return "fake" }

func (f *fakeProvider) SpinUp(_ context.Context, req SpinUpRequest) (WorkerInstance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.spinUpErr != nil {
		return WorkerInstance{}, f.spinUpErr
	}
	inst := WorkerInstance{
		ID:         req.InstanceID,
		ProviderID: "fake-" + req.InstanceID,
		Pools:      req.Pools,
		Status:     InstanceProvisioning,
		CreatedAt:  time.Now(),
	}
	f.spunUp = append(f.spunUp, inst)
	return inst, nil
}

func (f *fakeProvider) SpinDown(_ context.Context, providerID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.spinDownErr != nil {
		return f.spinDownErr
	}
	f.spunDown = append(f.spunDown, providerID)
	return nil
}

func (f *fakeProvider) List(_ context.Context) ([]WorkerInstance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.listed, nil
}

func (f *fakeProvider) spunUpCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.spunUp)
}

func (f *fakeProvider) spunDownCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.spunDown)
}

// ---- Fake store that returns configurable pool stats ----

type fakeStatsSource struct {
	mu    sync.Mutex
	stats map[string]PoolStats
}

func (f *fakeStatsSource) PoolStats() map[string]PoolStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]PoolStats, len(f.stats))
	for k, v := range f.stats {
		out[k] = v
	}
	return out
}

func (f *fakeStatsSource) WorkerActiveTasks(_ string) int { return 0 }

// ---- Fake instance store (cloud_instances table substitute) ----

type fakeInstanceStore struct {
	mu        sync.Mutex
	instances map[string]*WorkerInstance
}

func newFakeInstanceStore() *fakeInstanceStore {
	return &fakeInstanceStore{instances: make(map[string]*WorkerInstance)}
}

func (f *fakeInstanceStore) SaveInstance(inst WorkerInstance) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := inst
	f.instances[inst.ID] = &cp
	return nil
}

func (f *fakeInstanceStore) UpdateInstanceStatus(id string, status InstanceStatus) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if inst, ok := f.instances[id]; ok {
		inst.Status = status
	}
	return nil
}

func (f *fakeInstanceStore) UpdateInstanceWorkerID(id, workerID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if inst, ok := f.instances[id]; ok {
		inst.WorkerID = workerID
	}
	return nil
}

func (f *fakeInstanceStore) ListActiveInstances() ([]WorkerInstance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []WorkerInstance
	for _, inst := range f.instances {
		if inst.Status != InstanceTerminated {
			out = append(out, *inst)
		}
	}
	return out, nil
}

func (f *fakeInstanceStore) TerminateInstance(id string, terminatedAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if inst, ok := f.instances[id]; ok {
		inst.Status = InstanceTerminated
		inst.TerminatedAt = terminatedAt
	}
	return nil
}

func (f *fakeInstanceStore) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.instances)
}

// ---- Helper: build an Autoscaler with fake dependencies ----

func newTestAutoscaler(cfg config.AutoscalerConfig, provider CloudProvider, stats *fakeStatsSource) (*Autoscaler, *fakeInstanceStore) {
	istore := newFakeInstanceStore()
	a := &Autoscaler{
		cfg:           cfg,
		provider:      provider,
		statsSource:   stats,
		instanceStore: istore,
		activeDAGs:    nil,
		instances:     make(map[string]*WorkerInstance),
		lastScaleUp:   make(map[string]time.Time),
		idleSince:     make(map[string]time.Time),
	}
	return a, istore
}

// ---- Tests ----

func TestAutoscaler_NoScaleUpWhenDisabled(t *testing.T) {
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: map[string]PoolStats{
		"default": {Pool: "default", QueuedTasks: 10},
	}}
	cfg := config.AutoscalerConfig{
		Enabled:          false,
		MaxCloudWorkers:  5,
		ScaleUpThreshold: 3,
	}
	a, _ := newTestAutoscaler(cfg, provider, stats)
	a.tick(context.Background(), "http://master:8080", "")

	if provider.spunUpCount() != 0 {
		t.Errorf("expected 0 SpinUp calls when disabled, got %d", provider.spunUpCount())
	}
}

func TestAutoscaler_ScalesUpWhenQueueExceedsThreshold(t *testing.T) {
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: map[string]PoolStats{
		"default": {Pool: "default", QueuedTasks: 5, ActiveWorkers: 0},
	}}
	cfg := config.AutoscalerConfig{
		Enabled:          true,
		MaxCloudWorkers:  5,
		ScaleUpThreshold: 3,
		CooldownSecs:     0, // disable cooldown for tests
	}
	a, istore := newTestAutoscaler(cfg, provider, stats)
	a.tick(context.Background(), "http://master:8080", "")

	if provider.spunUpCount() != 1 {
		t.Errorf("expected 1 SpinUp call, got %d", provider.spunUpCount())
	}
	if istore.count() != 1 {
		t.Errorf("expected instance saved in store, got %d", istore.count())
	}
}

func TestAutoscaler_NoScaleUpBelowThreshold(t *testing.T) {
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: map[string]PoolStats{
		"default": {Pool: "default", QueuedTasks: 2, ActiveWorkers: 1},
	}}
	cfg := config.AutoscalerConfig{
		Enabled:          true,
		MaxCloudWorkers:  5,
		ScaleUpThreshold: 3,
		CooldownSecs:     0,
	}
	a, _ := newTestAutoscaler(cfg, provider, stats)
	a.tick(context.Background(), "http://master:8080", "")

	if provider.spunUpCount() != 0 {
		t.Errorf("expected 0 SpinUp calls below threshold, got %d", provider.spunUpCount())
	}
}

func TestAutoscaler_RespectsMaxCloudWorkers(t *testing.T) {
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: map[string]PoolStats{
		"default": {Pool: "default", QueuedTasks: 10, ActiveWorkers: 0, CloudWorkers: 5},
	}}
	cfg := config.AutoscalerConfig{
		Enabled:          true,
		MaxCloudWorkers:  5, // cap already reached
		ScaleUpThreshold: 3,
		CooldownSecs:     0,
	}
	a, _ := newTestAutoscaler(cfg, provider, stats)
	// Pre-fill instances to hit the cap.
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("inst-%d", i)
		inst := WorkerInstance{ID: id, ProviderID: "fake-" + id, Status: InstanceRunning}
		a.instances[id] = &inst
	}

	a.tick(context.Background(), "http://master:8080", "")

	if provider.spunUpCount() != 0 {
		t.Errorf("expected 0 SpinUp calls when at cap, got %d", provider.spunUpCount())
	}
}

func TestAutoscaler_CooldownPreventsRapidScaleUp(t *testing.T) {
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: map[string]PoolStats{
		"default": {Pool: "default", QueuedTasks: 10, ActiveWorkers: 0},
	}}
	cfg := config.AutoscalerConfig{
		Enabled:          true,
		MaxCloudWorkers:  10,
		ScaleUpThreshold: 3,
		CooldownSecs:     60, // 60-second cooldown
	}
	a, _ := newTestAutoscaler(cfg, provider, stats)

	// First tick — should scale up.
	a.tick(context.Background(), "http://master:8080", "")
	if provider.spunUpCount() != 1 {
		t.Fatalf("expected 1 SpinUp on first tick, got %d", provider.spunUpCount())
	}

	// Second tick immediately — cooldown should block further scale-up.
	a.tick(context.Background(), "http://master:8080", "")
	if provider.spunUpCount() != 1 {
		t.Errorf("expected still 1 SpinUp after cooldown block, got %d", provider.spunUpCount())
	}
}

func TestAutoscaler_CooldownExpiryAllowsNextScaleUp(t *testing.T) {
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: map[string]PoolStats{
		"default": {Pool: "default", QueuedTasks: 10, ActiveWorkers: 0},
	}}
	cfg := config.AutoscalerConfig{
		Enabled:          true,
		MaxCloudWorkers:  10,
		ScaleUpThreshold: 3,
		CooldownSecs:     1, // very short
	}
	a, _ := newTestAutoscaler(cfg, provider, stats)

	a.tick(context.Background(), "http://master:8080", "")
	if provider.spunUpCount() != 1 {
		t.Fatalf("expected 1 SpinUp on first tick")
	}

	// Manually back-date the last scale-up to simulate cooldown expiry.
	a.mu.Lock()
	a.lastScaleUp["default"] = time.Now().Add(-2 * time.Second)
	a.mu.Unlock()

	a.tick(context.Background(), "http://master:8080", "")
	if provider.spunUpCount() != 2 {
		t.Errorf("expected 2 SpinUp after cooldown expired, got %d", provider.spunUpCount())
	}
}

func TestAutoscaler_ScalesDownIdleInstance(t *testing.T) {
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: map[string]PoolStats{
		"default": {Pool: "default", QueuedTasks: 0, ActiveWorkers: 1, CloudWorkers: 1},
	}}
	cfg := config.AutoscalerConfig{
		Enabled:           true,
		MaxCloudWorkers:   5,
		ScaleUpThreshold:  3,
		ScaleDownIdleMins: 0, // immediate for testing
		CooldownSecs:      0,
	}
	a, istore := newTestAutoscaler(cfg, provider, stats)

	// Pre-register a cloud instance that has been idle for 10 minutes.
	inst := &WorkerInstance{
		ID:         "idle-inst",
		ProviderID: "fake-idle",
		Pools:      []string{"default"},
		Status:     InstanceRunning,
		CreatedAt:  time.Now().Add(-15 * time.Minute),
	}
	a.instances["idle-inst"] = inst
	istore.SaveInstance(*inst) //nolint:errcheck

	// Record idle start far in the past.
	a.mu.Lock()
	a.idleSince = map[string]time.Time{
		"idle-inst": time.Now().Add(-15 * time.Minute),
	}
	a.mu.Unlock()

	a.tick(context.Background(), "http://master:8080", "")

	if provider.spunDownCount() != 1 {
		t.Errorf("expected 1 SpinDown call, got %d", provider.spunDownCount())
	}
	if provider.spunDown[0] != "fake-idle" {
		t.Errorf("expected SpinDown for provider ID %q, got %q", "fake-idle", provider.spunDown[0])
	}
}

func TestAutoscaler_DoesNotScaleDownLocalWorkers(t *testing.T) {
	// Local workers (not in a.instances) must never be terminated.
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: map[string]PoolStats{
		"default": {Pool: "default", QueuedTasks: 0, ActiveWorkers: 4, CloudWorkers: 0},
	}}
	cfg := config.AutoscalerConfig{
		Enabled:           true,
		MaxCloudWorkers:   5,
		ScaleDownIdleMins: 0,
		CooldownSecs:      0,
	}
	a, _ := newTestAutoscaler(cfg, provider, stats)
	// No entries in a.instances → all workers are local.

	a.tick(context.Background(), "http://master:8080", "")

	if provider.spunDownCount() != 0 {
		t.Errorf("expected 0 SpinDown for local workers, got %d", provider.spunDownCount())
	}
}

func TestAutoscaler_MultiplePoolsScaleIndependently(t *testing.T) {
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: map[string]PoolStats{
		"default":     {Pool: "default", QueuedTasks: 5},     // above threshold
		"gpu_workers": {Pool: "gpu_workers", QueuedTasks: 1}, // below threshold
	}}
	cfg := config.AutoscalerConfig{
		Enabled:          true,
		MaxCloudWorkers:  10,
		ScaleUpThreshold: 3,
		CooldownSecs:     0,
	}
	a, _ := newTestAutoscaler(cfg, provider, stats)
	a.tick(context.Background(), "http://master:8080", "")

	// Only "default" should have triggered a scale-up.
	if provider.spunUpCount() != 1 {
		t.Errorf("expected 1 SpinUp (only for default pool), got %d", provider.spunUpCount())
	}
	spunPools := provider.spunUp[0].Pools
	if len(spunPools) != 1 || spunPools[0] != "default" {
		t.Errorf("expected worker spun up for default pool, got %v", spunPools)
	}
}

func TestAutoscaler_SpinUpErrorIsLogged(t *testing.T) {
	// SpinUp errors must not crash the tick loop.
	provider := &fakeProvider{spinUpErr: fmt.Errorf("cloud unavailable")}
	stats := &fakeStatsSource{stats: map[string]PoolStats{
		"default": {Pool: "default", QueuedTasks: 10},
	}}
	cfg := config.AutoscalerConfig{
		Enabled:          true,
		MaxCloudWorkers:  5,
		ScaleUpThreshold: 3,
		CooldownSecs:     0,
	}
	a, _ := newTestAutoscaler(cfg, provider, stats)

	// Must not panic.
	a.tick(context.Background(), "http://master:8080", "")
}

func TestAutoscaler_NotifyWorkerOnline_UpdatesStatus(t *testing.T) {
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: make(map[string]PoolStats)}
	cfg := config.AutoscalerConfig{Enabled: true, MaxCloudWorkers: 5}
	a, istore := newTestAutoscaler(cfg, provider, stats)

	inst := &WorkerInstance{
		ID:         "inst-online",
		ProviderID: "fake-online",
		Pools:      []string{"default"},
		Status:     InstanceProvisioning,
	}
	a.instances["inst-online"] = inst
	istore.SaveInstance(*inst) //nolint:errcheck

	a.NotifyWorkerOnline("inst-online", "worker-xyz")

	a.mu.RLock()
	updated := a.instances["inst-online"]
	a.mu.RUnlock()

	if updated.Status != InstanceRunning {
		t.Errorf("expected status Running after NotifyWorkerOnline, got %q", updated.Status)
	}
	if updated.WorkerID != "worker-xyz" {
		t.Errorf("expected WorkerID worker-xyz, got %q", updated.WorkerID)
	}
}

func TestAutoscaler_NotifyWorkerOffline_TriggersSpinDown(t *testing.T) {
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: make(map[string]PoolStats)}
	cfg := config.AutoscalerConfig{Enabled: true, MaxCloudWorkers: 5}
	a, istore := newTestAutoscaler(cfg, provider, stats)

	inst := &WorkerInstance{
		ID:         "inst-dead",
		ProviderID: "fake-dead",
		Pools:      []string{"default"},
		Status:     InstanceRunning,
	}
	a.instances["inst-dead"] = inst
	istore.SaveInstance(*inst) //nolint:errcheck

	a.NotifyWorkerOffline("inst-dead")

	if provider.spunDownCount() != 1 {
		t.Errorf("expected 1 SpinDown after offline notification, got %d", provider.spunDownCount())
	}

	a.mu.RLock()
	_, stillTracked := a.instances["inst-dead"]
	a.mu.RUnlock()
	if stillTracked {
		t.Error("terminated instance should be removed from in-memory map")
	}
}

func TestAutoscaler_Status(t *testing.T) {
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: map[string]PoolStats{
		"default": {Pool: "default", QueuedTasks: 2, ActiveWorkers: 1},
	}}
	cfg := config.AutoscalerConfig{
		Enabled:          true,
		MaxCloudWorkers:  5,
		ScaleUpThreshold: 3,
		Provider:         "fake",
	}
	a, _ := newTestAutoscaler(cfg, provider, stats)

	status := a.Status()
	if !status.Enabled {
		t.Error("expected Enabled=true in status")
	}
	if status.Provider != "fake" {
		t.Errorf("expected provider %q, got %q", "fake", status.Provider)
	}
	if status.CloudWorkers != 0 {
		t.Errorf("expected 0 cloud workers, got %d", status.CloudWorkers)
	}
	if status.MaxCloudWorkers != 5 {
		t.Errorf("expected MaxCloudWorkers=5, got %d", status.MaxCloudWorkers)
	}
	if len(status.Pools) != 1 {
		t.Errorf("expected 1 pool in status, got %d", len(status.Pools))
	}
}

func TestAutoscaler_NewProvider_Docker(t *testing.T) {
	// NewProvider should return a DockerProvider when provider=="docker".
	// We can't spin up a real container in unit tests, but we can verify
	// that the factory returns the right type without error when Docker
	// is available, or an appropriate error otherwise.
	cfg := config.AutoscalerConfig{
		Provider: "docker",
		Docker:   config.DockerProviderConfig{Image: "nagare:latest"},
	}
	p, err := NewProvider(cfg)
	if err != nil {
		// Docker may not be available in CI — skip but don't fail.
		t.Skipf("Docker not available: %v", err)
	}
	if p.Name() != "docker" {
		t.Errorf("expected provider name %q, got %q", "docker", p.Name())
	}
}

func TestAutoscaler_NewProvider_Unknown(t *testing.T) {
	cfg := config.AutoscalerConfig{Provider: "nonexistent"}
	_, err := NewProvider(cfg)
	if err == nil {
		t.Error("expected error for unknown provider")
	}
	if !strings.Contains(err.Error(), "nonexistent") {
		t.Errorf("expected error to mention provider name, got: %v", err)
	}
}

// ---- TryClaimWorker tests ----

func TestAutoscaler_TryClaimWorker_MatchByPool(t *testing.T) {
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: make(map[string]PoolStats)}
	cfg := config.AutoscalerConfig{Enabled: true, MaxCloudWorkers: 5}
	a, istore := newTestAutoscaler(cfg, provider, stats)

	// Seed a provisioning instance for pool "default".
	inst := &WorkerInstance{
		ID:         "docker-abc001",
		ProviderID: "container-abc001",
		Pools:      []string{"default"},
		Status:     InstanceProvisioning,
	}
	a.instances[inst.ID] = inst
	istore.SaveInstance(*inst) //nolint:errcheck

	instanceID, ok := a.TryClaimWorker("worker-001", []string{"default"})
	if !ok {
		t.Fatal("expected TryClaimWorker to match a provisioning instance")
	}
	if instanceID != inst.ID {
		t.Errorf("expected instance ID %q, got %q", inst.ID, instanceID)
	}

	// Instance status must now be Running.
	a.mu.RLock()
	gotStatus := a.instances[inst.ID].Status
	gotWorkerID := a.instances[inst.ID].WorkerID
	a.mu.RUnlock()

	if gotStatus != InstanceRunning {
		t.Errorf("expected status Running after claim, got %q", gotStatus)
	}
	if gotWorkerID != "worker-001" {
		t.Errorf("expected WorkerID worker-001 after claim, got %q", gotWorkerID)
	}
}

func TestAutoscaler_TryClaimWorker_NoMatchWhenDisabled(t *testing.T) {
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: make(map[string]PoolStats)}
	cfg := config.AutoscalerConfig{Enabled: false, MaxCloudWorkers: 5}
	a, _ := newTestAutoscaler(cfg, provider, stats)

	// Even with a provisioning instance present, disabled autoscaler must never claim.
	inst := &WorkerInstance{
		ID:     "docker-disabled",
		Pools:  []string{"default"},
		Status: InstanceProvisioning,
	}
	a.instances[inst.ID] = inst

	_, ok := a.TryClaimWorker("worker-x", []string{"default"})
	if ok {
		t.Error("expected TryClaimWorker to return false when autoscaler is disabled")
	}
}

func TestAutoscaler_TryClaimWorker_NoMatchWhenPoolDiffers(t *testing.T) {
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: make(map[string]PoolStats)}
	cfg := config.AutoscalerConfig{Enabled: true, MaxCloudWorkers: 5}
	a, _ := newTestAutoscaler(cfg, provider, stats)

	// Provisioning instance is for "gpu", worker registers for "default".
	inst := &WorkerInstance{
		ID:     "docker-gpu001",
		Pools:  []string{"gpu"},
		Status: InstanceProvisioning,
	}
	a.instances[inst.ID] = inst

	_, ok := a.TryClaimWorker("worker-default", []string{"default"})
	if ok {
		t.Error("expected no match when worker pools don't overlap with provisioning instance")
	}
}

func TestAutoscaler_TryClaimWorker_DoesNotClaimRunningInstance(t *testing.T) {
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: make(map[string]PoolStats)}
	cfg := config.AutoscalerConfig{Enabled: true, MaxCloudWorkers: 5}
	a, _ := newTestAutoscaler(cfg, provider, stats)

	// Instance is already Running — should not be claimed again.
	inst := &WorkerInstance{
		ID:       "docker-already-running",
		Pools:    []string{"default"},
		Status:   InstanceRunning,
		WorkerID: "existing-worker",
	}
	a.instances[inst.ID] = inst

	_, ok := a.TryClaimWorker("worker-new", []string{"default"})
	if ok {
		t.Error("expected no match for an already-Running instance")
	}
}

func TestAutoscaler_TryClaimWorker_OverlapOnOneOfManyPools(t *testing.T) {
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: make(map[string]PoolStats)}
	cfg := config.AutoscalerConfig{Enabled: true, MaxCloudWorkers: 5}
	a, istore := newTestAutoscaler(cfg, provider, stats)

	// Provisioning instance handles ["default", "etl"].
	inst := &WorkerInstance{
		ID:     "docker-multi001",
		Pools:  []string{"default", "etl"},
		Status: InstanceProvisioning,
	}
	a.instances[inst.ID] = inst
	istore.SaveInstance(*inst) //nolint:errcheck

	// Worker only registers for "etl" — the overlap is sufficient to claim.
	instanceID, ok := a.TryClaimWorker("worker-etl", []string{"etl"})
	if !ok {
		t.Fatal("expected TryClaimWorker to match on pool overlap")
	}
	if instanceID != inst.ID {
		t.Errorf("expected instance ID %q, got %q", inst.ID, instanceID)
	}
}

func TestAutoscaler_InstanceIDForWorker_Found(t *testing.T) {
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: make(map[string]PoolStats)}
	cfg := config.AutoscalerConfig{Enabled: true, MaxCloudWorkers: 5}
	a, _ := newTestAutoscaler(cfg, provider, stats)

	inst := &WorkerInstance{
		ID:       "docker-idfind",
		Pools:    []string{"default"},
		Status:   InstanceRunning,
		WorkerID: "worker-lookup",
	}
	a.instances[inst.ID] = inst

	id, ok := a.InstanceIDForWorker("worker-lookup")
	if !ok {
		t.Fatal("expected InstanceIDForWorker to find the worker")
	}
	if id != inst.ID {
		t.Errorf("expected instance ID %q, got %q", inst.ID, id)
	}
}

func TestAutoscaler_InstanceIDForWorker_NotFound(t *testing.T) {
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: make(map[string]PoolStats)}
	cfg := config.AutoscalerConfig{Enabled: true, MaxCloudWorkers: 5}
	a, _ := newTestAutoscaler(cfg, provider, stats)

	_, ok := a.InstanceIDForWorker("ghost-worker")
	if ok {
		t.Error("expected InstanceIDForWorker to return false for unknown worker")
	}
}

// ---- Per-DAG autoscaler override tests ------------------------------------

// newTestAutoscalerWithDAGs builds an Autoscaler that reads DAG definitions
// from the supplied map (simulating what the scheduler would provide).
func newTestAutoscalerWithDAGs(
	cfg config.AutoscalerConfig,
	provider CloudProvider,
	stats *fakeStatsSource,
	dags map[string]*models.DAGDef,
) (*Autoscaler, *fakeInstanceStore) {
	istore := newFakeInstanceStore()
	a := &Autoscaler{
		cfg:           cfg,
		provider:      provider,
		statsSource:   stats,
		instanceStore: istore,
		instances:     make(map[string]*WorkerInstance),
		lastScaleUp:   make(map[string]time.Time),
		idleSince:     make(map[string]time.Time),
		activeDAGs:    func() map[string]*models.DAGDef { return dags },
	}
	return a, istore
}

func TestAutoscaler_PerDAG_LowerThresholdTriggersScaleUp(t *testing.T) {
	// Global threshold=5, DAG override=2. Pool has 3 queued tasks.
	// Without the override: no scale-up. With override: scale-up.
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: map[string]PoolStats{
		"default": {Pool: "default", QueuedTasks: 3},
	}}
	cfg := config.AutoscalerConfig{
		Enabled:          true,
		MaxCloudWorkers:  5,
		ScaleUpThreshold: 5, // global: would NOT trigger at 3
		CooldownSecs:     0,
	}
	dags := map[string]*models.DAGDef{
		"stress": {
			ID: "stress",
			Autoscaler: &models.DAGAutoscalerConfig{
				ScaleUpThreshold: 2, // per-DAG: triggers at 3
			},
		},
	}
	a, _ := newTestAutoscalerWithDAGs(cfg, provider, stats, dags)
	a.tick(context.Background(), "http://master:8080", "")

	if provider.spunUpCount() != 1 {
		t.Errorf("expected 1 SpinUp due to per-DAG lower threshold, got %d", provider.spunUpCount())
	}
}

func TestAutoscaler_PerDAG_NoScaleUpWhenAboveOverrideButBelowGlobal(t *testing.T) {
	// Global threshold=2. DAG override is absent. Pool has 1 queued task.
	// Should respect global threshold — no scale-up.
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: map[string]PoolStats{
		"default": {Pool: "default", QueuedTasks: 1},
	}}
	cfg := config.AutoscalerConfig{
		Enabled:          true,
		MaxCloudWorkers:  5,
		ScaleUpThreshold: 2,
		CooldownSecs:     0,
	}
	a, _ := newTestAutoscalerWithDAGs(cfg, provider, stats, nil)
	a.tick(context.Background(), "http://master:8080", "")

	if provider.spunUpCount() != 0 {
		t.Errorf("expected 0 SpinUp below global threshold, got %d", provider.spunUpCount())
	}
}

func TestAutoscaler_PerDAG_MostPermissiveThresholdWins(t *testing.T) {
	// Two DAGs active on "default": one with threshold=4, one with threshold=1.
	// Pool has 2 queued tasks. Most permissive (1) should win → scale-up.
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: map[string]PoolStats{
		"default": {Pool: "default", QueuedTasks: 2},
	}}
	cfg := config.AutoscalerConfig{
		Enabled:          true,
		MaxCloudWorkers:  5,
		ScaleUpThreshold: 10, // global very high
		CooldownSecs:     0,
	}
	dags := map[string]*models.DAGDef{
		"dag_a": {ID: "dag_a", Autoscaler: &models.DAGAutoscalerConfig{ScaleUpThreshold: 4}},
		"dag_b": {ID: "dag_b", Autoscaler: &models.DAGAutoscalerConfig{ScaleUpThreshold: 1}},
	}
	a, _ := newTestAutoscalerWithDAGs(cfg, provider, stats, dags)
	a.tick(context.Background(), "http://master:8080", "")

	if provider.spunUpCount() != 1 {
		t.Errorf("expected 1 SpinUp (most permissive threshold wins), got %d", provider.spunUpCount())
	}
}

func TestAutoscaler_PerDAG_MaxCloudWorkersCapIsRespected(t *testing.T) {
	// DAG override sets max_cloud_workers=2. Global is 10.
	// 2 instances already running → should not spin up another.
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: map[string]PoolStats{
		"default": {Pool: "default", QueuedTasks: 10},
	}}
	cfg := config.AutoscalerConfig{
		Enabled:          true,
		MaxCloudWorkers:  10, // global cap not reached
		ScaleUpThreshold: 3,
		CooldownSecs:     0,
	}
	dags := map[string]*models.DAGDef{
		"stress": {
			ID: "stress",
			Autoscaler: &models.DAGAutoscalerConfig{
				ScaleUpThreshold: 1,
				MaxCloudWorkers:  2, // per-DAG cap
			},
		},
	}
	a, istore := newTestAutoscalerWithDAGs(cfg, provider, stats, dags)
	// Pre-fill 2 running instances — hitting the per-DAG cap.
	for i := 0; i < 2; i++ {
		id := fmt.Sprintf("inst-%d", i)
		inst := WorkerInstance{ID: id, ProviderID: "fake-" + id, Status: InstanceRunning}
		a.instances[id] = &inst
		istore.SaveInstance(inst) //nolint:errcheck
	}

	a.tick(context.Background(), "http://master:8080", "")

	if provider.spunUpCount() != 0 {
		t.Errorf("expected 0 SpinUp: per-DAG MaxCloudWorkers cap (2) already reached, got %d", provider.spunUpCount())
	}
}

func TestAutoscaler_PerDAG_MostPermissiveMaxWins(t *testing.T) {
	// Two DAGs: one with max=1, one with max=4. 2 instances running.
	// Most permissive (4) wins → should still allow scale-up.
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: map[string]PoolStats{
		"default": {Pool: "default", QueuedTasks: 10},
	}}
	cfg := config.AutoscalerConfig{
		Enabled:          true,
		MaxCloudWorkers:  10,
		ScaleUpThreshold: 3,
		CooldownSecs:     0,
	}
	dags := map[string]*models.DAGDef{
		"dag_a": {ID: "dag_a", Autoscaler: &models.DAGAutoscalerConfig{MaxCloudWorkers: 1}},
		"dag_b": {ID: "dag_b", Autoscaler: &models.DAGAutoscalerConfig{MaxCloudWorkers: 4}},
	}
	a, istore := newTestAutoscalerWithDAGs(cfg, provider, stats, dags)
	for i := 0; i < 2; i++ {
		id := fmt.Sprintf("inst-%d", i)
		inst := WorkerInstance{ID: id, ProviderID: "fake-" + id, Status: InstanceRunning}
		a.instances[id] = &inst
		istore.SaveInstance(inst) //nolint:errcheck
	}

	a.tick(context.Background(), "http://master:8080", "")

	if provider.spunUpCount() != 1 {
		t.Errorf("expected 1 SpinUp (most permissive max=4 wins over max=1), got %d", provider.spunUpCount())
	}
}

func TestAutoscaler_PerDAG_StatusIncludesOverrides(t *testing.T) {
	provider := &fakeProvider{}
	stats := &fakeStatsSource{stats: map[string]PoolStats{
		"default": {Pool: "default", QueuedTasks: 0},
	}}
	cfg := config.AutoscalerConfig{
		Enabled:          true,
		MaxCloudWorkers:  5,
		ScaleUpThreshold: 3,
		Provider:         "fake",
	}
	dags := map[string]*models.DAGDef{
		"stress": {
			ID: "stress",
			Autoscaler: &models.DAGAutoscalerConfig{
				ScaleUpThreshold: 2,
				MaxCloudWorkers:  3,
			},
		},
	}
	a, _ := newTestAutoscalerWithDAGs(cfg, provider, stats, dags)
	snap := a.Status()

	if len(snap.PerDAGOverrides) != 1 {
		t.Fatalf("expected 1 per-DAG override in status, got %d", len(snap.PerDAGOverrides))
	}
	override, ok := snap.PerDAGOverrides["stress"]
	if !ok {
		t.Fatal("expected override for dag 'stress'")
	}
	if override.ScaleUpThreshold != 2 {
		t.Errorf("expected ScaleUpThreshold=2, got %d", override.ScaleUpThreshold)
	}
	if override.MaxCloudWorkers != 3 {
		t.Errorf("expected MaxCloudWorkers=3, got %d", override.MaxCloudWorkers)
	}
}
