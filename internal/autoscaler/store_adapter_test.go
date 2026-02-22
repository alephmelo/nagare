package autoscaler

import (
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/models"
)

// newAdapterStore returns a fresh in-memory models.Store wrapped in a
// storeAdapter, plus a cleanup function that closes the store.
func newAdapterStore(t *testing.T) (InstanceStore, func()) {
	t.Helper()
	s, err := models.NewStore("file::memory:?cache=shared&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("create test store: %v", err)
	}
	return NewStoreAdapter(s), func() { s.Close() }
}

// sampleInstance returns a WorkerInstance suitable for round-trip tests.
func sampleInstance(id string) WorkerInstance {
	return WorkerInstance{
		ID:           id,
		ProviderID:   "provider-" + id,
		WorkerID:     "",
		Pools:        []string{"default", "gpu"},
		InstanceType: "t3.small",
		Region:       "us-east-1",
		Status:       InstanceProvisioning,
		CostPerHour:  0.025,
		CreatedAt:    time.Now().Truncate(time.Second),
	}
}

// TestStoreAdapter_SaveAndList verifies that SaveInstance persists a
// WorkerInstance and that ListActiveInstances returns it with fields intact.
func TestStoreAdapter_SaveAndList(t *testing.T) {
	store, cleanup := newAdapterStore(t)
	defer cleanup()

	inst := sampleInstance("docker-aabbcc")
	if err := store.SaveInstance(inst); err != nil {
		t.Fatalf("SaveInstance: %v", err)
	}

	active, err := store.ListActiveInstances()
	if err != nil {
		t.Fatalf("ListActiveInstances: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("expected 1 active instance, got %d", len(active))
	}

	got := active[0]
	if got.ID != inst.ID {
		t.Errorf("ID: got %q, want %q", got.ID, inst.ID)
	}
	if got.ProviderID != inst.ProviderID {
		t.Errorf("ProviderID: got %q, want %q", got.ProviderID, inst.ProviderID)
	}
	if got.InstanceType != inst.InstanceType {
		t.Errorf("InstanceType: got %q, want %q", got.InstanceType, inst.InstanceType)
	}
	if got.Region != inst.Region {
		t.Errorf("Region: got %q, want %q", got.Region, inst.Region)
	}
	if got.Status != inst.Status {
		t.Errorf("Status: got %q, want %q", got.Status, inst.Status)
	}
	if got.CostPerHour != inst.CostPerHour {
		t.Errorf("CostPerHour: got %v, want %v", got.CostPerHour, inst.CostPerHour)
	}
	if len(got.Pools) != len(inst.Pools) {
		t.Errorf("Pools: got %v, want %v", got.Pools, inst.Pools)
	}
}

// TestStoreAdapter_UpdateInstanceStatus ensures UpdateInstanceStatus changes
// the persisted status of an existing instance.
func TestStoreAdapter_UpdateInstanceStatus(t *testing.T) {
	store, cleanup := newAdapterStore(t)
	defer cleanup()

	inst := sampleInstance("docker-111111")
	if err := store.SaveInstance(inst); err != nil {
		t.Fatalf("SaveInstance: %v", err)
	}

	if err := store.UpdateInstanceStatus(inst.ID, InstanceRunning); err != nil {
		t.Fatalf("UpdateInstanceStatus: %v", err)
	}

	active, err := store.ListActiveInstances()
	if err != nil {
		t.Fatalf("ListActiveInstances: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("expected 1 active instance, got %d", len(active))
	}
	if active[0].Status != InstanceRunning {
		t.Errorf("expected status %q after update, got %q", InstanceRunning, active[0].Status)
	}
}

// TestStoreAdapter_UpdateInstanceWorkerID verifies that UpdateInstanceWorkerID
// stores the nagare worker ID against the instance row.
func TestStoreAdapter_UpdateInstanceWorkerID(t *testing.T) {
	store, cleanup := newAdapterStore(t)
	defer cleanup()

	inst := sampleInstance("docker-222222")
	if err := store.SaveInstance(inst); err != nil {
		t.Fatalf("SaveInstance: %v", err)
	}

	const workerID = "worker-abc123"
	if err := store.UpdateInstanceWorkerID(inst.ID, workerID); err != nil {
		t.Fatalf("UpdateInstanceWorkerID: %v", err)
	}

	active, err := store.ListActiveInstances()
	if err != nil {
		t.Fatalf("ListActiveInstances: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("expected 1 active instance, got %d", len(active))
	}
	if active[0].WorkerID != workerID {
		t.Errorf("WorkerID: got %q, want %q", active[0].WorkerID, workerID)
	}
}

// TestStoreAdapter_TerminateInstance checks that TerminateInstance removes the
// row from the active list (terminated instances are excluded by ListActiveInstances).
func TestStoreAdapter_TerminateInstance(t *testing.T) {
	store, cleanup := newAdapterStore(t)
	defer cleanup()

	inst := sampleInstance("docker-333333")
	if err := store.SaveInstance(inst); err != nil {
		t.Fatalf("SaveInstance: %v", err)
	}

	if err := store.TerminateInstance(inst.ID, time.Now()); err != nil {
		t.Fatalf("TerminateInstance: %v", err)
	}

	active, err := store.ListActiveInstances()
	if err != nil {
		t.Fatalf("ListActiveInstances: %v", err)
	}
	if len(active) != 0 {
		t.Errorf("expected 0 active instances after termination, got %d", len(active))
	}
}

// TestStoreAdapter_MultipleInstances_PartialTermination ensures that
// terminating one instance does not affect others in the list.
func TestStoreAdapter_MultipleInstances_PartialTermination(t *testing.T) {
	store, cleanup := newAdapterStore(t)
	defer cleanup()

	a := sampleInstance("docker-aaaa01")
	b := sampleInstance("docker-bbbb02")

	for _, inst := range []WorkerInstance{a, b} {
		if err := store.SaveInstance(inst); err != nil {
			t.Fatalf("SaveInstance(%s): %v", inst.ID, err)
		}
	}

	// Terminate only the first instance.
	if err := store.TerminateInstance(a.ID, time.Now()); err != nil {
		t.Fatalf("TerminateInstance: %v", err)
	}

	active, err := store.ListActiveInstances()
	if err != nil {
		t.Fatalf("ListActiveInstances: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("expected 1 active instance after partial termination, got %d", len(active))
	}
	if active[0].ID != b.ID {
		t.Errorf("expected remaining instance %q, got %q", b.ID, active[0].ID)
	}
}

// TestStoreAdapter_PoolsRoundTrip verifies that multi-element pool slices
// survive the JSON serialisation round-trip through the adapter.
func TestStoreAdapter_PoolsRoundTrip(t *testing.T) {
	store, cleanup := newAdapterStore(t)
	defer cleanup()

	inst := sampleInstance("docker-pools01")
	inst.Pools = []string{"alpha", "beta", "gamma"}

	if err := store.SaveInstance(inst); err != nil {
		t.Fatalf("SaveInstance: %v", err)
	}

	active, err := store.ListActiveInstances()
	if err != nil {
		t.Fatalf("ListActiveInstances: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(active))
	}

	want := map[string]bool{"alpha": true, "beta": true, "gamma": true}
	for _, p := range active[0].Pools {
		delete(want, p)
	}
	if len(want) != 0 {
		t.Errorf("missing pools after round-trip: %v", want)
	}
}

// TestStoreAdapter_ProviderNameFromID tests the unexported helper directly
// since store_adapter_test.go is in the same package.
func TestStoreAdapter_ProviderNameFromID(t *testing.T) {
	cases := []struct {
		id   string
		want string
	}{
		{"docker-a1b2c3", "docker"},
		{"aws-deadbeef", "aws"},
		{"nodash", "unknown"},
		{"", "unknown"},
	}
	for _, tc := range cases {
		got := providerNameFromID(tc.id)
		if got != tc.want {
			t.Errorf("providerNameFromID(%q) = %q, want %q", tc.id, got, tc.want)
		}
	}
}
