package autoscaler

import "time"

// ForceAddInstance injects a provisioning WorkerInstance directly into the
// autoscaler's in-memory map without going through SpinUp.
//
// This method is intended for use in tests that need a provisioning instance
// present before a worker registers, bypassing the normal SpinUp path.
func (a *Autoscaler) ForceAddInstance(id string, pools []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.instances[id] = &WorkerInstance{
		ID:        id,
		Pools:     pools,
		Status:    InstanceProvisioning,
		CreatedAt: time.Now(),
	}
}

// ForceAddRunningInstance injects a running WorkerInstance with a known
// workerID directly into the autoscaler's in-memory map.
//
// This method is intended for use in tests that need to verify
// offline-notification paths without going through TryClaimWorker.
func (a *Autoscaler) ForceAddRunningInstance(id, workerID string, pools []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.instances[id] = &WorkerInstance{
		ID:        id,
		Pools:     pools,
		WorkerID:  workerID,
		Status:    InstanceRunning,
		CreatedAt: time.Now(),
	}
}
