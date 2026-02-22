package autoscaler

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/alephmelo/nagare/internal/models"
)

// storeAdapter wraps *models.Store and implements InstanceStore, translating
// between autoscaler.WorkerInstance and models.CloudInstance.
//
// This indirection prevents a circular import: the models package must not
// import autoscaler (autoscaler depends on models, not the other way around).
type storeAdapter struct {
	store *models.Store
}

// NewStoreAdapter returns an InstanceStore backed by the given models.Store.
func NewStoreAdapter(s *models.Store) InstanceStore {
	return &storeAdapter{store: s}
}

// SaveInstance converts a WorkerInstance to a CloudInstance row and persists it.
func (a *storeAdapter) SaveInstance(inst WorkerInstance) error {
	poolsJSON, err := json.Marshal(inst.Pools)
	if err != nil {
		poolsJSON = []byte(`[]`)
	}

	ci := models.CloudInstance{
		ID:           inst.ID,
		Provider:     providerNameFromID(inst.ID),
		ProviderID:   inst.ProviderID,
		WorkerID:     inst.WorkerID,
		Pools:        string(poolsJSON),
		InstanceType: inst.InstanceType,
		Region:       inst.Region,
		Status:       string(inst.Status),
		CostPerHour:  inst.CostPerHour,
		CreatedAt:    inst.CreatedAt,
	}
	if !inst.TerminatedAt.IsZero() {
		t := inst.TerminatedAt
		ci.TerminatedAt = &t
	}
	return a.store.SaveCloudInstance(ci)
}

// UpdateInstanceStatus updates the status column for the given instance.
func (a *storeAdapter) UpdateInstanceStatus(id string, status InstanceStatus) error {
	return a.store.UpdateCloudInstanceStatus(id, string(status))
}

// UpdateInstanceWorkerID stores the nagare worker ID that the spawned process registered with.
func (a *storeAdapter) UpdateInstanceWorkerID(id, workerID string) error {
	return a.store.UpdateCloudInstanceWorkerID(id, workerID)
}

// ListActiveInstances returns all non-terminated instances, translated back to WorkerInstance.
func (a *storeAdapter) ListActiveInstances() ([]WorkerInstance, error) {
	rows, err := a.store.ListActiveCloudInstances()
	if err != nil {
		return nil, err
	}

	out := make([]WorkerInstance, 0, len(rows))
	for _, ci := range rows {
		var pools []string
		if err := json.Unmarshal([]byte(ci.Pools), &pools); err != nil {
			pools = []string{}
		}

		wi := WorkerInstance{
			ID:           ci.ID,
			ProviderID:   ci.ProviderID,
			WorkerID:     ci.WorkerID,
			Pools:        pools,
			InstanceType: ci.InstanceType,
			Region:       ci.Region,
			Status:       InstanceStatus(ci.Status),
			CostPerHour:  ci.CostPerHour,
			CreatedAt:    ci.CreatedAt,
		}
		if ci.TerminatedAt != nil {
			wi.TerminatedAt = *ci.TerminatedAt
		}
		out = append(out, wi)
	}
	return out, nil
}

// TerminateInstance marks an instance as terminated in the database.
func (a *storeAdapter) TerminateInstance(id string, terminatedAt time.Time) error {
	return a.store.TerminateCloudInstance(id, terminatedAt)
}

// providerNameFromID extracts the provider name from a Nagare instance ID.
// Instance IDs follow the format "<provider>-<suffix>" (e.g. "docker-a3f1b2").
func providerNameFromID(id string) string {
	if idx := strings.Index(id, "-"); idx != -1 {
		return id[:idx]
	}
	return "unknown"
}
