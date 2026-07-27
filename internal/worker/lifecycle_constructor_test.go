package worker

import (
	"path/filepath"
	"testing"

	"github.com/alephmelo/nagare/internal/logbroker"
	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/tasklifecycle"
)

func TestNewPoolWithLifecycleUsesProvidedLifecycle(t *testing.T) {
	store, err := models.NewStore(filepath.Join(t.TempDir(), "worker.db"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	lifecycle := tasklifecycle.New(store)

	pool := NewPoolWithLifecycle(
		store,
		lifecycle,
		func(string) (*models.DAGDef, bool) { return nil, false },
		func(string, string, map[string]string) (*models.DagRun, error) { return nil, nil },
		map[string]int{"default": 1},
		logbroker.NewBroker(),
	)

	if pool.lifecycle != lifecycle {
		t.Fatal("pool replaced the provided lifecycle")
	}
}
