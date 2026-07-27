package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/tasklifecycle"
)

func TestNewCoordinatorWithLifecycleUsesProvidedLifecycle(t *testing.T) {
	store, err := models.NewStore(fmt.Sprintf(
		"file:%s?mode=memory&cache=private&_busy_timeout=5000",
		t.Name(),
	))
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	lifecycle := tasklifecycle.New(store)

	coordinator := NewCoordinatorWithLifecycle(
		store,
		lifecycle,
		nil,
		30*time.Second,
		"token",
	)

	if coordinator.lifecycle != lifecycle {
		t.Fatal("coordinator did not retain the provided lifecycle")
	}
}
