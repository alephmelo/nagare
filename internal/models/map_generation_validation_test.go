package models

import (
	"strings"
	"testing"
)

func TestDAGValidateReservesOnlyInternalMapGenerationNamespace(t *testing.T) {
	for _, taskID := range []string{"publish@g2", "foo[bar]", "foo[0]@g1", "foo[0]@green"} {
		t.Run("allows_"+taskID, func(t *testing.T) {
			dag := &DAGDef{ID: "test", Tasks: []TaskDef{{ID: taskID, Type: "command"}}}
			if err := dag.Validate(); err != nil {
				t.Fatalf("Validate() rejected legitimate task ID %q: %v", taskID, err)
			}
		})
	}

	for _, taskID := range []string{"foo[0]@g2", "foo[12]@g10"} {
		t.Run("rejects_"+taskID, func(t *testing.T) {
			dag := &DAGDef{ID: "test", Tasks: []TaskDef{{ID: taskID, Type: "command"}}}
			err := dag.Validate()
			if err == nil || !strings.Contains(err.Error(), "reserved map-generation namespace") {
				t.Fatalf("Validate() error = %v, want reserved namespace error", err)
			}
		})
	}
}
