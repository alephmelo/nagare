package models

import (
	"strings"
	"testing"
)

func TestDAGValidateReservesOnlyInternalMapGenerationNamespace(t *testing.T) {
	for _, taskID := range []string{"publish@g2", "foo[bar]", "foo[0]@g1", "foo[0]@green", "foo[0]@g02"} {
		t.Run("allows_"+taskID, func(t *testing.T) {
			dag := &DAGDef{ID: "test", Tasks: []TaskDef{{ID: taskID, Type: "command"}}}
			if err := dag.Validate(); err != nil {
				t.Fatalf("Validate() rejected legitimate task ID %q: %v", taskID, err)
			}
		})
	}

	for _, taskID := range []string{"foo[0]@g2", "foo[00]@g2", "foo[12]@g10"} {
		t.Run("rejects_"+taskID, func(t *testing.T) {
			dag := &DAGDef{ID: "test", Tasks: []TaskDef{{ID: taskID, Type: "command"}}}
			err := dag.Validate()
			if err == nil || !strings.Contains(err.Error(), "reserved map-generation namespace") {
				t.Fatalf("Validate() error = %v, want reserved namespace error", err)
			}
		})
	}
}

func TestBaseTaskIDOnlyStripsNumericMapChildSyntax(t *testing.T) {
	tests := map[string]string{
		"map[0]":          "map",
		"map[12]@g2":      "map",
		"foo[bar][3]@g10": "foo[bar]",
		"publish@g2":      "publish@g2",
		"foo[bar]":        "foo[bar]",
		"foo[0]@g1":       "foo[0]@g1",
		"foo[]":           "foo[]",
		"foo[-1]":         "foo[-1]",
		"foo[00]":         "foo",
		"foo[0]@g02":      "foo[0]@g02",
	}
	for taskID, want := range tests {
		if got := BaseTaskID(taskID); got != want {
			t.Errorf("BaseTaskID(%q) = %q, want %q", taskID, got, want)
		}
	}
}

func TestFindTaskForInstancePrefersLegitimateExactBracketID(t *testing.T) {
	dag := &DAGDef{Tasks: []TaskDef{
		{ID: "foo", Command: "mapped"},
		{ID: "foo[0]", Command: "exact"},
		{ID: "foo[bar]", Command: "bracket"},
	}}
	for taskID, wantCommand := range map[string]string{
		"foo[0]":   "exact",
		"foo[1]":   "mapped",
		"foo[bar]": "bracket",
	} {
		task := dag.FindTaskForInstance(taskID)
		if task == nil || task.Command != wantCommand {
			t.Errorf("FindTaskForInstance(%q) = %#v, want command %q", taskID, task, wantCommand)
		}
	}
}
