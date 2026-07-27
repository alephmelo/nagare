package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/logbroker"
	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/scheduler"
)

func TestRunInspectionLogMetadataResolvesHistoricalAttemptExactly(t *testing.T) {
	store, err := models.NewStore(filepath.Join(t.TempDir(), "exact-logs.db"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()

	dagsDir := t.TempDir()
	dag := []byte(`
id: exact-logs
schedule: workflow_dispatch
tasks:
  - id: source
    type: command
    command: echo source
  - id: map
    type: map
    map_over: source
    command: echo {{item}}
`)
	if err := os.WriteFile(filepath.Join(dagsDir, "exact-logs.yaml"), dag, 0o600); err != nil {
		t.Fatalf("write DAG: %v", err)
	}
	sched := scheduler.NewScheduler(store)
	if err := sched.LoadDAGs(dagsDir); err != nil {
		t.Fatalf("LoadDAGs: %v", err)
	}

	runID := "exact_log_run"
	now := time.Date(2026, time.July, 27, 11, 0, 0, 0, time.UTC)
	if err := store.CreateDagRun(&models.DagRun{
		ID: runID, DAGID: "exact-logs", Status: models.RunSuccess,
		ExecDate: now, TriggerType: "manual", CreatedAt: now,
	}); err != nil {
		t.Fatalf("CreateDagRun: %v", err)
	}
	item := "value"
	for i, task := range []models.TaskInstance{
		{
			ID: runID + "_map", RunID: runID, TaskID: "map",
			Status: models.TaskFailed, Attempt: 1,
		},
		{
			ID: runID + "_map_2", RunID: runID, TaskID: "map",
			Status: models.TaskSuccess, Attempt: 2,
		},
		{
			ID: runID + "_map[0]", RunID: runID, TaskID: "map[0]",
			Status: models.TaskFailed, Output: "obsolete-generation\n",
			ItemValue: &item, Attempt: 1,
		},
		{
			ID: runID + "_map[0]@g2", RunID: runID, TaskID: "map[0]@g2",
			Status: models.TaskSuccess, Output: "current-generation\n",
			ItemValue: &item, Attempt: 1,
		},
	} {
		task.CreatedAt = now.Add(time.Duration(i) * time.Second)
		task.UpdatedAt = task.CreatedAt
		if err := store.CreateTaskInstance(&task); err != nil {
			t.Fatalf("CreateTaskInstance(%s): %v", task.ID, err)
		}
	}

	server := NewServer(store, sched, nil, logbroker.NewBroker(), nil, "")
	handler := server.routes(nil)
	inspectionRecorder := httptest.NewRecorder()
	handler.ServeHTTP(
		inspectionRecorder,
		httptest.NewRequest(
			http.MethodGet,
			"/api/runs/"+runID+"/inspection",
			nil,
		),
	)
	if inspectionRecorder.Code != http.StatusOK {
		t.Fatalf(
			"inspection status = %d, body = %s",
			inspectionRecorder.Code,
			inspectionRecorder.Body.String(),
		)
	}

	var inspection struct {
		LogicalTasks []struct {
			ID       string `json:"id"`
			Attempts []struct {
				ID         string `json:"id"`
				Generation int    `json:"generation"`
				Logs       struct {
					TaskAttemptID string `json:"task_attempt_id"`
					Exact         bool   `json:"exact"`
				} `json:"logs"`
			} `json:"attempts"`
		} `json:"logical_tasks"`
	}
	if err := json.Unmarshal(inspectionRecorder.Body.Bytes(), &inspection); err != nil {
		t.Fatalf("decode inspection: %v", err)
	}

	var historicalID string
	for _, task := range inspection.LogicalTasks {
		if task.ID != "map[0]" {
			continue
		}
		for _, attempt := range task.Attempts {
			if attempt.Generation != 1 {
				continue
			}
			if !attempt.Logs.Exact {
				t.Fatal("historical log metadata did not request exact resolution")
			}
			if attempt.Logs.TaskAttemptID != attempt.ID {
				t.Fatalf(
					"log ID = %q, attempt ID = %q",
					attempt.Logs.TaskAttemptID,
					attempt.ID,
				)
			}
			historicalID = attempt.Logs.TaskAttemptID
		}
	}
	if historicalID != runID+"_map[0]" {
		t.Fatalf("historical log ID = %q", historicalID)
	}

	exactRecorder := httptest.NewRecorder()
	handler.ServeHTTP(
		exactRecorder,
		httptest.NewRequest(
			http.MethodGet,
			"/api/runs/"+runID+"/tasks/"+historicalID+"/logs?exact=true",
			nil,
		),
	)
	if exactRecorder.Code != http.StatusOK ||
		!strings.Contains(exactRecorder.Body.String(), "obsolete-generation") ||
		strings.Contains(exactRecorder.Body.String(), "current-generation") {
		t.Fatalf(
			"exact logs: status = %d, body = %q",
			exactRecorder.Code,
			exactRecorder.Body.String(),
		)
	}

	legacyRecorder := httptest.NewRecorder()
	handler.ServeHTTP(
		legacyRecorder,
		httptest.NewRequest(
			http.MethodGet,
			"/api/runs/"+runID+"/tasks/"+historicalID+"/logs",
			nil,
		),
	)
	if legacyRecorder.Code != http.StatusOK ||
		!strings.Contains(legacyRecorder.Body.String(), "current-generation") ||
		strings.Contains(legacyRecorder.Body.String(), "obsolete-generation") {
		t.Fatalf(
			"legacy logs: status = %d, body = %q",
			legacyRecorder.Code,
			legacyRecorder.Body.String(),
		)
	}

	wrongRunRecorder := httptest.NewRecorder()
	handler.ServeHTTP(
		wrongRunRecorder,
		httptest.NewRequest(
			http.MethodGet,
			"/api/runs/other-run/tasks/"+historicalID+"/logs?exact=true",
			nil,
		),
	)
	if wrongRunRecorder.Code != http.StatusNotFound {
		t.Fatalf("wrong-run exact status = %d, want 404", wrongRunRecorder.Code)
	}
}
