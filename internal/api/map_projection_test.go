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

func TestMapAPIsProjectCurrentGenerationWithoutMutatingStorage(t *testing.T) {
	store, err := models.NewStore(filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()

	dagsDir := t.TempDir()
	dagYAML := []byte(`
id: map_api
schedule: workflow_dispatch
tasks:
  - id: source
    type: command
    command: echo source
  - id: map
    type: map
    map_over: source
    command: echo {{item}}
  - id: publish@g2
    type: command
    command: echo publish
  - id: foo[bar]
    type: command
    command: echo exact-bracket
`)
	if err := os.WriteFile(filepath.Join(dagsDir, "map.yaml"), dagYAML, 0o600); err != nil {
		t.Fatalf("write DAG: %v", err)
	}
	sched := scheduler.NewScheduler(store)
	if err := sched.LoadDAGs(dagsDir); err != nil {
		t.Fatalf("LoadDAGs: %v", err)
	}

	runID := "map_api_run"
	now := time.Now().UTC()
	if err := store.CreateDagRun(&models.DagRun{
		ID: runID, DAGID: "map_api", Status: models.RunRunning,
		ExecDate: now, TriggerType: "manual", CreatedAt: now,
	}); err != nil {
		t.Fatalf("CreateDagRun: %v", err)
	}
	item := "item"
	for _, task := range []models.TaskInstance{
		{ID: runID + "_source", RunID: runID, TaskID: "source", Status: models.TaskSuccess, Attempt: 1},
		{ID: runID + "_map", RunID: runID, TaskID: "map", Status: models.TaskUpForRetry, Attempt: 1},
		{ID: runID + "_map_2", RunID: runID, TaskID: "map", Status: models.TaskRunning, Attempt: 2},
		{ID: runID + "_map[0]", RunID: runID, TaskID: "map[0]", Status: models.TaskFailed, Output: "obsolete-one\n", ItemValue: &item, Attempt: 1},
		{ID: runID + "_map[0]_2", RunID: runID, TaskID: "map[0]", Status: models.TaskFailed, Output: "obsolete-two\n", ItemValue: &item, Attempt: 2},
		{ID: runID + "_map[0]@g2", RunID: runID, TaskID: "map[0]@g2", Status: models.TaskFailed, Output: "current-one\n", ItemValue: &item, Attempt: 1},
		{ID: runID + "_map[0]@g2_2", RunID: runID, TaskID: "map[0]@g2", Status: models.TaskSuccess, Output: "hello\n", ItemValue: &item, Attempt: 2},
		{ID: runID + "_publish@g2", RunID: runID, TaskID: "publish@g2", Status: models.TaskSuccess, Attempt: 1},
		{ID: runID + "_foo[bar]", RunID: runID, TaskID: "foo[bar]", Status: models.TaskSuccess, Attempt: 1},
	} {
		task.CreatedAt, task.UpdatedAt = now, now
		if err := store.CreateTaskInstance(&task); err != nil {
			t.Fatalf("CreateTaskInstance(%s): %v", task.ID, err)
		}
	}
	rawInstanceID := runID + "_map[0]@g2_2"
	if err := store.InsertTaskMetrics(&models.TaskMetrics{
		TaskInstanceID: rawInstanceID,
		RunID:          runID,
		DAGID:          "map_api",
		TaskID:         "map[0]@g2",
		DurationMs:     42,
		CreatedAt:      now,
	}); err != nil {
		t.Fatalf("InsertTaskMetrics: %v", err)
	}
	if err := store.InsertTaskMetrics(&models.TaskMetrics{
		TaskInstanceID: runID + "_map[0]_2",
		RunID:          runID,
		DAGID:          "map_api",
		TaskID:         "map[0]",
		DurationMs:     99,
		CreatedAt:      now.Add(-time.Second),
	}); err != nil {
		t.Fatalf("InsertTaskMetrics(obsolete): %v", err)
	}

	server := NewServer(store, sched, nil, logbroker.NewBroker(), nil, "")

	t.Run("task list", func(t *testing.T) { assertProjectedTaskList(t, server, runID) })
	t.Run("attempt history", func(t *testing.T) { assertProjectedAttempts(t, server, runID) })
	t.Run("projected log identity", func(t *testing.T) { assertProjectedLogs(t, server, runID) })
	t.Run("metrics responses", func(t *testing.T) {
		assertProjectedMetrics(t, server, runID)
	})

	persistedTask, err := store.GetTaskInstance(rawInstanceID)
	if err != nil || persistedTask.TaskID != "map[0]@g2" || persistedTask.ID != rawInstanceID {
		t.Fatalf("stored task was mutated: %+v, %v", persistedTask, err)
	}
	persistedMetric, err := store.GetTaskMetrics(rawInstanceID)
	if err != nil || persistedMetric.TaskID != "map[0]@g2" || persistedMetric.TaskInstanceID != rawInstanceID {
		t.Fatalf("stored metric was mutated: %+v, %v", persistedMetric, err)
	}
}

func assertProjectedTaskList(t *testing.T, server *Server, runID string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/runs/"+runID+"/tasks", nil)
	server.handleGetRunTasks(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var tasks []enrichedTask
	if err := json.Unmarshal(recorder.Body.Bytes(), &tasks); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	var child, publish, bracket *enrichedTask
	for i := range tasks {
		switch tasks[i].TaskID {
		case "map[0]":
			child = &tasks[i]
		case "publish@g2":
			publish = &tasks[i]
		case "foo[bar]":
			bracket = &tasks[i]
		}
	}
	if child == nil {
		t.Fatal("current projected map child missing")
	}
	if child.ID != runID+"_map[0]_2" || child.Metrics == nil ||
		child.Metrics.TaskID != "map[0]" || child.Metrics.TaskInstanceID != runID+"_map[0]_2" {
		t.Fatalf("projected child = %+v", child)
	}
	if publish == nil || publish.ID != runID+"_publish@g2" {
		t.Fatalf("legitimate @g ID was changed: %+v", publish)
	}
	if bracket == nil || bracket.Command != "echo exact-bracket" {
		t.Fatalf("legitimate bracket ID was changed: %+v", bracket)
	}
}

func assertProjectedAttempts(t *testing.T, server *Server, runID string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/runs/"+runID+"/tasks/map[0]/attempts", nil)
	server.handleGetTaskAttempts(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var attempts []enrichedTask
	if err := json.Unmarshal(recorder.Body.Bytes(), &attempts); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(attempts) != 2 ||
		attempts[0].TaskID != "map[0]" || attempts[0].ID != runID+"_map[0]" ||
		attempts[1].TaskID != "map[0]" || attempts[1].ID != runID+"_map[0]_2" {
		t.Fatalf("projected attempts = %+v", attempts)
	}
}

func assertProjectedLogs(t *testing.T, server *Server, runID string) {
	t.Helper()
	for _, test := range []struct {
		instanceID string
		output     string
	}{
		{runID + "_map[0]", "data: current-one"},
		{runID + "_map[0]_2", "data: hello"},
	} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/api/runs/"+runID+"/tasks/"+test.instanceID+"/logs", nil)
		server.handleTaskLogs(recorder, request)
		if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), test.output) ||
			strings.Contains(recorder.Body.String(), "obsolete") {
			t.Fatalf("instance %s: status = %d, body = %q", test.instanceID, recorder.Code, recorder.Body.String())
		}
	}
}

func assertProjectedMetrics(t *testing.T, server *Server, runID string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/metrics/tasks/"+runID+"_map[0]_2", nil)
	server.handleGetTaskMetrics(recorder, request)
	var metric models.TaskMetrics
	if err := json.Unmarshal(recorder.Body.Bytes(), &metric); err != nil {
		t.Fatalf("decode task metric: %v", err)
	}
	if metric.TaskID != "map[0]" || metric.TaskInstanceID != runID+"_map[0]_2" || metric.DurationMs != 42 {
		t.Fatalf("projected task metric = %+v", metric)
	}

	recorder = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/api/metrics/runs/"+runID, nil)
	server.handleGetRunMetrics(recorder, request)
	var metrics []models.TaskMetrics
	if err := json.Unmarshal(recorder.Body.Bytes(), &metrics); err != nil {
		t.Fatalf("decode run metrics: %v", err)
	}
	if len(metrics) != 1 || metrics[0].TaskID != "map[0]" || metrics[0].TaskInstanceID != runID+"_map[0]_2" {
		t.Fatalf("projected run metrics = %+v", metrics)
	}

	recorder = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/api/metrics/timeseries?dag_id=map_api&since=24h", nil)
	server.handleGetMetricsTimeSeries(recorder, request)
	var series []models.TimeSeriesPoint
	if err := json.Unmarshal(recorder.Body.Bytes(), &series); err != nil {
		t.Fatalf("decode time series: %v", err)
	}
	if len(series) != 1 || series[0].TaskID != "map[0]" || series[0].DurationMs != 42 {
		t.Fatalf("projected time series = %+v", series)
	}
}
