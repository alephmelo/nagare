package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/logbroker"
	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/scheduler"
)

// TestRunInspectionAssemblesLifecycleAndTopologyInOneResponse fixes the public
// boundary expected by the run page. In particular, callers must not have to
// infer a current attempt, mapped ancestry, dependencies, or log identity from
// scheduler IDs.
func TestRunInspectionAssemblesLifecycleAndTopologyInOneResponse(t *testing.T) {
	store, err := models.NewStore(filepath.Join(t.TempDir(), "inspection.db"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()

	dagsDir := t.TempDir()
	dagYAML := []byte(`
id: inspection
schedule: workflow_dispatch
tasks:
  - id: source
    type: command
    command: echo source
  - id: map
    type: map
    map_over: source
    command: echo {{item}}
  - id: foo[bar]
    type: command
    command: echo ordinary
    depends_on: [map]
`)
	if err := os.WriteFile(filepath.Join(dagsDir, "inspection.yaml"), dagYAML, 0o600); err != nil {
		t.Fatalf("write DAG: %v", err)
	}
	sched := scheduler.NewScheduler(store)
	if err := sched.LoadDAGs(dagsDir); err != nil {
		t.Fatalf("LoadDAGs: %v", err)
	}

	runID := "cancelled_inspection"
	created := time.Date(2026, time.July, 27, 8, 0, 0, 0, time.UTC)
	completed := created.Add(2 * time.Minute)
	if err := store.CreateDagRun(&models.DagRun{
		ID: runID, DAGID: "inspection", Status: models.RunCancelled,
		ExecDate: created, TriggerType: "manual", CreatedAt: created,
		CompletedAt: &completed,
	}); err != nil {
		t.Fatalf("CreateDagRun: %v", err)
	}

	firstStarted := created.Add(10 * time.Second)
	itemA, itemB := "a", "b"
	attempts := []models.TaskInstance{
		{ID: runID + "_source", RunID: runID, TaskID: "source", Status: models.TaskFailed, Attempt: 1, StartedAt: &firstStarted},
		{ID: runID + "_source_2", RunID: runID, TaskID: "source", Status: models.TaskCancelled, Attempt: 2},
		{ID: runID + "_map", RunID: runID, TaskID: "map", Status: models.TaskCancelled, Attempt: 1},
		{ID: runID + "_map[0]", RunID: runID, TaskID: "map[0]", Status: models.TaskCancelled, Attempt: 1, ItemValue: &itemA},
		{ID: runID + "_map[1]", RunID: runID, TaskID: "map[1]", Status: models.TaskCancelled, Attempt: 1, ItemValue: &itemB},
		{ID: runID + "_foo[bar]", RunID: runID, TaskID: "foo[bar]", Status: models.TaskCancelled, Attempt: 1},
	}
	for i := range attempts {
		attempts[i].CreatedAt = created.Add(time.Duration(i) * time.Second)
		attempts[i].UpdatedAt = attempts[i].CreatedAt
		if err := store.CreateTaskInstance(&attempts[i]); err != nil {
			t.Fatalf("CreateTaskInstance(%s): %v", attempts[i].ID, err)
		}
	}
	if err := store.InsertTaskMetrics(&models.TaskMetrics{
		TaskInstanceID: runID + "_source_2", RunID: runID, DAGID: "inspection",
		TaskID: "source", DurationMs: 17, CreatedAt: completed,
	}); err != nil {
		t.Fatalf("InsertTaskMetrics: %v", err)
	}

	server := NewServer(store, sched, nil, logbroker.NewBroker(), nil, "")
	response := requestRunInspection(t, server, runID)

	run := objectField(t, response, "run")
	assertStringField(t, run, "status", string(models.RunCancelled))
	if _, ok := run["completed_at"]; !ok {
		t.Fatal("cancelled run omitted terminal completed_at")
	}

	tasks := arrayField(t, response, "logical_tasks")
	byID := indexObjectsByStringField(t, tasks, "id")
	for _, id := range []string{"source", "map", "map[0]", "map[1]", "foo[bar]"} {
		if byID[id] == nil {
			t.Errorf("logical task %q missing; present IDs: %v", id, sortedKeys(byID))
		}
	}

	// Exact DAG definitions win over bracket syntax. Only numeric children of
	// the known map definition are classified as mapped children.
	assertStringField(t, byID["map"], "kind", "definition")
	assertStringField(t, byID["map[0]"], "kind", "mapped-child")
	assertStringField(t, byID["map[0]"], "parent_id", "map")
	assertStringField(t, byID["map[1]"], "kind", "mapped-child")
	assertStringField(t, byID["foo[bar]"], "kind", "definition")
	if _, exists := byID["foo[bar]"]["parent_id"]; exists {
		t.Fatal("ordinary bracketed definition was assigned a mapped parent")
	}
	if got := stringArrayField(t, byID["map"], "dependencies"); !reflect.DeepEqual(got, []string{"source"}) {
		t.Fatalf("map dependencies = %v, want [source] (including map_over)", got)
	}
	if got := stringArrayField(t, byID["foo[bar]"], "dependencies"); !reflect.DeepEqual(got, []string{"map"}) {
		t.Fatalf("foo[bar] dependencies = %v, want [map]", got)
	}

	sourceAttempts := arrayField(t, byID["source"], "attempts")
	if len(sourceAttempts) != 2 {
		t.Fatalf("source attempts = %d, want complete retry history", len(sourceAttempts))
	}
	assertNumberField(t, sourceAttempts[0].(map[string]any), "attempt", 1)
	assertNumberField(t, sourceAttempts[1].(map[string]any), "attempt", 2)
	current := objectField(t, byID["source"], "current_attempt")
	assertNumberField(t, current, "attempt", 2)
	assertStringField(t, current, "status", string(models.TaskCancelled))
	if current["started_at"] != nil {
		t.Fatalf("missing optional started_at became %v, want null", current["started_at"])
	}
	if current["duration_ms"] != nil {
		t.Fatalf("missing optional duration_ms became %v, want null", current["duration_ms"])
	}
	metrics := objectField(t, current, "metrics")
	assertNumberField(t, metrics, "duration_ms", 17)
	logs := objectField(t, current, "logs")
	assertStringField(t, logs, "task_attempt_id", runID+"_source_2")

	// The query owns deterministic ordering; status or map expansion must not
	// make the page reconstruct a topology order.
	second := requestRunInspection(t, server, runID)
	if !reflect.DeepEqual(taskIDs(t, tasks), taskIDs(t, arrayField(t, second, "logical_tasks"))) {
		t.Fatalf("logical task order changed between identical reads: %v then %v",
			taskIDs(t, tasks), taskIDs(t, arrayField(t, second, "logical_tasks")))
	}
}

func requestRunInspection(t *testing.T, server *Server, runID string) map[string]any {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/runs/"+runID+"/inspection", nil)
	server.handleGetRunInspection(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("inspection status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode inspection response: %v", err)
	}
	return response
}

func objectField(t *testing.T, object map[string]any, field string) map[string]any {
	t.Helper()
	value, ok := object[field].(map[string]any)
	if !ok {
		t.Fatalf("%s = %#v, want object", field, object[field])
	}
	return value
}

func arrayField(t *testing.T, object map[string]any, field string) []any {
	t.Helper()
	value, ok := object[field].([]any)
	if !ok {
		t.Fatalf("%s = %#v, want array", field, object[field])
	}
	return value
}

func stringArrayField(t *testing.T, object map[string]any, field string) []string {
	t.Helper()
	values := arrayField(t, object, field)
	result := make([]string, len(values))
	for i, value := range values {
		var ok bool
		result[i], ok = value.(string)
		if !ok {
			t.Fatalf("%s[%d] = %#v, want string", field, i, value)
		}
	}
	return result
}

func assertStringField(t *testing.T, object map[string]any, field, want string) {
	t.Helper()
	if got, ok := object[field].(string); !ok || got != want {
		t.Fatalf("%s = %#v, want %q", field, object[field], want)
	}
}

func assertNumberField(t *testing.T, object map[string]any, field string, want float64) {
	t.Helper()
	if got, ok := object[field].(float64); !ok || got != want {
		t.Fatalf("%s = %#v, want %v", field, object[field], want)
	}
}

func indexObjectsByStringField(t *testing.T, values []any, field string) map[string]map[string]any {
	t.Helper()
	result := make(map[string]map[string]any, len(values))
	for i, value := range values {
		object, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("value %d = %#v, want object", i, value)
		}
		id, ok := object[field].(string)
		if !ok {
			t.Fatalf("value %d field %s = %#v, want string", i, field, object[field])
		}
		result[id] = object
	}
	return result
}

func sortedKeys(values map[string]map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	// The diagnostic does not define contract ordering.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

func taskIDs(t *testing.T, values []any) []string {
	t.Helper()
	ids := make([]string, len(values))
	for i, value := range values {
		object, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("task %d = %#v, want object", i, value)
		}
		id, ok := object["id"].(string)
		if !ok {
			t.Fatalf("task %d id = %#v, want string", i, object["id"])
		}
		ids[i] = id
	}
	return ids
}
