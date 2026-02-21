package cluster_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/cluster"
	"github.com/alephmelo/nagare/internal/logbroker"
	"github.com/alephmelo/nagare/internal/models"
)

// ----- helpers ---------------------------------------------------------------

func newTestStore(t *testing.T) *models.Store {
	t.Helper()
	store, err := models.NewStore("file::memory:?cache=shared&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("create test store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func newTestDag() *models.DAGDef {
	return &models.DAGDef{
		ID: "test_dag",
		Tasks: []models.TaskDef{
			{ID: "t1", Command: "echo hello", Pool: "default"},
		},
	}
}

func postJSON(t *testing.T, handler http.Handler, path string, body interface{}) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w
}

func getJSON(t *testing.T, handler http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w
}

// ----- registration ----------------------------------------------------------

func TestCoordinator_Register(t *testing.T) {
	store := newTestStore(t)
	coord := cluster.NewCoordinator(store, nil, 30*time.Second, "")
	mux := coord.Handler()

	reg := cluster.WorkerRegistration{
		WorkerID: "w1",
		Pools:    []string{"default"},
		Hostname: "host1",
		MaxTasks: 4,
	}
	w := postJSON(t, mux, "/api/workers/register", reg)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	workers := coord.ListWorkers()
	if len(workers) != 1 {
		t.Fatalf("expected 1 worker, got %d", len(workers))
	}
	if workers[0].WorkerID != "w1" {
		t.Errorf("expected worker_id w1, got %q", workers[0].WorkerID)
	}
	if workers[0].Status != "online" {
		t.Errorf("expected status online, got %q", workers[0].Status)
	}
}

func TestCoordinator_RegisterDuplicate(t *testing.T) {
	store := newTestStore(t)
	coord := cluster.NewCoordinator(store, nil, 30*time.Second, "")
	mux := coord.Handler()

	reg := cluster.WorkerRegistration{WorkerID: "w1", Pools: []string{"default"}, MaxTasks: 2}
	postJSON(t, mux, "/api/workers/register", reg)
	// Re-register (heartbeat semantics): should update, not duplicate.
	w := postJSON(t, mux, "/api/workers/register", reg)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 on re-register, got %d", w.Code)
	}
	if len(coord.ListWorkers()) != 1 {
		t.Errorf("expected 1 worker after re-register, got %d", len(coord.ListWorkers()))
	}
}

// ----- heartbeat -------------------------------------------------------------

func TestCoordinator_Heartbeat(t *testing.T) {
	store := newTestStore(t)
	coord := cluster.NewCoordinator(store, nil, 30*time.Second, "")
	mux := coord.Handler()

	reg := cluster.WorkerRegistration{WorkerID: "w1", Pools: []string{"default"}, MaxTasks: 2}
	postJSON(t, mux, "/api/workers/register", reg)

	w := postJSON(t, mux, "/api/workers/heartbeat", cluster.WorkerRegistration{WorkerID: "w1"})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCoordinator_OfflineAfterTimeout(t *testing.T) {
	store := newTestStore(t)
	// Very short timeout so we can test expiry quickly.
	coord := cluster.NewCoordinator(store, nil, 50*time.Millisecond, "")
	coord.Register(cluster.WorkerRegistration{WorkerID: "w1", Pools: []string{"default"}, MaxTasks: 1})

	time.Sleep(200 * time.Millisecond)
	coord.ExpireStaleWorkers()

	workers := coord.ListWorkers()
	if len(workers) == 0 {
		t.Fatal("expected worker to still exist (just offline), got 0")
	}
	if workers[0].Status != "offline" {
		t.Errorf("expected status offline after timeout, got %q", workers[0].Status)
	}
}

// ----- poll ------------------------------------------------------------------

func TestCoordinator_Poll_NoWork(t *testing.T) {
	store := newTestStore(t)
	coord := cluster.NewCoordinator(store, nil, 30*time.Second, "")
	mux := coord.Handler()

	coord.Register(cluster.WorkerRegistration{WorkerID: "w1", Pools: []string{"default"}, MaxTasks: 1})

	w := postJSON(t, mux, "/api/workers/poll", cluster.PollRequest{WorkerID: "w1", Pools: []string{"default"}})
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 when no work, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCoordinator_Poll_AssignsMatchingTask(t *testing.T) {
	store := newTestStore(t)

	dag := newTestDag()
	getDAG := func(id string) (*models.DAGDef, bool) {
		if id == dag.ID {
			return dag, true
		}
		return nil, false
	}

	coord := cluster.NewCoordinator(store, getDAG, 30*time.Second, "")
	mux := coord.Handler()

	// Create a run and a queued task.
	_ = store.CreateDagRun(&models.DagRun{ID: "run_1", DAGID: "test_dag", Status: models.RunRunning})
	_ = store.CreateTaskInstance(&models.TaskInstance{
		ID: "task_1", RunID: "run_1", TaskID: "t1", Status: models.TaskQueued, Attempt: 1,
	})

	coord.Register(cluster.WorkerRegistration{WorkerID: "w1", Pools: []string{"default"}, MaxTasks: 1})

	w := postJSON(t, mux, "/api/workers/poll", cluster.PollRequest{WorkerID: "w1", Pools: []string{"default"}})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 with task assignment, got %d: %s", w.Code, w.Body.String())
	}

	var assignment cluster.TaskAssignmentDTO
	if err := json.NewDecoder(w.Body).Decode(&assignment); err != nil {
		t.Fatalf("decode assignment: %v", err)
	}
	if assignment.TaskInstanceID != "task_1" {
		t.Errorf("expected task_instance_id task_1, got %q", assignment.TaskInstanceID)
	}
	if assignment.Command != "echo hello" {
		t.Errorf("expected command 'echo hello', got %q", assignment.Command)
	}

	// Task should be marked running in DB.
	status, _ := store.GetTaskStatus("run_1", "t1")
	if status != models.TaskRunning {
		t.Errorf("expected task status running after poll, got %s", status)
	}
}

func TestCoordinator_Poll_IgnoresNonMatchingPool(t *testing.T) {
	store := newTestStore(t)

	dag := &models.DAGDef{
		ID:    "test_dag",
		Tasks: []models.TaskDef{{ID: "t1", Command: "echo hi", Pool: "gpu"}},
	}
	getDAG := func(id string) (*models.DAGDef, bool) {
		if id == dag.ID {
			return dag, true
		}
		return nil, false
	}

	coord := cluster.NewCoordinator(store, getDAG, 30*time.Second, "")
	mux := coord.Handler()

	_ = store.CreateDagRun(&models.DagRun{ID: "run_1", DAGID: "test_dag", Status: models.RunRunning})
	_ = store.CreateTaskInstance(&models.TaskInstance{
		ID: "task_1", RunID: "run_1", TaskID: "t1", Status: models.TaskQueued, Attempt: 1,
	})

	coord.Register(cluster.WorkerRegistration{WorkerID: "w1", Pools: []string{"default"}, MaxTasks: 1})

	// Worker serves "default" but task is in "gpu" pool — should get no work.
	w := postJSON(t, mux, "/api/workers/poll", cluster.PollRequest{WorkerID: "w1", Pools: []string{"default"}})
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for pool mismatch, got %d", w.Code)
	}

	// Task must still be queued.
	status, _ := store.GetTaskStatus("run_1", "t1")
	if status != models.TaskQueued {
		t.Errorf("expected task still queued, got %s", status)
	}
}

// ----- result ----------------------------------------------------------------

func TestCoordinator_Result_Success(t *testing.T) {
	store := newTestStore(t)
	broker := logbroker.NewBroker()
	coord := cluster.NewCoordinator(store, nil, 30*time.Second, "")
	coord.SetBroker(broker)
	mux := coord.Handler()

	_ = store.CreateDagRun(&models.DagRun{ID: "run_1", DAGID: "test_dag", Status: models.RunRunning})
	_ = store.CreateTaskInstance(&models.TaskInstance{
		ID: "task_1", RunID: "run_1", TaskID: "t1", Status: models.TaskRunning, Attempt: 1,
	})

	result := cluster.TaskResult{
		TaskInstanceID: "task_1",
		Status:         "success",
		Output:         "all done",
	}
	w := postJSON(t, mux, "/api/workers/result", result)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	status, _ := store.GetTaskStatus("run_1", "t1")
	if status != models.TaskSuccess {
		t.Errorf("expected success status in DB, got %s", status)
	}
}

func TestCoordinator_Result_Failed(t *testing.T) {
	store := newTestStore(t)
	coord := cluster.NewCoordinator(store, nil, 30*time.Second, "")
	mux := coord.Handler()

	_ = store.CreateDagRun(&models.DagRun{ID: "run_1", DAGID: "test_dag", Status: models.RunRunning})
	_ = store.CreateTaskInstance(&models.TaskInstance{
		ID: "task_1", RunID: "run_1", TaskID: "t1", Status: models.TaskRunning, Attempt: 1,
	})

	result := cluster.TaskResult{
		TaskInstanceID: "task_1",
		Status:         "failed",
		Output:         "boom",
	}
	w := postJSON(t, mux, "/api/workers/result", result)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	status, _ := store.GetTaskStatus("run_1", "t1")
	if status != models.TaskFailed {
		t.Errorf("expected failed status, got %s", status)
	}
}

// ----- log streaming ---------------------------------------------------------

func TestCoordinator_Log_PublishesToBroker(t *testing.T) {
	store := newTestStore(t)
	broker := logbroker.NewBroker()
	coord := cluster.NewCoordinator(store, nil, 30*time.Second, "")
	coord.SetBroker(broker)
	mux := coord.Handler()

	_ = store.CreateDagRun(&models.DagRun{ID: "run_1", DAGID: "test_dag", Status: models.RunRunning})
	_ = store.CreateTaskInstance(&models.TaskInstance{
		ID: "task_1", RunID: "run_1", TaskID: "t1", Status: models.TaskRunning, Attempt: 1,
	})

	ch, unsub := broker.Subscribe("task_1")
	defer unsub()

	batch := cluster.LogBatch{
		TaskInstanceID: "task_1",
		Lines:          []string{"line one", "line two"},
	}
	w := postJSON(t, mux, "/api/workers/log", batch)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Drain the two lines with a timeout.
	received := make([]string, 0, 2)
	deadline := time.After(2 * time.Second)
	for len(received) < 2 {
		select {
		case line, ok := <-ch:
			if !ok {
				goto done
			}
			received = append(received, line)
		case <-deadline:
			t.Fatalf("timed out waiting for log lines; got %v", received)
		}
	}
done:
	if len(received) != 2 || received[0] != "line one" || received[1] != "line two" {
		t.Errorf("unexpected log lines: %v", received)
	}
}

// ----- cancel detection ------------------------------------------------------

func TestCoordinator_CancelCheck_Cancelled(t *testing.T) {
	store := newTestStore(t)
	coord := cluster.NewCoordinator(store, nil, 30*time.Second, "")
	mux := coord.Handler()

	_ = store.CreateDagRun(&models.DagRun{ID: "run_1", DAGID: "test_dag", Status: models.RunRunning})
	_ = store.CreateTaskInstance(&models.TaskInstance{
		ID: "task_1", RunID: "run_1", TaskID: "t1", Status: models.TaskCancelled, Attempt: 1,
	})

	w := getJSON(t, mux, "/api/workers/tasks/task_1/cancel")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp cluster.CancelCheck
	json.NewDecoder(w.Body).Decode(&resp)
	if !resp.Cancel {
		t.Error("expected cancel=true for a cancelled task")
	}
}

func TestCoordinator_CancelCheck_NotCancelled(t *testing.T) {
	store := newTestStore(t)
	coord := cluster.NewCoordinator(store, nil, 30*time.Second, "")
	mux := coord.Handler()

	_ = store.CreateDagRun(&models.DagRun{ID: "run_1", DAGID: "test_dag", Status: models.RunRunning})
	_ = store.CreateTaskInstance(&models.TaskInstance{
		ID: "task_1", RunID: "run_1", TaskID: "t1", Status: models.TaskRunning, Attempt: 1,
	})

	w := getJSON(t, mux, "/api/workers/tasks/task_1/cancel")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp cluster.CancelCheck
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Cancel {
		t.Error("expected cancel=false for a running task")
	}
}

// ----- auth middleware -------------------------------------------------------

func TestCoordinator_AuthMiddleware_Blocks(t *testing.T) {
	store := newTestStore(t)
	coord := cluster.NewCoordinator(store, nil, 30*time.Second, "supersecret")
	mux := coord.Handler()

	// No Authorization header → 401.
	reg := cluster.WorkerRegistration{WorkerID: "w1", Pools: []string{"default"}, MaxTasks: 1}
	w := postJSON(t, mux, "/api/workers/register", reg)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without auth, got %d", w.Code)
	}
}

func TestCoordinator_AuthMiddleware_Allows(t *testing.T) {
	store := newTestStore(t)
	coord := cluster.NewCoordinator(store, nil, 30*time.Second, "supersecret")
	mux := coord.Handler()

	reg := cluster.WorkerRegistration{WorkerID: "w1", Pools: []string{"default"}, MaxTasks: 1}
	b, _ := json.Marshal(reg)
	req := httptest.NewRequest(http.MethodPost, "/api/workers/register", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer supersecret")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 with valid token, got %d: %s", w.Code, w.Body.String())
	}
}

// ----- GET /api/workers ------------------------------------------------------

func TestCoordinator_ListWorkers_HTTP(t *testing.T) {
	store := newTestStore(t)
	coord := cluster.NewCoordinator(store, nil, 30*time.Second, "")
	coord.Register(cluster.WorkerRegistration{WorkerID: "w1", Pools: []string{"default"}, MaxTasks: 2})
	coord.Register(cluster.WorkerRegistration{WorkerID: "w2", Pools: []string{"gpu"}, MaxTasks: 1})
	mux := coord.Handler()

	w := getJSON(t, mux, "/api/workers")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var workers []cluster.WorkerInfo
	if err := json.NewDecoder(w.Body).Decode(&workers); err != nil {
		t.Fatalf("decode workers: %v", err)
	}
	if len(workers) != 2 {
		t.Errorf("expected 2 workers, got %d", len(workers))
	}
}
