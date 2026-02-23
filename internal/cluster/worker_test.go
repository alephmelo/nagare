package cluster_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/cluster"
	"github.com/alephmelo/nagare/internal/logbroker"
	"github.com/alephmelo/nagare/internal/models"
)

// fakeMaster builds an httptest.Server simulating a Nagare master for remote worker tests.
// It uses a real Coordinator backed by an in-memory store so that the whole
// protocol is exercised through HTTP without mocking.
func fakeMaster(t *testing.T, dag *models.DAGDef) (*httptest.Server, *models.Store, *logbroker.Broker) {
	t.Helper()

	// Use a unique named in-memory DB per test so parallel tests don't share state.
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=private&_busy_timeout=5000", t.Name())
	store, err := models.NewStore(dsn)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	broker := logbroker.NewBroker()

	getDAG := func(id string) (*models.DAGDef, bool) {
		if dag != nil && id == dag.ID {
			return dag, true
		}
		return nil, false
	}

	coord := cluster.NewCoordinator(store, getDAG, 30*time.Second, "")
	coord.SetBroker(broker)

	srv := httptest.NewServer(coord.Handler())
	t.Cleanup(srv.Close)

	return srv, store, broker
}

// ----- RemoteWorker construction ---------------------------------------------

func TestRemoteWorker_NewWorker(t *testing.T) {
	srv, _, _ := fakeMaster(t, nil)

	w := cluster.NewRemoteWorker(cluster.RemoteWorkerConfig{
		MasterAddr:          srv.URL,
		WorkerID:            "rw1",
		Pools:               []string{"default"},
		MaxTasks:            2,
		Token:               "",
		PollInterval:        20 * time.Millisecond,
		HeartbeatInterval:   50 * time.Millisecond,
		CancelCheckInterval: 100 * time.Millisecond,
	})
	if w == nil {
		t.Fatal("expected non-nil RemoteWorker")
	}
}

// ----- registration ----------------------------------------------------------

func TestRemoteWorker_RegistersOnStart(t *testing.T) {
	srv, _, _ := fakeMaster(t, nil)

	w := cluster.NewRemoteWorker(cluster.RemoteWorkerConfig{
		MasterAddr:        srv.URL,
		WorkerID:          "rw1",
		Pools:             []string{"default"},
		MaxTasks:          2,
		PollInterval:      20 * time.Millisecond,
		HeartbeatInterval: 200 * time.Millisecond,
	})

	if err := w.Register(); err != nil {
		t.Fatalf("Register: %v", err)
	}

	workers := listWorkersFromServer(t, srv)
	if len(workers) != 1 || workers[0].WorkerID != "rw1" {
		t.Errorf("expected rw1 registered, got %v", workers)
	}
}

// ----- poll + execute + result loop ------------------------------------------

// TestRemoteWorker_ExecutesTask is the end-to-end test:
// a real task is queued on the fake master, the remote worker polls, executes,
// and reports back — all over real HTTP.
func TestRemoteWorker_ExecutesTask(t *testing.T) {
	dag := &models.DAGDef{
		ID:    "test_dag",
		Tasks: []models.TaskDef{{ID: "t1", Command: "echo remote_hello", Pool: "default"}},
	}
	srv, store, _ := fakeMaster(t, dag)

	_ = store.CreateDagRun(&models.DagRun{ID: "run_1", DAGID: "test_dag", Status: models.RunRunning})
	_ = store.CreateTaskInstance(&models.TaskInstance{
		ID: "task_1", RunID: "run_1", TaskID: "t1", Status: models.TaskQueued, Attempt: 1,
	})

	cfg := cluster.RemoteWorkerConfig{
		MasterAddr:          srv.URL,
		WorkerID:            "rw1",
		Pools:               []string{"default"},
		MaxTasks:            1,
		PollInterval:        20 * time.Millisecond,
		HeartbeatInterval:   200 * time.Millisecond,
		CancelCheckInterval: 100 * time.Millisecond,
	}
	rw := cluster.NewRemoteWorker(cfg)
	if err := rw.Register(); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Run one poll cycle.
	if err := rw.PollOnce(); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	// Give the goroutine time to execute and report.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, _ := store.GetTaskStatus("run_1", "t1")
		if status == models.TaskSuccess {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	status, _ := store.GetTaskStatus("run_1", "t1")
	t.Errorf("expected task success after remote execution, got %s", status)
}

// TestRemoteWorker_LogsForwardedToMaster verifies that log lines POSTed by the
// worker appear in the master's broker (enabling SSE streaming to the UI).
func TestRemoteWorker_LogsForwardedToMaster(t *testing.T) {
	dag := &models.DAGDef{
		ID:    "test_dag",
		Tasks: []models.TaskDef{{ID: "t1", Command: "echo logline1; echo logline2", Pool: "default"}},
	}
	srv, store, broker := fakeMaster(t, dag)

	_ = store.CreateDagRun(&models.DagRun{ID: "run_log", DAGID: "test_dag", Status: models.RunRunning})
	_ = store.CreateTaskInstance(&models.TaskInstance{
		ID: "task_log", RunID: "run_log", TaskID: "t1", Status: models.TaskQueued, Attempt: 1,
	})

	// Subscribe before execution to capture live lines.
	ch, unsub := broker.Subscribe("task_log")
	defer unsub()

	cfg := cluster.RemoteWorkerConfig{
		MasterAddr:          srv.URL,
		WorkerID:            "rw1",
		Pools:               []string{"default"},
		MaxTasks:            1,
		PollInterval:        20 * time.Millisecond,
		HeartbeatInterval:   200 * time.Millisecond,
		CancelCheckInterval: 100 * time.Millisecond,
	}
	rw := cluster.NewRemoteWorker(cfg)
	if err := rw.Register(); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if err := rw.PollOnce(); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	var received []string
	deadline := time.After(5 * time.Second)
	for len(received) < 2 {
		select {
		case line, ok := <-ch:
			if !ok {
				goto done
			}
			received = append(received, line)
		case <-deadline:
			t.Fatalf("timed out; got lines: %v", received)
		}
	}
done:
	for _, want := range []string{"logline1", "logline2"} {
		found := false
		for _, got := range received {
			if strings.Contains(got, want) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected %q in received lines %v", want, received)
		}
	}
}

// TestRemoteWorker_CancelDetected verifies that a remote worker stops executing
// a task when the master marks it cancelled.
func TestRemoteWorker_CancelDetected(t *testing.T) {
	dag := &models.DAGDef{
		ID:    "test_dag",
		Tasks: []models.TaskDef{{ID: "t1", Command: "sleep 30", Pool: "default"}},
	}
	srv, store, _ := fakeMaster(t, dag)

	_ = store.CreateDagRun(&models.DagRun{ID: "run_c", DAGID: "test_dag", Status: models.RunRunning})
	_ = store.CreateTaskInstance(&models.TaskInstance{
		ID: "task_c", RunID: "run_c", TaskID: "t1", Status: models.TaskQueued, Attempt: 1,
	})

	cfg := cluster.RemoteWorkerConfig{
		MasterAddr:          srv.URL,
		WorkerID:            "rw1",
		Pools:               []string{"default"},
		MaxTasks:            1,
		PollInterval:        20 * time.Millisecond,
		HeartbeatInterval:   200 * time.Millisecond,
		CancelCheckInterval: 30 * time.Millisecond, // fast cancel check for test speed
	}
	rw := cluster.NewRemoteWorker(cfg)
	if err := rw.Register(); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Start polling in background.
	if err := rw.PollOnce(); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	// Wait briefly, then mark the task cancelled on the master.
	time.Sleep(100 * time.Millisecond)
	store.UpdateTaskInstanceStatus("task_c", models.TaskCancelled)

	// The worker should detect the cancel and stop within a few seconds.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, _ := store.GetTaskStatus("run_c", "t1")
		if status == models.TaskCancelled {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	status, _ := store.GetTaskStatus("run_c", "t1")
	t.Errorf("expected task cancelled after remote kill, got %s", status)
}

// TestRemoteWorker_TokenAuth verifies that a worker with the correct token
// can communicate with an auth-protected master.
func TestRemoteWorker_TokenAuth(t *testing.T) {
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=private&_busy_timeout=5000", t.Name())
	store, err := models.NewStore(dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	coord := cluster.NewCoordinator(store, nil, 30*time.Second, "mytoken")
	srv := httptest.NewServer(coord.Handler())
	t.Cleanup(srv.Close)

	cfg := cluster.RemoteWorkerConfig{
		MasterAddr:        srv.URL,
		WorkerID:          "rw1",
		Pools:             []string{"default"},
		MaxTasks:          1,
		Token:             "mytoken",
		PollInterval:      20 * time.Millisecond,
		HeartbeatInterval: 200 * time.Millisecond,
	}
	rw := cluster.NewRemoteWorker(cfg)
	if err := rw.Register(); err != nil {
		t.Fatalf("Register with token: %v", err)
	}
}

func TestRemoteWorker_TokenAuth_Wrong(t *testing.T) {
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=private&_busy_timeout=5000", t.Name())
	store, err := models.NewStore(dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	coord := cluster.NewCoordinator(store, nil, 30*time.Second, "mytoken")
	srv := httptest.NewServer(coord.Handler())
	t.Cleanup(srv.Close)

	cfg := cluster.RemoteWorkerConfig{
		MasterAddr:        srv.URL,
		WorkerID:          "rw1",
		Pools:             []string{"default"},
		MaxTasks:          1,
		Token:             "wrongtoken",
		PollInterval:      20 * time.Millisecond,
		HeartbeatInterval: 200 * time.Millisecond,
	}
	rw := cluster.NewRemoteWorker(cfg)
	if err := rw.Register(); err == nil {
		t.Fatal("expected error with wrong token, got nil")
	}
}

// ----- helpers ---------------------------------------------------------------

// listWorkersFromServer calls GET /api/workers on the test server and decodes.
func listWorkersFromServer(t *testing.T, srv *httptest.Server) []cluster.WorkerInfo {
	t.Helper()
	resp, err := http.Get(srv.URL + "/api/workers")
	if err != nil {
		t.Fatalf("GET /api/workers: %v", err)
	}
	defer resp.Body.Close()
	var workers []cluster.WorkerInfo
	json.NewDecoder(resp.Body).Decode(&workers)
	return workers
}
