package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/logbroker"
	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/scheduler"
	"github.com/alephmelo/nagare/internal/worker"
)

func TestTaskKillRouteResolvesLogicalTaskToCurrentRemoteAttempt(t *testing.T) {
	store, err := models.NewStore("file:api-kill-current-attempt?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	now := time.Now()
	const (
		runID  = "run-remote-retry"
		taskID = "publish"
	)
	if err := store.CreateDagRun(&models.DagRun{
		ID:          runID,
		DAGID:       "release",
		Status:      models.RunRunning,
		ExecDate:    now,
		TriggerType: "manual",
		CreatedAt:   now,
	}); err != nil {
		t.Fatalf("CreateDagRun: %v", err)
	}

	first := &models.TaskInstance{
		ID:        runID + "_" + taskID + "_1",
		RunID:     runID,
		TaskID:    taskID,
		Status:    models.TaskFailed,
		Output:    "remote attempt one failed",
		Attempt:   1,
		CreatedAt: now.Add(-time.Minute),
		UpdatedAt: now.Add(-time.Minute),
	}
	current := &models.TaskInstance{
		ID:        runID + "_" + taskID + "_2",
		RunID:     runID,
		TaskID:    taskID,
		Status:    models.TaskRunning,
		Attempt:   2,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := store.CreateTaskInstance(first); err != nil {
		t.Fatalf("CreateTaskInstance(first): %v", err)
	}
	if err := store.CreateTaskInstance(current); err != nil {
		t.Fatalf("CreateTaskInstance(current): %v", err)
	}

	broker := logbroker.NewBroker()
	sched := scheduler.NewScheduler(store)
	pool := worker.NewPool(
		store,
		func(string) (*models.DAGDef, bool) { return nil, false },
		sched.TriggerDAG,
		map[string]int{},
		broker,
	)
	server := NewServer(store, sched, pool, broker, nil, "")

	request := httptest.NewRequest(
		http.MethodPost,
		"/api/runs/"+runID+"/tasks/"+taskID+"/kill",
		nil,
	)
	response := httptest.NewRecorder()
	server.handleKillTask(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%q", response.Code, response.Body.String())
	}
	if got, want := strings.TrimSpace(response.Body.String()), `{"message":"Task killed successfully"}`; got != want {
		t.Fatalf("response body = %q, want %q", got, want)
	}

	attempts, err := store.GetTaskAttempts(runID, taskID)
	if err != nil {
		t.Fatalf("GetTaskAttempts: %v", err)
	}
	if len(attempts) != 2 {
		t.Fatalf("attempt history length = %d, want 2", len(attempts))
	}
	if attempts[0].Status != models.TaskFailed || attempts[0].Output != "remote attempt one failed" {
		t.Fatalf("first attempt was rewritten: status=%s output=%q", attempts[0].Status, attempts[0].Output)
	}
	if attempts[1].ID != current.ID {
		t.Fatalf("resolved attempt ID = %q, want %q", attempts[1].ID, current.ID)
	}
	if attempts[1].Status != models.TaskCancelled {
		t.Fatalf("current remote attempt status = %s, want %s", attempts[1].Status, models.TaskCancelled)
	}
}
