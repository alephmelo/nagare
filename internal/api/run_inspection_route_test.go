package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/logbroker"
	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/scheduler"
)

func TestRunInspectionProductionRouteAuthAndErrors(t *testing.T) {
	store, err := models.NewStore(filepath.Join(t.TempDir(), "route.db"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	now := time.Date(2026, time.July, 27, 10, 0, 0, 0, time.UTC)
	if err := store.CreateDagRun(&models.DagRun{
		ID: "known", DAGID: "missing-definition", Status: models.RunRunning,
		ExecDate: now, TriggerType: "manual", CreatedAt: now,
	}); err != nil {
		t.Fatalf("CreateDagRun: %v", err)
	}

	server := NewServer(
		store,
		scheduler.NewScheduler(store),
		nil,
		logbroker.NewBroker(),
		nil,
		"secret",
	)
	handler := server.routes(nil)

	t.Run("route is authenticated", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(
			recorder,
			httptest.NewRequest(http.MethodGet, "/api/runs/known/inspection", nil),
		)
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", recorder.Code)
		}
	})

	t.Run("registered route returns inspection", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet, "/api/runs/known/inspection", nil)
		request.Header.Set("Authorization", "Bearer secret")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
		}
		var body struct {
			LogicalTasks []json.RawMessage `json:"logical_tasks"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if body.LogicalTasks == nil || len(body.LogicalTasks) != 0 {
			t.Fatalf("logical_tasks = %#v, want []", body.LogicalTasks)
		}
	})

	t.Run("missing run is not found", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet, "/api/runs/absent/inspection", nil)
		request.Header.Set("Authorization", "Bearer secret")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("status = %d, body = %s, want 404", recorder.Code, recorder.Body.String())
		}
	})

	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	t.Run("storage failure is internal error", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet, "/api/runs/known/inspection", nil)
		request.Header.Set("Authorization", "Bearer secret")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, body = %s, want 500", recorder.Code, recorder.Body.String())
		}
	})
}
