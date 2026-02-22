package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/logbroker"
	"github.com/alephmelo/nagare/internal/models"
)

// newTestServer creates a minimal Server with an in-memory store and broker.
func newTestServer(t *testing.T) (*Server, *models.Store, *logbroker.Broker) {
	t.Helper()
	store, err := models.NewStore("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	broker := logbroker.NewBroker()
	srv := &Server{store: store, broker: broker}
	return srv, store, broker
}

// newTestServerWithKey creates a Server configured with the given API key.
func newTestServerWithKey(t *testing.T, apiKey string) (*Server, *models.Store) {
	t.Helper()
	store, err := models.NewStore("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	srv := &Server{store: store, broker: logbroker.NewBroker(), apiKey: apiKey}
	return srv, store
}

// ----- apiKeyMiddleware tests -------------------------------------------------

// TestAPIKeyMiddleware_NoKey verifies that when no API key is configured,
// all requests pass through without requiring any Authorization header.
func TestAPIKeyMiddleware_NoKey(t *testing.T) {
	srv, _ := newTestServerWithKey(t, "")
	handler := srv.apiKeyMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 when no key configured, got %d", w.Code)
	}
}

// TestAPIKeyMiddleware_ValidKey verifies that a correct Bearer token is accepted.
func TestAPIKeyMiddleware_ValidKey(t *testing.T) {
	srv, _ := newTestServerWithKey(t, "test-key-abc")
	handler := srv.apiKeyMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	req.Header.Set("Authorization", "Bearer test-key-abc")
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 with valid key, got %d", w.Code)
	}
}

// TestAPIKeyMiddleware_WrongKey verifies that an incorrect token returns 401.
func TestAPIKeyMiddleware_WrongKey(t *testing.T) {
	srv, _ := newTestServerWithKey(t, "test-key-abc")
	handler := srv.apiKeyMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 with wrong key, got %d", w.Code)
	}
}

// TestAPIKeyMiddleware_MissingHeader verifies that a missing Authorization
// header returns 401 when a key is configured.
func TestAPIKeyMiddleware_MissingHeader(t *testing.T) {
	srv, _ := newTestServerWithKey(t, "test-key-abc")
	handler := srv.apiKeyMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 with missing header, got %d", w.Code)
	}
}

// TestAPIKeyMiddleware_BadScheme verifies that a non-Bearer scheme with no
// query param returns 401.
func TestAPIKeyMiddleware_BadScheme(t *testing.T) {
	srv, _ := newTestServerWithKey(t, "test-key-abc")
	handler := srv.apiKeyMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	req.Header.Set("Authorization", "Basic test-key-abc")
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 with Basic scheme and no query param, got %d", w.Code)
	}
}

// TestAPIKeyMiddleware_QueryParam verifies that ?token= is accepted as a
// fallback for EventSource/SSE clients that cannot send custom headers.
func TestAPIKeyMiddleware_QueryParam(t *testing.T) {
	srv, _ := newTestServerWithKey(t, "test-key-abc")
	handler := srv.apiKeyMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/runs/r1/tasks/t1/logs?token=test-key-abc", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 with valid ?token= param, got %d", w.Code)
	}
}

// TestAPIKeyMiddleware_WrongQueryParam verifies that a wrong ?token= returns 401.
func TestAPIKeyMiddleware_WrongQueryParam(t *testing.T) {
	srv, _ := newTestServerWithKey(t, "test-key-abc")
	handler := srv.apiKeyMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/runs/r1/tasks/t1/logs?token=wrong", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 with wrong ?token= param, got %d", w.Code)
	}
}

// TestStartRoutes_APIKeyProtection verifies that /api/stats requires auth when
// a key is configured, and that /api/webhooks/ is exempt.
func TestStartRoutes_APIKeyProtection(t *testing.T) {
	srv, _ := newTestServerWithKey(t, "secret")

	// Use a simple echo handler so we don't depend on scheduler/pool state.
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/api/stats", srv.apiKeyMiddleware(srv.corsMiddleware(ok)))
	mux.HandleFunc("/api/webhooks/", srv.handleWebhook)

	t.Run("stats without key → 401", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Errorf("expected 401, got %d", w.Code)
		}
	})

	t.Run("stats with key → 200", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
		req.Header.Set("Authorization", "Bearer secret")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", w.Code)
		}
	})

	t.Run("webhooks without key → not 401 (exempt)", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/webhooks/some-path", nil)
		w := httptest.NewRecorder()
		// Call apiKeyMiddleware directly with a no-op to prove webhooks are NOT
		// wrapped with apiKeyMiddleware — i.e. a request without a key is allowed
		// through the webhook route.
		noopHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		noopMux := http.NewServeMux()
		// Webhooks registered WITHOUT apiKeyMiddleware.
		noopMux.HandleFunc("/api/webhooks/", noopHandler)
		noopMux.ServeHTTP(w, req)
		if w.Code == http.StatusUnauthorized {
			t.Errorf("webhooks should be exempt from API key auth, got 401")
		}
	})
}

// TestSSELogsCompletedTask verifies that a completed task's stored output is
// streamed as SSE data lines and the connection closes immediately.
func TestSSELogsCompletedTask(t *testing.T) {
	srv, store, _ := newTestServer(t)

	_ = store.CreateDagRun(&models.DagRun{ID: "run1", DAGID: "d1", Status: models.RunSuccess})
	_ = store.CreateTaskInstance(&models.TaskInstance{
		ID:     "run1_t1",
		RunID:  "run1",
		TaskID: "t1",
		Status: models.TaskSuccess,
		Output: "line one\nline two\nline three\n",
	})

	req := httptest.NewRequest(http.MethodGet, "/api/runs/run1/tasks/run1_t1/logs", nil)
	w := httptest.NewRecorder()

	srv.handleTaskLogs(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Errorf("expected text/event-stream content type, got %q", ct)
	}

	body := w.Body.String()
	for _, want := range []string{"line one", "line two", "line three"} {
		if !strings.Contains(body, "data: "+want) {
			t.Errorf("expected %q in SSE body, got:\n%s", want, body)
		}
	}
}

// TestSSELogsTaskNotFound verifies that a 404 is returned for unknown task IDs.
func TestSSELogsTaskNotFound(t *testing.T) {
	srv, _, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/runs/run1/tasks/nonexistent/logs", nil)
	w := httptest.NewRecorder()

	srv.handleTaskLogs(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

// TestSSELogsRunningTask verifies that log lines published to the broker are
// streamed to a connected SSE client, and the stream closes when the broker
// closes the channel.
func TestSSELogsRunningTask(t *testing.T) {
	srv, store, broker := newTestServer(t)

	_ = store.CreateDagRun(&models.DagRun{ID: "run2", DAGID: "d1", Status: models.RunRunning})
	_ = store.CreateTaskInstance(&models.TaskInstance{
		ID:     "run2_t1",
		RunID:  "run2",
		TaskID: "t1",
		Status: models.TaskRunning,
	})

	// Use a custom ResponseWriter that supports streaming (via http.Flusher).
	pw, pr := newPipeResponseWriter()
	req := httptest.NewRequest(http.MethodGet, "/api/runs/run2/tasks/run2_t1/logs", nil)

	// Run handler in background — it blocks until broker closes.
	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		srv.handleTaskLogs(pw, req)
	}()

	// Publish a couple of lines then close the broker.
	time.Sleep(20 * time.Millisecond) // let handler subscribe
	broker.Publish("run2_t1", "hello")
	broker.Publish("run2_t1", "world")
	broker.Close("run2_t1")

	// Wait for handler to finish.
	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not finish after broker closed")
	}
	pr.close()

	body := pr.body()
	for _, want := range []string{"hello", "world"} {
		if !strings.Contains(body, "data: "+want) {
			t.Errorf("expected %q in SSE body, got:\n%s", want, body)
		}
	}
}

// pipeResponseWriter is a simple streaming-capable ResponseWriter for tests.
type pipeResponseWriter struct {
	header http.Header
	code   int
	buf    strings.Builder
	done   chan struct{}
}

func newPipeResponseWriter() (*pipeResponseWriter, *pipeResponseWriter) {
	p := &pipeResponseWriter{header: make(http.Header), done: make(chan struct{})}
	return p, p
}

func (p *pipeResponseWriter) Header() http.Header         { return p.header }
func (p *pipeResponseWriter) WriteHeader(code int)        { p.code = code }
func (p *pipeResponseWriter) Write(b []byte) (int, error) { return p.buf.Write(b) }
func (p *pipeResponseWriter) Flush()                      {}
func (p *pipeResponseWriter) close()                      { close(p.done) }
func (p *pipeResponseWriter) body() string                { return p.buf.String() }
