package api

import (
	"bufio"
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

// readSSELines reads from an SSE response until the channel is closed or the
// deadline passes. It returns the data values (stripping the "data: " prefix).
func readSSELines(t *testing.T, resp *http.Response, deadline time.Duration) []string {
	t.Helper()
	var lines []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				lines = append(lines, strings.TrimPrefix(line, "data: "))
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(deadline):
		t.Error("timed out reading SSE response")
	}
	return lines
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
