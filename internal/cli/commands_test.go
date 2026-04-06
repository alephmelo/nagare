package cli

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// captureStdout temporarily redirects os.Stdout to capture everything printed
// during the execution of function f.
func captureStdout(f func() error) (string, error) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := f()

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r) //nolint:errcheck // safe in test

	return buf.String(), err
}

// setupMockServer creates an httptest server with canned responses
func setupMockServer(handlers map[string]http.HandlerFunc) *httptest.Server {
	mux := http.NewServeMux()
	for pattern, handler := range handlers {
		mux.HandleFunc(pattern, handler)
	}
	// Fallback to 404 for unhandled routes to catch mistakes
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	return httptest.NewServer(mux)
}

func TestListCommands_TableOutput(t *testing.T) {
	ts := setupMockServer(map[string]http.HandlerFunc{
		"GET /api/dags": func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`[{"id":"dag_1","schedule":"@daily","Paused":false,"tasks":[{"id":"t1"}]}]`)) //nolint:errcheck
		},
		"GET /api/runs": func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`{"data":[{"ID":"run_1","DAGID":"dag_1","Status":"success","TriggerType":"manual","CreatedAt":"2023-01-01T12:00:00Z"}],"total":1}`)) //nolint:errcheck
		},
		"GET /api/runs/run_1/tasks": func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`[{"ID":"run_1_t1","TaskID":"t1","Status":"running","Attempt":1,"Command":"echo hello"}]`)) //nolint:errcheck
		},
		"GET /api/workers": func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`[{"worker_id":"w1","hostname":"host1","pools":["default"],"status":"active","active_tasks":1,"max_tasks":4}]`)) //nolint:errcheck
		},
		"GET /api/stats": func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`{"active_runs":5,"total_runs":100}`)) //nolint:errcheck
		},
	})
	defer ts.Close()

	client := NewClient(ts.URL, "")

	tests := []struct {
		name     string
		run      func() error
		wantText []string // Substrings we expect to see in formatted stdOut
	}{
		{
			name: "dags command",
			run:  func() error { return cmdDags(client, false) },
			wantText: []string{
				"DAG ID", "SCHEDULE", "TASKS", "STATUS",
				"dag_1", "@daily", "1", "active",
			},
		},
		{
			name: "runs command",
			run:  func() error { return cmdRuns(client, "", "", 10, false) },
			wantText: []string{
				"RUN ID", "DAG", "STATUS", "TRIGGER", "CREATED",
				"run_1", "dag_1", "success", "manual", "2023-01-01",
				"Showing 1 of 1 total runs",
			},
		},
		{
			name: "tasks command",
			run:  func() error { return cmdTasks(client, "run_1", false) },
			wantText: []string{
				"INSTANCE ID", "TASK", "STATUS", "ATTEMPT", "COMMAND",
				"run_1_t1", "t1", "running", "1", "echo hello",
			},
		},
		{
			name: "workers command",
			run:  func() error { return cmdWorkers(client, false) },
			wantText: []string{
				"WORKER ID", "HOSTNAME", "POOLS", "STATUS", "TASKS",
				"w1", "host1", "default", "active", "1/4",
			},
		},
		{
			name: "stats command",
			run:  func() error { return cmdStats(client, false) },
			wantText: []string{
				"active_runs", "5",
				"total_runs", "100",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := captureStdout(tt.run)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for _, w := range tt.wantText {
				if !strings.Contains(out, w) {
					t.Errorf("expected string %q not found in output:\n%s", w, out)
				}
			}
		})
	}
}

func TestListCommands_JSONOutput(t *testing.T) {
	ts := setupMockServer(map[string]http.HandlerFunc{
		"GET /api/dags": func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`{"raw": "json"}`)) //nolint:errcheck
		},
	})
	defer ts.Close()

	client := NewClient(ts.URL, "")

	out, err := captureStdout(func() error {
		return cmdDags(client, true)
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(out, `{"raw": "json"}`) {
		t.Errorf("expected raw json string, got \n%s", out)
	}
}

func TestActionCommands(t *testing.T) {
	ts := setupMockServer(map[string]http.HandlerFunc{
		"POST /api/dags/my_dag/runs": func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`{"ID":"new_run_123"}`)) //nolint:errcheck
		},
		"POST /api/dags/my_dag/pause": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		},
		"POST /api/dags/my_dag/activate": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		},
		"POST /api/runs/my_run/kill": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		},
		"POST /api/runs/my_run/tasks/my_task/kill": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		},
		"POST /api/runs/my_run/tasks/my_task/retry": func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		},
	})
	defer ts.Close()

	client := NewClient(ts.URL, "")

	tests := []struct {
		name     string
		run      func() error
		wantText string // Substring we expect to see
	}{
		{
			name:     "trigger",
			run:      func() error { return cmdTrigger(client, "my_dag", false) },
			wantText: "Triggered DAG my_dag → run new_run_123",
		},
		{
			name:     "pause",
			run:      func() error { return cmdPause(client, "my_dag") },
			wantText: "Paused DAG my_dag",
		},
		{
			name:     "activate",
			run:      func() error { return cmdActivate(client, "my_dag") },
			wantText: "Activated DAG my_dag",
		},
		{
			name:     "kill run",
			run:      func() error { return cmdKillRun(client, "my_run") },
			wantText: "Killed run my_run",
		},
		{
			name:     "kill task",
			run:      func() error { return cmdKillTask(client, "my_run", "my_task") },
			wantText: "Killed task my_task in run my_run",
		},
		{
			name:     "retry task",
			run:      func() error { return cmdRetry(client, "my_run", "my_task") },
			wantText: "Queued retry for task my_task in run my_run",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := captureStdout(tt.run)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !strings.Contains(out, tt.wantText) {
				t.Errorf("expected %q not found in output:\n%s", tt.wantText, out)
			}
		})
	}
}

func TestCmdLogs(t *testing.T) {
	ts := setupMockServer(map[string]http.HandlerFunc{
		"GET /api/runs/my_run/tasks/my_task/logs": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Write([]byte("data: first log line\n\n"))   //nolint:errcheck
			w.Write([]byte("data: second log line\n\n"))  //nolint:errcheck
			w.Write([]byte("data: stream complete\n\n")) //nolint:errcheck
		},
	})
	defer ts.Close()
	client := NewClient(ts.URL, "")

	out, err := captureStdout(func() error {
		return cmdLogs(client, "my_run", "my_task")
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedLines := []string{"first log line", "second log line", "stream complete"}
	for _, expected := range expectedLines {
		if !strings.Contains(out, expected) {
			t.Errorf("expected line %q not found in output: \n%s", expected, out)
		}
	}
}
