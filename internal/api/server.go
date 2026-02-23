package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/alephmelo/nagare/internal/autoscaler"
	"github.com/alephmelo/nagare/internal/cluster"
	"github.com/alephmelo/nagare/internal/logbroker"
	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/scheduler"
	"github.com/alephmelo/nagare/internal/worker"
	"github.com/itchyny/gojq"
)

// Server encapsulates the dependencies for the HTTP API
type Server struct {
	store          *models.Store
	scheduler      *scheduler.Scheduler
	pool           *worker.Pool
	broker         *logbroker.Broker
	coordinator    *cluster.Coordinator   // optional; nil in standalone mode
	as             *autoscaler.Autoscaler // optional; nil when autoscaler not wired in
	allowedOrigins []string
	// apiKey is the shared secret that protects all /api/* routes except
	// /api/webhooks/.  Empty string disables API key enforcement.
	apiKey  string
	dagsDir string // directory where DAG YAML files are stored
}

// NewServer creates a new API Server instance
func NewServer(store *models.Store, sched *scheduler.Scheduler, pool *worker.Pool, broker *logbroker.Broker, allowedOrigins []string, apiKey string) *Server {
	return &Server{
		store:          store,
		scheduler:      sched,
		pool:           pool,
		broker:         broker,
		allowedOrigins: allowedOrigins,
		apiKey:         apiKey,
	}
}

// WithDAGsDir sets the directory path where DAG YAML files are stored.
// This enables the /api/dags/{id}/yaml endpoint to serve raw DAG source files.
func (s *Server) WithDAGsDir(dir string) {
	s.dagsDir = dir
}

// WithCoordinator attaches a cluster Coordinator so that /api/workers/* routes
// are served alongside the normal API. Call this before Start().
func (s *Server) WithCoordinator(c *cluster.Coordinator) {
	s.coordinator = c
}

// WithAutoscaler attaches an Autoscaler so that /api/autoscaler/* routes are
// served.  Call this before Start().
func (s *Server) WithAutoscaler(a *autoscaler.Autoscaler) {
	s.as = a
}

// corsMiddleware applies CORS headers based on the server's allowed-origins list.
//
// If allowedOrigins is non-empty, the incoming Origin header is matched against
// the list.  A matching origin is echoed back in Access-Control-Allow-Origin so
// that browsers accept the response.  Non-matching origins receive no CORS
// headers and are therefore blocked by the browser.
//
// If allowedOrigins is empty the middleware falls back to the wildcard "*" so
// that the server is usable out of the box without configuration (a warning is
// emitted at construction time via NewServer — see Start()).
func (s *Server) corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")

		allowedOrigin := ""
		if len(s.allowedOrigins) == 0 {
			// Wildcard fallback — no allowlist configured.
			allowedOrigin = "*"
		} else {
			for _, o := range s.allowedOrigins {
				if o == origin {
					allowedOrigin = o
					break
				}
			}
		}

		if allowedOrigin != "" {
			w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			// Tell the browser it may cache the preflight result for 10 minutes.
			w.Header().Set("Access-Control-Max-Age", "600")
			// Vary so proxies do not serve a cached response to the wrong origin.
			w.Header().Add("Vary", "Origin")
		}

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	}
}

// apiKeyMiddleware enforces Bearer token authentication on API routes.
// When apiKey is empty the middleware is a no-op (open access).
// Accepts the key via:
//   - Authorization: Bearer <key> header (preferred)
//   - ?token=<key> query parameter (fallback for EventSource/SSE clients that
//     cannot set custom headers)
//
// Uses constant-time comparison to prevent timing-based attacks.
func (s *Server) apiKeyMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.apiKey == "" {
			next(w, r)
			return
		}

		// Prefer Authorization header.
		token := ""
		auth := r.Header.Get("Authorization")
		if strings.HasPrefix(auth, "Bearer ") {
			token = strings.TrimPrefix(auth, "Bearer ")
		} else {
			// Fallback: ?token= query param (used by EventSource for SSE streams).
			token = r.URL.Query().Get("token")
		}

		if subtle.ConstantTimeCompare([]byte(token), []byte(s.apiKey)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (s *Server) handleGetDAGs(w http.ResponseWriter, r *http.Request) {
	dagsMap := s.scheduler.GetDAGs()

	type DAGResponse struct {
		*models.DAGDef
		Paused bool `json:"Paused"`
	}

	// Convert map to slice for frontend convenience
	dagsList := make([]DAGResponse, 0, len(dagsMap))
	for _, dag := range dagsMap {
		dagsList = append(dagsList, DAGResponse{
			DAGDef: dag,
			Paused: s.scheduler.IsDAGPaused(dag.ID),
		})
	}

	sort.Slice(dagsList, func(i, j int) bool {
		return dagsList[i].ID < dagsList[j].ID
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(dagsList)
}

func (s *Server) handleGetDAGErrors(w http.ResponseWriter, r *http.Request) {
	errorsMap := s.scheduler.GetDAGErrors()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(errorsMap)
}

// handleGetDAGYAML serves the raw YAML source of a loaded DAG.
// Route: GET /api/dags/{id}/yaml
func (s *Server) handleGetDAGYAML(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	dagID := parts[3]

	// Verify the DAG is actually loaded to avoid arbitrary file reads.
	dags := s.scheduler.GetDAGs()
	if _, ok := dags[dagID]; !ok {
		http.Error(w, "dag not found", http.StatusNotFound)
		return
	}

	if s.dagsDir == "" {
		http.Error(w, "dags directory not configured", http.StatusInternalServerError)
		return
	}

	// Walk the dags directory to find the file whose parsed DAG ID matches.
	entries, err := os.ReadDir(s.dagsDir)
	if err != nil {
		http.Error(w, "failed to read dags directory", http.StatusInternalServerError)
		return
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".yaml" {
			continue
		}
		filePath := filepath.Join(s.dagsDir, entry.Name())
		content, err := os.ReadFile(filePath)
		if err != nil {
			continue
		}
		// Quick check: parse just to match the ID without re-using the expanded
		// in-memory copy, so the raw on-disk content is returned as-is.
		dag, err := models.ParseDAG(content)
		if err != nil {
			continue
		}
		if dag.ID == dagID {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.Write(content) //nolint:errcheck
			return
		}
	}

	http.Error(w, "yaml source file not found", http.StatusNotFound)
}

func (s *Server) handleTriggerDAG(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 4 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	dagID := parts[3]

	run, err := s.scheduler.TriggerDAG(dagID, "manual", nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(run)
}

// handlePauseDAG handles POST /api/dags/{id}/pause
func (s *Server) handlePauseDAG(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 4 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	dagID := parts[3]

	if err := s.scheduler.PauseDAG(dagID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"dag_id": dagID, "paused": true})
}

// handleActivateDAG handles POST /api/dags/{id}/activate
func (s *Server) handleActivateDAG(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 4 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	dagID := parts[3]

	if err := s.scheduler.ActivateDAG(dagID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"dag_id": dagID, "paused": false})
}

func (s *Server) handleGetRuns(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	page := 1
	if p := query.Get("page"); p != "" {
		if parsed, err := strconv.Atoi(p); err == nil && parsed > 0 {
			page = parsed
		}
	}

	limit := 10
	if l := query.Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	dagID := query.Get("dag_id")
	if dagID == "all" {
		dagID = ""
	}

	status := query.Get("status")
	if status == "all" {
		status = ""
	}

	trigger := query.Get("trigger")
	if trigger == "all" {
		trigger = ""
	}

	offset := (page - 1) * limit

	runs, err := s.store.GetDagRuns(limit, offset, dagID, status, trigger)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	total, err := s.store.GetDagRunsCount(dagID, status, trigger)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"data":  runs,
		"total": total,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleGetStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.GetSystemStats()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	dagsMap := s.scheduler.GetDAGs()

	response := map[string]interface{}{
		"active_runs":     stats.ActiveRuns,
		"failed_runs_24h": stats.FailedRuns24h,
		"total_runs":      stats.TotalRuns,
		"loaded_dags":     len(dagsMap),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleGetRun(w http.ResponseWriter, r *http.Request) {
	// Route: GET /api/runs/{runID}
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 4 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	runID := parts[3]

	run, err := s.store.GetDagRun(runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(run)
}

func (s *Server) handleGetRunTasks(w http.ResponseWriter, r *http.Request) {
	// Simple path parameter extraction (e.g. /api/runs/run_1/tasks)
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	runID := parts[3]

	tasks, err := s.store.GetTaskInstancesByRun(runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// We enrich the TaskInstance with the Command from the DAG definition for the UI
	// and with resource metrics if available.
	type EnrichedTask struct {
		models.TaskInstance
		Command string              `json:"Command"`
		Metrics *models.TaskMetrics `json:"Metrics,omitempty"`
	}

	run, err := s.store.GetDagRun(runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	dags := s.scheduler.GetDAGs()
	dag, ok := dags[run.DAGID]
	var enriched []EnrichedTask

	for _, t := range tasks {
		cmd := ""
		if ok {
			for _, def := range dag.Tasks {
				if def.ID == t.TaskID {
					cmd = def.Command
					break
				}
			}
		}
		m, _ := s.store.GetTaskMetrics(t.ID)
		enriched = append(enriched, EnrichedTask{
			TaskInstance: t,
			Command:      cmd,
			Metrics:      m,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(enriched)
}

func (s *Server) handleGetTaskAttempts(w http.ResponseWriter, r *http.Request) {
	// Route: /api/runs/{runID}/tasks/{taskID}/attempts
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 7 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	runID := parts[3]
	taskID := parts[5]

	attempts, err := s.store.GetTaskAttempts(runID, taskID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// We enrich the TaskInstance with the Command from the DAG definition for the UI
	type EnrichedTask struct {
		models.TaskInstance
		Command string `json:"Command"`
	}

	run, err := s.store.GetDagRun(runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	dags := s.scheduler.GetDAGs()
	dag, ok := dags[run.DAGID]
	var enriched []EnrichedTask

	for _, t := range attempts {
		cmd := ""
		if ok {
			for _, def := range dag.Tasks {
				if def.ID == t.TaskID {
					cmd = def.Command
					break
				}
			}
		}
		enriched = append(enriched, EnrichedTask{
			TaskInstance: t,
			Command:      cmd,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(enriched)
}

func (s *Server) handleRetryTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Route mapping: /api/runs/{run_id}/tasks/{task_id}/retry
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 7 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	runID := parts[3]
	taskID := parts[5]

	err := s.scheduler.RetryTask(runID, taskID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Task queued for retry successfully"})
}

func (s *Server) handleKillRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 4 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	runID := parts[3]

	err := s.scheduler.KillDagRun(runID, s.pool)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Run killed successfully"})
}

func (s *Server) handleKillTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 7 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	// /api/runs/{run_id}/tasks/{task_id}/kill
	_ = parts[3] // runID
	taskID := parts[5]

	err := s.pool.KillTask(taskID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Task killed successfully"})
}

func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	// e.g. /api/webhooks/github -> Match against DAG Trigger configurations
	path := r.URL.Path

	var matchedDAG *models.DAGDef

	dags := s.scheduler.GetDAGs()
	for _, dag := range dags {
		if dag.Trigger != nil && dag.Trigger.Type == "webhook" {
			// e.g. match /api/webhooks/github
			if dag.Trigger.Path == path && dag.Trigger.Method == r.Method {
				matchedDAG = dag
				break
			}
		}
	}

	if matchedDAG == nil {
		http.Error(w, "No matching webhook trigger found", http.StatusNotFound)
		return
	}

	// Read the raw body once so we can both verify the HMAC signature and
	// decode the JSON payload without consuming the reader twice.
	rawBody, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	// HMAC-SHA256 signature verification.
	// Only enforced when the DAG's trigger defines a secret.
	if matchedDAG.Trigger.Secret != "" {
		sigHeader := matchedDAG.Trigger.SignatureHeader
		if sigHeader == "" {
			sigHeader = "X-Hub-Signature-256"
		}

		gotSig := r.Header.Get(sigHeader)
		if gotSig == "" {
			http.Error(w, "Missing signature header", http.StatusForbidden)
			return
		}

		// GitHub and compatible systems send "sha256=<hex digest>".
		// Accept both the bare hex and the "sha256=" prefixed form.
		hexSig := strings.TrimPrefix(gotSig, "sha256=")

		mac := hmac.New(sha256.New, []byte(matchedDAG.Trigger.Secret))
		mac.Write(rawBody)
		expectedSig := hex.EncodeToString(mac.Sum(nil))

		// Use constant-time comparison to prevent timing attacks.
		if subtle.ConstantTimeCompare([]byte(hexSig), []byte(expectedSig)) != 1 {
			http.Error(w, "Invalid signature", http.StatusForbidden)
			return
		}
	}

	// Parse JSON payload
	var payload interface{}
	if len(rawBody) > 0 {
		if err := json.Unmarshal(rawBody, &payload); err != nil {
			http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
			return
		}
	}

	// Extract Payload using gojq
	conf := make(map[string]string)
	if payload != nil && matchedDAG.Trigger.ExtractPayload != nil {
		for envKey, jqQuery := range matchedDAG.Trigger.ExtractPayload {
			query, err := gojq.Parse(jqQuery)
			if err != nil {
				log.Printf("Invalid jq query '%s' in DAG %s: %v", jqQuery, matchedDAG.ID, err)
				continue
			}

			iter := query.Run(payload)
			v, ok := iter.Next()
			if !ok {
				continue
			}
			if err, isErr := v.(error); isErr {
				log.Printf("Error extracting payload for %s with query %s: %v", matchedDAG.ID, jqQuery, err)
				continue
			}

			// Format value as string for environment variable
			switch val := v.(type) {
			case string:
				conf[envKey] = val
			case float64:
				// format floats without trailing zeroes if possible
				conf[envKey] = strconv.FormatFloat(val, 'f', -1, 64)
			case bool:
				conf[envKey] = strconv.FormatBool(val)
			case nil:
				conf[envKey] = ""
			default:
				// For objects/arrays, marshal back to JSON string
				b, _ := json.Marshal(val)
				conf[envKey] = string(b)
			}
		}
	}

	// Trigger the DAG
	run, err := s.scheduler.TriggerDAG(matchedDAG.ID, "webhook", conf)
	if err != nil {
		http.Error(w, "Failed to trigger DAG", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"message": "Webhook received, DAG triggered",
		"run_id":  run.ID,
	})
}

// handleTaskLogs streams log lines for a task instance using Server-Sent Events.
//
// Route: GET /api/runs/{runID}/tasks/{taskInstanceID}/logs
//
// Behaviour:
//   - If the task is not found → 404.
//   - If the task is already finished → replay stored output as SSE data lines,
//     then close the stream immediately.
//   - If the task is still running → subscribe to the broker, stream live lines,
//     and close the stream when the broker signals completion.
func (s *Server) handleTaskLogs(w http.ResponseWriter, r *http.Request) {
	// /api/runs/{runID}/tasks/{taskInstanceID}/logs  →  parts[5] = taskInstanceID
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 7 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	taskInstanceID := parts[5]

	inst, err := s.store.GetTaskInstance(taskInstanceID)
	if err != nil {
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, canFlush := w.(http.Flusher)

	sendLine := func(line string) {
		fmt.Fprintf(w, "data: %s\n\n", line) //nolint:gosec // G705: SSE is an internal API behind auth; not a browser-rendered context
		if canFlush {
			flusher.Flush()
		}
	}

	// For a finished task, replay the stored output and close.
	if inst.Status != models.TaskRunning && inst.Status != models.TaskQueued {
		for _, line := range strings.Split(strings.TrimRight(inst.Output, "\n"), "\n") {
			if line != "" {
				sendLine(line)
			}
		}
		return
	}

	// Task is running (or queued): subscribe to the broker and stream live lines.
	// Subscribe also replays any lines already buffered (history).
	ch, unsub := s.broker.Subscribe(taskInstanceID)
	defer unsub()

	ctx := r.Context()
	for {
		select {
		case line, ok := <-ch:
			if !ok {
				// Broker closed the channel — task finished.
				return
			}
			sendLine(line)
		case <-ctx.Done():
			// Client disconnected.
			return
		}
	}
}

// handleGetTaskMetrics returns metrics for a specific task instance.
// Route: GET /api/metrics/tasks/{taskInstanceID}
func (s *Server) handleGetTaskMetrics(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	// /api/metrics/tasks/{id} → parts = ["", "api", "metrics", "tasks", "{id}"]
	if len(parts) < 5 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	taskInstanceID := parts[4]

	m, err := s.store.GetTaskMetrics(taskInstanceID)
	if err != nil {
		http.Error(w, "Metrics not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(m)
}

// handleGetRunMetrics returns all task metrics for a specific run.
// Route: GET /api/metrics/runs/{runID}
func (s *Server) handleGetRunMetrics(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	// /api/metrics/runs/{id} → parts = ["", "api", "metrics", "runs", "{id}"]
	if len(parts) < 5 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	runID := parts[4]

	metrics, err := s.store.GetMetricsByRunID(runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

// handleGetDAGMetrics returns recent task metrics and aggregate stats for a DAG.
// Route: GET /api/metrics/dags/{dagID}?since=24h&limit=200
func (s *Server) handleGetDAGMetrics(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	// /api/metrics/dags/{id} → parts = ["", "api", "metrics", "dags", "{id}"]
	if len(parts) < 5 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	dagID := parts[4]

	since, limit := parseSinceAndLimit(r)

	agg, err := s.store.GetAggregateMetrics(dagID, since)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	series, err := s.store.GetMetricsTimeSeries(dagID, since, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"aggregate":   agg,
		"time_series": series,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleGetMetricsOverview returns system-wide metrics for the Metrics dashboard.
// Route: GET /api/metrics/overview?since=24h
func (s *Server) handleGetMetricsOverview(w http.ResponseWriter, r *http.Request) {
	since, _ := parseSinceAndLimit(r)

	overview, err := s.store.GetOverviewMetrics(since)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(overview)
}

// handleGetMetricsTimeSeries returns global time-series data for charting.
// Route: GET /api/metrics/timeseries?dag_id=...&since=24h&limit=500
func (s *Server) handleGetMetricsTimeSeries(w http.ResponseWriter, r *http.Request) {
	dagID := r.URL.Query().Get("dag_id")
	since, limit := parseSinceAndLimit(r)

	series, err := s.store.GetMetricsTimeSeries(dagID, since, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(series)
}

// handleAutoscalerStatus returns a snapshot of the autoscaler's current state.
// Route: GET /api/autoscaler/status
func (s *Server) handleAutoscalerStatus(w http.ResponseWriter, r *http.Request) {
	if s.as == nil {
		http.Error(w, "autoscaler not configured", http.StatusServiceUnavailable)
		return
	}
	snap := s.as.Status()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(snap)
}

// handleAutoscalerCosts returns aggregate cost metrics for all cloud instances.
// Route: GET /api/autoscaler/costs
func (s *Server) handleAutoscalerCosts(w http.ResponseWriter, r *http.Request) {
	summary, err := s.store.GetCloudCostSummary()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(summary)
}

// handleAutoscalerEnable enables the autoscaler at runtime.
// Route: POST /api/autoscaler/enable
//
// This endpoint is intended for operators who want to enable autoscaling
// without restarting the master.  It is a no-op when the autoscaler is
// already enabled.
func (s *Server) handleAutoscalerEnable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.as == nil {
		http.Error(w, "autoscaler not configured", http.StatusServiceUnavailable)
		return
	}
	snap := s.as.Status()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"enabled": snap.Enabled,
		"message": "use the autoscaler.enabled flag in nagare.yaml to permanently enable autoscaling",
	})
}

// parseSinceAndLimit is a helper that extracts ?since= (duration like "24h", "7d")
// and ?limit= from a request, returning a time.Time and int with sensible defaults.
func parseSinceAndLimit(r *http.Request) (time.Time, int) {
	sinceStr := r.URL.Query().Get("since")
	if sinceStr == "" {
		sinceStr = "24h"
	}
	// Allow shorthand like "7d" in addition to standard Go durations.
	sinceStr = strings.ReplaceAll(sinceStr, "d", "h") // crude: 7d → 168h (7*24=168)
	// Handle days properly
	if strings.HasSuffix(r.URL.Query().Get("since"), "d") {
		daysStr := strings.TrimSuffix(r.URL.Query().Get("since"), "d")
		if days, err := strconv.Atoi(daysStr); err == nil {
			return time.Now().Add(-time.Duration(days) * 24 * time.Hour), parseLimit(r)
		}
	}
	d, err := time.ParseDuration(sinceStr)
	if err != nil {
		d = 24 * time.Hour
	}
	return time.Now().Add(-d), parseLimit(r)
}

func parseLimit(r *http.Request) int {
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			return v
		}
	}
	return 500
}

// Start launches the HTTP server
func (s *Server) Start(addr string, frontendFS fs.FS) error {
	if len(s.allowedOrigins) == 0 {
		log.Println("WARNING: cors.allowed_origins is not configured — CORS is open to all origins (*). Set allowed_origins in nagare.yaml for production use.")
	}
	if s.apiKey == "" {
		log.Println("WARNING: api_key is not configured — all API routes are unauthenticated. Set api_key in nagare.yaml or use --api-key for production use.")
	}

	// auth is a convenience alias that composes apiKeyMiddleware + corsMiddleware.
	// Applied to every /api/* route except /api/webhooks/ (which uses HMAC).
	auth := func(h http.HandlerFunc) http.HandlerFunc {
		return s.apiKeyMiddleware(s.corsMiddleware(h))
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/api/stats", auth(s.handleGetStats))
	mux.HandleFunc("/api/dags", auth(s.handleGetDAGs))
	mux.HandleFunc("/api/dags/errors", auth(s.handleGetDAGErrors))
	mux.HandleFunc("/api/dags/", auth(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/runs") && r.Method == http.MethodPost {
			s.handleTriggerDAG(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/yaml") && r.Method == http.MethodGet {
			s.handleGetDAGYAML(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/pause") && r.Method == http.MethodPost {
			s.handlePauseDAG(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/activate") && r.Method == http.MethodPost {
			s.handleActivateDAG(w, r)
			return
		}
		http.NotFound(w, r)
	}))
	// Webhooks are exempt from API key auth — they use per-DAG HMAC-SHA256.
	mux.HandleFunc("/api/webhooks/", s.handleWebhook)

	mux.HandleFunc("/api/runs", auth(s.handleGetRuns))
	// Generic handler for anything starting with /api/runs/ to catch /api/runs/{id}/tasks and retries
	mux.HandleFunc("/api/runs/", auth(func(w http.ResponseWriter, r *http.Request) {
		if (strings.HasSuffix(r.URL.Path, "/logs") || strings.HasSuffix(r.URL.Path, "/logs/")) && r.Method == http.MethodGet {
			s.handleTaskLogs(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/retry") && r.Method == http.MethodPost {
			s.handleRetryTask(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/kill") && r.Method == http.MethodPost {
			if strings.Contains(r.URL.Path, "/tasks/") {
				s.handleKillTask(w, r)
			} else {
				s.handleKillRun(w, r)
			}
			return
		}
		if strings.HasSuffix(r.URL.Path, "/attempts") {
			s.handleGetTaskAttempts(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/tasks") {
			s.handleGetRunTasks(w, r)
			return
		}
		// GET /api/runs/{id} — exactly 4 path segments: ["", "api", "runs", "{id}"]
		if r.Method == http.MethodGet && len(strings.Split(strings.TrimSuffix(r.URL.Path, "/"), "/")) == 4 {
			s.handleGetRun(w, r)
			return
		}
		http.NotFound(w, r)
	}))

	// Metrics endpoints
	mux.HandleFunc("/api/metrics/overview", auth(s.handleGetMetricsOverview))
	mux.HandleFunc("/api/metrics/timeseries", auth(s.handleGetMetricsTimeSeries))
	mux.HandleFunc("/api/metrics/tasks/", auth(s.handleGetTaskMetrics))
	mux.HandleFunc("/api/metrics/runs/", auth(s.handleGetRunMetrics))
	mux.HandleFunc("/api/metrics/dags/", auth(s.handleGetDAGMetrics))

	// Autoscaler endpoints
	mux.HandleFunc("/api/autoscaler/status", auth(s.handleAutoscalerStatus))
	mux.HandleFunc("/api/autoscaler/costs", auth(s.handleAutoscalerCosts))
	mux.HandleFunc("/api/autoscaler/enable", auth(s.handleAutoscalerEnable))

	if frontendFS != nil {
		mux.Handle("/", http.FileServer(http.FS(frontendFS)))
	}

	// Mount cluster worker endpoints when running as a master node.
	if s.coordinator != nil {
		mux.Handle("/api/workers", s.coordinator.Handler())
		mux.Handle("/api/workers/", s.coordinator.Handler())
	}

	log.Printf("Starting Nagare API on %s", addr)
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
		// Prevent slow-loris / header-flooding attacks and stale keepalive
		// connections from exhausting goroutines. WriteTimeout is intentionally
		// absent so that long-lived SSE streams (handleTaskLogs) are not killed.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	return srv.ListenAndServe()
}
