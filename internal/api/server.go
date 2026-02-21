package api

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/alephmelo/nagare/internal/logbroker"
	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/scheduler"
	"github.com/alephmelo/nagare/internal/worker"
	"github.com/itchyny/gojq"
)

// Server encapsulates the dependencies for the HTTP API
type Server struct {
	store     *models.Store
	scheduler *scheduler.Scheduler
	pool      *worker.Pool
	broker    *logbroker.Broker
}

// NewServer creates a new API Server instance
func NewServer(store *models.Store, sched *scheduler.Scheduler, pool *worker.Pool, broker *logbroker.Broker) *Server {
	return &Server{
		store:     store,
		scheduler: sched,
		pool:      pool,
		broker:    broker,
	}
}

// corsMiddleware allows cross-origin requests from the Next.js frontend
func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	}
}

func (s *Server) handleGetDAGs(w http.ResponseWriter, r *http.Request) {
	dagsMap := s.scheduler.GetDAGs()

	// Convert map to slice for frontend convenience
	dagsList := make([]*models.DAGDef, 0, len(dagsMap))
	for _, dag := range dagsMap {
		dagsList = append(dagsList, dag)
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
		enriched = append(enriched, EnrichedTask{
			TaskInstance: t,
			Command:      cmd,
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

	// Parse JSON payload
	var payload interface{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		// Log but continue if empty body (some webhooks might just be pings)
		if err.Error() != "EOF" {
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
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, canFlush := w.(http.Flusher)

	sendLine := func(line string) {
		fmt.Fprintf(w, "data: %s\n\n", line)
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

// Start launches the HTTP server
func (s *Server) Start(addr string, frontendFS fs.FS) error {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/stats", corsMiddleware(s.handleGetStats))
	mux.HandleFunc("/api/dags", corsMiddleware(s.handleGetDAGs))
	mux.HandleFunc("/api/dags/errors", corsMiddleware(s.handleGetDAGErrors))
	mux.HandleFunc("/api/dags/", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/runs") && r.Method == http.MethodPost {
			s.handleTriggerDAG(w, r)
			return
		}
		http.NotFound(w, r)
	}))
	mux.HandleFunc("/api/webhooks/", s.handleWebhook) // Unauthenticated endpoint intentionally (can add auth later)

	mux.HandleFunc("/api/runs", corsMiddleware(s.handleGetRuns))
	// Generic handler for anything starting with /api/runs/ to catch /api/runs/{id}/tasks and retries
	mux.HandleFunc("/api/runs/", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
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
		http.NotFound(w, r)
	}))

	if frontendFS != nil {
		mux.Handle("/", http.FileServer(http.FS(frontendFS)))
	}

	log.Printf("Starting Nagare API on %s", addr)
	return http.ListenAndServe(addr, mux)
}
