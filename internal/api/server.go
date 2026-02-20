package api

import (
	"encoding/json"
	"io/fs"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/scheduler"
)

// Server encapsulates the dependencies for the HTTP API
type Server struct {
	store     *models.Store
	scheduler *scheduler.Scheduler
}

// NewServer creates a new API Server instance
func NewServer(store *models.Store, sched *scheduler.Scheduler) *Server {
	return &Server{
		store:     store,
		scheduler: sched,
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

	run, err := s.scheduler.TriggerDAG(dagID)
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

	offset := (page - 1) * limit

	runs, err := s.store.GetDagRuns(limit, offset, dagID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	total, err := s.store.GetDagRunsCount(dagID)
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

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tasks)
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
	mux.HandleFunc("/api/runs", corsMiddleware(s.handleGetRuns))
	// Generic handler for anything starting with /api/runs/ to catch /api/runs/{id}/tasks and retries
	mux.HandleFunc("/api/runs/", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/retry") && r.Method == http.MethodPost {
			s.handleRetryTask(w, r)
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
