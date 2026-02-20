package models

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// RunStatus represents the state of a DagRun
type RunStatus string

const (
	RunRunning RunStatus = "running"
	RunSuccess RunStatus = "success"
	RunFailed  RunStatus = "failed"
)

// TaskStatus represents the state of a TaskInstance
type TaskStatus string

// SystemStats represents the high-level dashboard health metrics
type SystemStats struct {
	ActiveRuns    int `json:"active_runs"`
	FailedRuns24h int `json:"failed_runs_24h"`
	TotalRuns     int `json:"total_runs"`
}

const (
	TaskPending TaskStatus = "pending"
	TaskQueued  TaskStatus = "queued"
	TaskRunning TaskStatus = "running"
	TaskSuccess TaskStatus = "success"
	TaskFailed  TaskStatus = "failed"
)

// DagRun represents a single execution of a DAG
type DagRun struct {
	ID          string
	DAGID       string
	Status      RunStatus
	ExecDate    time.Time
	TriggerType string
	CreatedAt   time.Time
	CompletedAt *time.Time
}

// TaskInstance represents the execution of a specific task within a DagRun
type TaskInstance struct {
	ID        string
	RunID     string
	TaskID    string
	Status    TaskStatus
	Output    string
	Attempt   int
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Store handles all database operations for the scheduler
type Store struct {
	db *sql.DB
}

// NewStore initializes an SQLite database and creates necessary tables
func NewStore(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	store := &Store{db: db}

	if err := store.InitSchema(); err != nil {
		db.Close()
		return nil, err
	}

	return store, nil
}

// Close closes the database connection
func (s *Store) Close() error {
	return s.db.Close()
}

// InitSchema creates the tables if they don't exist
func (s *Store) InitSchema() error {
	dagRunsSchema := `
	CREATE TABLE IF NOT EXISTS dag_runs (
		id TEXT PRIMARY KEY,
		dag_id TEXT NOT NULL,
		status TEXT NOT NULL,
		exec_date DATETIME NOT NULL,
		trigger_type TEXT DEFAULT 'scheduled',
		created_at DATETIME NOT NULL,
		completed_at DATETIME
	);`

	taskInstancesSchema := `
	CREATE TABLE IF NOT EXISTS task_instances (
		id TEXT PRIMARY KEY,
		run_id TEXT NOT NULL,
		task_id TEXT NOT NULL,
		status TEXT NOT NULL,
		output TEXT,
		attempt INT NOT NULL DEFAULT 1,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL,
		FOREIGN KEY(run_id) REFERENCES dag_runs(id)
	);`

	if _, err := s.db.Exec(dagRunsSchema); err != nil {
		return err
	}
	if _, err := s.db.Exec(taskInstancesSchema); err != nil {
		return err
	}

	// Active Migrations — ignore errors if columns already exist
	s.db.Exec(`ALTER TABLE dag_runs ADD COLUMN trigger_type TEXT DEFAULT 'scheduled'`)
	s.db.Exec(`ALTER TABLE task_instances ADD COLUMN attempt INT NOT NULL DEFAULT 1`)

	return nil
}

// CreateDagRun inserts a new DagRun into the database
func (s *Store) CreateDagRun(run *DagRun) error {
	query := `INSERT INTO dag_runs (id, dag_id, status, exec_date, trigger_type, created_at, completed_at) VALUES (?, ?, ?, ?, ?, ?, ?)`
	_, err := s.db.Exec(query, run.ID, run.DAGID, run.Status, run.ExecDate, run.TriggerType, run.CreatedAt, run.CompletedAt)
	return err
}

// UpdateDagRunStatus updates the status of a specific DagRun
func (s *Store) UpdateDagRunStatus(runID string, status RunStatus) error {
	var completedAt *time.Time
	if status == RunSuccess || status == RunFailed {
		t := time.Now()
		completedAt = &t
	}

	query := `UPDATE dag_runs SET status = ?, completed_at = ? WHERE id = ?`
	_, err := s.db.Exec(query, status, completedAt, runID)
	return err
}

// GetDagRuns retrieves recent DAG runs, optionally filtered by dagID, status, and triggerType, with pagination
func (s *Store) GetDagRuns(limit int, offset int, dagID string, status string, triggerType string) ([]DagRun, error) {
	var query string
	var rows *sql.Rows
	var err error

	where := ""
	params := []interface{}{}

	if dagID != "" && dagID != "all" {
		where = "WHERE dag_id = ?"
		params = append(params, dagID)
	}

	if status != "" && status != "all" {
		if where == "" {
			where = "WHERE status = ?"
		} else {
			where += " AND status = ?"
		}
		params = append(params, status)
	}

	if triggerType != "" && triggerType != "all" {
		if where == "" {
			where = "WHERE trigger_type = ?"
		} else {
			where += " AND trigger_type = ?"
		}
		params = append(params, triggerType)
	}

	query = fmt.Sprintf(`SELECT id, dag_id, status, exec_date, trigger_type, created_at, completed_at FROM dag_runs %s ORDER BY created_at DESC LIMIT ? OFFSET ?`, where)
	params = append(params, limit, offset)

	rows, err = s.db.Query(query, params...)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []DagRun
	for rows.Next() {
		var r DagRun
		if err := rows.Scan(&r.ID, &r.DAGID, &r.Status, &r.ExecDate, &r.TriggerType, &r.CreatedAt, &r.CompletedAt); err != nil {
			return nil, err
		}
		runs = append(runs, r)
	}
	return runs, nil
}

// GetDagRunsCount gets the total number of runs, optionally filtered by dagID, status, and triggerType
func (s *Store) GetDagRunsCount(dagID string, status string, triggerType string) (int, error) {
	var query string
	where := ""
	params := []interface{}{}

	if dagID != "" && dagID != "all" {
		where = "WHERE dag_id = ?"
		params = append(params, dagID)
	}

	if status != "" && status != "all" {
		if where == "" {
			where = "WHERE status = ?"
		} else {
			where += " AND status = ?"
		}
		params = append(params, status)
	}

	if triggerType != "" && triggerType != "all" {
		if where == "" {
			where = "WHERE trigger_type = ?"
		} else {
			where += " AND trigger_type = ?"
		}
		params = append(params, triggerType)
	}

	query = fmt.Sprintf(`SELECT COUNT(*) FROM dag_runs %s`, where)
	row := s.db.QueryRow(query, params...)

	var count int
	err := row.Scan(&count)
	return count, err
}

// GetSystemStats retrieves high-level metrics for the dashboard banner
func (s *Store) GetSystemStats() (*SystemStats, error) {
	stats := &SystemStats{}

	// 1. Total Runs
	err := s.db.QueryRow(`SELECT COUNT(*) FROM dag_runs`).Scan(&stats.TotalRuns)
	if err != nil {
		return nil, err
	}

	// 2. Active Runs
	err = s.db.QueryRow(`SELECT COUNT(*) FROM dag_runs WHERE status = ?`, RunRunning).Scan(&stats.ActiveRuns)
	if err != nil {
		return nil, err
	}

	// 3. Failed Runs in last 24h
	yesterday := time.Now().Add(-24 * time.Hour).Format(time.RFC3339)
	err = s.db.QueryRow(`SELECT COUNT(*) FROM dag_runs WHERE status = ? AND created_at >= ?`, RunFailed, yesterday).Scan(&stats.FailedRuns24h)
	// SQLite created_at strings are comparable directly in iso8601
	err = s.db.QueryRow(`SELECT COUNT(*) FROM dag_runs WHERE status = ? AND created_at >= ?`, RunFailed, yesterday).Scan(&stats.FailedRuns24h)
	if err != nil {
		return nil, err
	}

	return stats, nil
}

// GetActiveDagRuns retrieves all DAG runs currently markes as 'running'
func (s *Store) GetActiveDagRuns() ([]DagRun, error) {
	query := `SELECT id, dag_id, status, exec_date, trigger_type, created_at, completed_at FROM dag_runs WHERE status = ?`
	rows, err := s.db.Query(query, RunRunning)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []DagRun
	for rows.Next() {
		var r DagRun
		if err := rows.Scan(&r.ID, &r.DAGID, &r.Status, &r.ExecDate, &r.TriggerType, &r.CreatedAt, &r.CompletedAt); err != nil {
			return nil, err
		}
		runs = append(runs, r)
	}
	return runs, nil
}

// GetTaskInstancesByRun retrieves all task instances for a specific run.
// Returns the latest attempt for each task (for backward compatibility).
func (s *Store) GetTaskInstancesByRun(runID string) ([]TaskInstance, error) {
	return s.GetLatestTaskAttempts(runID)
}

// GetLatestTaskAttempts returns the most recent attempt for each task in a run.
func (s *Store) GetLatestTaskAttempts(runID string) ([]TaskInstance, error) {
	query := `
		SELECT id, run_id, task_id, status, COALESCE(output,''), attempt, created_at, updated_at
		FROM task_instances
		WHERE run_id = ?
		  AND attempt = (
			SELECT MAX(t2.attempt)
			FROM task_instances t2
			WHERE t2.run_id = task_instances.run_id
			  AND t2.task_id = task_instances.task_id
		  )
		ORDER BY created_at ASC`
	rows, err := s.db.Query(query, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return s.scanTaskInstances(rows)
}

// GetTaskAttempts returns all attempts for a single task within a run, ordered oldest first.
func (s *Store) GetTaskAttempts(runID, taskID string) ([]TaskInstance, error) {
	query := `
		SELECT id, run_id, task_id, status, COALESCE(output,''), attempt, created_at, updated_at
		FROM task_instances
		WHERE run_id = ? AND task_id = ?
		ORDER BY attempt ASC`
	rows, err := s.db.Query(query, runID, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return s.scanTaskInstances(rows)
}

// CreateNewTaskAttempt inserts a new queued TaskInstance row for a task,
// incrementing the attempt counter. Returns the new instance's ID.
func (s *Store) CreateNewTaskAttempt(runID, taskID string) (string, error) {
	var maxAttempt int
	err := s.db.QueryRow(
		`SELECT COALESCE(MAX(attempt), 0) FROM task_instances WHERE run_id = ? AND task_id = ?`,
		runID, taskID,
	).Scan(&maxAttempt)
	if err != nil {
		return "", fmt.Errorf("finding max attempt for %s/%s: %w", runID, taskID, err)
	}

	newAttempt := maxAttempt + 1
	newID := fmt.Sprintf("%s_%s_%d", runID, taskID, newAttempt)
	now := time.Now()

	_, err = s.db.Exec(
		`INSERT INTO task_instances (id, run_id, task_id, status, attempt, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		newID, runID, taskID, TaskQueued, newAttempt, now, now,
	)
	if err != nil {
		return "", fmt.Errorf("inserting new attempt row: %w", err)
	}
	return newID, nil
}

// scanTaskInstances is a shared helper to scan rows into []TaskInstance.
func (s *Store) scanTaskInstances(rows *sql.Rows) ([]TaskInstance, error) {
	var tasks []TaskInstance
	for rows.Next() {
		var ti TaskInstance
		if err := rows.Scan(&ti.ID, &ti.RunID, &ti.TaskID, &ti.Status, &ti.Output, &ti.Attempt, &ti.CreatedAt, &ti.UpdatedAt); err != nil {
			return nil, err
		}
		tasks = append(tasks, ti)
	}
	return tasks, rows.Err()
}

// CreateTaskInstance inserts a new TaskInstance into the database
func (s *Store) CreateTaskInstance(ti *TaskInstance) error {
	if ti.Attempt == 0 {
		ti.Attempt = 1
	}
	query := `INSERT INTO task_instances (id, run_id, task_id, status, output, attempt, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := s.db.Exec(query, ti.ID, ti.RunID, ti.TaskID, ti.Status, ti.Output, ti.Attempt, ti.CreatedAt, ti.UpdatedAt)
	return err
}

// GetTasksByStatus retrieves all TaskInstances with a specific status
func (s *Store) GetTasksByStatus(status TaskStatus) ([]TaskInstance, error) {
	query := `SELECT id, run_id, task_id, status, output, created_at, updated_at FROM task_instances WHERE status = ?`
	rows, err := s.db.Query(query, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []TaskInstance
	for rows.Next() {
		var ti TaskInstance
		var output sql.NullString
		if err := rows.Scan(&ti.ID, &ti.RunID, &ti.TaskID, &ti.Status, &output, &ti.CreatedAt, &ti.UpdatedAt); err != nil {
			return nil, err
		}
		if output.Valid {
			ti.Output = output.String
		}
		tasks = append(tasks, ti)
	}
	return tasks, nil
}

// GetQueuedTasks retrieves all TaskInstances with 'queued' status
func (s *Store) GetQueuedTasks() ([]TaskInstance, error) {
	return s.GetTasksByStatus(TaskQueued)
}

// GetDagRun retrieves a DagRun by ID
func (s *Store) GetDagRun(runID string) (*DagRun, error) {
	query := `SELECT id, dag_id, status, exec_date, trigger_type, created_at, completed_at FROM dag_runs WHERE id = ?`
	row := s.db.QueryRow(query, runID)

	var r DagRun
	if err := row.Scan(&r.ID, &r.DAGID, &r.Status, &r.ExecDate, &r.TriggerType, &r.CreatedAt, &r.CompletedAt); err != nil {
		return nil, err
	}
	return &r, nil
}

// GetTaskStatus retrieves the status of the latest attempt for a specific task within a run
func (s *Store) GetTaskStatus(runID, taskID string) (TaskStatus, error) {
	query := `SELECT status FROM task_instances WHERE run_id = ? AND task_id = ? ORDER BY attempt DESC LIMIT 1`
	var status TaskStatus
	err := s.db.QueryRow(query, runID, taskID).Scan(&status)
	return status, err
}

// UpdateTaskInstanceStatus updates the status of a specific task
func (s *Store) UpdateTaskInstanceStatus(taskID string, status TaskStatus) error {
	query := `UPDATE task_instances SET status = ?, updated_at = ? WHERE id = ?`
	_, err := s.db.Exec(query, status, time.Now(), taskID)
	return err
}

// UpdateTaskInstanceStatusAndOutput updates the status AND the shell output
func (s *Store) UpdateTaskInstanceStatusAndOutput(taskID string, status TaskStatus, output string) error {
	query := `UPDATE task_instances SET status = ?, output = ?, updated_at = ? WHERE id = ?`
	_, err := s.db.Exec(query, status, output, time.Now(), taskID)
	return err
}

// GetDagRuns retrieves recent DAG runs ordered by creation mostly recent first
