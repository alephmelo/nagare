package models

import (
	"database/sql"
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

	// Active Migrations
	// Ignore error if column already exists
	s.db.Exec(`ALTER TABLE dag_runs ADD COLUMN trigger_type TEXT DEFAULT 'scheduled'`)

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

// GetDagRuns retrieves recent DAG runs, optionally filtered by dagID, with pagination
func (s *Store) GetDagRuns(limit int, offset int, dagID string) ([]DagRun, error) {
	var query string
	var rows *sql.Rows
	var err error

	if dagID != "" {
		query = `SELECT id, dag_id, status, exec_date, trigger_type, created_at, completed_at FROM dag_runs WHERE dag_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?`
		rows, err = s.db.Query(query, dagID, limit, offset)
	} else {
		query = `SELECT id, dag_id, status, exec_date, trigger_type, created_at, completed_at FROM dag_runs ORDER BY created_at DESC LIMIT ? OFFSET ?`
		rows, err = s.db.Query(query, limit, offset)
	}

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

// GetDagRunsCount gets the total number of runs, optionally filtered by dagID
func (s *Store) GetDagRunsCount(dagID string) (int, error) {
	var query string
	var row *sql.Row

	if dagID != "" {
		query = `SELECT COUNT(*) FROM dag_runs WHERE dag_id = ?`
		row = s.db.QueryRow(query, dagID)
	} else {
		query = `SELECT COUNT(*) FROM dag_runs`
		row = s.db.QueryRow(query)
	}

	var count int
	err := row.Scan(&count)
	return count, err
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

// GetTaskInstancesByRun retrieves all task instances for a specific run
func (s *Store) GetTaskInstancesByRun(runID string) ([]TaskInstance, error) {
	query := `SELECT id, run_id, task_id, status, output, created_at, updated_at FROM task_instances WHERE run_id = ? ORDER BY created_at ASC`
	rows, err := s.db.Query(query, runID)
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

// CreateTaskInstance inserts a new TaskInstance into the database
func (s *Store) CreateTaskInstance(ti *TaskInstance) error {
	query := `INSERT INTO task_instances (id, run_id, task_id, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`
	_, err := s.db.Exec(query, ti.ID, ti.RunID, ti.TaskID, ti.Status, ti.CreatedAt, ti.UpdatedAt)
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

// GetTaskStatus retrieves the status of a specific task within a run
func (s *Store) GetTaskStatus(runID, taskID string) (TaskStatus, error) {
	query := `SELECT status FROM task_instances WHERE run_id = ? AND task_id = ?`
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
