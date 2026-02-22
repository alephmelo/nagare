package models

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// TaskMetrics stores resource usage captured during a task execution.
type TaskMetrics struct {
	TaskInstanceID  string
	RunID           string
	DAGID           string
	TaskID          string
	DurationMs      int64
	CpuUserMs       int64
	CpuSystemMs     int64
	PeakMemoryBytes int64
	ExitCode        int
	ExecutorType    string // "local" or "docker"
	CreatedAt       time.Time
}

// AggregateMetrics holds pre-computed aggregate stats for a DAG.
type AggregateMetrics struct {
	DAGID          string  `json:"dag_id"`
	RunCount       int     `json:"run_count"`
	AvgDurationMs  float64 `json:"avg_duration_ms"`
	P50DurationMs  float64 `json:"p50_duration_ms"`
	P95DurationMs  float64 `json:"p95_duration_ms"`
	MaxDurationMs  float64 `json:"max_duration_ms"`
	AvgMemoryBytes float64 `json:"avg_memory_bytes"`
	MaxMemoryBytes int64   `json:"max_memory_bytes"`
	AvgCpuMs       float64 `json:"avg_cpu_ms"`
	SuccessRate    float64 `json:"success_rate"`
}

// TimeSeriesPoint is a single data point for charting.
type TimeSeriesPoint struct {
	Timestamp   time.Time `json:"timestamp"`
	DurationMs  int64     `json:"duration_ms"`
	MemoryBytes int64     `json:"memory_bytes"`
	CpuMs       int64     `json:"cpu_ms"`
	TaskID      string    `json:"task_id"`
	RunID       string    `json:"run_id"`
	Status      string    `json:"status"`
}

// RunStatus represents the state of a DagRun
type RunStatus string

const (
	RunRunning   RunStatus = "running"
	RunSuccess   RunStatus = "success"
	RunFailed    RunStatus = "failed"
	RunCancelled RunStatus = "cancelled"
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
	TaskPending    TaskStatus = "pending"
	TaskQueued     TaskStatus = "queued"
	TaskRunning    TaskStatus = "running"
	TaskSuccess    TaskStatus = "success"
	TaskFailed     TaskStatus = "failed"
	TaskUpForRetry TaskStatus = "up_for_retry"
	TaskCancelled  TaskStatus = "cancelled"
)

// DagRun represents a single execution of a DAG
type DagRun struct {
	ID          string
	DAGID       string
	Status      RunStatus
	ExecDate    time.Time
	TriggerType string
	Conf        map[string]string // Dynamically extracted payloads as env variables
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
	ItemValue *string
	Attempt   int
	CreatedAt time.Time
	UpdatedAt time.Time
	StartedAt *time.Time
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
		_ = db.Close()
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
		conf TEXT DEFAULT '{}',
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
		item_value TEXT,
		attempt INT NOT NULL DEFAULT 1,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL,
		started_at DATETIME,
		FOREIGN KEY(run_id) REFERENCES dag_runs(id)
	);`

	taskMetricsSchema := `
	CREATE TABLE IF NOT EXISTS task_metrics (
		task_instance_id  TEXT PRIMARY KEY,
		run_id            TEXT NOT NULL,
		dag_id            TEXT NOT NULL,
		task_id           TEXT NOT NULL,
		duration_ms       INTEGER NOT NULL DEFAULT 0,
		cpu_user_ms       INTEGER NOT NULL DEFAULT 0,
		cpu_system_ms     INTEGER NOT NULL DEFAULT 0,
		peak_memory_bytes INTEGER NOT NULL DEFAULT 0,
		exit_code         INTEGER NOT NULL DEFAULT 0,
		executor_type     TEXT NOT NULL DEFAULT 'local',
		created_at        DATETIME NOT NULL,
		FOREIGN KEY(task_instance_id) REFERENCES task_instances(id)
	);`

	if _, err := s.db.Exec(dagRunsSchema); err != nil {
		return err
	}
	if _, err := s.db.Exec(taskInstancesSchema); err != nil {
		return err
	}
	if _, err := s.db.Exec(taskMetricsSchema); err != nil {
		return err
	}

	// Active Migrations — ignore errors if columns already exist (idempotent ALTER TABLE).
	_, _ = s.db.Exec(`ALTER TABLE dag_runs ADD COLUMN trigger_type TEXT DEFAULT 'scheduled'`)
	_, _ = s.db.Exec(`ALTER TABLE dag_runs ADD COLUMN conf TEXT DEFAULT '{}'`)
	_, _ = s.db.Exec(`ALTER TABLE task_instances ADD COLUMN attempt INT NOT NULL DEFAULT 1`)
	_, _ = s.db.Exec(`ALTER TABLE task_instances ADD COLUMN item_value TEXT`)
	_, _ = s.db.Exec(`ALTER TABLE task_instances ADD COLUMN started_at DATETIME`)

	return nil
}

// CreateDagRun inserts a new DagRun into the database
func (s *Store) CreateDagRun(run *DagRun) error {
	confJSON := "{}"
	if run.Conf != nil {
		if b, err := json.Marshal(run.Conf); err == nil {
			confJSON = string(b)
		}
	}

	// Normalise to UTC so that RFC3339 string comparisons in SQLite
	// are unambiguous regardless of the server's local timezone.
	createdAt := run.CreatedAt.UTC()

	query := `INSERT INTO dag_runs (id, dag_id, status, exec_date, trigger_type, conf, created_at, completed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := s.db.Exec(query, run.ID, run.DAGID, run.Status, run.ExecDate, run.TriggerType, confJSON, createdAt, run.CompletedAt)
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

	query = fmt.Sprintf(`SELECT id, dag_id, status, exec_date, trigger_type, conf, created_at, completed_at FROM dag_runs %s ORDER BY created_at DESC LIMIT ? OFFSET ?`, where)
	params = append(params, limit, offset)

	rows, err = s.db.Query(query, params...)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []DagRun
	for rows.Next() {
		var r DagRun
		var confStr sql.NullString
		if err := rows.Scan(&r.ID, &r.DAGID, &r.Status, &r.ExecDate, &r.TriggerType, &confStr, &r.CreatedAt, &r.CompletedAt); err != nil {
			return nil, err
		}
		if confStr.Valid && confStr.String != "" {
			var conf map[string]string
			if err := json.Unmarshal([]byte(confStr.String), &conf); err == nil {
				r.Conf = conf
			}
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
	// Use datetime() on both sides so SQLite parses the values as proper
	// datetimes rather than doing a raw string comparison. The threshold is
	// expressed in UTC so it is unambiguous regardless of server timezone.
	yesterday := time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339)
	err = s.db.QueryRow(
		`SELECT COUNT(*) FROM dag_runs WHERE status = ? AND datetime(created_at) >= datetime(?)`,
		RunFailed, yesterday,
	).Scan(&stats.FailedRuns24h)
	if err != nil {
		return nil, err
	}

	return stats, nil
}

// GetActiveDagRuns retrieves all DAG runs currently marked as 'running'.
func (s *Store) GetActiveDagRuns() ([]DagRun, error) {
	query := `SELECT id, dag_id, status, exec_date, trigger_type, conf, created_at, completed_at FROM dag_runs WHERE status = ?`
	rows, err := s.db.Query(query, RunRunning)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []DagRun
	for rows.Next() {
		var r DagRun
		var confStr sql.NullString
		if err := rows.Scan(&r.ID, &r.DAGID, &r.Status, &r.ExecDate, &r.TriggerType, &confStr, &r.CreatedAt, &r.CompletedAt); err != nil {
			return nil, err
		}
		if confStr.Valid && confStr.String != "" {
			var conf map[string]string
			if err := json.Unmarshal([]byte(confStr.String), &conf); err == nil {
				r.Conf = conf
			}
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
		SELECT id, run_id, task_id, status, COALESCE(output,''), item_value, attempt, created_at, updated_at, started_at
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
		SELECT id, run_id, task_id, status, COALESCE(output,''), item_value, attempt, created_at, updated_at, started_at
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
		if err := rows.Scan(&ti.ID, &ti.RunID, &ti.TaskID, &ti.Status, &ti.Output, &ti.ItemValue, &ti.Attempt, &ti.CreatedAt, &ti.UpdatedAt, &ti.StartedAt); err != nil {
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
	query := `INSERT INTO task_instances (id, run_id, task_id, status, output, item_value, attempt, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := s.db.Exec(query, ti.ID, ti.RunID, ti.TaskID, ti.Status, ti.Output, ti.ItemValue, ti.Attempt, ti.CreatedAt, ti.UpdatedAt)
	return err
}

// GetTasksByStatus retrieves all TaskInstances with a specific status
func (s *Store) GetTasksByStatus(status TaskStatus) ([]TaskInstance, error) {
	query := `SELECT id, run_id, task_id, status, output, item_value, attempt, created_at, updated_at, started_at FROM task_instances WHERE status = ?`
	rows, err := s.db.Query(query, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []TaskInstance
	for rows.Next() {
		var ti TaskInstance
		var output sql.NullString
		if err := rows.Scan(&ti.ID, &ti.RunID, &ti.TaskID, &ti.Status, &output, &ti.ItemValue, &ti.Attempt, &ti.CreatedAt, &ti.UpdatedAt, &ti.StartedAt); err != nil {
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

// GetTaskInstance retrieves a specific TaskInstance by its unique ID
func (s *Store) GetTaskInstance(id string) (*TaskInstance, error) {
	query := `SELECT id, run_id, task_id, status, COALESCE(output,''), item_value, attempt, created_at, updated_at, started_at FROM task_instances WHERE id = ?`
	row := s.db.QueryRow(query, id)

	var ti TaskInstance
	if err := row.Scan(&ti.ID, &ti.RunID, &ti.TaskID, &ti.Status, &ti.Output, &ti.ItemValue, &ti.Attempt, &ti.CreatedAt, &ti.UpdatedAt, &ti.StartedAt); err != nil {
		return nil, err
	}
	return &ti, nil
}

// GetDagRun retrieves a DagRun by ID
func (s *Store) GetDagRun(runID string) (*DagRun, error) {
	query := `SELECT id, dag_id, status, exec_date, trigger_type, conf, created_at, completed_at FROM dag_runs WHERE id = ?`
	row := s.db.QueryRow(query, runID)

	var r DagRun
	var confStr sql.NullString
	if err := row.Scan(&r.ID, &r.DAGID, &r.Status, &r.ExecDate, &r.TriggerType, &confStr, &r.CreatedAt, &r.CompletedAt); err != nil {
		return nil, err
	}
	if confStr.Valid && confStr.String != "" {
		var conf map[string]string
		if err := json.Unmarshal([]byte(confStr.String), &conf); err == nil {
			r.Conf = conf
		}
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

// AppendTaskOutput appends a chunk of text to the task's output column.
// Used during streaming execution to persist partial output incrementally.
func (s *Store) AppendTaskOutput(taskID, chunk string) error {
	query := `UPDATE task_instances SET output = COALESCE(output, '') || ?, updated_at = ? WHERE id = ?`
	_, err := s.db.Exec(query, chunk, time.Now(), taskID)
	return err
}

// SetTaskStartedAt records when a task actually began executing (as opposed to
// when it was created/queued). Idempotent — safe to call multiple times.
func (s *Store) SetTaskStartedAt(taskID string, t time.Time) error {
	query := `UPDATE task_instances SET started_at = ?, updated_at = ? WHERE id = ? AND started_at IS NULL`
	_, err := s.db.Exec(query, t, t, taskID)
	return err
}

// InsertTaskMetrics persists resource usage captured during a task execution.
func (s *Store) InsertTaskMetrics(m *TaskMetrics) error {
	query := `
		INSERT INTO task_metrics
			(task_instance_id, run_id, dag_id, task_id, duration_ms, cpu_user_ms, cpu_system_ms, peak_memory_bytes, exit_code, executor_type, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(task_instance_id) DO UPDATE SET
			duration_ms       = excluded.duration_ms,
			cpu_user_ms       = excluded.cpu_user_ms,
			cpu_system_ms     = excluded.cpu_system_ms,
			peak_memory_bytes = excluded.peak_memory_bytes,
			exit_code         = excluded.exit_code,
			executor_type     = excluded.executor_type`
	_, err := s.db.Exec(query,
		m.TaskInstanceID, m.RunID, m.DAGID, m.TaskID,
		m.DurationMs, m.CpuUserMs, m.CpuSystemMs, m.PeakMemoryBytes,
		m.ExitCode, m.ExecutorType, m.CreatedAt,
	)
	return err
}

// GetTaskMetrics retrieves metrics for a specific task instance.
func (s *Store) GetTaskMetrics(taskInstanceID string) (*TaskMetrics, error) {
	query := `
		SELECT task_instance_id, run_id, dag_id, task_id, duration_ms, cpu_user_ms, cpu_system_ms, peak_memory_bytes, exit_code, executor_type, created_at
		FROM task_metrics WHERE task_instance_id = ?`
	row := s.db.QueryRow(query, taskInstanceID)
	var m TaskMetrics
	err := row.Scan(&m.TaskInstanceID, &m.RunID, &m.DAGID, &m.TaskID,
		&m.DurationMs, &m.CpuUserMs, &m.CpuSystemMs, &m.PeakMemoryBytes,
		&m.ExitCode, &m.ExecutorType, &m.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// GetMetricsByRunID retrieves all task metrics for a specific DAG run.
func (s *Store) GetMetricsByRunID(runID string) ([]TaskMetrics, error) {
	query := `
		SELECT task_instance_id, run_id, dag_id, task_id, duration_ms, cpu_user_ms, cpu_system_ms, peak_memory_bytes, exit_code, executor_type, created_at
		FROM task_metrics WHERE run_id = ? ORDER BY created_at ASC`
	rows, err := s.db.Query(query, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return s.scanTaskMetrics(rows)
}

// GetMetricsByDAGID retrieves the most recent N task metrics for a given DAG.
func (s *Store) GetMetricsByDAGID(dagID string, limit int) ([]TaskMetrics, error) {
	query := `
		SELECT task_instance_id, run_id, dag_id, task_id, duration_ms, cpu_user_ms, cpu_system_ms, peak_memory_bytes, exit_code, executor_type, created_at
		FROM task_metrics WHERE dag_id = ? ORDER BY created_at DESC LIMIT ?`
	rows, err := s.db.Query(query, dagID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return s.scanTaskMetrics(rows)
}

// GetAggregateMetrics returns aggregate statistics for a DAG since a given time.
// Percentile approximations use SQLite's built-in ordering (no extension needed).
func (s *Store) GetAggregateMetrics(dagID string, since time.Time) (*AggregateMetrics, error) {
	sinceStr := since.Format(time.RFC3339)

	var agg AggregateMetrics
	agg.DAGID = dagID

	// Basic aggregates
	baseQuery := `
		SELECT
			COUNT(*),
			COALESCE(AVG(duration_ms), 0),
			COALESCE(MAX(duration_ms), 0),
			COALESCE(AVG(peak_memory_bytes), 0),
			COALESCE(MAX(peak_memory_bytes), 0),
			COALESCE(AVG(cpu_user_ms + cpu_system_ms), 0),
			COALESCE(SUM(CASE WHEN exit_code = 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 0)
		FROM task_metrics
		WHERE dag_id = ? AND created_at >= ?`

	err := s.db.QueryRow(baseQuery, dagID, sinceStr).Scan(
		&agg.RunCount, &agg.AvgDurationMs, &agg.MaxDurationMs,
		&agg.AvgMemoryBytes, &agg.MaxMemoryBytes,
		&agg.AvgCpuMs, &agg.SuccessRate,
	)
	if err != nil {
		return nil, err
	}

	if agg.RunCount == 0 {
		return &agg, nil
	}

	// P50 approximation
	p50Offset := agg.RunCount / 2
	p50Query := `SELECT duration_ms FROM task_metrics WHERE dag_id = ? AND created_at >= ? ORDER BY duration_ms LIMIT 1 OFFSET ?`
	_ = s.db.QueryRow(p50Query, dagID, sinceStr, p50Offset).Scan(&agg.P50DurationMs)

	// P95 approximation
	p95Offset := int(float64(agg.RunCount) * 0.95)
	if p95Offset >= agg.RunCount {
		p95Offset = agg.RunCount - 1
	}
	p95Query := `SELECT duration_ms FROM task_metrics WHERE dag_id = ? AND created_at >= ? ORDER BY duration_ms LIMIT 1 OFFSET ?`
	_ = s.db.QueryRow(p95Query, dagID, sinceStr, p95Offset).Scan(&agg.P95DurationMs)

	return &agg, nil
}

// GetMetricsTimeSeries returns time-series data points for a DAG (or all DAGs if dagID is empty).
// Points are ordered chronologically, limited to the given count.
func (s *Store) GetMetricsTimeSeries(dagID string, since time.Time, limit int) ([]TimeSeriesPoint, error) {
	sinceStr := since.Format(time.RFC3339)

	var rows *sql.Rows
	var err error

	if dagID == "" {
		rows, err = s.db.Query(`
			SELECT tm.created_at, tm.duration_ms, tm.peak_memory_bytes, tm.cpu_user_ms+tm.cpu_system_ms, tm.task_id, tm.run_id, ti.status
			FROM task_metrics tm
			LEFT JOIN task_instances ti ON ti.id = tm.task_instance_id
			WHERE tm.created_at >= ?
			ORDER BY tm.created_at ASC LIMIT ?`, sinceStr, limit)
	} else {
		rows, err = s.db.Query(`
			SELECT tm.created_at, tm.duration_ms, tm.peak_memory_bytes, tm.cpu_user_ms+tm.cpu_system_ms, tm.task_id, tm.run_id, ti.status
			FROM task_metrics tm
			LEFT JOIN task_instances ti ON ti.id = tm.task_instance_id
			WHERE tm.dag_id = ? AND tm.created_at >= ?
			ORDER BY tm.created_at ASC LIMIT ?`, dagID, sinceStr, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var points []TimeSeriesPoint
	for rows.Next() {
		var p TimeSeriesPoint
		var status sql.NullString
		if err := rows.Scan(&p.Timestamp, &p.DurationMs, &p.MemoryBytes, &p.CpuMs, &p.TaskID, &p.RunID, &status); err != nil {
			return nil, err
		}
		if status.Valid {
			p.Status = status.String
		}
		points = append(points, p)
	}
	return points, rows.Err()
}

// GetOverviewMetrics returns system-wide aggregate metrics across all DAGs.
func (s *Store) GetOverviewMetrics(since time.Time) (map[string]interface{}, error) {
	sinceStr := since.Format(time.RFC3339)

	var totalTasks int
	var avgDuration, maxMemory, totalCpu float64

	err := s.db.QueryRow(`
		SELECT
			COUNT(*),
			COALESCE(AVG(duration_ms), 0),
			COALESCE(MAX(peak_memory_bytes), 0),
			COALESCE(SUM(cpu_user_ms + cpu_system_ms), 0)
		FROM task_metrics WHERE created_at >= ?`, sinceStr).Scan(
		&totalTasks, &avgDuration, &maxMemory, &totalCpu,
	)
	if err != nil {
		return nil, err
	}

	var successRate float64
	if totalTasks > 0 {
		_ = s.db.QueryRow(`
			SELECT SUM(CASE WHEN exit_code = 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*)
			FROM task_metrics WHERE created_at >= ?`, sinceStr).Scan(&successRate)
	}

	// Per-DAG aggregates for the table
	dagRows, err := s.db.Query(`
		SELECT dag_id, COUNT(*), COALESCE(AVG(duration_ms),0), COALESCE(MAX(duration_ms),0),
			COALESCE(AVG(peak_memory_bytes),0), COALESCE(MAX(peak_memory_bytes),0),
			COALESCE(SUM(CASE WHEN exit_code=0 THEN 1 ELSE 0 END)*1.0/COUNT(*),0)
		FROM task_metrics WHERE created_at >= ?
		GROUP BY dag_id ORDER BY COUNT(*) DESC`, sinceStr)
	if err != nil {
		return nil, err
	}
	defer dagRows.Close()

	type dagStat struct {
		DAGID          string  `json:"dag_id"`
		Count          int     `json:"count"`
		AvgDurationMs  float64 `json:"avg_duration_ms"`
		MaxDurationMs  float64 `json:"max_duration_ms"`
		AvgMemoryBytes float64 `json:"avg_memory_bytes"`
		MaxMemoryBytes float64 `json:"max_memory_bytes"`
		SuccessRate    float64 `json:"success_rate"`
	}
	var dagStats []dagStat
	for dagRows.Next() {
		var d dagStat
		if err := dagRows.Scan(&d.DAGID, &d.Count, &d.AvgDurationMs, &d.MaxDurationMs,
			&d.AvgMemoryBytes, &d.MaxMemoryBytes, &d.SuccessRate); err != nil {
			return nil, err
		}
		dagStats = append(dagStats, d)
	}

	// Top 10 slowest tasks
	slowRows, err := s.db.Query(`
		SELECT task_instance_id, dag_id, task_id, run_id, duration_ms, peak_memory_bytes, exit_code, created_at
		FROM task_metrics WHERE created_at >= ?
		ORDER BY duration_ms DESC LIMIT 10`, sinceStr)
	if err != nil {
		return nil, err
	}
	defer slowRows.Close()

	type slowTask struct {
		TaskInstanceID  string    `json:"task_instance_id"`
		DAGID           string    `json:"dag_id"`
		TaskID          string    `json:"task_id"`
		RunID           string    `json:"run_id"`
		DurationMs      int64     `json:"duration_ms"`
		PeakMemoryBytes int64     `json:"peak_memory_bytes"`
		ExitCode        int       `json:"exit_code"`
		CreatedAt       time.Time `json:"created_at"`
	}
	var slowTasks []slowTask
	for slowRows.Next() {
		var t slowTask
		if err := slowRows.Scan(&t.TaskInstanceID, &t.DAGID, &t.TaskID, &t.RunID,
			&t.DurationMs, &t.PeakMemoryBytes, &t.ExitCode, &t.CreatedAt); err != nil {
			return nil, err
		}
		slowTasks = append(slowTasks, t)
	}

	return map[string]interface{}{
		"total_tasks":      totalTasks,
		"avg_duration_ms":  avgDuration,
		"max_memory_bytes": maxMemory,
		"total_cpu_ms":     totalCpu,
		"success_rate":     successRate,
		"dag_stats":        dagStats,
		"slowest_tasks":    slowTasks,
	}, nil
}

// scanTaskMetrics is a shared helper to scan rows into []TaskMetrics.
func (s *Store) scanTaskMetrics(rows *sql.Rows) ([]TaskMetrics, error) {
	var metrics []TaskMetrics
	for rows.Next() {
		var m TaskMetrics
		if err := rows.Scan(&m.TaskInstanceID, &m.RunID, &m.DAGID, &m.TaskID,
			&m.DurationMs, &m.CpuUserMs, &m.CpuSystemMs, &m.PeakMemoryBytes,
			&m.ExitCode, &m.ExecutorType, &m.CreatedAt); err != nil {
			return nil, err
		}
		metrics = append(metrics, m)
	}
	return metrics, rows.Err()
}

// GetDagRuns retrieves recent DAG runs ordered by creation mostly recent first
