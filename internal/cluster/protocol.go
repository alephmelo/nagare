package cluster

import (
	"time"

	"github.com/alephmelo/nagare/internal/models"
)

// WorkerRegistration is sent by remote workers on startup and as a heartbeat.
type WorkerRegistration struct {
	WorkerID string   `json:"worker_id"` // UUID generated at worker startup
	Pools    []string `json:"pools"`     // Pool names this worker serves
	Hostname string   `json:"hostname"`  // OS hostname for display
	MaxTasks int      `json:"max_tasks"` // Concurrent task capacity
}

// WorkerInfo is stored on the master and returned by GET /api/workers.
type WorkerInfo struct {
	WorkerID string    `json:"worker_id"`
	Pools    []string  `json:"pools"`
	Hostname string    `json:"hostname"`
	MaxTasks int       `json:"max_tasks"`
	LastSeen time.Time `json:"last_seen"`
	Status   string    `json:"status"` // "online" | "offline"
}

// PollRequest is sent by a worker when asking for a task to execute.
type PollRequest struct {
	WorkerID string   `json:"worker_id"`
	Pools    []string `json:"pools"`
}

// TaskAssignmentDTO is returned to a worker when the master has a matching queued task.
// It carries everything the remote worker needs to execute the task without
// access to the local DAG files or the master's SQLite database.
type TaskAssignmentDTO struct {
	TaskInstanceID string   `json:"task_instance_id"`
	RunID          string   `json:"run_id"`
	Command        string   `json:"command"`
	Env            []string `json:"env"`
	TimeoutSecs    int      `json:"timeout_secs"`
	Retries        int      `json:"retries"`
	Attempt        int      `json:"attempt"`

	// Container executor fields — only populated when the task specifies an image.
	Image     string               `json:"image,omitempty"`
	Workdir   string               `json:"workdir,omitempty"`
	Volumes   []string             `json:"volumes,omitempty"`
	Resources *models.ResourcesDef `json:"resources,omitempty"`
}

// TaskResult is posted by a worker after a task finishes (success, failure, or cancel).
type TaskResult struct {
	TaskInstanceID string `json:"task_instance_id"`
	Status         string `json:"status"` // "success" | "failed" | "cancelled"
	Output         string `json:"output"`
	TimedOut       bool   `json:"timed_out"`

	// Resource metrics collected on the remote worker during execution.
	// Zero values are ignored by the master (e.g. cancelled tasks).
	DurationMs      int64  `json:"duration_ms,omitempty"`
	CpuUserMs       int64  `json:"cpu_user_ms,omitempty"`
	CpuSystemMs     int64  `json:"cpu_system_ms,omitempty"`
	PeakMemoryBytes int64  `json:"peak_memory_bytes,omitempty"`
	ExitCode        int    `json:"exit_code,omitempty"`
	ExecutorType    string `json:"executor_type,omitempty"`
}

// LogBatch is sent by workers to stream log lines back to the master in real time.
type LogBatch struct {
	TaskInstanceID string   `json:"task_instance_id"`
	Lines          []string `json:"lines"`
}

// CancelCheck is the response from GET /api/workers/tasks/{id}/cancel — used
// by workers to detect whether the master has requested cancellation.
type CancelCheck struct {
	Cancel bool `json:"cancel"`
}
