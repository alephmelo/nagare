package cluster

import (
	"time"

	"github.com/alephmelo/nagare/internal/worker"
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

	// ActiveTasks is the number of tasks currently executing on this worker.
	// Incremented when a task is dispatched via handlePoll; decremented when
	// the worker posts a result via handleResult.
	ActiveTasks int `json:"active_tasks"`

	// IsCloudManaged is true when this worker was auto-provisioned by the
	// autoscaler rather than started manually.  The UI uses this flag to
	// display a cloud badge and to show the associated cloud instance ID.
	IsCloudManaged bool `json:"is_cloud_managed"`

	// CloudInstanceID is the Nagare-internal autoscaler instance ID
	// (e.g. "docker-a3f1b2") when IsCloudManaged is true.  Empty otherwise.
	CloudInstanceID string `json:"cloud_instance_id,omitempty"`
}

// PollRequest is sent by a worker when asking for a task to execute.
type PollRequest struct {
	WorkerID string   `json:"worker_id"`
	Pools    []string `json:"pools"`
}

// TaskAssignmentDTO is returned to a worker when the master has a matching queued task.
// It is a type alias for worker.TaskAssignment — both share identical JSON tags.
type TaskAssignmentDTO = worker.TaskAssignment

// TaskResult is posted by a worker after a task finishes (success, failure, or cancel).
type TaskResult struct {
	TaskInstanceID string `json:"task_instance_id"`
	WorkerID       string `json:"worker_id"` // ID of the worker that executed this task
	Status         string `json:"status"`    // "success" | "failed" | "cancelled"
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
