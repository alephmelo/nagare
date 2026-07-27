// Package runinspection assembles the stable read model used by the run page.
// It owns lifecycle projection, but neither persistence nor HTTP concerns.
package runinspection

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/scheduler"
)

type inputReader interface {
	GetRunInspectionInputs(context.Context, string) (*models.RunInspectionInputs, error)
}

// Definitions supplies the immutable DAG definition and resolves scheduler
// generation identities without exposing those identities in the DTO.
type Definitions interface {
	DAG(string) (*models.DAGDef, bool)
}

// Query is the narrow run-inspection query interface.
type Query struct {
	reader      inputReader
	definitions Definitions
}

func NewQuery(reader inputReader, definitions Definitions) *Query {
	return &Query{reader: reader, definitions: definitions}
}

type RunInspection struct {
	Run          Run           `json:"run"`
	LogicalTasks []LogicalTask `json:"logical_tasks"`
}

type Run struct {
	ID          string           `json:"id"`
	DAGID       string           `json:"dag_id"`
	Status      models.RunStatus `json:"status"`
	ExecDate    time.Time        `json:"exec_date"`
	TriggerType string           `json:"trigger_type"`
	CreatedAt   time.Time        `json:"created_at"`
	CompletedAt *time.Time       `json:"completed_at"`
	DurationMs  *int64           `json:"duration_ms"`
}

type LogicalTask struct {
	ID             string    `json:"id"`
	Kind           string    `json:"kind"`
	DefinitionID   *string   `json:"definition_id,omitempty"`
	ParentID       *string   `json:"parent_id,omitempty"`
	Dependencies   []string  `json:"dependencies"`
	MapOver        *string   `json:"map_over,omitempty"`
	Command        string    `json:"command"`
	Attempts       []Attempt `json:"attempts"`
	CurrentAttempt *Attempt  `json:"current_attempt"`
}

type Attempt struct {
	ID              string            `json:"id"`
	TaskID          string            `json:"task_id"`
	Attempt         int               `json:"attempt"`
	Generation      int               `json:"generation"`
	DisplaySequence int               `json:"display_sequence"`
	Status          models.TaskStatus `json:"status"`
	Command         string            `json:"command"`
	Output          string            `json:"output"`
	ItemValue       *string           `json:"item_value"`
	CreatedAt       time.Time         `json:"created_at"`
	UpdatedAt       time.Time         `json:"updated_at"`
	StartedAt       *time.Time        `json:"started_at"`
	CompletedAt     *time.Time        `json:"completed_at"`
	DurationMs      *int64            `json:"duration_ms"`
	Metrics         *Metrics          `json:"metrics,omitempty"`
	Logs            Logs              `json:"logs"`
}

type Logs struct {
	TaskAttemptID string `json:"task_attempt_id"`
	Exact         bool   `json:"exact"`
}

type Metrics struct {
	DurationMs      int64  `json:"duration_ms"`
	CpuUserMs       int64  `json:"cpu_user_ms"`
	CpuSystemMs     int64  `json:"cpu_system_ms"`
	PeakMemoryBytes int64  `json:"peak_memory_bytes"`
	ExitCode        int    `json:"exit_code"`
	ExecutorType    string `json:"executor_type"`
}

func (q *Query) Inspect(ctx context.Context, runID string) (*RunInspection, error) {
	input, err := q.reader.GetRunInspectionInputs(ctx, runID)
	if err != nil {
		return nil, err
	}
	result := &RunInspection{
		Run:          projectRun(input.Run),
		LogicalTasks: []LogicalTask{},
	}
	dag, dagFound := q.definitions.DAG(input.Run.DAGID)

	metrics := make(map[string]*models.TaskMetrics, len(input.Metrics))
	for i := range input.Metrics {
		metric := input.Metrics[i]
		metrics[metric.TaskInstanceID] = &metric
	}

	defs := make(map[string]*models.TaskDef)
	order := make([]string, 0)
	if dagFound {
		for i := range dag.Tasks {
			def := &dag.Tasks[i]
			defs[def.ID] = def
			order = append(order, def.ID)
		}
	}

	grouped := make(map[string][]models.TaskInstance)
	for _, persisted := range input.Attempts {
		publicID := scheduler.PublicTaskID(persisted.TaskID)
		if _, exists := grouped[publicID]; !exists {
			order = append(order, publicID)
		}
		grouped[publicID] = append(grouped[publicID], persisted)
	}
	order = unique(order)

	for _, id := range order {
		def, exact := defs[id]
		parentID, mappedChild := mappedParent(id, defs)
		kind := "runtime-only"
		var definitionID *string
		if exact {
			kind = "definition"
			value := id
			definitionID = &value
		} else if mappedChild {
			kind = "mapped-child"
			def = defs[parentID]
			value := parentID
			definitionID = &value
		}

		task := LogicalTask{
			ID: id, Kind: kind, DefinitionID: definitionID,
			Dependencies: []string{}, Attempts: []Attempt{},
		}
		if mappedChild {
			value := parentID
			task.ParentID = &value
		}
		if def != nil {
			task.Command = def.Command
			task.Dependencies = dependencies(def)
			if def.MapOver != "" {
				value := def.MapOver
				task.MapOver = &value
			}
		}
		attempts := grouped[id]
		commandTemplate := task.Command
		sort.SliceStable(attempts, func(i, j int) bool {
			iGeneration := mappedGeneration(attempts[i].TaskID)
			jGeneration := mappedGeneration(attempts[j].TaskID)
			if iGeneration != jGeneration {
				return iGeneration < jGeneration
			}
			if attempts[i].Attempt != attempts[j].Attempt {
				return attempts[i].Attempt < attempts[j].Attempt
			}
			if !attempts[i].CreatedAt.Equal(attempts[j].CreatedAt) {
				return attempts[i].CreatedAt.Before(attempts[j].CreatedAt)
			}
			return attempts[i].ID < attempts[j].ID
		})
		for i, persisted := range attempts {
			attempt := projectAttempt(
				persisted,
				id,
				commandTemplate,
				mappedGeneration(persisted.TaskID),
				i+1,
				metrics[persisted.ID],
			)
			task.Attempts = append(task.Attempts, attempt)
		}
		if len(task.Attempts) > 0 {
			current := task.Attempts[len(task.Attempts)-1]
			task.CurrentAttempt = &current
			if current.ItemValue != nil {
				task.Command = strings.ReplaceAll(task.Command, "{{item}}", *current.ItemValue)
			}
		}
		result.LogicalTasks = append(result.LogicalTasks, task)
	}
	return result, nil
}

func mappedGeneration(taskID string) int {
	publicID := scheduler.PublicTaskID(taskID)
	if publicID == taskID {
		return 1
	}
	generation, err := strconv.Atoi(strings.TrimPrefix(taskID[len(publicID):], "@g"))
	if err != nil {
		return 1
	}
	return generation
}

func projectRun(run models.DagRun) Run {
	projected := Run{
		ID: run.ID, DAGID: run.DAGID, Status: run.Status, ExecDate: run.ExecDate,
		TriggerType: run.TriggerType, CreatedAt: run.CreatedAt, CompletedAt: run.CompletedAt,
	}
	if run.CompletedAt != nil {
		duration := run.CompletedAt.Sub(run.CreatedAt).Milliseconds()
		projected.DurationMs = &duration
	}
	return projected
}

func projectAttempt(
	value models.TaskInstance,
	publicTaskID string,
	commandTemplate string,
	generation int,
	displaySequence int,
	metric *models.TaskMetrics,
) Attempt {
	var completed *time.Time
	var duration *int64
	if recordsCompletion(value) {
		t := value.UpdatedAt
		completed = &t
		if value.StartedAt != nil && !t.Before(*value.StartedAt) {
			ms := t.Sub(*value.StartedAt).Milliseconds()
			duration = &ms
		}
	}
	command := commandTemplate
	if value.ItemValue != nil {
		command = strings.ReplaceAll(command, "{{item}}", *value.ItemValue)
	}
	var projectedMetric *Metrics
	if metric != nil {
		projectedMetric = &Metrics{
			DurationMs: metric.DurationMs, CpuUserMs: metric.CpuUserMs,
			CpuSystemMs: metric.CpuSystemMs, PeakMemoryBytes: metric.PeakMemoryBytes,
			ExitCode: metric.ExitCode, ExecutorType: metric.ExecutorType,
		}
	}
	return Attempt{
		ID: value.ID, TaskID: publicTaskID, Attempt: value.Attempt,
		Generation: generation, DisplaySequence: displaySequence,
		Status: value.Status, Command: command, Output: value.Output, ItemValue: value.ItemValue,
		CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt,
		StartedAt: value.StartedAt, CompletedAt: completed, DurationMs: duration,
		Metrics: projectedMetric, Logs: Logs{TaskAttemptID: value.ID, Exact: true},
	}
}

func recordsCompletion(value models.TaskInstance) bool {
	if value.Status == models.TaskCancelled && value.StartedAt == nil {
		return false
	}
	return value.Status == models.TaskSuccess ||
		value.Status == models.TaskFailed ||
		value.Status == models.TaskUpForRetry ||
		value.Status == models.TaskCancelled
}

func mappedParent(id string, definitions map[string]*models.TaskDef) (string, bool) {
	if _, exact := definitions[id]; exact {
		return "", false
	}
	open := strings.LastIndexByte(id, '[')
	if open <= 0 || !strings.HasSuffix(id, "]") {
		return "", false
	}
	if _, err := strconv.Atoi(id[open+1 : len(id)-1]); err != nil {
		return "", false
	}
	parent := id[:open]
	def := definitions[parent]
	return parent, def != nil && def.Type == "map"
}

func dependencies(def *models.TaskDef) []string {
	values := append([]string(nil), def.DependsOn...)
	if def.MapOver != "" {
		values = append(values, def.MapOver)
	}
	return unique(values)
}

func unique(values []string) []string {
	seen := make(map[string]bool, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	return result
}
