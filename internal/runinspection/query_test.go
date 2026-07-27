package runinspection

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/models"
)

type inspectionReader struct {
	input *models.RunInspectionInputs
}

func (r inspectionReader) GetRunInspectionInputs(context.Context, string) (*models.RunInspectionInputs, error) {
	return r.input, nil
}

type inspectionDefinitions struct {
	dag *models.DAGDef
}

func (d inspectionDefinitions) DAG(string) (*models.DAGDef, bool) {
	return d.dag, d.dag != nil
}

func TestInspectRetainsMappedChildHistoryAcrossParentRetries(t *testing.T) {
	now := time.Now()
	itemA, itemB := "a", "b"
	query := NewQuery(inspectionReader{input: &models.RunInspectionInputs{
		Run: models.DagRun{ID: "run", DAGID: "dag"},
		Attempts: []models.TaskInstance{
			{ID: "run_map[0]", RunID: "run", TaskID: "map[0]", Attempt: 1, Status: models.TaskFailed, ItemValue: &itemA, CreatedAt: now},
			{ID: "run_map[0]@g2", RunID: "run", TaskID: "map[0]@g2", Attempt: 1, Status: models.TaskSuccess, ItemValue: &itemB, CreatedAt: now.Add(time.Second)},
			{ID: "run_map[0]@g2_2", RunID: "run", TaskID: "map[0]@g2", Attempt: 2, Status: models.TaskSuccess, ItemValue: &itemB, CreatedAt: now.Add(2 * time.Second)},
		},
	}}, inspectionDefinitions{dag: &models.DAGDef{
		ID: "dag",
		Tasks: []models.TaskDef{
			{ID: "source", Type: "command"},
			{ID: "map", Type: "map", MapOver: "source", Command: "echo {{item}}"},
		},
	}})

	result, err := query.Inspect(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}

	var child *LogicalTask
	for i := range result.LogicalTasks {
		if result.LogicalTasks[i].ID == "map[0]" {
			child = &result.LogicalTasks[i]
			break
		}
	}
	if child == nil {
		t.Fatal("mapped child not projected")
	}
	if got := len(child.Attempts); got != 3 {
		t.Fatalf("mapped child attempts = %d, want complete history of 3", got)
	}
	wantIDs := []string{"run_map[0]", "run_map[0]@g2", "run_map[0]@g2_2"}
	wantGenerations := []int{1, 2, 2}
	wantAttempts := []int{1, 1, 2}
	wantCommands := []string{"echo a", "echo b", "echo b"}
	for i, want := range wantIDs {
		if got := child.Attempts[i].ID; got != want {
			t.Errorf("attempt %d ID = %q, want persisted opaque ID %q", i, got, want)
		}
		if got := child.Attempts[i].Logs.TaskAttemptID; got != want {
			t.Errorf("attempt %d log ID = %q, want persisted opaque ID %q", i, got, want)
		}
		if !child.Attempts[i].Logs.Exact {
			t.Errorf("attempt %d logs are not marked for exact resolution", i)
		}
		if got := child.Attempts[i].TaskID; got != "map[0]" {
			t.Errorf("attempt %d logical task ID = %q, want map[0]", i, got)
		}
		if got := child.Attempts[i].Attempt; got != wantAttempts[i] {
			t.Errorf("attempt %d stored attempt = %d, want %d", i, got, wantAttempts[i])
		}
		if got := child.Attempts[i].Generation; got != wantGenerations[i] {
			t.Errorf("attempt %d generation = %d, want %d", i, got, wantGenerations[i])
		}
		if got := child.Attempts[i].DisplaySequence; got != i+1 {
			t.Errorf("attempt %d display sequence = %d, want %d", i, got, i+1)
		}
		if got := child.Attempts[i].Command; got != wantCommands[i] {
			t.Errorf("attempt %d command = %q, want %q", i, got, wantCommands[i])
		}
	}
	if child.CurrentAttempt == nil || child.CurrentAttempt.Attempt != 2 {
		t.Fatalf("current attempt = %+v, want latest snapshot attempt", child.CurrentAttempt)
	}
	if child.MapOver == nil || *child.MapOver != "source" {
		t.Fatalf("map_over = %v, want source", child.MapOver)
	}
	if child.Command != "echo b" {
		t.Fatalf("command = %q, want current mapped item substitution", child.Command)
	}
	if child.CurrentAttempt.ItemValue == nil || *child.CurrentAttempt.ItemValue != itemB {
		t.Fatalf("current item value = %v, want %q", child.CurrentAttempt.ItemValue, itemB)
	}
}

func TestInspectSelectsCurrentMappedGenerationBeforeAttemptNumber(t *testing.T) {
	now := time.Now()
	query := NewQuery(inspectionReader{input: &models.RunInspectionInputs{
		Run: models.DagRun{ID: "run", DAGID: "dag"},
		Attempts: []models.TaskInstance{
			{ID: "generation-one-retry", TaskID: "map[0]", Attempt: 2, Status: models.TaskFailed, CreatedAt: now},
			{ID: "generation-two", TaskID: "map[0]@g2", Attempt: 1, Status: models.TaskRunning, CreatedAt: now.Add(time.Second)},
		},
	}}, inspectionDefinitions{dag: &models.DAGDef{
		ID: "dag",
		Tasks: []models.TaskDef{
			{ID: "source", Type: "command"},
			{ID: "map", Type: "map", MapOver: "source"},
		},
	}})

	result, err := query.Inspect(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}

	for i := range result.LogicalTasks {
		task := &result.LogicalTasks[i]
		if task.ID == "map[0]" {
			if task.CurrentAttempt == nil || task.CurrentAttempt.ID != "generation-two" {
				t.Fatalf("current attempt = %+v, want generation two", task.CurrentAttempt)
			}
			return
		}
	}
	t.Fatal("mapped child not projected")
}

func TestInspectAttemptCompletionTiming(t *testing.T) {
	now := time.Date(2026, time.July, 27, 10, 0, 0, 0, time.UTC)
	started := now.Add(time.Second)
	query := NewQuery(inspectionReader{input: &models.RunInspectionInputs{
		Run: models.DagRun{ID: "run", DAGID: "dag"},
		Attempts: []models.TaskInstance{
			{
				ID: "cancelled-before-start", TaskID: "cancelled-before-start",
				Status: models.TaskCancelled, Attempt: 1,
				CreatedAt: now, UpdatedAt: now.Add(2 * time.Second),
			},
			{
				ID: "retrying", TaskID: "retrying", Status: models.TaskUpForRetry,
				Attempt: 1, CreatedAt: now, StartedAt: &started,
				UpdatedAt: started.Add(3 * time.Second),
			},
		},
	}}, inspectionDefinitions{})

	result, err := query.Inspect(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}

	byID := make(map[string]LogicalTask, len(result.LogicalTasks))
	for _, task := range result.LogicalTasks {
		byID[task.ID] = task
	}
	cancelled := byID["cancelled-before-start"].CurrentAttempt
	if cancelled == nil {
		t.Fatal("cancelled attempt missing")
	}
	if cancelled.CompletedAt != nil || cancelled.DurationMs != nil {
		t.Fatalf(
			"cancelled-before-start timing = completed %v duration %v, want nil/nil",
			cancelled.CompletedAt,
			cancelled.DurationMs,
		)
	}
	retrying := byID["retrying"].CurrentAttempt
	if retrying == nil {
		t.Fatal("up_for_retry attempt missing")
	}
	if retrying.CompletedAt == nil || !retrying.CompletedAt.Equal(started.Add(3*time.Second)) {
		t.Fatalf("up_for_retry completed_at = %v", retrying.CompletedAt)
	}
	if retrying.DurationMs == nil || *retrying.DurationMs != 3000 {
		t.Fatalf("up_for_retry duration_ms = %v, want 3000", retrying.DurationMs)
	}
}

func TestInspectEncodesEmptyLogicalTasksAsArray(t *testing.T) {
	query := NewQuery(inspectionReader{input: &models.RunInspectionInputs{
		Run: models.DagRun{ID: "empty", DAGID: "missing"},
	}}, inspectionDefinitions{})

	result, err := query.Inspect(context.Background(), "empty")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) == "" || !jsonContainsEmptyLogicalTasks(encoded) {
		t.Fatalf("encoded inspection = %s, want logical_tasks:[]", encoded)
	}
}

func jsonContainsEmptyLogicalTasks(encoded []byte) bool {
	var decoded struct {
		LogicalTasks []json.RawMessage `json:"logical_tasks"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return false
	}
	return decoded.LogicalTasks != nil && len(decoded.LogicalTasks) == 0
}
