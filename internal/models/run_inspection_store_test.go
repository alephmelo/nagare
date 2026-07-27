package models

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestGetRunInspectionInputsUsesOneReadSnapshot(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "snapshot.db"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()

	now := time.Date(2026, time.July, 27, 10, 0, 0, 0, time.UTC)
	if err := store.CreateDagRun(&DagRun{
		ID: "run", DAGID: "dag", Status: RunRunning, ExecDate: now,
		TriggerType: "version-one", CreatedAt: now,
	}); err != nil {
		t.Fatalf("CreateDagRun: %v", err)
	}
	if err := store.CreateTaskInstance(&TaskInstance{
		ID: "run_task", RunID: "run", TaskID: "task", Status: TaskRunning,
		Output: "version-one", Attempt: 1, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("CreateTaskInstance: %v", err)
	}
	if err := store.InsertTaskMetrics(&TaskMetrics{
		TaskInstanceID: "run_task", RunID: "run", DAGID: "dag", TaskID: "task",
		DurationMs: 1, CreatedAt: now,
	}); err != nil {
		t.Fatalf("InsertTaskMetrics: %v", err)
	}

	runRead := make(chan struct{})
	continueRead := make(chan struct{})
	type readResult struct {
		input *RunInspectionInputs
		err   error
	}
	result := make(chan readResult, 1)
	go func() {
		input, readErr := store.getRunInspectionInputs(context.Background(), "run", func() {
			close(runRead)
			<-continueRead
		})
		result <- readResult{input: input, err: readErr}
	}()

	<-runRead
	writer, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin writer: %v", err)
	}
	for _, update := range []struct {
		query string
		args  []any
	}{
		{
			query: `UPDATE dag_runs SET trigger_type = ? WHERE id = ?`,
			args:  []any{"version-two", "run"},
		},
		{
			query: `UPDATE task_instances SET output = ? WHERE id = ?`,
			args:  []any{"version-two", "run_task"},
		},
		{
			query: `UPDATE task_metrics SET duration_ms = ? WHERE task_instance_id = ?`,
			args:  []any{int64(2), "run_task"},
		},
	} {
		if _, err := writer.Exec(update.query, update.args...); err != nil {
			_ = writer.Rollback()
			t.Fatalf("writer update: %v", err)
		}
	}
	if err := writer.Commit(); err != nil {
		t.Fatalf("writer commit: %v", err)
	}
	close(continueRead)

	snapshot := <-result
	if snapshot.err != nil {
		t.Fatalf("GetRunInspectionInputs: %v", snapshot.err)
	}
	assertInspectionInputVersion(t, snapshot.input, "version-one", 1)

	current, err := store.GetRunInspectionInputs(context.Background(), "run")
	if err != nil {
		t.Fatalf("current GetRunInspectionInputs: %v", err)
	}
	assertInspectionInputVersion(t, current, "version-two", 2)
}

func assertInspectionInputVersion(
	t *testing.T,
	input *RunInspectionInputs,
	wantText string,
	wantDuration int64,
) {
	t.Helper()
	if input.Run.TriggerType != wantText {
		t.Errorf("run trigger = %q, want %q", input.Run.TriggerType, wantText)
	}
	if len(input.Attempts) != 1 || input.Attempts[0].Output != wantText {
		t.Errorf("attempts = %+v, want output %q", input.Attempts, wantText)
	}
	if len(input.Metrics) != 1 || input.Metrics[0].DurationMs != wantDuration {
		t.Errorf("metrics = %+v, want duration %d", input.Metrics, wantDuration)
	}
}
