package worker

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/models"
)

// TestRunCommand_Success verifies a simple command runs and returns output.
func TestRunCommand_Success(t *testing.T) {
	ctx := context.Background()
	var lines []string
	result, err := RunCommand(ctx, "echo hello", nil, 0, func(line string) {
		lines = append(lines, line)
	}, nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !strings.Contains(result.Output, "hello") {
		t.Errorf("expected output to contain 'hello', got %q", result.Output)
	}
	if len(lines) == 0 || lines[0] != "hello" {
		t.Errorf("expected callback to receive 'hello', got %v", lines)
	}
}

// TestRunCommand_Failure verifies a failing command returns an error.
func TestRunCommand_Failure(t *testing.T) {
	ctx := context.Background()
	_, err := RunCommand(ctx, "exit 1", nil, 0, nil, nil)
	if err == nil {
		t.Fatal("expected error for exit 1, got nil")
	}
}

// TestRunCommand_Timeout verifies that timeout kills the command and sets TimedOut.
func TestRunCommand_Timeout(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	result, err := RunCommand(ctx, "sleep 10", nil, 1, nil, nil)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !result.TimedOut {
		t.Error("expected result.TimedOut to be true")
	}
	// Should finish well under 10 seconds (the sleep duration)
	if elapsed > 5*time.Second {
		t.Errorf("command took too long to timeout: %v", elapsed)
	}
}

// TestRunCommand_EnvInjection verifies injected env vars are visible to the command.
func TestRunCommand_EnvInjection(t *testing.T) {
	ctx := context.Background()
	// Provide the full env with our extra var appended.
	env := []string{"MY_SECRET=hunter2", "PATH=/usr/bin:/bin"}
	result, err := RunCommand(ctx, "echo $MY_SECRET", env, 0, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result.Output, "hunter2") {
		t.Errorf("expected 'hunter2' in output, got %q", result.Output)
	}
}

// TestRunCommand_ContextCancel verifies parent context cancellation stops the command.
func TestRunCommand_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := RunCommand(ctx, "sleep 10", nil, 0, nil, nil)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error after context cancel, got nil")
	}
	if elapsed > 5*time.Second {
		t.Errorf("command took too long after cancel: %v", elapsed)
	}
}

// TestRunCommand_MultilineOutput verifies each line is delivered via callback.
func TestRunCommand_MultilineOutput(t *testing.T) {
	ctx := context.Background()
	var lines []string
	result, err := RunCommand(ctx, "printf 'a\\nb\\nc\\n'", nil, 0, func(line string) {
		lines = append(lines, line)
	}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(lines) != 3 {
		t.Errorf("expected 3 callback lines, got %d: %v", len(lines), lines)
	}
	if !strings.Contains(result.Output, "a\nb\nc\n") {
		t.Errorf("unexpected output: %q", result.Output)
	}
}

// TestRunCommand_OnStart verifies that onStart is called with a live cmd.
func TestRunCommand_OnStart(t *testing.T) {
	ctx := context.Background()
	started := false
	_, err := RunCommand(ctx, "echo ok", nil, 0, nil, func(_ *exec.Cmd, _ *os.File) {
		started = true
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !started {
		t.Error("expected onStart to be called")
	}
}

// TestPrepareTaskAssignment verifies PrepareTaskAssignment resolves command and env correctly.
func TestPrepareTaskAssignment(t *testing.T) {
	run := &models.DagRun{
		ID:    "run_1",
		DAGID: "dag_1",
		Conf:  map[string]string{"RUN_KEY": "run_val"},
	}
	ti := models.TaskInstance{
		ID:      "task_1",
		RunID:   "run_1",
		TaskID:  "t1",
		Attempt: 1,
	}
	dag := &models.DAGDef{
		ID: "dag_1",
		Tasks: []models.TaskDef{
			{
				ID:             "t1",
				Command:        "echo hello",
				TimeoutSeconds: 30,
				Env:            map[string]string{"TASK_VAR": "task_val"},
			},
		},
	}

	assignment, err := PrepareTaskAssignment(run, ti, dag)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if assignment.Command != "echo hello" {
		t.Errorf("expected command 'echo hello', got %q", assignment.Command)
	}
	if assignment.TimeoutSecs != 30 {
		t.Errorf("expected timeout 30, got %d", assignment.TimeoutSecs)
	}

	// Env should include both RUN_KEY and TASK_VAR
	envMap := make(map[string]string)
	for _, e := range assignment.Env {
		parts := strings.SplitN(e, "=", 2)
		if len(parts) == 2 {
			envMap[parts[0]] = parts[1]
		}
	}
	if envMap["RUN_KEY"] != "run_val" {
		t.Errorf("expected RUN_KEY=run_val in env, got %v", envMap["RUN_KEY"])
	}
	if envMap["TASK_VAR"] != "task_val" {
		t.Errorf("expected TASK_VAR=task_val in env, got %v", envMap["TASK_VAR"])
	}
}

// TestPrepareTaskAssignment_ItemTemplate verifies {{item}} substitution for map tasks.
func TestPrepareTaskAssignment_ItemTemplate(t *testing.T) {
	itemVal := "file.csv"
	run := &models.DagRun{ID: "r1", DAGID: "d1"}
	ti := models.TaskInstance{TaskID: "t1", ItemValue: &itemVal}
	dag := &models.DAGDef{
		ID: "d1",
		Tasks: []models.TaskDef{
			{ID: "t1", Command: "process {{item}}"},
		},
	}

	assignment, err := PrepareTaskAssignment(run, ti, dag)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if assignment.Command != "process file.csv" {
		t.Errorf("expected 'process file.csv', got %q", assignment.Command)
	}
}

// TestPrepareTaskAssignment_MissingTask verifies an error is returned when task is not found.
func TestPrepareTaskAssignment_MissingTask(t *testing.T) {
	run := &models.DagRun{ID: "r1", DAGID: "d1"}
	ti := models.TaskInstance{TaskID: "nonexistent"}
	dag := &models.DAGDef{ID: "d1", Tasks: []models.TaskDef{{ID: "t1", Command: "echo"}}}

	_, err := PrepareTaskAssignment(run, ti, dag)
	if err == nil {
		t.Fatal("expected error for missing task, got nil")
	}
}
