package models

import (
	"testing"
)

func TestParseDAG(t *testing.T) {
	yamlContent := []byte(`
id: test_dag
description: "A simple test DAG"
schedule: "*/5 * * * *"
tasks:
  - id: t1
    type: command
    command: "echo 1"
  - id: t2
    type: command
    command: "echo 2"
    depends_on:
      - t1
`)

	dag, err := ParseDAG(yamlContent)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if dag.ID != "test_dag" {
		t.Errorf("expected ID 'test_dag', got '%s'", dag.ID)
	}

	if dag.Schedule != "*/5 * * * *" {
		t.Errorf("expected schedule '*/5 * * * *', got '%s'", dag.Schedule)
	}

	if len(dag.Tasks) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(dag.Tasks))
	}

	if dag.Tasks[0].ID != "t1" || dag.Tasks[0].Command != "echo 1" {
		t.Errorf("unexpected first task: %+v", dag.Tasks[0])
	}

	if len(dag.Tasks[1].DependsOn) != 1 || dag.Tasks[1].DependsOn[0] != "t1" {
		t.Errorf("unexpected dependencies on second task: %+v", dag.Tasks[1])
	}
}

func TestParseDAGInvalidYAML(t *testing.T) {
	yamlContent := []byte(`
id: test_dag
  invalid_indentation: true
`)

	_, err := ParseDAG(yamlContent)
	if err == nil {
		t.Fatal("expected error on invalid YAML, got nil")
	}
}

func TestValidate_ValidDAG(t *testing.T) {
	dag := &DAGDef{
		ID:       "valid_dag",
		Schedule: "*/5 * * * *",
		Tasks: []TaskDef{
			{ID: "t1", Type: "command", Command: "echo 1"},
			{ID: "t2", Type: "command", Command: "echo 2", DependsOn: []string{"t1"}},
		},
	}
	if err := dag.Validate(); err != nil {
		t.Errorf("expected valid DAG to have no errors, got: %v", err)
	}
}

func TestValidate_EmptyDAG(t *testing.T) {
	dag := &DAGDef{ID: ""}
	if err := dag.Validate(); err == nil {
		t.Error("expected error for empty DAG ID")
	}

	dag = &DAGDef{ID: "no_tasks", Schedule: "* * * * *"}
	if err := dag.Validate(); err == nil {
		t.Error("expected error for DAG with no tasks")
	}
}

func TestValidate_InvalidCron(t *testing.T) {
	dag := &DAGDef{
		ID:       "invalid_cron",
		Schedule: "invalid cron string",
		Tasks: []TaskDef{
			{ID: "t1", Type: "command", Command: "echo 1"},
		},
	}
	if err := dag.Validate(); err == nil {
		t.Error("expected error for invalid cron schedule")
	}
}

func TestValidate_DuplicateTaskID(t *testing.T) {
	dag := &DAGDef{
		ID:       "dup_tasks",
		Schedule: "* * * * *",
		Tasks: []TaskDef{
			{ID: "t1", Type: "command", Command: "echo 1"},
			{ID: "t1", Type: "command", Command: "echo 2"},
		},
	}
	if err := dag.Validate(); err == nil {
		t.Error("expected error for duplicated task ID 't1'")
	}
}

func TestValidate_MissingDependency(t *testing.T) {
	dag := &DAGDef{
		ID:       "missing_dep",
		Schedule: "* * * * *",
		Tasks: []TaskDef{
			{ID: "t1", Type: "command", Command: "echo 1"},
			{ID: "t2", Type: "command", Command: "echo 2", DependsOn: []string{"t3"}},
		},
	}
	if err := dag.Validate(); err == nil {
		t.Error("expected error for missing dependency 't3'")
	}
}

func TestValidate_SelfReference(t *testing.T) {
	dag := &DAGDef{
		ID:       "self_ref",
		Schedule: "* * * * *",
		Tasks: []TaskDef{
			{ID: "t1", Type: "command", Command: "echo 1", DependsOn: []string{"t1"}},
		},
	}
	if err := dag.Validate(); err == nil {
		t.Error("expected error for self-referencing dependency 't1'")
	}
}

func TestValidate_CircularDependency(t *testing.T) {
	dag := &DAGDef{
		ID:       "circular_dep",
		Schedule: "* * * * *",
		Tasks: []TaskDef{
			{ID: "t1", Type: "command", Command: "echo 1", DependsOn: []string{"t2"}},
			{ID: "t2", Type: "command", Command: "echo 2", DependsOn: []string{"t1"}},
		},
	}
	if err := dag.Validate(); err == nil {
		t.Error("expected error for circular dependency between 't1' and 't2'")
	}
}

func TestValidate_ComplexCircularDependency(t *testing.T) {
	dag := &DAGDef{
		ID:       "complex_circular",
		Schedule: "* * * * *",
		Tasks: []TaskDef{
			{ID: "t1", Type: "command", Command: "echo 1", DependsOn: []string{"t3"}},
			{ID: "t2", Type: "command", Command: "echo 2", DependsOn: []string{"t1"}},
			{ID: "t3", Type: "command", Command: "echo 3", DependsOn: []string{"t2"}},
		},
	}
	if err := dag.Validate(); err == nil {
		t.Error("expected error for complex circular dependency (t1 -> t3 -> t2 -> t1)")
	}
}
