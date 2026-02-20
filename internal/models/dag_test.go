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
