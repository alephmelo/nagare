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

func TestParseDAG_Catchup(t *testing.T) {
	yamlContentFalse := []byte(`
id: test_dag_false
description: "A simple test DAG"
schedule: "*/5 * * * *"
catchup: false
tasks:
  - id: t1
    type: command
    command: "echo 1"
`)
	dagF, err := ParseDAG(yamlContentFalse)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if dagF.Catchup == nil || *dagF.Catchup != false {
		t.Errorf("expected catchup false, got %v", dagF.Catchup)
	}

	yamlContentTrue := []byte(`
id: test_dag_true
description: "A simple test DAG"
schedule: "*/5 * * * *"
catchup: true
tasks:
  - id: t1
    type: command
    command: "echo 1"
`)
	dagT, err := ParseDAG(yamlContentTrue)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if dagT.Catchup == nil || *dagT.Catchup != true {
		t.Errorf("expected catchup true, got %v", dagT.Catchup)
	}

	yamlContentMissing := []byte(`
id: test_dag_missing
description: "A simple test DAG"
schedule: "*/5 * * * *"
tasks:
  - id: t1
    type: command
    command: "echo 1"
`)
	dagM, err := ParseDAG(yamlContentMissing)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if dagM.Catchup != nil {
		t.Errorf("expected catchup nil, got %v", dagM.Catchup)
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

func TestParseDAGWithItems(t *testing.T) {
	validYAML := []byte(`
id: sample_loop
description: "Test Loop"
schedule: "* * * * *"
tasks:
  - id: download
    type: command
    command: "curl http://api/{{item}}"
    with_items:
      - "a"
      - "b"
  - id: process
    type: command
    command: "echo process"
    depends_on:
      - download
`)

	dag, err := ParseDAG(validYAML)
	if err != nil {
		t.Fatalf("Expected valid DAG to parse, got error: %v", err)
	}

	if len(dag.Tasks) != 3 {
		t.Fatalf("Expected 3 tasks after expansion, got %d", len(dag.Tasks))
	}

	taskMap := make(map[string]TaskDef)
	for _, tDef := range dag.Tasks {
		taskMap[tDef.ID] = tDef
	}

	downloadA, ok := taskMap["download_a"]
	if !ok || downloadA.Command != "curl http://api/a" {
		t.Errorf("Expected download_a task with replaced command, got %+v", downloadA)
	}

	downloadB, ok := taskMap["download_b"]
	if !ok || downloadB.Command != "curl http://api/b" {
		t.Errorf("Expected download_b task with replaced command, got %+v", downloadB)
	}

	process, ok := taskMap["process"]
	if !ok {
		t.Fatalf("Expected process task to exist")
	}

	if len(process.DependsOn) != 2 || process.DependsOn[0] != "download_a" || process.DependsOn[1] != "download_b" {
		t.Errorf("Expected process to depend on download_a and download_b, got %v", process.DependsOn)
	}
}

func TestParseDAG_Trigger(t *testing.T) {
	yamlContent := []byte(`
id: webhook_dag
schedule: "workflow_dispatch"
description: "A webhook triggered DAG"
trigger:
  type: webhook
  path: /api/webhooks/test
  extract_payload:
    MY_VAR: .data.id
tasks:
  - id: t1
    type: command
    command: "echo $MY_VAR"
`)

	dag, err := ParseDAG(yamlContent)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if dag.Trigger == nil {
		t.Fatal("expected trigger to be parsed")
	}

	if dag.Trigger.Type != "webhook" {
		t.Errorf("expected trigger type 'webhook', got '%s'", dag.Trigger.Type)
	}

	if dag.Trigger.Path != "/api/webhooks/test" {
		t.Errorf("expected trigger path '/api/webhooks/test', got '%s'", dag.Trigger.Path)
	}

	if val, ok := dag.Trigger.ExtractPayload["MY_VAR"]; !ok || val != ".data.id" {
		t.Errorf("expected extract_payload to contain MY_VAR=.data.id")
	}

	// Validation tests Defaults
	if err := dag.Validate(); err != nil {
		t.Fatalf("expected valid DAG: %v", err)
	}
	if dag.Trigger.Method != "POST" { // validate applies defaults
		t.Errorf("expected default Method 'POST', got '%s'", dag.Trigger.Method)
	}
}

// ---- Container executor validation -----------------------------------------

func TestValidate_ContainerTask_Valid(t *testing.T) {
	dag := &DAGDef{
		ID:       "container_dag",
		Schedule: "* * * * *",
		Tasks: []TaskDef{
			{
				ID:      "t1",
				Command: "python train.py",
				Image:   "pytorch/pytorch:latest",
				Workdir: "/workspace",
				Volumes: []string{"./data:/data", "/models:/models:ro"},
				Resources: &ResourcesDef{
					CPUs:   "2.0",
					Memory: "4g",
					GPUs:   "1",
				},
			},
		},
	}
	if err := dag.Validate(); err != nil {
		t.Errorf("expected valid container task, got: %v", err)
	}
}

func TestValidate_ContainerTask_MissingCommand(t *testing.T) {
	dag := &DAGDef{
		ID:       "d",
		Schedule: "* * * * *",
		Tasks: []TaskDef{
			{ID: "t1", Image: "alpine:latest"}, // no command
		},
	}
	if err := dag.Validate(); err == nil {
		t.Error("expected error for image set without command")
	}
}

func TestValidate_ContainerTask_ImageWithTriggerDag(t *testing.T) {
	dag := &DAGDef{
		ID:       "d",
		Schedule: "* * * * *",
		Tasks: []TaskDef{
			{ID: "t1", Type: "trigger_dag", DagID: "other", Image: "alpine:latest"},
		},
	}
	if err := dag.Validate(); err == nil {
		t.Error("expected error for image set on trigger_dag task")
	}
}

func TestValidate_ContainerTask_ResourcesWithoutImage(t *testing.T) {
	dag := &DAGDef{
		ID:       "d",
		Schedule: "* * * * *",
		Tasks: []TaskDef{
			{
				ID:        "t1",
				Command:   "echo hi",
				Resources: &ResourcesDef{CPUs: "1.0"},
			},
		},
	}
	if err := dag.Validate(); err == nil {
		t.Error("expected error for resources set without image")
	}
}

func TestValidate_ContainerTask_VolumesWithoutImage(t *testing.T) {
	dag := &DAGDef{
		ID:       "d",
		Schedule: "* * * * *",
		Tasks: []TaskDef{
			{ID: "t1", Command: "echo hi", Volumes: []string{"./data:/data"}},
		},
	}
	if err := dag.Validate(); err == nil {
		t.Error("expected error for volumes set without image")
	}
}

func TestValidate_ContainerTask_WorkdirWithoutImage(t *testing.T) {
	dag := &DAGDef{
		ID:       "d",
		Schedule: "* * * * *",
		Tasks: []TaskDef{
			{ID: "t1", Command: "echo hi", Workdir: "/app"},
		},
	}
	if err := dag.Validate(); err == nil {
		t.Error("expected error for workdir set without image")
	}
}

func TestValidate_ContainerTask_InvalidCPUs(t *testing.T) {
	dag := &DAGDef{
		ID:       "d",
		Schedule: "* * * * *",
		Tasks: []TaskDef{
			{
				ID:        "t1",
				Command:   "echo hi",
				Image:     "alpine:latest",
				Resources: &ResourcesDef{CPUs: "bad"},
			},
		},
	}
	if err := dag.Validate(); err == nil {
		t.Error("expected error for invalid CPUs value")
	}
}

func TestValidate_ContainerTask_InvalidMemory(t *testing.T) {
	dag := &DAGDef{
		ID:       "d",
		Schedule: "* * * * *",
		Tasks: []TaskDef{
			{
				ID:        "t1",
				Command:   "echo hi",
				Image:     "alpine:latest",
				Resources: &ResourcesDef{Memory: "4x"},
			},
		},
	}
	if err := dag.Validate(); err == nil {
		t.Error("expected error for invalid memory value")
	}
}

func TestValidate_ContainerTask_InvalidGPUs(t *testing.T) {
	dag := &DAGDef{
		ID:       "d",
		Schedule: "* * * * *",
		Tasks: []TaskDef{
			{
				ID:        "t1",
				Command:   "echo hi",
				Image:     "alpine:latest",
				Resources: &ResourcesDef{GPUs: "0"},
			},
		},
	}
	if err := dag.Validate(); err == nil {
		t.Error("expected error for invalid GPUs value (0)")
	}
}

func TestValidate_ContainerTask_InvalidVolume(t *testing.T) {
	cases := []struct {
		vol string
	}{
		{"/only/one/part"},
		{"host:container:invalid_mode"},
		{":container"},
		{"host:"},
	}
	for _, tc := range cases {
		dag := &DAGDef{
			ID:       "d",
			Schedule: "* * * * *",
			Tasks: []TaskDef{
				{ID: "t1", Command: "echo hi", Image: "alpine:latest", Volumes: []string{tc.vol}},
			},
		}
		if err := dag.Validate(); err == nil {
			t.Errorf("expected error for invalid volume %q", tc.vol)
		}
	}
}

func TestValidate_ContainerTask_GPUs_All(t *testing.T) {
	dag := &DAGDef{
		ID:       "d",
		Schedule: "* * * * *",
		Tasks: []TaskDef{
			{
				ID:        "t1",
				Command:   "echo hi",
				Image:     "alpine:latest",
				Resources: &ResourcesDef{GPUs: "all"},
			},
		},
	}
	if err := dag.Validate(); err != nil {
		t.Errorf("expected valid GPU 'all', got: %v", err)
	}
}

func TestParseDAG_ContainerFields(t *testing.T) {
	yamlContent := []byte(`
id: ml_dag
schedule: "* * * * *"
tasks:
  - id: train
    command: python train.py
    image: pytorch/pytorch:2.1.0-cuda12.1-cudnn8-runtime
    workdir: /workspace
    volumes:
      - ./data:/workspace/data
      - ./models:/workspace/models:ro
    resources:
      cpus: "4.0"
      memory: 8g
      gpus: all
`)
	dag, err := ParseDAG(yamlContent)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(dag.Tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(dag.Tasks))
	}
	task := dag.Tasks[0]
	if task.Image != "pytorch/pytorch:2.1.0-cuda12.1-cudnn8-runtime" {
		t.Errorf("Image: got %q", task.Image)
	}
	if task.Workdir != "/workspace" {
		t.Errorf("Workdir: got %q", task.Workdir)
	}
	if len(task.Volumes) != 2 {
		t.Fatalf("expected 2 volumes, got %d", len(task.Volumes))
	}
	if task.Resources == nil {
		t.Fatal("expected Resources to be set")
	}
	if task.Resources.CPUs != "4.0" {
		t.Errorf("CPUs: got %q", task.Resources.CPUs)
	}
	if task.Resources.Memory != "8g" {
		t.Errorf("Memory: got %q", task.Resources.Memory)
	}
	if task.Resources.GPUs != "all" {
		t.Errorf("GPUs: got %q", task.Resources.GPUs)
	}
}
