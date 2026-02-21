package worker

import (
	"context"
	"strings"
	"testing"

	"github.com/alephmelo/nagare/internal/models"
)

// ---- parseMemory ------------------------------------------------------------

func TestParseMemory(t *testing.T) {
	cases := []struct {
		input   string
		wantErr bool
		wantVal int64
	}{
		{"512m", false, 512 * 1024 * 1024},
		{"2g", false, 2 * 1024 * 1024 * 1024},
		{"1024k", false, 1024 * 1024},
		{"4096", false, 4096},
		{"1G", false, 1 * 1024 * 1024 * 1024}, // case-insensitive
		{"128M", false, 128 * 1024 * 1024},
		{"0m", false, 0},
		{"", true, 0},
		{"abc", true, 0},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseMemory(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("parseMemory(%q): expected error, got nil", tc.input)
				}
				return
			}
			if err != nil {
				t.Errorf("parseMemory(%q): unexpected error: %v", tc.input, err)
				return
			}
			if got != tc.wantVal {
				t.Errorf("parseMemory(%q): got %d, want %d", tc.input, got, tc.wantVal)
			}
		})
	}
}

// ---- gpuCount ---------------------------------------------------------------

func TestGPUCount(t *testing.T) {
	if gpuCount("all") != -1 {
		t.Errorf("expected -1 for 'all'")
	}
	if gpuCount("2") != 2 {
		t.Errorf("expected 2 for '2'")
	}
	if gpuCount("0") != -1 {
		t.Errorf("expected -1 (invalid) for '0'")
	}
	if gpuCount("bad") != -1 {
		t.Errorf("expected -1 (invalid) for 'bad'")
	}
}

// ---- buildResourceConfig ----------------------------------------------------

func TestBuildResourceConfig_CPUs(t *testing.T) {
	assignment := &TaskAssignment{
		Resources: &models.ResourcesDef{CPUs: "2.5"},
	}
	cfg := buildResourceConfig(assignment)
	expected := int64(2.5 * 1e9)
	if cfg.NanoCPUs != expected {
		t.Errorf("NanoCPUs: got %d, want %d", cfg.NanoCPUs, expected)
	}
}

func TestBuildResourceConfig_Memory(t *testing.T) {
	assignment := &TaskAssignment{
		Resources: &models.ResourcesDef{Memory: "256m"},
	}
	cfg := buildResourceConfig(assignment)
	expected := int64(256 * 1024 * 1024)
	if cfg.Memory != expected {
		t.Errorf("Memory: got %d, want %d", cfg.Memory, expected)
	}
}

func TestBuildResourceConfig_GPUs(t *testing.T) {
	assignment := &TaskAssignment{
		Resources: &models.ResourcesDef{GPUs: "all"},
	}
	cfg := buildResourceConfig(assignment)
	if len(cfg.DeviceRequests) != 1 {
		t.Fatalf("expected 1 DeviceRequest, got %d", len(cfg.DeviceRequests))
	}
	dr := cfg.DeviceRequests[0]
	if dr.Count != -1 {
		t.Errorf("Count: got %d, want -1 (all)", dr.Count)
	}
	if dr.Driver != "nvidia" {
		t.Errorf("Driver: got %q, want 'nvidia'", dr.Driver)
	}
	if len(dr.Capabilities) == 0 || dr.Capabilities[0][0] != "gpu" {
		t.Errorf("Capabilities: got %v, want [[gpu]]", dr.Capabilities)
	}
}

func TestBuildResourceConfig_Nil(t *testing.T) {
	assignment := &TaskAssignment{}
	cfg := buildResourceConfig(assignment)
	if cfg.NanoCPUs != 0 || cfg.Memory != 0 || len(cfg.DeviceRequests) != 0 {
		t.Errorf("expected empty config for nil Resources, got %+v", cfg)
	}
}

// ---- NewExecutor routing ----------------------------------------------------

func TestNewExecutor_LocalWhenNoImage(t *testing.T) {
	a := &TaskAssignment{Command: "echo hi"}
	e := NewExecutor(a)
	if _, ok := e.(*LocalExecutor); !ok {
		t.Errorf("expected LocalExecutor, got %T", e)
	}
}

func TestNewExecutor_DockerWhenImageSet(t *testing.T) {
	a := &TaskAssignment{Image: "alpine:latest", Command: "echo hi"}
	e := NewExecutor(a)
	if _, ok := e.(*DockerExecutor); !ok {
		t.Errorf("expected DockerExecutor, got %T", e)
	}
}

// ---- LocalExecutor ----------------------------------------------------------

func TestLocalExecutor_RunSuccess(t *testing.T) {
	e := &LocalExecutor{}
	var lines []string
	result, err := e.Run(context.Background(), &TaskAssignment{
		Command: "echo hello",
	}, func(line string) {
		lines = append(lines, line)
	}, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result.Output, "hello") {
		t.Errorf("expected output to contain 'hello', got %q", result.Output)
	}
	if len(lines) == 0 || lines[0] != "hello" {
		t.Errorf("expected callback line 'hello', got %v", lines)
	}
}

func TestLocalExecutor_RunFailure(t *testing.T) {
	e := &LocalExecutor{}
	_, err := e.Run(context.Background(), &TaskAssignment{
		Command: "exit 1",
	}, nil, nil)
	if err == nil {
		t.Fatal("expected error for exit 1, got nil")
	}
}

func TestLocalExecutor_CancelCallback(t *testing.T) {
	e := &LocalExecutor{}
	var gotCancel func()
	_, _ = e.Run(context.Background(), &TaskAssignment{
		Command: "echo ok",
	}, nil, func(cancelFn func()) {
		gotCancel = cancelFn
	})
	if gotCancel == nil {
		t.Error("expected cancel callback to be called")
	}
}

// ---- PrepareTaskAssignment with container fields ----------------------------

func TestPrepareTaskAssignment_ContainerFields(t *testing.T) {
	run := &models.DagRun{ID: "r1", DAGID: "d1"}
	ti := models.TaskInstance{ID: "ti1", RunID: "r1", TaskID: "t1", Attempt: 1}
	dag := &models.DAGDef{
		ID: "d1",
		Tasks: []models.TaskDef{
			{
				ID:      "t1",
				Command: "python train.py",
				Image:   "pytorch/pytorch:latest",
				Workdir: "/workspace",
				Volumes: []string{"./data:/data"},
				Resources: &models.ResourcesDef{
					CPUs:   "4.0",
					Memory: "8g",
					GPUs:   "1",
				},
			},
		},
	}

	assignment, err := PrepareTaskAssignment(run, ti, dag)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if assignment.Image != "pytorch/pytorch:latest" {
		t.Errorf("Image: got %q", assignment.Image)
	}
	if assignment.Workdir != "/workspace" {
		t.Errorf("Workdir: got %q", assignment.Workdir)
	}
	if len(assignment.Volumes) != 1 || assignment.Volumes[0] != "./data:/data" {
		t.Errorf("Volumes: got %v", assignment.Volumes)
	}
	if assignment.Resources == nil {
		t.Fatal("expected Resources to be non-nil")
	}
	if assignment.Resources.CPUs != "4.0" {
		t.Errorf("Resources.CPUs: got %q", assignment.Resources.CPUs)
	}
	if assignment.Resources.Memory != "8g" {
		t.Errorf("Resources.Memory: got %q", assignment.Resources.Memory)
	}
	if assignment.Resources.GPUs != "1" {
		t.Errorf("Resources.GPUs: got %q", assignment.Resources.GPUs)
	}
}
