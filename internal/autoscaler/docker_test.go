package autoscaler

import (
	"context"
	"strings"
	"testing"

	"github.com/alephmelo/nagare/internal/config"
)

// ---- DockerProvider unit tests (no real Docker daemon required) ----
//
// These tests exercise SpinUp / SpinDown / List using a fake Docker client
// injected via the dockerClient interface.  Integration tests that require
// a real daemon are skipped automatically when Docker is unavailable.

// fakeDockerClient implements dockerClient with in-memory state.
type fakeDockerClient struct {
	containers  map[string]fakeContainer // keyed by container ID
	nextID      int
	spinUpErr   error
	spinDownErr error
	listErr     error
}

type fakeContainer struct {
	id     string
	image  string
	cmd    []string
	labels map[string]string
	alive  bool
}

func newFakeDockerClient() *fakeDockerClient {
	return &fakeDockerClient{containers: make(map[string]fakeContainer)}
}

func (f *fakeDockerClient) ContainerCreate(ctx context.Context, image string, cmd []string, labels map[string]string, network string) (string, error) {
	if f.spinUpErr != nil {
		return "", f.spinUpErr
	}
	f.nextID++
	id := strings.Repeat("a", 12) // deterministic-enough fake ID
	f.containers[id] = fakeContainer{id: id, image: image, cmd: cmd, labels: labels, alive: true}
	return id, nil
}

func (f *fakeDockerClient) ContainerStart(ctx context.Context, id string) error {
	if f.spinUpErr != nil {
		return f.spinUpErr
	}
	return nil
}

func (f *fakeDockerClient) ContainerRemove(ctx context.Context, id string) error {
	if f.spinDownErr != nil {
		return f.spinDownErr
	}
	if c, ok := f.containers[id]; ok {
		c.alive = false
		f.containers[id] = c
	}
	return nil
}

func (f *fakeDockerClient) ContainerList(ctx context.Context, labelFilter map[string]string) ([]containerInfo, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	var out []containerInfo
	for _, c := range f.containers {
		if !c.alive {
			continue
		}
		match := true
		for k, v := range labelFilter {
			if c.labels[k] != v {
				match = false
				break
			}
		}
		if match {
			out = append(out, containerInfo{ID: c.id, Labels: c.labels})
		}
	}
	return out, nil
}

// ---- Tests ----

func TestDockerProvider_Name(t *testing.T) {
	p := newDockerProviderWithClient(config.DockerProviderConfig{Image: "nagare:latest"}, newFakeDockerClient())
	if p.Name() != "docker" {
		t.Errorf("expected name %q, got %q", "docker", p.Name())
	}
}

func TestDockerProvider_SpinUp_SetsFields(t *testing.T) {
	fake := newFakeDockerClient()
	p := newDockerProviderWithClient(config.DockerProviderConfig{
		Image:   "nagare:test",
		Network: "host",
	}, fake)

	req := SpinUpRequest{
		InstanceID: "test-instance-1",
		Pools:      []string{"default"},
		MasterAddr: "http://localhost:8080",
		Token:      "secret",
	}

	inst, err := p.SpinUp(context.Background(), req)
	if err != nil {
		t.Fatalf("SpinUp: %v", err)
	}

	if inst.ID != req.InstanceID {
		t.Errorf("expected ID %q, got %q", req.InstanceID, inst.ID)
	}
	if inst.ProviderID == "" {
		t.Error("ProviderID should not be empty after SpinUp")
	}
	if inst.Status != InstanceProvisioning {
		t.Errorf("expected status %q, got %q", InstanceProvisioning, inst.Status)
	}
	if len(inst.Pools) != 1 || inst.Pools[0] != "default" {
		t.Errorf("unexpected pools: %v", inst.Pools)
	}
	if inst.CreatedAt.IsZero() {
		t.Error("CreatedAt should be set")
	}
}

func TestDockerProvider_SpinUp_InjectsCorrectCommand(t *testing.T) {
	fake := newFakeDockerClient()
	p := newDockerProviderWithClient(config.DockerProviderConfig{Image: "nagare:test"}, fake)

	req := SpinUpRequest{
		InstanceID: "inst-2",
		Pools:      []string{"gpu_workers", "default"},
		MasterAddr: "http://10.0.0.1:8080",
		Token:      "tok123",
	}

	_, err := p.SpinUp(context.Background(), req)
	if err != nil {
		t.Fatalf("SpinUp: %v", err)
	}

	// Exactly one container should have been created.
	if len(fake.containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(fake.containers))
	}
	for _, c := range fake.containers {
		cmdStr := strings.Join(c.cmd, " ")
		if !strings.Contains(cmdStr, "--worker") {
			t.Errorf("command missing --worker flag: %q", cmdStr)
		}
		if !strings.Contains(cmdStr, "--join http://10.0.0.1:8080") {
			t.Errorf("command missing --join: %q", cmdStr)
		}
		if !strings.Contains(cmdStr, "--pools gpu_workers,default") {
			t.Errorf("command missing --pools: %q", cmdStr)
		}
		if !strings.Contains(cmdStr, "--token tok123") {
			t.Errorf("command missing --token: %q", cmdStr)
		}
	}
}

func TestDockerProvider_SpinUp_Labels(t *testing.T) {
	fake := newFakeDockerClient()
	p := newDockerProviderWithClient(config.DockerProviderConfig{Image: "nagare:test"}, fake)

	req := SpinUpRequest{InstanceID: "inst-3", Pools: []string{"default"}, MasterAddr: "http://host:8080"}
	_, err := p.SpinUp(context.Background(), req)
	if err != nil {
		t.Fatalf("SpinUp: %v", err)
	}

	for _, c := range fake.containers {
		if c.labels[labelManagedBy] != labelManagedByValue {
			t.Errorf("expected label %s=%s, got %s", labelManagedBy, labelManagedByValue, c.labels[labelManagedBy])
		}
		if c.labels[labelInstanceID] != "inst-3" {
			t.Errorf("expected label %s=inst-3, got %s", labelInstanceID, c.labels[labelInstanceID])
		}
	}
}

func TestDockerProvider_SpinDown_RemovesContainer(t *testing.T) {
	fake := newFakeDockerClient()
	p := newDockerProviderWithClient(config.DockerProviderConfig{Image: "nagare:test"}, fake)

	req := SpinUpRequest{InstanceID: "inst-4", Pools: []string{"default"}, MasterAddr: "http://host:8080"}
	inst, err := p.SpinUp(context.Background(), req)
	if err != nil {
		t.Fatalf("SpinUp: %v", err)
	}

	if err := p.SpinDown(context.Background(), inst.ProviderID); err != nil {
		t.Fatalf("SpinDown: %v", err)
	}

	c := fake.containers[inst.ProviderID]
	if c.alive {
		t.Error("container should be marked not-alive after SpinDown")
	}
}

func TestDockerProvider_List_ReturnsOnlyManagedContainers(t *testing.T) {
	fake := newFakeDockerClient()
	// Pre-populate one unmanaged container.
	fake.containers["unmanaged"] = fakeContainer{
		id:     "unmanaged",
		alive:  true,
		labels: map[string]string{"some.other.label": "value"},
	}

	p := newDockerProviderWithClient(config.DockerProviderConfig{Image: "nagare:test"}, fake)

	req := SpinUpRequest{InstanceID: "inst-5", Pools: []string{"default"}, MasterAddr: "http://host:8080"}
	if _, err := p.SpinUp(context.Background(), req); err != nil {
		t.Fatalf("SpinUp: %v", err)
	}

	instances, err := p.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(instances) != 1 {
		t.Errorf("expected 1 managed instance, got %d", len(instances))
	}
	if instances[0].ID != "inst-5" {
		t.Errorf("expected instance ID inst-5, got %q", instances[0].ID)
	}
}

func TestDockerProvider_SpinUp_ErrorPropagates(t *testing.T) {
	fake := newFakeDockerClient()
	fake.spinUpErr = errFake("docker daemon unavailable")
	p := newDockerProviderWithClient(config.DockerProviderConfig{Image: "nagare:test"}, fake)

	_, err := p.SpinUp(context.Background(), SpinUpRequest{
		InstanceID: "inst-6",
		Pools:      []string{"default"},
		MasterAddr: "http://host:8080",
	})
	if err == nil {
		t.Error("expected error from SpinUp when docker client fails")
	}
}

func TestDockerProvider_List_Empty(t *testing.T) {
	fake := newFakeDockerClient()
	p := newDockerProviderWithClient(config.DockerProviderConfig{Image: "nagare:test"}, fake)

	instances, err := p.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(instances) != 0 {
		t.Errorf("expected 0 instances, got %d", len(instances))
	}
}

// errFake is a minimal error type used in tests.
type errFake string

func (e errFake) Error() string { return string(e) }
