package autoscaler

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
	dockerclient "github.com/docker/docker/client"

	"github.com/alephmelo/nagare/internal/config"
)

// Label keys stamped on every container created by the Docker provider.
// These are used by List() to distinguish Nagare-managed containers from
// other containers running on the same Docker daemon.
const (
	labelManagedBy      = "nagare.managed-by"
	labelManagedByValue = "nagare-autoscaler"
	labelInstanceID     = "nagare.instance-id"
	labelPools          = "nagare.pools" // comma-separated pool names
)

// dockerClient is the subset of the Docker SDK API that DockerProvider uses.
// The interface exists so tests can substitute a fake implementation without
// requiring a real Docker daemon.
type dockerClient interface {
	// ContainerCreate creates a container and returns its ID.
	ContainerCreate(ctx context.Context, image string, cmd []string, labels map[string]string, network string) (string, error)
	// ContainerStart starts a created container.
	ContainerStart(ctx context.Context, id string) error
	// ContainerRemove force-removes a container (running or stopped).
	ContainerRemove(ctx context.Context, id string) error
	// ContainerList returns containers matching the given label filter.
	ContainerList(ctx context.Context, labelFilter map[string]string) ([]containerInfo, error)
}

// containerInfo is a minimal descriptor returned by ContainerList.
type containerInfo struct {
	ID     string
	Labels map[string]string
}

// realDockerClient wraps the Docker SDK and implements dockerClient.
type realDockerClient struct {
	cli *dockerclient.Client
}

func newRealDockerClient() (*realDockerClient, error) {
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("docker: create client: %w", err)
	}
	return &realDockerClient{cli: cli}, nil
}

func (r *realDockerClient) ContainerCreate(ctx context.Context, image string, cmd []string, labels map[string]string, networkMode string) (string, error) {
	resp, err := r.cli.ContainerCreate(
		ctx,
		&container.Config{
			Image:  image,
			Cmd:    cmd,
			Labels: labels,
		},
		&container.HostConfig{
			NetworkMode: container.NetworkMode(networkMode),
			AutoRemove:  false, // we manage removal ourselves via SpinDown
		},
		&network.NetworkingConfig{},
		nil,
		"", // auto-generated container name
	)
	if err != nil {
		return "", fmt.Errorf("docker: ContainerCreate: %w", err)
	}
	return resp.ID, nil
}

func (r *realDockerClient) ContainerStart(ctx context.Context, id string) error {
	if err := r.cli.ContainerStart(ctx, id, types.ContainerStartOptions{}); err != nil {
		return fmt.Errorf("docker: ContainerStart %s: %w", id, err)
	}
	return nil
}

func (r *realDockerClient) ContainerRemove(ctx context.Context, id string) error {
	if err := r.cli.ContainerRemove(ctx, id, types.ContainerRemoveOptions{Force: true}); err != nil {
		return fmt.Errorf("docker: ContainerRemove %s: %w", id, err)
	}
	return nil
}

func (r *realDockerClient) ContainerList(ctx context.Context, labelFilter map[string]string) ([]containerInfo, error) {
	f := filters.NewArgs()
	for k, v := range labelFilter {
		f.Add("label", fmt.Sprintf("%s=%s", k, v))
	}
	containers, err := r.cli.ContainerList(ctx, types.ContainerListOptions{Filters: f})
	if err != nil {
		return nil, fmt.Errorf("docker: ContainerList: %w", err)
	}
	out := make([]containerInfo, 0, len(containers))
	for _, c := range containers {
		out = append(out, containerInfo{ID: c.ID, Labels: c.Labels})
	}
	return out, nil
}

// DockerProvider implements CloudProvider using the local Docker daemon.
//
// Each call to SpinUp creates and starts a new Docker container running:
//
//	nagare --worker --join <MasterAddr> --pools <pool1,pool2> --token <Token>
//
// The container must be built from an image that has the nagare binary on
// PATH (e.g. /usr/local/bin/nagare).  The default image tag is "nagare:latest"
// but can be overridden via nagare.yaml → autoscaler.docker.image.
//
// All containers created by this provider are labelled with
// nagare.managed-by=nagare-autoscaler so that List() can find them on
// master restart without storing state elsewhere.
type DockerProvider struct {
	cfg    config.DockerProviderConfig
	client dockerClient
}

// NewDockerProvider creates a DockerProvider that connects to the Docker daemon
// via the standard environment variables (DOCKER_HOST, etc.).
func NewDockerProvider(cfg config.DockerProviderConfig) (*DockerProvider, error) {
	cli, err := newRealDockerClient()
	if err != nil {
		return nil, err
	}
	return &DockerProvider{cfg: cfg, client: cli}, nil
}

// newDockerProviderWithClient creates a DockerProvider with an injected client.
// Used exclusively in unit tests.
func newDockerProviderWithClient(cfg config.DockerProviderConfig, cli dockerClient) *DockerProvider {
	return &DockerProvider{cfg: cfg, client: cli}
}

// Name implements CloudProvider.
func (d *DockerProvider) Name() string { return "docker" }

// SpinUp implements CloudProvider.
//
// It creates a Docker container from the configured image, passing the full
// nagare worker CLI invocation as the container command.  The method returns
// as soon as the container has been started — it does NOT wait for the nagare
// process to register with the master.
func (d *DockerProvider) SpinUp(ctx context.Context, req SpinUpRequest) (WorkerInstance, error) {
	image := d.cfg.Image
	if image == "" {
		image = "nagare:latest"
	}
	networkMode := d.cfg.Network
	if networkMode == "" {
		networkMode = "host"
	}

	// Build the worker command: nagare --worker --join <addr> --pools <p1,p2> [--token <t>]
	masterAddr := req.MasterAddr
	if d.cfg.MasterAddr != "" {
		masterAddr = d.cfg.MasterAddr
	}
	cmd := []string{
		"nagare",
		"--worker",
		"--join", masterAddr,
		"--pools", strings.Join(req.Pools, ","),
	}
	if req.Token != "" {
		cmd = append(cmd, "--token", req.Token)
	}

	labels := map[string]string{
		labelManagedBy:  labelManagedByValue,
		labelInstanceID: req.InstanceID,
		labelPools:      strings.Join(req.Pools, ","),
	}

	containerID, err := d.client.ContainerCreate(ctx, image, cmd, labels, networkMode)
	if err != nil {
		return WorkerInstance{}, fmt.Errorf("docker provider: SpinUp %s: %w", req.InstanceID, err)
	}

	if err := d.client.ContainerStart(ctx, containerID); err != nil {
		// Best-effort cleanup: remove the container we just created.
		_ = d.client.ContainerRemove(ctx, containerID)
		return WorkerInstance{}, fmt.Errorf("docker provider: start container %s: %w", containerID, err)
	}

	return WorkerInstance{
		ID:         req.InstanceID,
		ProviderID: containerID,
		Pools:      req.Pools,
		Status:     InstanceProvisioning,
		CreatedAt:  time.Now(),
	}, nil
}

// SpinDown implements CloudProvider.
//
// It force-removes the Docker container identified by providerID (the Docker
// container ID).  Any running process inside is killed immediately — callers
// should drain in-flight tasks before calling SpinDown.
func (d *DockerProvider) SpinDown(ctx context.Context, providerID string) error {
	if err := d.client.ContainerRemove(ctx, providerID); err != nil {
		return fmt.Errorf("docker provider: SpinDown %s: %w", providerID, err)
	}
	return nil
}

// List implements CloudProvider.
//
// It queries the Docker daemon for all containers labelled with
// nagare.managed-by=nagare-autoscaler, returning them as WorkerInstances in
// InstanceProvisioning status (the autoscaler reconciles their true status via
// the Coordinator's worker registry).
func (d *DockerProvider) List(ctx context.Context) ([]WorkerInstance, error) {
	containers, err := d.client.ContainerList(ctx, map[string]string{
		labelManagedBy: labelManagedByValue,
	})
	if err != nil {
		return nil, fmt.Errorf("docker provider: List: %w", err)
	}

	out := make([]WorkerInstance, 0, len(containers))
	for _, c := range containers {
		pools := strings.Split(c.Labels[labelPools], ",")
		out = append(out, WorkerInstance{
			ID:         c.Labels[labelInstanceID],
			ProviderID: c.ID,
			Pools:      pools,
			Status:     InstanceProvisioning, // will be reconciled by the autoscaler
		})
	}
	return out, nil
}
