package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
)

// DockerExecutor runs a task inside a Docker container.
// It pulls the image if not present locally, creates an ephemeral container,
// streams combined stdout+stderr via the onLine callback, and removes the
// container after execution completes.
type DockerExecutor struct{}

// Run implements Executor for Docker containers.
func (e *DockerExecutor) Run(ctx context.Context, assignment *TaskAssignment, onLine func(string), onCancel func(cancelFn func())) (RunResult, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return RunResult{}, fmt.Errorf("docker: failed to create client: %w", err)
	}
	defer cli.Close()

	// ---- timeout context ------------------------------------------------
	var runCtx context.Context
	var cancel context.CancelFunc
	if assignment.TimeoutSecs > 0 {
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(assignment.TimeoutSecs)*time.Second)
	} else {
		runCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	// ---- image pull (if not present) ------------------------------------
	if err := ensureImage(runCtx, cli, assignment.Image, onLine); err != nil {
		return RunResult{}, err
	}

	// ---- build container config ----------------------------------------
	containerCfg := &container.Config{
		Image: assignment.Image,
		Cmd:   []string{"sh", "-c", assignment.Command},
		Env:   assignment.Env,
	}
	if assignment.Workdir != "" {
		containerCfg.WorkingDir = assignment.Workdir
	}

	hostCfg := &container.HostConfig{
		Binds:      assignment.Volumes,
		AutoRemove: false, // we remove manually to capture output first
		Resources:  buildResourceConfig(assignment),
	}

	// ---- create container -----------------------------------------------
	resp, err := cli.ContainerCreate(runCtx, containerCfg, hostCfg, nil, nil, "")
	if err != nil {
		return RunResult{}, fmt.Errorf("docker: ContainerCreate: %w", err)
	}
	containerID := resp.ID

	// Ensure the container is removed when we're done, regardless of outcome.
	defer func() {
		rmCtx, rmCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer rmCancel()
		if err := cli.ContainerRemove(rmCtx, containerID, types.ContainerRemoveOptions{Force: true}); err != nil {
			log.Printf("docker: warning: failed to remove container %s: %v", containerID[:12], err)
		}
	}()

	// ---- register cancel hook -------------------------------------------
	if onCancel != nil {
		onCancel(func() {
			killCtx, killCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer killCancel()
			if err := cli.ContainerKill(killCtx, containerID, "SIGKILL"); err != nil {
				log.Printf("docker: warning: failed to kill container %s: %v", containerID[:12], err)
			}
		})
	}

	// ---- start container ------------------------------------------------
	startTime := time.Now()
	if err := cli.ContainerStart(runCtx, containerID, types.ContainerStartOptions{}); err != nil {
		return RunResult{}, fmt.Errorf("docker: ContainerStart: %w", err)
	}

	// ---- resource stats collection goroutine ----------------------------
	var peakMemoryBytes uint64
	var totalCPUDelta uint64
	var statsWg sync.WaitGroup
	statsCtx, statsCancel := context.WithCancel(context.Background())
	defer statsCancel()

	statsWg.Add(1)
	go func() {
		defer statsWg.Done()
		statsReader, err := cli.ContainerStats(statsCtx, containerID, true)
		if err != nil {
			return
		}
		defer statsReader.Body.Close()

		dec := json.NewDecoder(statsReader.Body)
		var prevCPU, prevSystem uint64
		first := true

		for {
			var s types.StatsJSON
			if err := dec.Decode(&s); err != nil {
				// EOF or context cancelled — normal shutdown
				return
			}
			// Track peak memory
			if s.MemoryStats.MaxUsage > 0 {
				if s.MemoryStats.MaxUsage > atomic.LoadUint64(&peakMemoryBytes) {
					atomic.StoreUint64(&peakMemoryBytes, s.MemoryStats.MaxUsage)
				}
			} else if s.MemoryStats.Usage > 0 {
				if s.MemoryStats.Usage > atomic.LoadUint64(&peakMemoryBytes) {
					atomic.StoreUint64(&peakMemoryBytes, s.MemoryStats.Usage)
				}
			}

			// Accumulate CPU delta (user+kernel total)
			cpuDelta := s.CPUStats.CPUUsage.TotalUsage - prevCPU
			systemDelta := s.CPUStats.SystemUsage - prevSystem
			if !first && systemDelta > 0 && cpuDelta > 0 {
				atomic.AddUint64(&totalCPUDelta, cpuDelta)
			}
			prevCPU = s.CPUStats.CPUUsage.TotalUsage
			prevSystem = s.CPUStats.SystemUsage
			first = false
		}
	}()

	// ---- stream logs ----------------------------------------------------
	logOpts := types.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
	}
	logReader, err := cli.ContainerLogs(runCtx, containerID, logOpts)
	if err != nil {
		return RunResult{}, fmt.Errorf("docker: ContainerLogs: %w", err)
	}
	defer logReader.Close()

	var buf strings.Builder
	scanDone := make(chan struct{})
	go func() {
		defer close(scanDone)
		// Docker multiplexes stdout and stderr into a single stream with an
		// 8-byte header per frame. We strip headers via io.Pipe + demux, but
		// since we merge them anyway we just read with a header-stripping loop.
		r := newDockerLogReader(logReader)
		scanner := bufio.NewScanner(r)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			buf.WriteString(line + "\n")
			if onLine != nil {
				onLine(line)
			}
		}
	}()

	// ---- wait for exit --------------------------------------------------
	statusCh, errCh := cli.ContainerWait(runCtx, containerID, container.WaitConditionNotRunning)

	var exitCode int64
	var waitErr error
	select {
	case status := <-statusCh:
		exitCode = status.StatusCode
		if status.Error != nil {
			waitErr = fmt.Errorf("%s", status.Error.Message)
		}
	case waitErr = <-errCh:
	}

	durationMs := time.Since(startTime).Milliseconds()

	// Stop the stats goroutine now that the container has exited.
	statsCancel()
	statsWg.Wait()

	// Wait for log scanning to finish.
	<-scanDone

	timedOut := runCtx.Err() == context.DeadlineExceeded

	// totalCPUDelta is in nanoseconds (Docker CPU accounting).
	cpuMs := int64(atomic.LoadUint64(&totalCPUDelta)) / int64(time.Millisecond)
	if cpuMs < 0 {
		cpuMs = 0
	}

	result := RunResult{
		Output:          buf.String(),
		TimedOut:        timedOut,
		DurationMs:      durationMs,
		CpuUserMs:       cpuMs, // Docker doesn't split user/kernel easily; store in CpuUserMs
		CpuSystemMs:     0,
		PeakMemoryBytes: int64(atomic.LoadUint64(&peakMemoryBytes)),
		ExitCode:        int(exitCode),
		ExecutorType:    "docker",
	}

	if waitErr != nil {
		return result, fmt.Errorf("docker: container wait: %w", waitErr)
	}
	if exitCode != 0 {
		return result, fmt.Errorf("docker: container exited with code %d", exitCode)
	}
	return result, nil
}

// ---- helpers ----------------------------------------------------------------

// ensureImage pulls the image if it is not available locally.
// Pull progress lines are forwarded via onLine so the user sees download status.
func ensureImage(ctx context.Context, cli *client.Client, image string, onLine func(string)) error {
	// Check if image exists locally first.
	filterArgs := filters.NewArgs()
	filterArgs.Add("reference", image)
	images, err := cli.ImageList(ctx, types.ImageListOptions{Filters: filterArgs})
	if err == nil && len(images) > 0 {
		return nil // already present
	}

	if onLine != nil {
		onLine(fmt.Sprintf("[nagare] pulling image %s ...", image))
	}
	log.Printf("docker: pulling image %s", image)

	pullReader, err := cli.ImagePull(ctx, image, types.ImagePullOptions{})
	if err != nil {
		return fmt.Errorf("docker: ImagePull %s: %w", image, err)
	}
	defer pullReader.Close()

	// Stream pull progress as log lines.
	decoder := json.NewDecoder(pullReader)
	for {
		var event map[string]interface{}
		if err := decoder.Decode(&event); err == io.EOF {
			break
		} else if err != nil {
			break
		}
		if status, ok := event["status"].(string); ok && onLine != nil {
			if progress, ok := event["progress"].(string); ok {
				onLine(fmt.Sprintf("[nagare] %s %s", status, progress))
			} else {
				onLine(fmt.Sprintf("[nagare] %s", status))
			}
		}
	}
	return nil
}

// buildResourceConfig converts ResourcesDef into a Docker container.Resources.
func buildResourceConfig(assignment *TaskAssignment) container.Resources {
	if assignment.Resources == nil {
		return container.Resources{}
	}
	r := assignment.Resources
	cfg := container.Resources{}

	if r.CPUs != "" {
		if v, err := strconv.ParseFloat(r.CPUs, 64); err == nil {
			cfg.NanoCPUs = int64(v * 1e9)
		}
	}

	if r.Memory != "" {
		if bytes, err := parseMemory(r.Memory); err == nil {
			cfg.Memory = bytes
		}
	}

	if r.GPUs != "" {
		cfg.DeviceRequests = []container.DeviceRequest{
			{
				Driver:       "nvidia",
				Count:        gpuCount(r.GPUs),
				DeviceIDs:    nil,
				Capabilities: [][]string{{"gpu"}},
				Options:      map[string]string{},
			},
		}
	}

	return cfg
}

// parseMemory converts a memory string (e.g. "512m", "2g") to bytes.
func parseMemory(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return 0, fmt.Errorf("empty memory string")
	}
	lower := strings.ToLower(s)
	suffixes := map[byte]int64{'b': 1, 'k': 1024, 'm': 1024 * 1024, 'g': 1024 * 1024 * 1024}
	last := lower[len(lower)-1]
	if mult, ok := suffixes[last]; ok {
		num, err := strconv.ParseInt(lower[:len(lower)-1], 10, 64)
		if err != nil {
			return 0, err
		}
		return num * mult, nil
	}
	// No suffix — treat as bytes.
	return strconv.ParseInt(lower, 10, 64)
}

// gpuCount returns -1 (all) for "all", or the parsed integer count.
func gpuCount(s string) int {
	if s == "all" {
		return -1
	}
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		return -1
	}
	return v
}

// dockerLogReader strips the 8-byte Docker multiplexed stream header from each
// log frame, returning a plain reader that delivers raw log bytes.
// Docker stream format: [stream_type(1)] [0 0 0(3)] [frame_size(4)] [data...]
type dockerLogReader struct {
	r      io.Reader
	cur    io.Reader
	header [8]byte
}

func newDockerLogReader(r io.Reader) io.Reader {
	return &dockerLogReader{r: r}
}

func (d *dockerLogReader) Read(p []byte) (int, error) {
	for {
		if d.cur != nil {
			n, err := d.cur.Read(p)
			if n > 0 {
				return n, nil
			}
			if err == io.EOF {
				d.cur = nil
				continue
			}
			return n, err
		}

		// Read the 8-byte header.
		_, err := io.ReadFull(d.r, d.header[:])
		if err != nil {
			return 0, err
		}

		// Bytes 4-7 (big-endian) give the frame payload size.
		size := int64(d.header[4])<<24 | int64(d.header[5])<<16 | int64(d.header[6])<<8 | int64(d.header[7])
		if size > 0 {
			d.cur = io.LimitReader(d.r, size)
		}
	}
}
