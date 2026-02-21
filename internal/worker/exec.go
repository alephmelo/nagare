package worker

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/alephmelo/nagare/internal/models"
)

// TaskAssignment holds everything a worker needs to execute a task, whether
// it runs locally or on a remote node.
type TaskAssignment struct {
	TaskInstanceID string
	RunID          string
	Command        string
	Env            []string
	TimeoutSecs    int
	Retries        int
	Attempt        int

	// Container executor fields — only populated when the task specifies an image.
	Image     string
	Workdir   string
	Volumes   []string
	Resources *models.ResourcesDef
}

// PrepareTaskAssignment resolves a TaskInstance + DagRun into a TaskAssignment
// by looking up the task definition in the DAG, applying {{item}} substitution,
// and assembling the full environment variable list.
//
// Returns an error if the task definition cannot be found in the DAG.
func PrepareTaskAssignment(run *models.DagRun, ti models.TaskInstance, dag *models.DAGDef) (*TaskAssignment, error) {
	baseTaskID := ti.TaskID
	if idx := strings.Index(baseTaskID, "["); idx != -1 {
		baseTaskID = baseTaskID[:idx]
	}

	var taskDef *models.TaskDef
	for i := range dag.Tasks {
		if dag.Tasks[i].ID == baseTaskID {
			taskDef = &dag.Tasks[i]
			break
		}
	}
	if taskDef == nil {
		return nil, fmt.Errorf("task definition %q not found in DAG %q", ti.TaskID, dag.ID)
	}

	// Apply {{item}} template substitution for map tasks.
	cmdStr := taskDef.Command
	if ti.ItemValue != nil {
		cmdStr = strings.ReplaceAll(cmdStr, "{{item}}", *ti.ItemValue)
	}

	// Assemble environment: OS env + execution metadata + run conf + task env.
	env := make([]string, 0, len(os.Environ())+10)
	env = append(env, os.Environ()...)

	execDateStr := run.ExecDate.Format(time.RFC3339)
	env = append(env, fmt.Sprintf("NAGARE_EXECUTION_DATE=%s", execDateStr))
	env = append(env, fmt.Sprintf("NAGARE_SCHEDULED_TIME=%s", execDateStr))

	for k, v := range run.Conf {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}
	for k, v := range taskDef.Env {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}

	return &TaskAssignment{
		TaskInstanceID: ti.ID,
		RunID:          ti.RunID,
		Command:        cmdStr,
		Env:            env,
		TimeoutSecs:    taskDef.TimeoutSeconds,
		Retries:        taskDef.Retries,
		Attempt:        ti.Attempt,
		Image:          taskDef.Image,
		Workdir:        taskDef.Workdir,
		Volumes:        taskDef.Volumes,
		Resources:      taskDef.Resources,
	}, nil
}

// RunResult is returned by RunCommand after a command finishes.
type RunResult struct {
	Output   string
	TimedOut bool
}

// killLocalProcess terminates a running local process and its entire process
// group, then closes the pipe reader to unblock the output scanner goroutine.
func killLocalProcess(cmd *exec.Cmd, pr *os.File) {
	if cmd != nil && cmd.Process != nil {
		pgid := cmd.Process.Pid
		if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil {
			cmd.Process.Kill()
		}
	}
	if pr != nil {
		pr.Close()
	}
}

// RunCommand executes cmdStr in a shell subprocess, optionally bounded by
// timeoutSecs (0 = no timeout beyond the parent ctx). extraEnv is the full
// environment slice (os.Environ() + extras already merged by caller).
//
// onLine, if non-nil, is called synchronously for each line of combined
// stdout+stderr output as it arrives.
//
// onStart, if non-nil, is called with the started *exec.Cmd immediately after
// cmd.Start() succeeds. The caller can use this to register the process for
// external kills.
//
// Returns a RunResult and any execution error.
func RunCommand(ctx context.Context, cmdStr string, extraEnv []string, timeoutSecs int, onLine func(string), onStart func(*exec.Cmd, *os.File)) (RunResult, error) {
	var cmdCtx context.Context
	var cancel context.CancelFunc

	if timeoutSecs > 0 {
		cmdCtx, cancel = context.WithTimeout(ctx, time.Duration(timeoutSecs)*time.Second)
	} else {
		cmdCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, "sh", "-c", cmdStr)

	// Place the child in its own process group so that killing the group also
	// terminates any subprocesses spawned by the shell command.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if len(extraEnv) > 0 {
		cmd.Env = extraEnv
	}

	// Use an os.Pipe so both stdout and stderr flow into a single *os.File.
	// Because cmd.Stdout is an *os.File, exec does NOT start an internal copy
	// goroutine; cmd.Wait() returns as soon as the process exits.
	pr, pw, pipeErr := os.Pipe()
	if pipeErr != nil {
		return RunResult{}, pipeErr
	}
	cmd.Stdout = pw
	cmd.Stderr = pw

	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		return RunResult{}, err
	}

	// Close the write end in the parent — the child holds its own inherited copy.
	pw.Close()

	// Notify caller that the process has started (for kill registration).
	if onStart != nil {
		onStart(cmd, pr)
	}

	var buf strings.Builder
	scanDone := make(chan struct{})
	go func() {
		defer close(scanDone)
		scanner := bufio.NewScanner(pr)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			buf.WriteString(line + "\n")
			if onLine != nil {
				onLine(line)
			}
		}
	}()

	runErr := cmd.Wait()
	pr.Close() // signal EOF to scanner
	<-scanDone

	result := RunResult{
		Output:   buf.String(),
		TimedOut: cmdCtx.Err() == context.DeadlineExceeded,
	}

	return result, runErr
}
