package worker

import (
	"context"
	"os"
	"os/exec"
)

// LocalExecutor runs a task as a local OS subprocess via "sh -c".
// It is the default executor when no container image is specified.
type LocalExecutor struct{}

// Run executes the task command in a local shell subprocess.
// It satisfies the Executor interface.
func (e *LocalExecutor) Run(ctx context.Context, assignment *TaskAssignment, onLine func(string), onCancel func(cancelFn func())) (RunResult, error) {
	return RunCommand(
		ctx,
		assignment.Command,
		assignment.Env,
		assignment.TimeoutSecs,
		onLine,
		func(cmd *exec.Cmd, pr *os.File) {
			if onCancel != nil {
				onCancel(func() {
					killLocalProcess(cmd, pr)
				})
			}
		},
	)
}
