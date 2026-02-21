package worker

import "context"

// Executor is the interface that all execution backends must implement.
// It runs a TaskAssignment and streams output line-by-line via onLine.
// onCancel is called once the execution has started and provides a function
// that, when invoked, cancels (kills) the running execution.
type Executor interface {
	Run(ctx context.Context, assignment *TaskAssignment, onLine func(string), onCancel func(cancelFn func())) (RunResult, error)
}

// NewExecutor returns the appropriate Executor for the given assignment.
// If the assignment specifies an Image, a DockerExecutor is returned.
// Otherwise, a LocalExecutor (sh -c) is returned.
func NewExecutor(assignment *TaskAssignment) Executor {
	if assignment.Image != "" {
		return &DockerExecutor{}
	}
	return &LocalExecutor{}
}
