// Package tasklifecycle owns persisted task-attempt transition policy.
package tasklifecycle

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/alephmelo/nagare/internal/models"
)

type Disposition uint8

const (
	Applied Disposition = iota + 1
	AlreadyApplied
)

type MissingAttemptError struct{ AttemptID string }

func (e *MissingAttemptError) Error() string {
	return fmt.Sprintf("task attempt %q does not exist", e.AttemptID)
}

type InvalidTransitionError struct {
	AttemptID string
	From      models.TaskStatus
	Operation string
}

func (e *InvalidTransitionError) Error() string {
	return fmt.Sprintf("cannot %s task attempt %q from %q", e.Operation, e.AttemptID, e.From)
}

type ConflictError struct {
	AttemptID string
	Persisted models.TaskStatus
	Requested models.TaskStatus
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("task attempt %q already has conflicting outcome %q (requested %q)", e.AttemptID, e.Persisted, e.Requested)
}

type store interface {
	GetTaskInstance(string) (*models.TaskInstance, error)
	CompareAndSetTaskAttempt(string, models.TaskStatus, models.TaskAttemptMutation) (bool, error)
	CancelCurrentTaskAttempt(string, string, time.Time) (models.CurrentAttemptResult, error)
	RetryCurrentTaskAttempt(string, string, time.Time) (models.CurrentAttemptResult, error)
}

type Lifecycle struct{ store store }

func New(store store) *Lifecycle { return &Lifecycle{store: store} }

type Completion struct {
	AttemptID   string
	Succeeded   bool
	Output      string
	TimedOut    bool
	Retries     int
	CompletedAt time.Time
}

// CurrentCancellation identifies the exact attempt atomically selected by a
// logical-task cancellation and how that cancellation was classified.
type CurrentCancellation struct {
	Disposition Disposition
	AttemptID   string
}

func (l *Lifecycle) Claim(attemptID string, claimedAt time.Time) (Disposition, error) {
	attempt, err := l.get(attemptID)
	if err != nil {
		return 0, err
	}
	if attempt.Status == models.TaskRunning {
		return AlreadyApplied, nil
	}
	if attempt.Status != models.TaskQueued {
		return 0, invalid(attempt, "claim")
	}
	applied, err := l.store.CompareAndSetTaskAttempt(attemptID, models.TaskQueued, models.TaskAttemptMutation{
		Status: models.TaskRunning, UpdatedAt: claimedAt, StartedAt: &claimedAt,
	})
	if err != nil {
		return 0, err
	}
	if applied {
		return Applied, nil
	}
	return l.classifyClaim(attemptID)
}

func (l *Lifecycle) classifyClaim(attemptID string) (Disposition, error) {
	attempt, err := l.get(attemptID)
	if err != nil {
		return 0, err
	}
	if attempt.Status == models.TaskRunning {
		return AlreadyApplied, nil
	}
	return 0, invalid(attempt, "claim")
}

func (l *Lifecycle) Promote(attemptID string, queuedAt time.Time) (Disposition, error) {
	attempt, err := l.get(attemptID)
	if err != nil {
		return 0, err
	}
	if attempt.Status == models.TaskQueued {
		return AlreadyApplied, nil
	}
	if attempt.Status != models.TaskPending {
		return 0, invalid(attempt, "promote")
	}
	applied, err := l.store.CompareAndSetTaskAttempt(attemptID, models.TaskPending, models.TaskAttemptMutation{
		Status: models.TaskQueued, UpdatedAt: queuedAt,
	})
	if err != nil {
		return 0, err
	}
	if applied {
		return Applied, nil
	}
	current, err := l.get(attemptID)
	if err != nil {
		return 0, err
	}
	if current.Status == models.TaskQueued {
		return AlreadyApplied, nil
	}
	return 0, invalid(current, "promote")
}

func (l *Lifecycle) Complete(input Completion) (Disposition, error) {
	attempt, err := l.get(input.AttemptID)
	if err != nil {
		return 0, err
	}
	return l.completeObserved(attempt, input)
}

// CancelAttempt cancels one exact persisted attempt. It never resolves the
// logical task's current attempt, so a stale caller cannot cancel a successor.
func (l *Lifecycle) CancelAttempt(attemptID string, cancelledAt time.Time) (Disposition, error) {
	for {
		attempt, err := l.get(attemptID)
		if err != nil {
			return 0, err
		}
		if attempt.Status == models.TaskCancelled {
			return AlreadyApplied, nil
		}
		if !isCancellable(attempt.Status) {
			return 0, invalid(attempt, "cancel")
		}

		applied, err := l.store.CompareAndSetTaskAttempt(attemptID, attempt.Status, models.TaskAttemptMutation{
			Status: models.TaskCancelled, UpdatedAt: cancelledAt,
		})
		if err != nil {
			return 0, err
		}
		if applied {
			return Applied, nil
		}
		// A concurrent lifecycle transition won the compare-and-set. Observe
		// its exact row again and either replay, reject, or cancel the still
		// eligible state without ever selecting a newer attempt.
	}
}

// CancelCurrent cancels the current attempt of one logical task.
func (l *Lifecycle) CancelCurrent(runID, taskID string, cancelledAt time.Time) (Disposition, error) {
	result, err := l.CancelCurrentAttempt(runID, taskID, cancelledAt)
	return result.Disposition, err
}

// CancelCurrentAttempt atomically selects and cancels the current attempt of
// one logical task, returning the exact attempt selected by the store.
func (l *Lifecycle) CancelCurrentAttempt(runID, taskID string, cancelledAt time.Time) (CurrentCancellation, error) {
	result, err := l.store.CancelCurrentTaskAttempt(runID, taskID, cancelledAt)
	if errors.Is(err, sql.ErrNoRows) {
		return CurrentCancellation{}, &MissingAttemptError{AttemptID: runID + "/" + taskID}
	}
	if err != nil {
		return CurrentCancellation{}, err
	}
	if result.Applied {
		return CurrentCancellation{Disposition: Applied, AttemptID: result.Attempt.ID}, nil
	}
	if result.Attempt.Status == models.TaskCancelled {
		return CurrentCancellation{Disposition: AlreadyApplied, AttemptID: result.Attempt.ID}, nil
	}
	return CurrentCancellation{}, invalid(result.Attempt, "cancel")
}

// RetryCurrent creates one queued successor for the current retryable attempt.
func (l *Lifecycle) RetryCurrent(runID, taskID string, retriedAt time.Time) (Disposition, error) {
	result, err := l.store.RetryCurrentTaskAttempt(runID, taskID, retriedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, &MissingAttemptError{AttemptID: runID + "/" + taskID}
	}
	if err != nil {
		return 0, err
	}
	if result.Applied {
		return Applied, nil
	}
	if result.Replay {
		return AlreadyApplied, nil
	}
	if result.Attempt.Status == models.TaskCancelled {
		return AlreadyApplied, nil
	}
	return 0, invalid(result.Attempt, "retry")
}

func (l *Lifecycle) completeObserved(attempt *models.TaskInstance, input Completion) (Disposition, error) {
	// Cancellation is authoritative; late executor results cannot replace it.
	if attempt.Status == models.TaskCancelled {
		return AlreadyApplied, nil
	}

	target := completionStatus(attempt.Attempt, input)
	if isCompleted(attempt.Status) {
		if attempt.Status == target && attempt.Output == input.Output {
			return AlreadyApplied, nil
		}
		return 0, &ConflictError{AttemptID: input.AttemptID, Persisted: attempt.Status, Requested: target}
	}
	if attempt.Status != models.TaskRunning {
		return 0, invalid(attempt, "complete")
	}

	applied, err := l.store.CompareAndSetTaskAttempt(input.AttemptID, models.TaskRunning, models.TaskAttemptMutation{
		Status: target, Output: input.Output, SetOutput: true, UpdatedAt: input.CompletedAt,
	})
	if err != nil {
		return 0, err
	}
	if applied {
		return Applied, nil
	}
	current, err := l.get(input.AttemptID)
	if err != nil {
		return 0, err
	}
	return l.completeObserved(current, input)
}

func completionStatus(attempt int, input Completion) models.TaskStatus {
	if input.TimedOut {
		return models.TaskFailed
	}
	if input.Succeeded {
		return models.TaskSuccess
	}
	if attempt <= input.Retries {
		return models.TaskUpForRetry
	}
	return models.TaskFailed
}

func isCompleted(status models.TaskStatus) bool {
	switch status {
	case models.TaskSuccess, models.TaskFailed, models.TaskUpForRetry:
		return true
	default:
		return false
	}
}

func isCancellable(status models.TaskStatus) bool {
	switch status {
	case models.TaskPending, models.TaskQueued, models.TaskRunning, models.TaskUpForRetry:
		return true
	default:
		return false
	}
}

func (l *Lifecycle) get(id string) (*models.TaskInstance, error) {
	attempt, err := l.store.GetTaskInstance(id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, &MissingAttemptError{AttemptID: id}
	}
	return attempt, err
}

func invalid(attempt *models.TaskInstance, operation string) error {
	return &InvalidTransitionError{AttemptID: attempt.ID, From: attempt.Status, Operation: operation}
}
