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
	Stale
	Invalid
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
	CompareAndSetTaskAttemptForRunStatus(
		string,
		models.TaskStatus,
		models.RunStatus,
		models.TaskAttemptMutation,
	) (bool, error)
	PromoteTaskAttemptForRunSnapshot(
		string,
		string,
		models.RunStatus,
		[]models.TaskInstance,
		time.Time,
	) (models.GuardedTaskPromotionResult, error)
	StartMapSetupForRunSnapshot(
		string,
		string,
		models.RunStatus,
		[]models.TaskInstance,
		models.MapSetup,
	) (models.GuardedTaskSetupResult, error)
	CancelCurrentTaskAttempt(string, string, time.Time) (models.CurrentAttemptResult, error)
	RetryCurrentTaskAttempt(string, string, time.Time) (models.CurrentAttemptResult, error)
	RetryCurrentTaskAttemptPending(string, string, time.Time) (models.CurrentAttemptResult, error)
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

// StartSetup atomically takes ownership of one exact pending attempt without
// exposing a queued intermediate to workers. Queued is accepted for recovery;
// it competes with worker Claim and only the compare-and-set winner is Applied.
func (l *Lifecycle) StartSetup(attemptID string, startedAt time.Time) (Disposition, error) {
	attempt, err := l.get(attemptID)
	if err != nil {
		return 0, err
	}
	switch attempt.Status {
	case models.TaskPending, models.TaskQueued:
		applied, err := l.store.CompareAndSetTaskAttemptForRunStatus(
			attemptID,
			attempt.Status,
			models.RunRunning,
			models.TaskAttemptMutation{
				Status: models.TaskRunning, UpdatedAt: startedAt, StartedAt: &startedAt,
			},
		)
		if err != nil {
			return 0, err
		}
		if applied {
			return Applied, nil
		}
	case models.TaskRunning:
		applied, err := l.guardReplay(attempt)
		if err != nil {
			return 0, err
		}
		if applied {
			return AlreadyApplied, nil
		}
	default:
		return 0, invalid(attempt, "start setup")
	}

	current, err := l.get(attemptID)
	if err != nil {
		return 0, err
	}
	switch current.Status {
	case models.TaskRunning:
		if attempt.Status == models.TaskRunning {
			return Stale, nil
		}
		return AlreadyApplied, nil
	case models.TaskPending, models.TaskQueued:
		return Stale, nil
	default:
		return 0, invalid(current, "start setup")
	}
}

func (l *Lifecycle) Claim(attemptID string, claimedAt time.Time) (Disposition, error) {
	attempt, err := l.get(attemptID)
	if err != nil {
		return 0, err
	}
	switch attempt.Status {
	case models.TaskRunning:
		applied, err := l.guardReplay(attempt)
		if err != nil {
			return 0, err
		}
		if applied {
			return AlreadyApplied, nil
		}
	case models.TaskQueued:
		applied, err := l.store.CompareAndSetTaskAttemptForRunStatus(
			attemptID,
			models.TaskQueued,
			models.RunRunning,
			models.TaskAttemptMutation{
				Status: models.TaskRunning, UpdatedAt: claimedAt, StartedAt: &claimedAt,
			},
		)
		if err != nil {
			return 0, err
		}
		if applied {
			return Applied, nil
		}
	default:
		return 0, invalid(attempt, "claim")
	}
	return l.classifyClaim(attemptID, attempt.Status)
}

func (l *Lifecycle) classifyClaim(
	attemptID string,
	observedStatus models.TaskStatus,
) (Disposition, error) {
	attempt, err := l.get(attemptID)
	if err != nil {
		return 0, err
	}
	if attempt.Status == models.TaskRunning {
		if observedStatus == models.TaskRunning {
			return Stale, nil
		}
		return AlreadyApplied, nil
	}
	if attempt.Status == models.TaskQueued {
		return Stale, nil
	}
	return 0, invalid(attempt, "claim")
}

func (l *Lifecycle) Promote(attemptID string, queuedAt time.Time) (Disposition, error) {
	attempt, err := l.get(attemptID)
	if err != nil {
		return 0, err
	}
	switch attempt.Status {
	case models.TaskQueued:
		applied, err := l.guardReplay(attempt)
		if err != nil {
			return 0, err
		}
		if applied {
			return AlreadyApplied, nil
		}
	case models.TaskPending:
		applied, err := l.store.CompareAndSetTaskAttemptForRunStatus(
			attemptID,
			models.TaskPending,
			models.RunRunning,
			models.TaskAttemptMutation{Status: models.TaskQueued, UpdatedAt: queuedAt},
		)
		if err != nil {
			return 0, err
		}
		if applied {
			return Applied, nil
		}
	default:
		return 0, invalid(attempt, "promote")
	}
	current, err := l.get(attemptID)
	if err != nil {
		return 0, err
	}
	if current.Status == models.TaskQueued {
		if attempt.Status == models.TaskQueued {
			return Stale, nil
		}
		return AlreadyApplied, nil
	}
	if current.Status == models.TaskPending {
		return Stale, nil
	}
	return 0, invalid(current, "promote")
}

// PromoteGuarded atomically promotes one exact pending attempt only while the
// observed run status and complete latest-attempt snapshot remain current.
// Stale is a normal reconciliation outcome: callers must reload and evaluate
// again instead of treating it as an illegal task transition.
func (l *Lifecycle) PromoteGuarded(
	target models.TaskInstance,
	observedRunStatus models.RunStatus,
	observedTasks []models.TaskInstance,
	queuedAt time.Time,
) (Disposition, error) {
	result, err := l.store.PromoteTaskAttemptForRunSnapshot(
		target.RunID,
		target.ID,
		observedRunStatus,
		observedTasks,
		queuedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, &MissingAttemptError{AttemptID: target.ID}
		}
		return 0, err
	}
	return guardedDisposition(target.ID, "promote", result)
}

// StartSetupGuarded atomically persists the exact map binding and takes
// scheduler setup ownership only while the observed running run and complete
// latest-attempt snapshot remain current.
func (l *Lifecycle) StartSetupGuarded(
	target models.TaskInstance,
	observedRunStatus models.RunStatus,
	observedTasks []models.TaskInstance,
	setup models.MapSetup,
) (Disposition, error) {
	result, err := l.store.StartMapSetupForRunSnapshot(
		target.RunID,
		target.ID,
		observedRunStatus,
		observedTasks,
		setup,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, &MissingAttemptError{AttemptID: target.ID}
		}
		return 0, err
	}
	return guardedDisposition(target.ID, "start setup", result)
}

func guardedDisposition(
	attemptID, operation string,
	result models.GuardedTaskPromotionResult,
) (Disposition, error) {
	switch result {
	case models.GuardedTaskPromotionApplied:
		return Applied, nil
	case models.GuardedTaskPromotionAlreadyApplied:
		return AlreadyApplied, nil
	case models.GuardedTaskPromotionStale:
		return Stale, nil
	case models.GuardedTaskPromotionInvalid:
		return Invalid, nil
	default:
		return 0, fmt.Errorf(
			"%s attempt %s returned unknown guarded result %d",
			operation, attemptID, result,
		)
	}
}

func (l *Lifecycle) guardReplay(attempt *models.TaskInstance) (bool, error) {
	return l.store.CompareAndSetTaskAttemptForRunStatus(
		attempt.ID,
		attempt.Status,
		models.RunRunning,
		models.TaskAttemptMutation{
			Status:    attempt.Status,
			UpdatedAt: attempt.UpdatedAt,
			StartedAt: attempt.StartedAt,
		},
	)
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

// RetryCurrentPending creates one pending successor for scheduler-owned setup.
// Unlike RetryCurrent, the successor is not visible to worker dispatch.
func (l *Lifecycle) RetryCurrentPending(runID, taskID string, retriedAt time.Time) (Disposition, error) {
	result, err := l.store.RetryCurrentTaskAttemptPending(runID, taskID, retriedAt)
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
