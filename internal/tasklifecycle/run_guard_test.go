package tasklifecycle_test

import (
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/tasklifecycle"
)

func TestTerminalRunGuardsOrdinaryOwnershipTransitions(t *testing.T) {
	tests := []struct {
		name      string
		status    models.TaskStatus
		operation func(*tasklifecycle.Lifecycle) (tasklifecycle.Disposition, error)
	}{
		{
			name:   "claim",
			status: models.TaskQueued,
			operation: func(lifecycle *tasklifecycle.Lifecycle) (tasklifecycle.Disposition, error) {
				return lifecycle.Claim("attempt-1", changedAt)
			},
		},
		{
			name:   "start setup",
			status: models.TaskPending,
			operation: func(lifecycle *tasklifecycle.Lifecycle) (tasklifecycle.Disposition, error) {
				return lifecycle.StartSetup("attempt-1", changedAt)
			},
		},
		{
			name:   "promote",
			status: models.TaskPending,
			operation: func(lifecycle *tasklifecycle.Lifecycle) (tasklifecycle.Disposition, error) {
				return lifecycle.Promote("attempt-1", changedAt)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newStore(t)
			createAttempt(t, store, test.status, 1, "preserved")
			cancelled, err := store.CompareAndSetDagRunStatus(
				"run-1", models.RunRunning, models.RunCancelled, changedAt,
			)
			if err != nil || !cancelled {
				t.Fatalf("cancel run = (%v, %v), want (true, nil)", cancelled, err)
			}
			before := snapshot(t, store)

			disposition, err := test.operation(tasklifecycle.New(store))
			if err != nil {
				t.Fatalf("%s: %v", test.name, err)
			}
			if disposition != tasklifecycle.Stale {
				t.Fatalf("%s disposition = %v, want Stale", test.name, disposition)
			}
			assertSnapshot(t, store, before)
		})
	}
}

func TestTerminalRunGuardsOwnershipReplays(t *testing.T) {
	tests := []struct {
		name      string
		status    models.TaskStatus
		operation func(*tasklifecycle.Lifecycle) (tasklifecycle.Disposition, error)
	}{
		{
			name:   "claim replay",
			status: models.TaskRunning,
			operation: func(lifecycle *tasklifecycle.Lifecycle) (tasklifecycle.Disposition, error) {
				return lifecycle.Claim("attempt-1", changedAt)
			},
		},
		{
			name:   "setup replay",
			status: models.TaskRunning,
			operation: func(lifecycle *tasklifecycle.Lifecycle) (tasklifecycle.Disposition, error) {
				return lifecycle.StartSetup("attempt-1", changedAt)
			},
		},
		{
			name:   "promotion replay",
			status: models.TaskQueued,
			operation: func(lifecycle *tasklifecycle.Lifecycle) (tasklifecycle.Disposition, error) {
				return lifecycle.Promote("attempt-1", changedAt)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newStore(t)
			createAttempt(t, store, test.status, 1, "preserved")
			cancelled, err := store.CompareAndSetDagRunStatus(
				"run-1", models.RunRunning, models.RunCancelled, changedAt,
			)
			if err != nil || !cancelled {
				t.Fatalf("cancel run = (%v, %v), want (true, nil)", cancelled, err)
			}
			before := snapshot(t, store)

			disposition, err := test.operation(tasklifecycle.New(store))
			if err != nil {
				t.Fatalf("%s: %v", test.name, err)
			}
			if disposition != tasklifecycle.Stale {
				t.Fatalf("%s disposition = %v, want Stale", test.name, disposition)
			}
			assertSnapshot(t, store, before)
		})
	}
}

func TestPromoteGuardedMapsAtomicStoreOutcomes(t *testing.T) {
	t.Run("applied stale replay and exact replay", func(t *testing.T) {
		store := newStore(t)
		createAttempt(t, store, models.TaskPending, 1, "")
		lifecycle := tasklifecycle.New(store)
		pending := latestAttempts(t, store)
		target := attemptByID(t, pending, "attempt-1")

		disposition, err := lifecycle.PromoteGuarded(
			target, models.RunRunning, pending, changedAt,
		)
		if err != nil || disposition != tasklifecycle.Applied {
			t.Fatalf("first PromoteGuarded = (%v, %v), want (Applied, nil)", disposition, err)
		}
		disposition, err = lifecycle.PromoteGuarded(
			target, models.RunRunning, pending, changedAt.Add(time.Second),
		)
		if err != nil || disposition != tasklifecycle.Stale {
			t.Fatalf("stale PromoteGuarded replay = (%v, %v), want (Stale, nil)", disposition, err)
		}

		queued := latestAttempts(t, store)
		target = attemptByID(t, queued, "attempt-1")
		disposition, err = lifecycle.PromoteGuarded(
			target, models.RunRunning, queued, changedAt.Add(2*time.Second),
		)
		if err != nil || disposition != tasklifecycle.AlreadyApplied {
			t.Fatalf("exact PromoteGuarded replay = (%v, %v), want (AlreadyApplied, nil)", disposition, err)
		}
	})

	t.Run("predecessor retry is stale", func(t *testing.T) {
		store := newStore(t)
		if err := store.CreateTaskInstance(&models.TaskInstance{
			ID: "parent-1", RunID: "run-1", TaskID: "parent",
			Status: models.TaskSuccess, Attempt: 1,
			CreatedAt: createdAt, UpdatedAt: createdAt,
		}); err != nil {
			t.Fatalf("CreateTaskInstance(parent): %v", err)
		}
		if err := store.CreateTaskInstance(&models.TaskInstance{
			ID: "child-1", RunID: "run-1", TaskID: "child",
			Status: models.TaskPending, Attempt: 1,
			CreatedAt: createdAt, UpdatedAt: createdAt,
		}); err != nil {
			t.Fatalf("CreateTaskInstance(child): %v", err)
		}
		lifecycle := tasklifecycle.New(store)
		snapshot := latestAttempts(t, store)
		target := attemptByID(t, snapshot, "child-1")
		if disposition, err := lifecycle.RetryCurrent(
			"run-1", "parent", changedAt,
		); err != nil || disposition != tasklifecycle.Applied {
			t.Fatalf("RetryCurrent(parent) = (%v, %v), want (Applied, nil)", disposition, err)
		}

		disposition, err := lifecycle.PromoteGuarded(
			target, models.RunRunning, snapshot, changedAt.Add(time.Second),
		)
		if err != nil || disposition != tasklifecycle.Stale {
			t.Fatalf("PromoteGuarded after predecessor retry = (%v, %v), want (Stale, nil)", disposition, err)
		}
		child, err := store.GetTaskInstance(target.ID)
		if err != nil {
			t.Fatalf("GetTaskInstance(child): %v", err)
		}
		if child.Status != models.TaskPending {
			t.Fatalf("stale promotion changed child to %s", child.Status)
		}
	})

	t.Run("run cancellation is stale", func(t *testing.T) {
		store := newStore(t)
		createAttempt(t, store, models.TaskPending, 1, "")
		snapshot := latestAttempts(t, store)
		target := attemptByID(t, snapshot, "attempt-1")
		cancelled, err := store.CompareAndSetDagRunStatus(
			"run-1", models.RunRunning, models.RunCancelled, changedAt,
		)
		if err != nil || !cancelled {
			t.Fatalf("cancel run = (%v, %v), want (true, nil)", cancelled, err)
		}

		disposition, err := tasklifecycle.New(store).PromoteGuarded(
			target, models.RunRunning, snapshot, changedAt.Add(time.Second),
		)
		if err != nil || disposition != tasklifecycle.Stale {
			t.Fatalf("PromoteGuarded after cancellation = (%v, %v), want (Stale, nil)", disposition, err)
		}
	})

	t.Run("invalid target is explicit", func(t *testing.T) {
		store := newStore(t)
		createAttempt(t, store, models.TaskRunning, 1, "")
		snapshot := latestAttempts(t, store)
		target := attemptByID(t, snapshot, "attempt-1")

		disposition, err := tasklifecycle.New(store).PromoteGuarded(
			target, models.RunRunning, snapshot, changedAt,
		)
		if err != nil || disposition != tasklifecycle.Invalid {
			t.Fatalf("invalid PromoteGuarded = (%v, %v), want (Invalid, nil)", disposition, err)
		}
	})
}

func TestStartSetupGuardedMapsAtomicStoreOutcomes(t *testing.T) {
	t.Run("applied stale replay and exact replay", func(t *testing.T) {
		store := newStore(t)
		createAttempt(t, store, models.TaskPending, 1, "")
		lifecycle := tasklifecycle.New(store)
		pending := latestAttempts(t, store)
		target := attemptByID(t, pending, "attempt-1")
		setup := models.MapSetup{
			ParentAttemptID:   target.ID,
			UpstreamAttemptID: "source-1",
			UpstreamOutput:    `["item"]`,
			StartedAt:         changedAt,
		}

		disposition, err := lifecycle.StartSetupGuarded(
			target, models.RunRunning, pending, setup,
		)
		if err != nil || disposition != tasklifecycle.Applied {
			t.Fatalf("first StartSetupGuarded = (%v, %v), want (Applied, nil)", disposition, err)
		}
		disposition, err = lifecycle.StartSetupGuarded(
			target, models.RunRunning, pending, setup,
		)
		if err != nil || disposition != tasklifecycle.Stale {
			t.Fatalf("stale StartSetupGuarded replay = (%v, %v), want (Stale, nil)", disposition, err)
		}

		running := latestAttempts(t, store)
		target = attemptByID(t, running, "attempt-1")
		disposition, err = lifecycle.StartSetupGuarded(
			target, models.RunRunning, running, setup,
		)
		if err != nil || disposition != tasklifecycle.AlreadyApplied {
			t.Fatalf("exact StartSetupGuarded replay = (%v, %v), want (AlreadyApplied, nil)", disposition, err)
		}
		binding, err := store.GetMapSetup(target.ID)
		if err != nil {
			t.Fatalf("GetMapSetup: %v", err)
		}
		if binding.ParentAttemptID != setup.ParentAttemptID ||
			binding.UpstreamAttemptID != setup.UpstreamAttemptID ||
			binding.UpstreamOutput != setup.UpstreamOutput ||
			!binding.StartedAt.Equal(setup.StartedAt) {
			t.Fatalf("binding = %+v, want %+v", binding, setup)
		}
	})

	t.Run("predecessor retry is stale and persists no binding", func(t *testing.T) {
		store := newStore(t)
		if err := store.CreateTaskInstance(&models.TaskInstance{
			ID: "source-1", RunID: "run-1", TaskID: "source",
			Status: models.TaskSuccess, Attempt: 1,
			CreatedAt: createdAt, UpdatedAt: createdAt,
		}); err != nil {
			t.Fatalf("CreateTaskInstance(source): %v", err)
		}
		if err := store.CreateTaskInstance(&models.TaskInstance{
			ID: "map-1", RunID: "run-1", TaskID: "map",
			Status: models.TaskPending, Attempt: 1,
			CreatedAt: createdAt, UpdatedAt: createdAt,
		}); err != nil {
			t.Fatalf("CreateTaskInstance(map): %v", err)
		}
		lifecycle := tasklifecycle.New(store)
		snapshot := latestAttempts(t, store)
		target := attemptByID(t, snapshot, "map-1")
		if disposition, err := lifecycle.RetryCurrent(
			"run-1", "source", changedAt,
		); err != nil || disposition != tasklifecycle.Applied {
			t.Fatalf("RetryCurrent(source) = (%v, %v), want (Applied, nil)", disposition, err)
		}

		disposition, err := lifecycle.StartSetupGuarded(
			target,
			models.RunRunning,
			snapshot,
			models.MapSetup{
				ParentAttemptID:   target.ID,
				UpstreamAttemptID: "source-1",
				UpstreamOutput:    `["item"]`,
				StartedAt:         changedAt.Add(time.Second),
			},
		)
		if err != nil || disposition != tasklifecycle.Stale {
			t.Fatalf("StartSetupGuarded after predecessor retry = (%v, %v), want (Stale, nil)", disposition, err)
		}
		if _, err := store.GetMapSetup(target.ID); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("stale setup persisted binding: %v", err)
		}
	})

	t.Run("run cancellation is stale", func(t *testing.T) {
		store := newStore(t)
		createAttempt(t, store, models.TaskPending, 1, "")
		snapshot := latestAttempts(t, store)
		target := attemptByID(t, snapshot, "attempt-1")
		cancelled, err := store.CompareAndSetDagRunStatus(
			"run-1", models.RunRunning, models.RunCancelled, changedAt,
		)
		if err != nil || !cancelled {
			t.Fatalf("cancel run = (%v, %v), want (true, nil)", cancelled, err)
		}

		disposition, err := tasklifecycle.New(store).StartSetupGuarded(
			target,
			models.RunRunning,
			snapshot,
			models.MapSetup{
				ParentAttemptID:   target.ID,
				UpstreamAttemptID: "source-1",
				UpstreamOutput:    "[]",
				StartedAt:         changedAt.Add(time.Second),
			},
		)
		if err != nil || disposition != tasklifecycle.Stale {
			t.Fatalf("StartSetupGuarded after cancellation = (%v, %v), want (Stale, nil)", disposition, err)
		}
		if _, err := store.GetMapSetup(target.ID); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("cancelled setup persisted binding: %v", err)
		}
	})

	t.Run("mismatched binding identity is invalid", func(t *testing.T) {
		store := newStore(t)
		createAttempt(t, store, models.TaskPending, 1, "")
		snapshot := latestAttempts(t, store)
		target := attemptByID(t, snapshot, "attempt-1")

		disposition, err := tasklifecycle.New(store).StartSetupGuarded(
			target,
			models.RunRunning,
			snapshot,
			models.MapSetup{
				ParentAttemptID:   "different-attempt",
				UpstreamAttemptID: "source-1",
				UpstreamOutput:    "[]",
				StartedAt:         changedAt,
			},
		)
		if err != nil || disposition != tasklifecycle.Invalid {
			t.Fatalf("invalid StartSetupGuarded = (%v, %v), want (Invalid, nil)", disposition, err)
		}
	})
}

func TestClaimSerializesWithRunCancellation(t *testing.T) {
	for iteration := 0; iteration < 20; iteration++ {
		path := filepath.Join(t.TempDir(), "claim-cancel.db")
		claimStore := openStoreAt(t, path)
		cancelStore := openStoreAt(t, path)
		createAttempt(t, claimStore, models.TaskQueued, 1, "")

		start := make(chan struct{})
		claimResult := make(chan lifecycleResult, 1)
		cancelResult := make(chan error, 1)
		go func() {
			<-start
			disposition, err := tasklifecycle.New(claimStore).Claim("attempt-1", changedAt)
			claimResult <- lifecycleResult{disposition: disposition, err: err}
		}()
		go func() {
			<-start
			applied, err := cancelStore.CompareAndSetDagRunStatus(
				"run-1", models.RunRunning, models.RunCancelled, changedAt,
			)
			if err == nil && !applied {
				err = errUnexpectedCancellationMiss
			}
			cancelResult <- err
		}()
		close(start)

		claim := <-claimResult
		if claim.err != nil {
			t.Fatalf("iteration %d Claim: %v", iteration, claim.err)
		}
		if err := <-cancelResult; err != nil {
			t.Fatalf("iteration %d cancellation: %v", iteration, err)
		}
		attempt, err := claimStore.GetTaskInstance("attempt-1")
		if err != nil {
			t.Fatalf("iteration %d GetTaskInstance: %v", iteration, err)
		}
		switch claim.disposition {
		case tasklifecycle.Applied:
			if attempt.Status != models.TaskRunning {
				t.Fatalf("iteration %d applied claim left task %s", iteration, attempt.Status)
			}
		case tasklifecycle.Stale:
			if attempt.Status != models.TaskQueued {
				t.Fatalf("iteration %d stale claim changed task to %s", iteration, attempt.Status)
			}
		default:
			t.Fatalf("iteration %d Claim disposition = %v, want Applied or Stale", iteration, claim.disposition)
		}
	}
}

var errUnexpectedCancellationMiss = errors.New("run cancellation CAS missed")

func latestAttempts(t *testing.T, store *models.Store) []models.TaskInstance {
	t.Helper()
	tasks, err := store.GetLatestTaskAttempts("run-1")
	if err != nil {
		t.Fatalf("GetLatestTaskAttempts: %v", err)
	}
	return tasks
}

func attemptByID(t *testing.T, tasks []models.TaskInstance, attemptID string) models.TaskInstance {
	t.Helper()
	for _, task := range tasks {
		if task.ID == attemptID {
			return task
		}
	}
	t.Fatalf("attempt %s not found in %#v", attemptID, tasks)
	return models.TaskInstance{}
}
