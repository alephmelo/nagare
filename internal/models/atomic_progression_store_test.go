package models

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestCreateDagRunWithTasksIsAtomic(t *testing.T) {
	t.Run("creates complete run", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 10, 0, 0, 0, time.UTC)
		run := DagRun{
			ID: "atomic-complete", DAGID: "dag", Status: RunRunning,
			ExecDate: now, TriggerType: "manual", CreatedAt: now,
		}
		tasks := []TaskInstance{
			{
				ID: "atomic-complete_root", RunID: run.ID, TaskID: "root",
				Status: TaskQueued, CreatedAt: now, UpdatedAt: now,
			},
			{
				ID: "atomic-complete_child", RunID: run.ID, TaskID: "child",
				Status: TaskPending, Attempt: 1, CreatedAt: now, UpdatedAt: now,
			},
		}

		if err := store.CreateDagRunWithTasks(&run, tasks); err != nil {
			t.Fatalf("CreateDagRunWithTasks: %v", err)
		}
		persisted, err := store.GetLatestTaskAttempts(run.ID)
		if err != nil {
			t.Fatalf("GetLatestTaskAttempts: %v", err)
		}
		if len(persisted) != len(tasks) {
			t.Fatalf("task count = %d, want %d", len(persisted), len(tasks))
		}
		if persisted[0].Attempt != 1 || persisted[1].Attempt != 1 {
			t.Fatalf("default attempts were not normalized: %#v", persisted)
		}
	})

	t.Run("task conflict rolls back run and earlier tasks", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 11, 0, 0, 0, time.UTC)
		run := DagRun{
			ID: "atomic-rollback", DAGID: "dag", Status: RunRunning,
			ExecDate: now, TriggerType: "manual", CreatedAt: now,
		}
		tasks := []TaskInstance{
			{
				ID: "atomic-rollback_first", RunID: run.ID, TaskID: "duplicate",
				Status: TaskQueued, Attempt: 1, CreatedAt: now, UpdatedAt: now,
			},
			{
				ID: "atomic-rollback_second", RunID: run.ID, TaskID: "duplicate",
				Status: TaskPending, Attempt: 1, CreatedAt: now, UpdatedAt: now,
			},
		}

		if err := store.CreateDagRunWithTasks(&run, tasks); err == nil {
			t.Fatal("CreateDagRunWithTasks accepted duplicate logical attempt")
		}
		if _, err := store.GetDagRun(run.ID); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("GetDagRun after rollback error = %v, want sql.ErrNoRows", err)
		}
		persisted, err := store.GetLatestTaskAttempts(run.ID)
		if err != nil {
			t.Fatalf("GetLatestTaskAttempts after rollback: %v", err)
		}
		if len(persisted) != 0 {
			t.Fatalf("rollback left partial task rows: %#v", persisted)
		}
	})

	t.Run("foreign run identity rolls back before visibility", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 12, 0, 0, 0, time.UTC)
		run := DagRun{
			ID: "atomic-identity", DAGID: "dag", Status: RunRunning,
			ExecDate: now, TriggerType: "manual", CreatedAt: now,
		}

		err := store.CreateDagRunWithTasks(&run, []TaskInstance{{
			ID: "foreign_task", RunID: "other-run", TaskID: "task",
			Status: TaskQueued, Attempt: 1, CreatedAt: now, UpdatedAt: now,
		}})
		if err == nil || !strings.Contains(err.Error(), "other-run") {
			t.Fatalf("foreign task error = %v", err)
		}
		if _, err := store.GetDagRun(run.ID); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("GetDagRun after identity rejection error = %v, want sql.ErrNoRows", err)
		}
	})
}

func TestPromoteTaskAttemptForRunSnapshot(t *testing.T) {
	t.Run("applies and classifies exact replay", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 13, 0, 0, 0, time.UTC)
		createRunProgressionRun(t, store, "promotion-replay", RunRunning, now)
		if err := store.CreateTaskInstance(&TaskInstance{
			ID: "promotion-replay_task", RunID: "promotion-replay", TaskID: "task",
			Status: TaskPending, Attempt: 1, CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateTaskInstance: %v", err)
		}
		pendingSnapshot, err := store.GetLatestTaskAttempts("promotion-replay")
		if err != nil {
			t.Fatalf("GetLatestTaskAttempts(pending): %v", err)
		}

		result, err := store.PromoteTaskAttemptForRunSnapshot(
			"promotion-replay", "promotion-replay_task", RunRunning,
			pendingSnapshot, now.Add(time.Second),
		)
		if err != nil {
			t.Fatalf("first guarded promotion: %v", err)
		}
		if result != GuardedTaskPromotionApplied {
			t.Fatalf("first guarded promotion = %v, want Applied", result)
		}

		result, err = store.PromoteTaskAttemptForRunSnapshot(
			"promotion-replay", "promotion-replay_task", RunRunning,
			pendingSnapshot, now.Add(2*time.Second),
		)
		if err != nil {
			t.Fatalf("stale replay: %v", err)
		}
		if result != GuardedTaskPromotionStale {
			t.Fatalf("old-snapshot replay = %v, want Stale", result)
		}

		queuedSnapshot, err := store.GetLatestTaskAttempts("promotion-replay")
		if err != nil {
			t.Fatalf("GetLatestTaskAttempts(queued): %v", err)
		}
		result, err = store.PromoteTaskAttemptForRunSnapshot(
			"promotion-replay", "promotion-replay_task", RunRunning,
			queuedSnapshot, now.Add(3*time.Second),
		)
		if err != nil {
			t.Fatalf("exact replay: %v", err)
		}
		if result != GuardedTaskPromotionAlreadyApplied {
			t.Fatalf("exact queued replay = %v, want AlreadyApplied", result)
		}
	})

	t.Run("cancellation before promotion is stale", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 14, 0, 0, 0, time.UTC)
		createRunProgressionRun(t, store, "promotion-cancel", RunRunning, now)
		if err := store.CreateTaskInstance(&TaskInstance{
			ID: "promotion-cancel_task", RunID: "promotion-cancel", TaskID: "task",
			Status: TaskPending, Attempt: 1, CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateTaskInstance: %v", err)
		}
		snapshot, err := store.GetLatestTaskAttempts("promotion-cancel")
		if err != nil {
			t.Fatalf("GetLatestTaskAttempts: %v", err)
		}
		applied, err := store.CompareAndSetDagRunStatus(
			"promotion-cancel", RunRunning, RunCancelled, now.Add(time.Second),
		)
		if err != nil || !applied {
			t.Fatalf("cancel run = (%v, %v), want (true, nil)", applied, err)
		}

		result, err := store.PromoteTaskAttemptForRunSnapshot(
			"promotion-cancel", "promotion-cancel_task", RunRunning,
			snapshot, now.Add(2*time.Second),
		)
		if err != nil {
			t.Fatalf("guarded promotion after cancellation: %v", err)
		}
		if result != GuardedTaskPromotionStale {
			t.Fatalf("guarded promotion after cancellation = %v, want Stale", result)
		}
		task, err := store.GetTaskInstance("promotion-cancel_task")
		if err != nil {
			t.Fatalf("GetTaskInstance: %v", err)
		}
		if task.Status != TaskPending {
			t.Fatalf("cancelled-run task status = %s, want pending", task.Status)
		}
	})

	t.Run("changed predecessor makes complete snapshot stale", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 15, 0, 0, 0, time.UTC)
		createRunProgressionRun(t, store, "promotion-stale", RunRunning, now)
		for _, task := range []TaskInstance{
			{
				ID: "promotion-stale_parent", RunID: "promotion-stale", TaskID: "parent",
				Status: TaskSuccess, Attempt: 1, CreatedAt: now, UpdatedAt: now,
			},
			{
				ID: "promotion-stale_child", RunID: "promotion-stale", TaskID: "child",
				Status: TaskPending, Attempt: 1, CreatedAt: now, UpdatedAt: now,
			},
		} {
			task := task
			if err := store.CreateTaskInstance(&task); err != nil {
				t.Fatalf("CreateTaskInstance(%s): %v", task.ID, err)
			}
		}
		snapshot, err := store.GetLatestTaskAttempts("promotion-stale")
		if err != nil {
			t.Fatalf("GetLatestTaskAttempts: %v", err)
		}
		if err := store.UpdateTaskInstanceStatus("promotion-stale_parent", TaskFailed); err != nil {
			t.Fatalf("change predecessor: %v", err)
		}

		result, err := store.PromoteTaskAttemptForRunSnapshot(
			"promotion-stale", "promotion-stale_child", RunRunning,
			snapshot, now.Add(time.Second),
		)
		if err != nil {
			t.Fatalf("guarded promotion with stale predecessor: %v", err)
		}
		if result != GuardedTaskPromotionStale {
			t.Fatalf("stale predecessor promotion = %v, want Stale", result)
		}
		child, err := store.GetTaskInstance("promotion-stale_child")
		if err != nil {
			t.Fatalf("GetTaskInstance(child): %v", err)
		}
		if child.Status != TaskPending {
			t.Fatalf("stale predecessor promoted child to %s", child.Status)
		}
	})

	t.Run("non-pending current attempt is invalid", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 16, 0, 0, 0, time.UTC)
		createRunProgressionRun(t, store, "promotion-invalid", RunRunning, now)
		if err := store.CreateTaskInstance(&TaskInstance{
			ID: "promotion-invalid_task", RunID: "promotion-invalid", TaskID: "task",
			Status: TaskRunning, Attempt: 1, CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateTaskInstance: %v", err)
		}
		snapshot, err := store.GetLatestTaskAttempts("promotion-invalid")
		if err != nil {
			t.Fatalf("GetLatestTaskAttempts: %v", err)
		}

		result, err := store.PromoteTaskAttemptForRunSnapshot(
			"promotion-invalid", "promotion-invalid_task", RunRunning,
			snapshot, now.Add(time.Second),
		)
		if err != nil {
			t.Fatalf("invalid guarded promotion: %v", err)
		}
		if result != GuardedTaskPromotionInvalid {
			t.Fatalf("running attempt promotion = %v, want Invalid", result)
		}
	})
}

func TestStartMapSetupForRunSnapshotAppliesAndReplays(t *testing.T) {
	t.Run("applies pending setup and classifies exact replay", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 16, 5, 0, 0, time.UTC)
		createRunProgressionRun(t, store, "setup-replay", RunRunning, now)
		if err := store.CreateTaskInstance(&TaskInstance{
			ID: "setup-replay_map", RunID: "setup-replay", TaskID: "map",
			Status: TaskPending, Attempt: 1, CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateTaskInstance: %v", err)
		}
		pendingSnapshot, err := store.GetLatestTaskAttempts("setup-replay")
		if err != nil {
			t.Fatalf("GetLatestTaskAttempts(pending): %v", err)
		}
		startedAt := now.Add(time.Second)
		setup := MapSetup{
			ParentAttemptID:   "setup-replay_map",
			UpstreamAttemptID: "setup-replay_source",
			UpstreamOutput:    "[]",
			StartedAt:         startedAt,
		}

		result, err := store.StartMapSetupForRunSnapshot(
			"setup-replay", "setup-replay_map", RunRunning,
			pendingSnapshot, setup,
		)
		if err != nil {
			t.Fatalf("first guarded setup: %v", err)
		}
		if result != GuardedTaskPromotionApplied {
			t.Fatalf("first guarded setup = %v, want Applied", result)
		}
		task, err := store.GetTaskInstance("setup-replay_map")
		if err != nil {
			t.Fatalf("GetTaskInstance: %v", err)
		}
		if task.Status != TaskRunning || task.StartedAt == nil ||
			!task.StartedAt.Equal(startedAt) || !task.UpdatedAt.Equal(startedAt) {
			t.Fatalf("guarded setup persisted %+v", task)
		}
		binding, err := store.GetMapSetup("setup-replay_map")
		if err != nil {
			t.Fatalf("GetMapSetup: %v", err)
		}
		if !sameMapSetup(*binding, setup) {
			t.Fatalf("persisted setup = %+v, want %+v", *binding, setup)
		}

		result, err = store.StartMapSetupForRunSnapshot(
			"setup-replay", "setup-replay_map", RunRunning,
			pendingSnapshot, setup,
		)
		if err != nil {
			t.Fatalf("old-snapshot setup replay: %v", err)
		}
		if result != GuardedTaskPromotionStale {
			t.Fatalf("old-snapshot setup replay = %v, want Stale", result)
		}

		runningSnapshot, err := store.GetLatestTaskAttempts("setup-replay")
		if err != nil {
			t.Fatalf("GetLatestTaskAttempts(running): %v", err)
		}
		result, err = store.StartMapSetupForRunSnapshot(
			"setup-replay", "setup-replay_map", RunRunning,
			runningSnapshot, setup,
		)
		if err != nil {
			t.Fatalf("exact setup replay: %v", err)
		}
		if result != GuardedTaskPromotionAlreadyApplied {
			t.Fatalf("exact setup replay = %v, want AlreadyApplied", result)
		}
	})

	t.Run("queued setup is eligible", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 16, 6, 0, 0, time.UTC)
		createRunProgressionRun(t, store, "setup-queued", RunRunning, now)
		if err := store.CreateTaskInstance(&TaskInstance{
			ID: "setup-queued_map", RunID: "setup-queued", TaskID: "map",
			Status: TaskQueued, Attempt: 1, CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateTaskInstance: %v", err)
		}
		snapshot, err := store.GetLatestTaskAttempts("setup-queued")
		if err != nil {
			t.Fatalf("GetLatestTaskAttempts: %v", err)
		}

		result, err := store.StartMapSetupForRunSnapshot(
			"setup-queued", "setup-queued_map", RunRunning,
			snapshot, MapSetup{
				ParentAttemptID:   "setup-queued_map",
				UpstreamAttemptID: "setup-queued_source",
				UpstreamOutput:    "[]",
				StartedAt:         now.Add(time.Second),
			},
		)
		if err != nil {
			t.Fatalf("guarded queued setup: %v", err)
		}
		if result != GuardedTaskPromotionApplied {
			t.Fatalf("guarded queued setup = %v, want Applied", result)
		}
	})
}

func TestStartMapSetupForRunSnapshotRejectsStaleSnapshots(t *testing.T) {
	t.Run("predecessor retry makes complete snapshot stale", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 16, 7, 0, 0, time.UTC)
		createRunProgressionRun(t, store, "setup-predecessor", RunRunning, now)
		for _, task := range []TaskInstance{
			{
				ID: "setup-predecessor_source", RunID: "setup-predecessor", TaskID: "source",
				Status: TaskSuccess, Attempt: 1, CreatedAt: now, UpdatedAt: now,
			},
			{
				ID: "setup-predecessor_map", RunID: "setup-predecessor", TaskID: "map",
				Status: TaskPending, Attempt: 1, CreatedAt: now, UpdatedAt: now,
			},
		} {
			task := task
			if err := store.CreateTaskInstance(&task); err != nil {
				t.Fatalf("CreateTaskInstance(%s): %v", task.ID, err)
			}
		}
		snapshot, err := store.GetLatestTaskAttempts("setup-predecessor")
		if err != nil {
			t.Fatalf("GetLatestTaskAttempts: %v", err)
		}
		if err := store.CreateTaskInstance(&TaskInstance{
			ID: "setup-predecessor_source_2", RunID: "setup-predecessor", TaskID: "source",
			Status: TaskQueued, Attempt: 2,
			CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second),
		}); err != nil {
			t.Fatalf("CreateTaskInstance(source retry): %v", err)
		}

		result, err := store.StartMapSetupForRunSnapshot(
			"setup-predecessor", "setup-predecessor_map", RunRunning,
			snapshot, MapSetup{
				ParentAttemptID:   "setup-predecessor_map",
				UpstreamAttemptID: "setup-predecessor_source",
				UpstreamOutput:    "[]",
				StartedAt:         now.Add(2 * time.Second),
			},
		)
		if err != nil {
			t.Fatalf("guarded setup with retried predecessor: %v", err)
		}
		if result != GuardedTaskPromotionStale {
			t.Fatalf("retried-predecessor setup = %v, want Stale", result)
		}
		task, err := store.GetTaskInstance("setup-predecessor_map")
		if err != nil {
			t.Fatalf("GetTaskInstance(map): %v", err)
		}
		if task.Status != TaskPending || task.StartedAt != nil {
			t.Fatalf("stale setup mutated map task: %+v", task)
		}
		if _, err := store.GetMapSetup("setup-predecessor_map"); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("stale setup persisted binding: %v", err)
		}
	})

	t.Run("run cancellation makes setup stale", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 16, 8, 0, 0, time.UTC)
		createRunProgressionRun(t, store, "setup-cancel", RunRunning, now)
		if err := store.CreateTaskInstance(&TaskInstance{
			ID: "setup-cancel_map", RunID: "setup-cancel", TaskID: "map",
			Status: TaskPending, Attempt: 1, CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateTaskInstance: %v", err)
		}
		snapshot, err := store.GetLatestTaskAttempts("setup-cancel")
		if err != nil {
			t.Fatalf("GetLatestTaskAttempts: %v", err)
		}
		applied, err := store.CompareAndSetDagRunStatus(
			"setup-cancel", RunRunning, RunCancelled, now.Add(time.Second),
		)
		if err != nil || !applied {
			t.Fatalf("cancel run = (%v, %v), want (true, nil)", applied, err)
		}

		result, err := store.StartMapSetupForRunSnapshot(
			"setup-cancel", "setup-cancel_map", RunRunning,
			snapshot, MapSetup{
				ParentAttemptID:   "setup-cancel_map",
				UpstreamAttemptID: "setup-cancel_source",
				UpstreamOutput:    "[]",
				StartedAt:         now.Add(2 * time.Second),
			},
		)
		if err != nil {
			t.Fatalf("guarded setup after cancellation: %v", err)
		}
		if result != GuardedTaskPromotionStale {
			t.Fatalf("cancelled-run setup = %v, want Stale", result)
		}
		task, err := store.GetTaskInstance("setup-cancel_map")
		if err != nil {
			t.Fatalf("GetTaskInstance(map): %v", err)
		}
		if task.Status != TaskPending || task.StartedAt != nil {
			t.Fatalf("cancelled-run setup mutated task: %+v", task)
		}
		if _, err := store.GetMapSetup("setup-cancel_map"); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("cancelled-run setup persisted binding: %v", err)
		}
	})
}

func TestStartMapSetupForRunSnapshotRejectsInvalidStates(t *testing.T) {
	t.Run("foreign running ownership is invalid", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 16, 9, 0, 0, time.UTC)
		createRunProgressionRun(t, store, "setup-invalid", RunRunning, now)
		if err := store.CreateTaskInstance(&TaskInstance{
			ID: "setup-invalid_map", RunID: "setup-invalid", TaskID: "map",
			Status: TaskQueued, Attempt: 1, CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateTaskInstance: %v", err)
		}
		foreignStartedAt := now.Add(time.Second)
		applied, err := store.CompareAndSetTaskAttemptForRunStatus(
			"setup-invalid_map", TaskQueued, RunRunning, TaskAttemptMutation{
				Status: TaskRunning, UpdatedAt: foreignStartedAt,
				StartedAt: &foreignStartedAt,
			},
		)
		if err != nil || !applied {
			t.Fatalf("foreign setup claim = (%v, %v), want (true, nil)", applied, err)
		}
		snapshot, err := store.GetLatestTaskAttempts("setup-invalid")
		if err != nil {
			t.Fatalf("GetLatestTaskAttempts: %v", err)
		}

		result, err := store.StartMapSetupForRunSnapshot(
			"setup-invalid", "setup-invalid_map", RunRunning,
			snapshot, MapSetup{
				ParentAttemptID:   "setup-invalid_map",
				UpstreamAttemptID: "setup-invalid_source",
				UpstreamOutput:    "[]",
				StartedAt:         foreignStartedAt.Add(time.Second),
			},
		)
		if err != nil {
			t.Fatalf("guarded setup with foreign ownership: %v", err)
		}
		if result != GuardedTaskPromotionInvalid {
			t.Fatalf("foreign ownership setup = %v, want Invalid", result)
		}
		if _, err := store.GetMapSetup("setup-invalid_map"); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("foreign ownership persisted binding: %v", err)
		}
	})

	t.Run("terminal attempt is invalid", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 16, 10, 0, 0, time.UTC)
		createRunProgressionRun(t, store, "setup-terminal", RunRunning, now)
		if err := store.CreateTaskInstance(&TaskInstance{
			ID: "setup-terminal_map", RunID: "setup-terminal", TaskID: "map",
			Status: TaskSuccess, Attempt: 1, CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateTaskInstance: %v", err)
		}
		snapshot, err := store.GetLatestTaskAttempts("setup-terminal")
		if err != nil {
			t.Fatalf("GetLatestTaskAttempts: %v", err)
		}

		result, err := store.StartMapSetupForRunSnapshot(
			"setup-terminal", "setup-terminal_map", RunRunning,
			snapshot, MapSetup{
				ParentAttemptID:   "setup-terminal_map",
				UpstreamAttemptID: "setup-terminal_source",
				UpstreamOutput:    "[]",
				StartedAt:         now.Add(time.Second),
			},
		)
		if err != nil {
			t.Fatalf("guarded terminal setup: %v", err)
		}
		if result != GuardedTaskPromotionInvalid {
			t.Fatalf("terminal setup = %v, want Invalid", result)
		}
		if _, err := store.GetMapSetup("setup-terminal_map"); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("invalid terminal setup persisted binding: %v", err)
		}
	})
}

func TestStartMapSetupForRunSnapshotRejectsParentIdentityMismatch(t *testing.T) {
	store := newRunProgressionStore(t)
	now := time.Date(2026, time.July, 27, 16, 11, 0, 0, time.UTC)
	createRunProgressionRun(t, store, "setup-identity", RunRunning, now)
	if err := store.CreateTaskInstance(&TaskInstance{
		ID: "setup-identity_map", RunID: "setup-identity", TaskID: "map",
		Status: TaskPending, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("CreateTaskInstance: %v", err)
	}
	snapshot, err := store.GetLatestTaskAttempts("setup-identity")
	if err != nil {
		t.Fatalf("GetLatestTaskAttempts: %v", err)
	}

	result, err := store.StartMapSetupForRunSnapshot(
		"setup-identity", "setup-identity_map", RunRunning,
		snapshot, MapSetup{
			ParentAttemptID:   "different-parent",
			UpstreamAttemptID: "setup-identity_source",
			UpstreamOutput:    "[]",
			StartedAt:         now.Add(time.Second),
		},
	)
	if err != nil {
		t.Fatalf("guarded setup with mismatched parent: %v", err)
	}
	if result != GuardedTaskSetupInvalid {
		t.Fatalf("mismatched-parent setup = %v, want Invalid", result)
	}
	if _, err := store.GetMapSetup("different-parent"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("mismatched parent persisted binding: %v", err)
	}
	task, err := store.GetTaskInstance("setup-identity_map")
	if err != nil {
		t.Fatalf("GetTaskInstance: %v", err)
	}
	if task.Status != TaskPending || task.StartedAt != nil {
		t.Fatalf("mismatched parent mutated task: %+v", task)
	}
}

func TestStartMapSetupForRunSnapshotRejectsConflictingBinding(t *testing.T) {
	store := newRunProgressionStore(t)
	now := time.Date(2026, time.July, 27, 16, 12, 0, 0, time.UTC)
	createRunProgressionRun(t, store, "setup-conflict", RunRunning, now)
	if err := store.CreateTaskInstance(&TaskInstance{
		ID: "setup-conflict_map", RunID: "setup-conflict", TaskID: "map",
		Status: TaskPending, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("CreateTaskInstance: %v", err)
	}
	persisted := MapSetup{
		ParentAttemptID:   "setup-conflict_map",
		UpstreamAttemptID: "setup-conflict_source_1",
		UpstreamOutput:    `["old"]`,
		StartedAt:         now.Add(time.Second),
	}
	if _, _, err := store.EnsureMapSetup(persisted); err != nil {
		t.Fatalf("EnsureMapSetup: %v", err)
	}
	snapshot, err := store.GetLatestTaskAttempts("setup-conflict")
	if err != nil {
		t.Fatalf("GetLatestTaskAttempts: %v", err)
	}

	result, err := store.StartMapSetupForRunSnapshot(
		"setup-conflict", "setup-conflict_map", RunRunning,
		snapshot, MapSetup{
			ParentAttemptID:   "setup-conflict_map",
			UpstreamAttemptID: "setup-conflict_source_2",
			UpstreamOutput:    `["new"]`,
			StartedAt:         now.Add(2 * time.Second),
		},
	)
	if err != nil {
		t.Fatalf("guarded setup with conflicting binding: %v", err)
	}
	if result != GuardedTaskSetupInvalid {
		t.Fatalf("conflicting setup = %v, want Invalid", result)
	}
	binding, err := store.GetMapSetup("setup-conflict_map")
	if err != nil {
		t.Fatalf("GetMapSetup: %v", err)
	}
	if !sameMapSetup(*binding, persisted) {
		t.Fatalf("conflicting setup changed binding: got %+v, want %+v", *binding, persisted)
	}
	task, err := store.GetTaskInstance("setup-conflict_map")
	if err != nil {
		t.Fatalf("GetTaskInstance: %v", err)
	}
	if task.Status != TaskPending || task.StartedAt != nil {
		t.Fatalf("conflicting setup mutated task: %+v", task)
	}
}

func TestEnsureTaskInstanceForRunSnapshotAppliesAndReplays(t *testing.T) {
	store := newRunProgressionStore(t)
	now := time.Date(2026, time.July, 27, 16, 20, 0, 0, time.UTC)
	parent, startedAt, snapshot := createGuardedChildParent(
		t, store, "child-replay", now,
	)
	item := "bound"
	child := TaskInstance{
		ID: "child-replay_map[0]", RunID: parent.RunID, TaskID: "map[0]",
		Status: TaskPending, ItemValue: &item,
		CreatedAt: now, UpdatedAt: now,
	}

	result, err := store.EnsureTaskInstanceForRunSnapshot(
		parent.RunID, parent.ID, startedAt, RunRunning, snapshot, &child,
	)
	if err != nil || result != GuardedTaskPromotionApplied {
		t.Fatalf("first guarded ensure = (%v, %v), want (Applied, nil)", result, err)
	}
	if child.Attempt != 1 {
		t.Fatalf("default child attempt = %d, want 1", child.Attempt)
	}

	result, err = store.EnsureTaskInstanceForRunSnapshot(
		parent.RunID, parent.ID, startedAt, RunRunning, snapshot, &child,
	)
	if err != nil || result != GuardedTaskPromotionStale {
		t.Fatalf("old-snapshot replay = (%v, %v), want (Stale, nil)", result, err)
	}
	if err := store.UpdateTaskInstanceStatus(child.ID, TaskQueued); err != nil {
		t.Fatalf("advance persisted child lifecycle: %v", err)
	}
	current, err := store.GetLatestTaskAttempts(parent.RunID)
	if err != nil {
		t.Fatalf("GetLatestTaskAttempts: %v", err)
	}
	result, err = store.EnsureTaskInstanceForRunSnapshot(
		parent.RunID, parent.ID, startedAt, RunRunning, current, &child,
	)
	if err != nil || result != GuardedTaskPromotionAlreadyApplied {
		t.Fatalf("exact guarded replay = (%v, %v), want (AlreadyApplied, nil)", result, err)
	}
	attempts, err := store.GetTaskAttempts(parent.RunID, child.TaskID)
	if err != nil || len(attempts) != 1 || attempts[0].ID != child.ID ||
		attempts[0].ItemValue == nil || *attempts[0].ItemValue != item {
		t.Fatalf("persisted child = %#v, %v, want one exact item-bound row", attempts, err)
	}
}

func TestEnsureTaskInstanceForRunSnapshotRejectsStaleAuthority(t *testing.T) {
	t.Run("run cancellation leaves no child", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 16, 21, 0, 0, time.UTC)
		parent, startedAt, snapshot := createGuardedChildParent(
			t, store, "child-cancel", now,
		)
		if applied, err := store.CompareAndSetDagRunStatus(
			parent.RunID, RunRunning, RunCancelled, now.Add(2*time.Second),
		); err != nil || !applied {
			t.Fatalf("cancel run = (%v, %v), want (true, nil)", applied, err)
		}
		child := guardedChild(parent.RunID, "child-cancel_map[0]", "map[0]", now)

		result, err := store.EnsureTaskInstanceForRunSnapshot(
			parent.RunID, parent.ID, startedAt, RunRunning, snapshot, &child,
		)
		if err != nil || result != GuardedTaskPromotionStale {
			t.Fatalf("cancelled-run ensure = (%v, %v), want (Stale, nil)", result, err)
		}
		assertNoTaskInstance(t, store, child.ID)
	})

	t.Run("parent retry leaves no child", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 16, 22, 0, 0, time.UTC)
		parent, startedAt, snapshot := createGuardedChildParent(
			t, store, "child-parent-retry", now,
		)
		if err := store.CreateTaskInstance(&TaskInstance{
			ID: "child-parent-retry_map_2", RunID: parent.RunID, TaskID: parent.TaskID,
			Status: TaskPending, Attempt: 2,
			CreatedAt: now.Add(2 * time.Second), UpdatedAt: now.Add(2 * time.Second),
		}); err != nil {
			t.Fatalf("create parent retry: %v", err)
		}
		child := guardedChild(
			parent.RunID, "child-parent-retry_map[0]@g1", "map[0]@g1", now,
		)

		result, err := store.EnsureTaskInstanceForRunSnapshot(
			parent.RunID, parent.ID, startedAt, RunRunning, snapshot, &child,
		)
		if err != nil || result != GuardedTaskPromotionStale {
			t.Fatalf("retried-parent ensure = (%v, %v), want (Stale, nil)", result, err)
		}
		assertNoTaskInstance(t, store, child.ID)
	})
}

func TestEnsureTaskInstanceForRunSnapshotRejectsInvalidParentAndChild(t *testing.T) {
	t.Run("ownership watermark mismatch", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 16, 23, 0, 0, time.UTC)
		parent, startedAt, snapshot := createGuardedChildParent(
			t, store, "child-watermark", now,
		)
		child := guardedChild(parent.RunID, "child-watermark_map[0]", "map[0]", now)

		result, err := store.EnsureTaskInstanceForRunSnapshot(
			parent.RunID, parent.ID, startedAt.Add(time.Second),
			RunRunning, snapshot, &child,
		)
		if err != nil || result != GuardedTaskPromotionInvalid {
			t.Fatalf("wrong-watermark ensure = (%v, %v), want (Invalid, nil)", result, err)
		}
		assertNoTaskInstance(t, store, child.ID)
	})

	t.Run("running parent without ownership watermark", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 16, 24, 0, 0, time.UTC)
		createRunProgressionRun(t, store, "child-unowned", RunRunning, now)
		parent := TaskInstance{
			ID: "child-unowned_map", RunID: "child-unowned", TaskID: "map",
			Status: TaskRunning, Attempt: 1, CreatedAt: now, UpdatedAt: now,
		}
		if err := store.CreateTaskInstance(&parent); err != nil {
			t.Fatalf("CreateTaskInstance(parent): %v", err)
		}
		snapshot, err := store.GetLatestTaskAttempts(parent.RunID)
		if err != nil {
			t.Fatalf("GetLatestTaskAttempts: %v", err)
		}
		child := guardedChild(parent.RunID, "child-unowned_map[0]", "map[0]", now)

		result, err := store.EnsureTaskInstanceForRunSnapshot(
			parent.RunID, parent.ID, now, RunRunning, snapshot, &child,
		)
		if err != nil || result != GuardedTaskPromotionInvalid {
			t.Fatalf("unowned-parent ensure = (%v, %v), want (Invalid, nil)", result, err)
		}
		assertNoTaskInstance(t, store, child.ID)
	})

	t.Run("foreign child run", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 16, 25, 0, 0, time.UTC)
		parent, startedAt, snapshot := createGuardedChildParent(
			t, store, "child-foreign", now,
		)
		child := guardedChild("different-run", "foreign_map[0]", "map[0]", now)

		result, err := store.EnsureTaskInstanceForRunSnapshot(
			parent.RunID, parent.ID, startedAt, RunRunning, snapshot, &child,
		)
		if err != nil || result != GuardedTaskPromotionInvalid {
			t.Fatalf("foreign-child ensure = (%v, %v), want (Invalid, nil)", result, err)
		}
		assertNoTaskInstance(t, store, child.ID)
	})

	for _, test := range []struct {
		name   string
		mutate func(*TaskInstance, time.Time)
	}{
		{
			name: "queued child",
			mutate: func(child *TaskInstance, _ time.Time) {
				child.Status = TaskQueued
			},
		},
		{
			name: "prestarted child",
			mutate: func(child *TaskInstance, now time.Time) {
				child.StartedAt = &now
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newRunProgressionStore(t)
			now := time.Date(2026, time.July, 27, 16, 25, 30, 0, time.UTC)
			parent, startedAt, snapshot := createGuardedChildParent(
				t, store, "child-invalid-state", now,
			)
			child := guardedChild(
				parent.RunID, "child-invalid-state_map[0]", "map[0]", now,
			)
			test.mutate(&child, now)

			result, err := store.EnsureTaskInstanceForRunSnapshot(
				parent.RunID, parent.ID, startedAt, RunRunning, snapshot, &child,
			)
			if err != nil || result != GuardedTaskPromotionInvalid {
				t.Fatalf("invalid child ensure = (%v, %v), want (Invalid, nil)", result, err)
			}
			assertNoTaskInstance(t, store, child.ID)
		})
	}
}

func TestEnsureTaskInstanceForRunSnapshotRejectsConflictWithoutMutation(t *testing.T) {
	store := newRunProgressionStore(t)
	now := time.Date(2026, time.July, 27, 16, 26, 0, 0, time.UTC)
	parent, startedAt, _ := createGuardedChildParent(
		t, store, "child-conflict", now,
	)
	item := "bound"
	existing := TaskInstance{
		ID: "child-conflict_existing", RunID: parent.RunID, TaskID: "map[0]",
		Status: TaskPending, ItemValue: &item, Attempt: 1,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := store.CreateTaskInstance(&existing); err != nil {
		t.Fatalf("CreateTaskInstance(existing): %v", err)
	}
	snapshot, err := store.GetLatestTaskAttempts(parent.RunID)
	if err != nil {
		t.Fatalf("GetLatestTaskAttempts: %v", err)
	}
	requested := existing
	requested.ID = "child-conflict_requested"

	result, err := store.EnsureTaskInstanceForRunSnapshot(
		parent.RunID, parent.ID, startedAt, RunRunning, snapshot, &requested,
	)
	if err == nil || result != 0 || !strings.Contains(err.Error(), "existing identity") {
		t.Fatalf("conflicting ensure = (%v, %v), want explicit identity error", result, err)
	}
	assertNoTaskInstance(t, store, requested.ID)
	persisted, err := store.GetTaskInstance(existing.ID)
	if err != nil || persisted.ItemValue == nil || *persisted.ItemValue != item {
		t.Fatalf("existing conflict row = %#v, %v, want unchanged", persisted, err)
	}
}

func TestEnsureTaskInstanceForRunSnapshotRollsBackInsertError(t *testing.T) {
	store := newRunProgressionStore(t)
	now := time.Date(2026, time.July, 27, 16, 27, 0, 0, time.UTC)
	parent, startedAt, snapshot := createGuardedChildParent(
		t, store, "child-rollback", now,
	)
	child := guardedChild(parent.RunID, "child-rollback_map[0]", "map[0]", now)
	if _, err := store.db.Exec(`
		CREATE TRIGGER reject_guarded_child
		AFTER INSERT ON task_instances
		WHEN NEW.id = 'child-rollback_map[0]'
		BEGIN
			SELECT RAISE(ABORT, 'forced guarded child failure');
		END
	`); err != nil {
		t.Fatalf("create rejection trigger: %v", err)
	}

	result, err := store.EnsureTaskInstanceForRunSnapshot(
		parent.RunID, parent.ID, startedAt, RunRunning, snapshot, &child,
	)
	if err == nil || result != 0 ||
		!strings.Contains(err.Error(), "forced guarded child failure") {
		t.Fatalf("failing ensure = (%v, %v), want trigger error", result, err)
	}
	assertNoTaskInstance(t, store, child.ID)
	reloaded, err := store.GetTaskInstance(parent.ID)
	if err != nil || reloaded.Status != TaskRunning || reloaded.StartedAt == nil ||
		!reloaded.StartedAt.Equal(startedAt) {
		t.Fatalf("parent after rollback = %#v, %v, want unchanged owner", reloaded, err)
	}
}

func createGuardedChildParent(
	t *testing.T,
	store *Store,
	runID string,
	now time.Time,
) (TaskInstance, time.Time, []TaskInstance) {
	t.Helper()
	createRunProgressionRun(t, store, runID, RunRunning, now)
	parent := TaskInstance{
		ID: runID + "_map", RunID: runID, TaskID: "map",
		Status: TaskPending, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.CreateTaskInstance(&parent); err != nil {
		t.Fatalf("CreateTaskInstance(parent): %v", err)
	}
	startedAt := now.Add(time.Second)
	applied, err := store.CompareAndSetTaskAttemptForRunStatus(
		parent.ID, TaskPending, RunRunning, TaskAttemptMutation{
			Status: TaskRunning, UpdatedAt: startedAt, StartedAt: &startedAt,
		},
	)
	if err != nil || !applied {
		t.Fatalf("start parent = (%v, %v), want (true, nil)", applied, err)
	}
	parent.Status = TaskRunning
	parent.UpdatedAt = startedAt
	parent.StartedAt = &startedAt
	snapshot, err := store.GetLatestTaskAttempts(runID)
	if err != nil {
		t.Fatalf("GetLatestTaskAttempts: %v", err)
	}
	return parent, startedAt, snapshot
}

func guardedChild(runID, id, taskID string, now time.Time) TaskInstance {
	item := "item"
	return TaskInstance{
		ID: id, RunID: runID, TaskID: taskID, Status: TaskPending,
		ItemValue: &item, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	}
}

func assertNoTaskInstance(t *testing.T, store *Store, attemptID string) {
	t.Helper()
	if _, err := store.GetTaskInstance(attemptID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("GetTaskInstance(%s) error = %v, want sql.ErrNoRows", attemptID, err)
	}
}

func TestStartMapSetupForRunSnapshotRollsBackBindingOnTransitionError(t *testing.T) {
	store := newRunProgressionStore(t)
	now := time.Date(2026, time.July, 27, 16, 13, 0, 0, time.UTC)
	createRunProgressionRun(t, store, "setup-rollback", RunRunning, now)
	if err := store.CreateTaskInstance(&TaskInstance{
		ID: "setup-rollback_map", RunID: "setup-rollback", TaskID: "map",
		Status: TaskPending, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("CreateTaskInstance: %v", err)
	}
	snapshot, err := store.GetLatestTaskAttempts("setup-rollback")
	if err != nil {
		t.Fatalf("GetLatestTaskAttempts: %v", err)
	}
	if _, err := store.db.Exec(`
		CREATE TRIGGER reject_setup_transition
		BEFORE UPDATE OF status ON task_instances
		WHEN NEW.id = 'setup-rollback_map' AND NEW.status = 'running'
		BEGIN
			SELECT RAISE(ABORT, 'reject setup transition');
		END`); err != nil {
		t.Fatalf("create rejection trigger: %v", err)
	}

	_, err = store.StartMapSetupForRunSnapshot(
		"setup-rollback", "setup-rollback_map", RunRunning,
		snapshot, MapSetup{
			ParentAttemptID:   "setup-rollback_map",
			UpstreamAttemptID: "setup-rollback_source",
			UpstreamOutput:    "[]",
			StartedAt:         now.Add(time.Second),
		},
	)
	if err == nil || !strings.Contains(err.Error(), "reject setup transition") {
		t.Fatalf("transition error = %v, want trigger rejection", err)
	}
	if _, err := store.GetMapSetup("setup-rollback_map"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("failed transition retained new binding: %v", err)
	}
	task, err := store.GetTaskInstance("setup-rollback_map")
	if err != nil {
		t.Fatalf("GetTaskInstance: %v", err)
	}
	if task.Status != TaskPending || task.StartedAt != nil {
		t.Fatalf("failed setup transition mutated task: %+v", task)
	}
}

func TestCompareAndSetTaskAttemptForRunStatus(t *testing.T) {
	t.Run("applies while run status matches", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 16, 15, 0, 0, time.UTC)
		createRunProgressionRun(t, store, "claim-current", RunRunning, now)
		if err := store.CreateTaskInstance(&TaskInstance{
			ID: "claim-current_task", RunID: "claim-current", TaskID: "task",
			Status: TaskQueued, Attempt: 1, CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateTaskInstance: %v", err)
		}
		startedAt := now.Add(time.Second)

		applied, err := store.CompareAndSetTaskAttemptForRunStatus(
			"claim-current_task", TaskQueued, RunRunning, TaskAttemptMutation{
				Status: TaskRunning, UpdatedAt: startedAt, StartedAt: &startedAt,
			},
		)
		if err != nil || !applied {
			t.Fatalf("guarded task claim = (%v, %v), want (true, nil)", applied, err)
		}
		task, err := store.GetTaskInstance("claim-current_task")
		if err != nil {
			t.Fatalf("GetTaskInstance: %v", err)
		}
		if task.Status != TaskRunning || task.StartedAt == nil ||
			!task.StartedAt.Equal(startedAt) {
			t.Fatalf("guarded task claim persisted %+v", task)
		}
	})

	t.Run("rejects cancellation before queued to running", func(t *testing.T) {
		store := newRunProgressionStore(t)
		now := time.Date(2026, time.July, 27, 16, 30, 0, 0, time.UTC)
		createRunProgressionRun(t, store, "claim-cancel", RunRunning, now)
		if err := store.CreateTaskInstance(&TaskInstance{
			ID: "claim-cancel_task", RunID: "claim-cancel", TaskID: "task",
			Status: TaskQueued, Attempt: 1, CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("CreateTaskInstance: %v", err)
		}
		applied, err := store.CompareAndSetDagRunStatus(
			"claim-cancel", RunRunning, RunCancelled, now.Add(time.Second),
		)
		if err != nil || !applied {
			t.Fatalf("cancel run = (%v, %v), want (true, nil)", applied, err)
		}
		startedAt := now.Add(2 * time.Second)

		applied, err = store.CompareAndSetTaskAttemptForRunStatus(
			"claim-cancel_task", TaskQueued, RunRunning, TaskAttemptMutation{
				Status: TaskRunning, UpdatedAt: startedAt, StartedAt: &startedAt,
			},
		)
		if err != nil {
			t.Fatalf("guarded task claim: %v", err)
		}
		if applied {
			t.Fatal("guarded task claim applied after run cancellation")
		}
		task, err := store.GetTaskInstance("claim-cancel_task")
		if err != nil {
			t.Fatalf("GetTaskInstance: %v", err)
		}
		if task.Status != TaskQueued || task.StartedAt != nil {
			t.Fatalf("cancelled-run claim mutated task: %+v", task)
		}
	})
}

func TestEnsureTaskInstanceValidatesImmutableItemBinding(t *testing.T) {
	store := newRunProgressionStore(t)
	now := time.Date(2026, time.July, 27, 17, 0, 0, 0, time.UTC)
	createRunProgressionRun(t, store, "ensure-item", RunRunning, now)
	item := "bound-item"
	instance := TaskInstance{
		ID: "ensure-item_map[0]", RunID: "ensure-item", TaskID: "map[0]",
		Status: TaskPending, ItemValue: &item, Attempt: 1,
		CreatedAt: now, UpdatedAt: now,
	}

	created, err := store.EnsureTaskInstance(&instance)
	if err != nil || !created {
		t.Fatalf("first EnsureTaskInstance = (%v, %v), want (true, nil)", created, err)
	}
	if err := store.UpdateTaskInstanceStatusAndOutput(
		instance.ID, TaskRunning, "progressed output",
	); err != nil {
		t.Fatalf("progress task: %v", err)
	}
	created, err = store.EnsureTaskInstance(&instance)
	if err != nil || created {
		t.Fatalf("exact immutable replay = (%v, %v), want (false, nil)", created, err)
	}

	differentItem := "different-item"
	conflict := instance
	conflict.ItemValue = &differentItem
	created, err = store.EnsureTaskInstance(&conflict)
	if err == nil || created {
		t.Fatalf("conflicting item replay = (%v, %v), want (false, error)", created, err)
	}
	if !strings.Contains(err.Error(), "item binding") {
		t.Fatalf("conflicting item error = %v, want item binding context", err)
	}

	nilItem := instance
	nilItem.ItemValue = nil
	created, err = store.EnsureTaskInstance(&nilItem)
	if err == nil || created {
		t.Fatalf("nil item replay = (%v, %v), want (false, error)", created, err)
	}
}
