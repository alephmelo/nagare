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
