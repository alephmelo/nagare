package scheduler

import (
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/tasklifecycle"
)

func TestManualMapRetryStaysPendingUntilSetupOwnership(t *testing.T) {
	store, sched, runID := newMapTestScheduler(t, 1)
	_, parent := createSourceAndParent(t, store, runID, `["item"]`, 1, models.TaskFailed)
	if err := store.UpdateDagRunStatus(runID, models.RunFailed); err != nil {
		t.Fatalf("UpdateDagRunStatus: %v", err)
	}

	if err := sched.RetryTask(runID, parent.TaskID); err != nil {
		t.Fatalf("RetryTask: %v", err)
	}
	attempts, err := store.GetTaskAttempts(runID, parent.TaskID)
	if err != nil {
		t.Fatalf("GetTaskAttempts: %v", err)
	}
	if len(attempts) != 2 || attempts[1].Status != models.TaskPending {
		t.Fatalf("attempts = %#v, want one pending retry successor", attempts)
	}
	run, err := store.GetDagRun(runID)
	if err != nil {
		t.Fatalf("GetDagRun: %v", err)
	}
	if run.Status != models.RunRunning {
		t.Fatalf("run status = %s, want running", run.Status)
	}

	// Run-level evaluation must not expose the map attempt to workers.
	if err := sched.evaluateRunCompletions(); err != nil {
		t.Fatalf("evaluateRunCompletions: %v", err)
	}
	retried, err := store.GetTaskInstance(attempts[1].ID)
	if err != nil {
		t.Fatalf("GetTaskInstance: %v", err)
	}
	if retried.Status != models.TaskPending {
		t.Fatalf("map retry status = %s before setup, want pending", retried.Status)
	}
	if _, err := store.GetMapSetup(retried.ID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("GetMapSetup before reconciliation error = %v, want sql.ErrNoRows", err)
	}

	if err := sched.PromotePendingTasks(); err != nil {
		t.Fatalf("PromotePendingTasks: %v", err)
	}
	retried, err = store.GetTaskInstance(attempts[1].ID)
	if err != nil {
		t.Fatalf("GetTaskInstance after reconciliation: %v", err)
	}
	if retried.Status != models.TaskRunning || retried.StartedAt == nil {
		t.Fatalf("map retry after setup = %#v, want scheduler-owned running attempt", retried)
	}
	setup, err := store.GetMapSetup(retried.ID)
	if err != nil {
		t.Fatalf("GetMapSetup after reconciliation: %v", err)
	}
	if !retried.StartedAt.Equal(setup.StartedAt) {
		t.Fatalf("ownership watermark = %v, setup watermark = %v", retried.StartedAt, setup.StartedAt)
	}
}

func TestManualRetryReopensCancelledRunWithoutDuplicateSuccessor(t *testing.T) {
	store := openControlStore(t, t.TempDir()+"/manual-cancelled.db")
	now := time.Now().UTC().Add(-time.Minute)
	createControlRun(t, store, "run-1", models.RunCancelled, now)
	createControlAttempt(t, store, models.TaskInstance{
		ID: "cancelled", RunID: "run-1", TaskID: "task",
		Status: models.TaskCancelled, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	})
	sched := NewScheduler(store)

	if err := sched.RetryTask("run-1", "task"); err != nil {
		t.Fatalf("RetryTask: %v", err)
	}
	if err := sched.RetryTask("run-1", "task"); err != nil {
		t.Fatalf("replayed RetryTask: %v", err)
	}
	attempts, err := store.GetTaskAttempts("run-1", "task")
	if err != nil {
		t.Fatalf("GetTaskAttempts: %v", err)
	}
	if len(attempts) != 2 || attempts[1].Status != models.TaskQueued {
		t.Fatalf("attempts = %#v, want exactly one queued successor", attempts)
	}
	run, err := store.GetDagRun("run-1")
	if err != nil {
		t.Fatalf("GetDagRun: %v", err)
	}
	if run.Status != models.RunRunning {
		t.Fatalf("run status = %s, want running", run.Status)
	}
}

func TestCreateRunContainsPartialMaterialization(t *testing.T) {
	store := openControlStore(t, t.TempDir()+"/partial-run.db")
	sched := NewScheduler(store)
	dag := &models.DAGDef{
		ID: "duplicate-materialization",
		Tasks: []models.TaskDef{
			{ID: "same"},
			{ID: "same"},
		},
	}

	if _, err := sched.createRun(dag, "manual", time.Now().UTC(), nil); err == nil ||
		!strings.Contains(err.Error(), "logical attempt already exists") {
		t.Fatalf("createRun error = %v, want propagated materialization conflict", err)
	}
	runs, err := store.GetDagRuns(10, 0, dag.ID, "all", "all")
	if err != nil {
		t.Fatalf("GetDagRuns: %v", err)
	}
	if len(runs) != 1 || runs[0].Status != models.RunFailed {
		t.Fatalf("runs = %#v, want one contained failed run", runs)
	}
	tasks, err := store.GetTaskInstancesByRun(runs[0].ID)
	if err != nil {
		t.Fatalf("GetTaskInstancesByRun: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("materialized task count = %d, want one durable partial task", len(tasks))
	}
}

func TestStaleSuccessEvaluationReevaluatesAfterRetrySuccessor(t *testing.T) {
	store := openControlStore(t, t.TempDir()+"/stale-success-retry.db")
	now := time.Now().UTC().Add(-time.Minute)
	createControlRun(t, store, "run-1", models.RunRunning, now)
	createControlAttempt(t, store, models.TaskInstance{
		ID: "success", RunID: "run-1", TaskID: "task",
		Status: models.TaskSuccess, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	})
	sched := NewScheduler(store)
	dag := &models.DAGDef{ID: "dag", Tasks: []models.TaskDef{{ID: "task"}}}
	sched.dags[dag.ID] = dag
	run, err := store.GetDagRun("run-1")
	if err != nil {
		t.Fatalf("GetDagRun: %v", err)
	}
	staleTasks, err := store.GetLatestTaskAttempts(run.ID)
	if err != nil {
		t.Fatalf("GetLatestTaskAttempts: %v", err)
	}
	if disposition, err := tasklifecycle.New(store).RetryCurrent(
		run.ID, "task", now.Add(time.Second),
	); err != nil || disposition != tasklifecycle.Applied {
		t.Fatalf("RetryCurrent = (%v, %v), want (Applied, nil)", disposition, err)
	}

	sched.orchestrate.Lock()
	err = sched.applyProgressionLocked(run, dag, staleTasks, now.Add(2*time.Second))
	sched.orchestrate.Unlock()
	if err != nil {
		t.Fatalf("apply stale success progression: %v", err)
	}
	reloaded, err := store.GetDagRun(run.ID)
	if err != nil {
		t.Fatalf("GetDagRun after progression: %v", err)
	}
	if reloaded.Status != models.RunRunning {
		t.Fatalf("run status = %s, want running with durable retry successor", reloaded.Status)
	}
}

func TestStaleSuccessEvaluationReevaluatesAfterCancellation(t *testing.T) {
	store := openControlStore(t, t.TempDir()+"/stale-success-cancel.db")
	now := time.Now().UTC().Add(-time.Minute)
	createControlRun(t, store, "run-1", models.RunRunning, now)
	createControlAttempt(t, store, models.TaskInstance{
		ID: "success", RunID: "run-1", TaskID: "task",
		Status: models.TaskSuccess, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	})
	sched := NewScheduler(store)
	dag := &models.DAGDef{ID: "dag", Tasks: []models.TaskDef{{ID: "task"}}}
	sched.dags[dag.ID] = dag
	run, err := store.GetDagRun("run-1")
	if err != nil {
		t.Fatalf("GetDagRun: %v", err)
	}
	staleTasks, err := store.GetLatestTaskAttempts(run.ID)
	if err != nil {
		t.Fatalf("GetLatestTaskAttempts: %v", err)
	}
	if err := store.UpdateTaskInstanceStatus("success", models.TaskCancelled); err != nil {
		t.Fatalf("cancel successful snapshot fixture: %v", err)
	}

	sched.orchestrate.Lock()
	err = sched.applyProgressionLocked(run, dag, staleTasks, now.Add(2*time.Second))
	sched.orchestrate.Unlock()
	if err != nil {
		t.Fatalf("apply stale success progression: %v", err)
	}
	reloaded, err := store.GetDagRun(run.ID)
	if err != nil {
		t.Fatalf("GetDagRun after progression: %v", err)
	}
	if reloaded.Status != models.RunCancelled {
		t.Fatalf("run status = %s, want cancellation to replace stale success", reloaded.Status)
	}
}

func TestManualRetryDoesNotReopenAfterSuccessorCancellation(t *testing.T) {
	store := openControlStore(t, t.TempDir()+"/manual-retry-cancel.db")
	now := time.Now().UTC().Add(-time.Minute)
	createControlRun(t, store, "run-1", models.RunCancelled, now)
	createControlAttempt(t, store, models.TaskInstance{
		ID: "cancelled", RunID: "run-1", TaskID: "task",
		Status: models.TaskCancelled, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	})
	sched := NewScheduler(store)
	dag := &models.DAGDef{ID: "dag", Tasks: []models.TaskDef{{ID: "task"}}}
	sched.dags[dag.ID] = dag
	lifecycle := tasklifecycle.New(store)
	if disposition, err := lifecycle.RetryCurrent(
		"run-1", "task", now.Add(time.Second),
	); err != nil || disposition != tasklifecycle.Applied {
		t.Fatalf("RetryCurrent = (%v, %v), want (Applied, nil)", disposition, err)
	}
	run, err := store.GetDagRun("run-1")
	if err != nil {
		t.Fatalf("GetDagRun: %v", err)
	}
	staleTasks, err := store.GetLatestTaskAttempts(run.ID)
	if err != nil {
		t.Fatalf("GetLatestTaskAttempts: %v", err)
	}
	if disposition, err := lifecycle.CancelAttempt(
		staleTasks[0].ID, now.Add(2*time.Second),
	); err != nil || disposition != tasklifecycle.Applied {
		t.Fatalf("CancelAttempt = (%v, %v), want (Applied, nil)", disposition, err)
	}

	sched.orchestrate.Lock()
	err = sched.applyProgressionIntentLocked(
		run, dag, staleTasks, now.Add(3*time.Second), progressionManualRetry,
	)
	sched.orchestrate.Unlock()
	if err != nil {
		t.Fatalf("apply stale manual retry progression: %v", err)
	}
	reloaded, err := store.GetDagRun(run.ID)
	if err != nil {
		t.Fatalf("GetDagRun after progression: %v", err)
	}
	if reloaded.Status != models.RunCancelled {
		t.Fatalf("run status = %s, want cancellation to remain authoritative", reloaded.Status)
	}
}
