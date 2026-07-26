package scheduler

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/tasklifecycle"
)

func TestManualMapRetryUsesGuardedSetupOwnership(t *testing.T) {
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
	if len(attempts) != 2 || attempts[1].Status != models.TaskRunning ||
		attempts[1].StartedAt == nil {
		t.Fatalf("attempts = %#v, want one scheduler-owned retry successor", attempts)
	}
	run, err := store.GetDagRun(runID)
	if err != nil {
		t.Fatalf("GetDagRun: %v", err)
	}
	if run.Status != models.RunRunning {
		t.Fatalf("run status = %s, want running", run.Status)
	}

	retried, err := store.GetTaskInstance(attempts[1].ID)
	if err != nil {
		t.Fatalf("GetTaskInstance: %v", err)
	}
	if retried.Status != models.TaskRunning {
		t.Fatalf("map retry status = %s, want running setup owner", retried.Status)
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
	sched.dags["dag"] = &models.DAGDef{
		ID: "dag", Tasks: []models.TaskDef{{ID: "task", Type: "command"}},
	}

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

func TestCreateRunRollsBackPartialMaterialization(t *testing.T) {
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
		!strings.Contains(err.Error(), "materialize DagRun") {
		t.Fatalf("createRun error = %v, want propagated materialization conflict", err)
	}
	runs, err := store.GetDagRuns(10, 0, dag.ID, "all", "all")
	if err != nil {
		t.Fatalf("GetDagRuns: %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("runs = %#v, want atomic rollback", runs)
	}
	tasks, err := store.GetTasksByStatus(models.TaskPending)
	if err != nil {
		t.Fatalf("GetTasksByStatus: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("materialized task count = %d, want atomic rollback", len(tasks))
	}
}

func TestCreateRunProgressionFailureLeavesCompletePendingRunForRecovery(t *testing.T) {
	store := openControlStore(t, t.TempDir()+"/progression-recovery.db")
	sched := NewScheduler(store)
	delegate := sched.lifecycle
	progressionErr := errors.New("guarded promotion unavailable")
	sched.lifecycle = &controlLifecycleFault{
		delegate: delegate, promoteErr: progressionErr,
	}
	dag := &models.DAGDef{
		ID: "recoverable",
		Tasks: []models.TaskDef{
			{ID: "first"},
			{ID: "second"},
		},
	}
	sched.dags[dag.ID] = dag

	if _, err := sched.createRun(dag, "manual", time.Now().UTC(), nil); !errors.Is(err, progressionErr) {
		t.Fatalf("createRun error = %v, want guarded promotion failure", err)
	}
	runs, err := store.GetDagRuns(10, 0, dag.ID, "all", "all")
	if err != nil || len(runs) != 1 || runs[0].Status != models.RunRunning {
		t.Fatalf("runs = %#v, %v, want one recoverable running run", runs, err)
	}
	tasks, err := store.GetLatestTaskAttempts(runs[0].ID)
	if err != nil || len(tasks) != len(dag.Tasks) {
		t.Fatalf("tasks = %#v, %v, want complete static set", tasks, err)
	}
	for _, task := range tasks {
		if task.Status != models.TaskPending {
			t.Fatalf("task %s status = %s, want pending", task.ID, task.Status)
		}
	}

	sched.lifecycle = delegate
	if err := sched.PromotePendingTasks(); err != nil {
		t.Fatalf("PromotePendingTasks recovery: %v", err)
	}
	tasks, err = store.GetLatestTaskAttempts(runs[0].ID)
	if err != nil {
		t.Fatalf("GetLatestTaskAttempts after recovery: %v", err)
	}
	for _, task := range tasks {
		if task.Status != models.TaskQueued {
			t.Fatalf("recovered task %s status = %s, want queued", task.ID, task.Status)
		}
	}
}

func TestEvaluateMissingDAGUsesGuardedTerminalState(t *testing.T) {
	for _, test := range []struct {
		name       string
		taskStatus models.TaskStatus
		want       models.RunStatus
	}{
		{name: "fails unresolved run", taskStatus: models.TaskSuccess, want: models.RunFailed},
		{name: "cancellation wins", taskStatus: models.TaskCancelled, want: models.RunCancelled},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := openControlStore(t, t.TempDir()+"/missing-dag.db")
			now := time.Now().UTC()
			createControlRun(t, store, "run-1", models.RunRunning, now)
			createControlAttempt(t, store, models.TaskInstance{
				ID: "attempt", RunID: "run-1", TaskID: "task",
				Status: test.taskStatus, Attempt: 1, CreatedAt: now, UpdatedAt: now,
			})
			sched := NewScheduler(store)

			sched.orchestrate.Lock()
			_, err := sched.evaluateRunLocked(models.DagRun{ID: "run-1"}, now)
			sched.orchestrate.Unlock()
			if err == nil {
				t.Fatal("evaluateRunLocked error = nil, want missing-DAG diagnostic")
			}
			run, err := store.GetDagRun("run-1")
			if err != nil || run.Status != test.want {
				t.Fatalf("run = %#v, %v, want status %s", run, err, test.want)
			}
		})
	}
}

func TestRetryKindUsesExactDefinitionThenDurableMissingDAGProof(t *testing.T) {
	store := openControlStore(t, t.TempDir()+"/retry-kind.db")
	now := time.Now().UTC()
	createControlRun(t, store, "run-1", models.RunFailed, now)
	first := models.TaskInstance{
		ID: "map-1", RunID: "run-1", TaskID: "map",
		Status: models.TaskFailed, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	}
	current := models.TaskInstance{
		ID: "map-2", RunID: "run-1", TaskID: "map",
		Status: models.TaskFailed, Attempt: 2, CreatedAt: now, UpdatedAt: now,
	}
	createControlAttempt(t, store, first)
	createControlAttempt(t, store, current)
	if _, _, err := store.EnsureMapSetup(models.MapSetup{
		ParentAttemptID:   first.ID,
		UpstreamAttemptID: first.ID,
		UpstreamOutput:    "[]",
		StartedAt:         now,
	}); err != nil {
		t.Fatalf("EnsureMapSetup: %v", err)
	}
	sched := NewScheduler(store)

	pending, err := sched.retryUsesPendingSuccessor(current)
	if err != nil || !pending {
		t.Fatalf("missing-DAG durable retry kind = (%v, %v), want pending", pending, err)
	}

	child := current
	child.TaskID = "map[0]"
	pending, err = sched.retryUsesPendingSuccessor(child)
	if err != nil || pending {
		t.Fatalf("missing-DAG mapped-child retry kind = (%v, %v), want ordinary", pending, err)
	}

	sched.dags["dag"] = &models.DAGDef{
		ID: "dag",
		Tasks: []models.TaskDef{{
			ID: "map", Type: "map", MapOver: "source",
		}},
	}
	pending, err = sched.retryUsesPendingSuccessor(child)
	if err != nil || pending {
		t.Fatalf("loaded-DAG mapped-child retry kind = (%v, %v), want ordinary", pending, err)
	}
}

func TestRetryTaskMissingDAGAmbiguousFailsWithoutMutation(t *testing.T) {
	store := openControlStore(t, t.TempDir()+"/retry-kind-ambiguous.db")
	now := time.Now().UTC()
	createControlRun(t, store, "run-1", models.RunFailed, now)
	createControlAttempt(t, store, models.TaskInstance{
		ID: "failed", RunID: "run-1", TaskID: "task",
		Status: models.TaskFailed, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	})

	err := NewScheduler(store).RetryTask("run-1", "task")
	var unresolved *RetryKindUnresolvedError
	if !errors.As(err, &unresolved) {
		t.Fatalf("RetryTask error = %v, want RetryKindUnresolvedError", err)
	}
	attempts, loadErr := store.GetTaskAttempts("run-1", "task")
	if loadErr != nil || len(attempts) != 1 ||
		attempts[0].Status != models.TaskFailed {
		t.Fatalf("attempts = %#v, %v, want unchanged failed attempt", attempts, loadErr)
	}
	run, loadErr := store.GetDagRun("run-1")
	if loadErr != nil || run.Status != models.RunFailed {
		t.Fatalf("run = %#v, %v, want unchanged failed run", run, loadErr)
	}
}

func TestRetryTaskMissingDAGUsesDurableMapSetupProof(t *testing.T) {
	store := openControlStore(t, t.TempDir()+"/retry-kind-durable-map.db")
	now := time.Now().UTC()
	createControlRun(t, store, "run-1", models.RunFailed, now)
	failed := models.TaskInstance{
		ID: "map-1", RunID: "run-1", TaskID: "map",
		Status: models.TaskFailed, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	}
	createControlAttempt(t, store, failed)
	if _, _, err := store.EnsureMapSetup(models.MapSetup{
		ParentAttemptID:   failed.ID,
		UpstreamAttemptID: failed.ID,
		UpstreamOutput:    "[]",
		StartedAt:         now,
	}); err != nil {
		t.Fatalf("EnsureMapSetup: %v", err)
	}

	if err := NewScheduler(store).RetryTask("run-1", "map"); err != nil {
		t.Fatalf("RetryTask: %v", err)
	}
	attempts, err := store.GetTaskAttempts("run-1", "map")
	if err != nil || len(attempts) != 2 ||
		attempts[1].Status != models.TaskPending {
		t.Fatalf("attempts = %#v, %v, want one pending map successor", attempts, err)
	}
	run, err := store.GetDagRun("run-1")
	if err != nil || run.Status != models.RunRunning {
		t.Fatalf("run = %#v, %v, want reopened running run", run, err)
	}
}

func TestAutomaticRetryMissingDAGRecognizesStoredMappedChild(t *testing.T) {
	store := openControlStore(t, t.TempDir()+"/retry-kind-mapped-child.db")
	now := time.Now().UTC()
	createControlRun(t, store, "run-1", models.RunRunning, now)
	child := models.TaskInstance{
		ID: "child-1", RunID: "run-1", TaskID: "map[0]@g2",
		Status: models.TaskUpForRetry, Attempt: 1,
		CreatedAt: now, UpdatedAt: now,
	}
	createControlAttempt(t, store, child)

	if err := NewScheduler(store).RetryTaskAutomatically(child, 0, now); err != nil {
		t.Fatalf("RetryTaskAutomatically: %v", err)
	}
	attempts, err := store.GetTaskAttempts("run-1", child.TaskID)
	if err != nil || len(attempts) != 2 ||
		attempts[1].Status != models.TaskQueued {
		t.Fatalf("attempts = %#v, %v, want one queued mapped-child successor", attempts, err)
	}
}

func TestRetryTaskLoadedDAGMissingExactDefinitionFailsWithoutMutation(t *testing.T) {
	store := openControlStore(t, t.TempDir()+"/retry-kind-missing-definition.db")
	now := time.Now().UTC()
	createControlRun(t, store, "run-1", models.RunFailed, now)
	createControlAttempt(t, store, models.TaskInstance{
		ID: "failed", RunID: "run-1", TaskID: "removed",
		Status: models.TaskFailed, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	})
	sched := NewScheduler(store)
	sched.dags["dag"] = &models.DAGDef{
		ID: "dag", Tasks: []models.TaskDef{{ID: "known", Type: "command"}},
	}

	err := sched.RetryTask("run-1", "removed")
	var unresolved *RetryKindUnresolvedError
	if !errors.As(err, &unresolved) {
		t.Fatalf("RetryTask error = %v, want RetryKindUnresolvedError", err)
	}
	attempts, loadErr := store.GetTaskAttempts("run-1", "removed")
	if loadErr != nil || len(attempts) != 1 {
		t.Fatalf("attempts = %#v, %v, want no successor", attempts, loadErr)
	}
}

func TestUndefinedTaskQuarantineLetsConcurrentCancellationWin(t *testing.T) {
	store := openControlStore(t, t.TempDir()+"/undefined-cancel.db")
	now := time.Now().UTC()
	createControlRun(t, store, "run-1", models.RunRunning, now)
	createControlAttempt(t, store, models.TaskInstance{
		ID: "known", RunID: "run-1", TaskID: "known",
		Status: models.TaskSuccess, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	})
	undefined := models.TaskInstance{
		ID: "undefined", RunID: "run-1", TaskID: "undefined",
		Status: models.TaskPending, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	}
	createControlAttempt(t, store, undefined)
	delegate := tasklifecycle.New(store)
	sched := NewScheduler(store)
	sched.dags["dag"] = &models.DAGDef{
		ID: "dag", Tasks: []models.TaskDef{{ID: "known"}},
	}
	sched.lifecycle = &controlLifecycleFault{
		delegate: delegate,
		startSetup: func() (tasklifecycle.Disposition, error) {
			if disposition, err := delegate.CancelAttempt(undefined.ID, now.Add(time.Second)); err != nil ||
				disposition != tasklifecycle.Applied {
				t.Fatalf("CancelAttempt = (%v, %v), want Applied", disposition, err)
			}
			return tasklifecycle.Stale, nil
		},
	}

	if err := sched.PromotePendingTasks(); err != nil {
		t.Fatalf("PromotePendingTasks: %v", err)
	}
	run, err := store.GetDagRun("run-1")
	if err != nil || run.Status != models.RunCancelled {
		t.Fatalf("run = %#v, %v, want cancelled", run, err)
	}
}

func TestUndefinedPersistedTaskAlwaysTerminalizesBeforeEvaluation(t *testing.T) {
	for _, test := range []struct {
		name            string
		undefinedStatus models.TaskStatus
		wantRunStatus   models.RunStatus
	}{
		{
			name:            "cancelled undefined task wins",
			undefinedStatus: models.TaskCancelled,
			wantRunStatus:   models.RunCancelled,
		},
		{
			name:            "successful undefined task fails run",
			undefinedStatus: models.TaskSuccess,
			wantRunStatus:   models.RunFailed,
		},
		{
			name:            "failed undefined task fails run",
			undefinedStatus: models.TaskFailed,
			wantRunStatus:   models.RunFailed,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := openControlStore(t, t.TempDir()+"/undefined-terminal.db")
			now := time.Now().UTC()
			createControlRun(t, store, "run-1", models.RunRunning, now)
			createControlAttempt(t, store, models.TaskInstance{
				ID: "known", RunID: "run-1", TaskID: "known",
				Status: models.TaskSuccess, Attempt: 1, CreatedAt: now, UpdatedAt: now,
			})
			createControlAttempt(t, store, models.TaskInstance{
				ID: "undefined", RunID: "run-1", TaskID: "undefined",
				Status: test.undefinedStatus, Attempt: 1, CreatedAt: now, UpdatedAt: now,
			})
			sched := NewScheduler(store)
			sched.dags["dag"] = &models.DAGDef{
				ID: "dag", Tasks: []models.TaskDef{{ID: "known", Type: "command"}},
			}

			if err := sched.evaluateRunCompletions(); err != nil {
				t.Fatalf("evaluateRunCompletions: %v", err)
			}
			run, err := store.GetDagRun("run-1")
			if err != nil || run.Status != test.wantRunStatus {
				t.Fatalf("run = %#v, %v, want %s", run, err, test.wantRunStatus)
			}
		})
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
