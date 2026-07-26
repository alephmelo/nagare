package scheduler

import (
	"encoding/json"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/models"
)

func newMapTestScheduler(t *testing.T, retries int) (*models.Store, *Scheduler, string) {
	t.Helper()
	store, err := models.NewStore(filepath.Join(t.TempDir(), "scheduler.db"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	sched := NewScheduler(store)
	sched.dags["map_dag"] = &models.DAGDef{
		ID: "map_dag",
		Tasks: []models.TaskDef{
			{ID: "source", Type: "command"},
			{ID: "map", Type: "map", MapOver: "source", DependsOn: []string{"source"}, Retries: retries},
			{ID: "publish@g2", Type: "command"},
			{ID: "foo[bar]", Type: "command"},
		},
	}
	runID := "map_run"
	now := time.Now().UTC()
	if err := store.CreateDagRun(&models.DagRun{
		ID: runID, DAGID: "map_dag", Status: models.RunRunning,
		ExecDate: now, TriggerType: "manual", CreatedAt: now,
	}); err != nil {
		t.Fatalf("CreateDagRun: %v", err)
	}
	return store, sched, runID
}

func createMapTestTask(t *testing.T, store *models.Store, task models.TaskInstance) {
	t.Helper()
	if task.Attempt == 0 {
		task.Attempt = 1
	}
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now().UTC()
	}
	if task.UpdatedAt.IsZero() {
		task.UpdatedAt = task.CreatedAt
	}
	if err := store.CreateTaskInstance(&task); err != nil {
		t.Fatalf("CreateTaskInstance(%s): %v", task.ID, err)
	}
}

func createSourceAndParent(t *testing.T, store *models.Store, runID, output string, parentAttempt int, parentStatus models.TaskStatus) (models.TaskInstance, models.TaskInstance) {
	t.Helper()
	source := models.TaskInstance{
		ID: runID + "_source", RunID: runID, TaskID: "source",
		Status: models.TaskSuccess, Output: output, Attempt: 1,
	}
	createMapTestTask(t, store, source)
	parentID := runID + "_map"
	if parentAttempt > 1 {
		parentID += "_" + strconv.Itoa(parentAttempt)
	}
	parent := models.TaskInstance{
		ID: parentID, RunID: runID, TaskID: "map",
		Status: parentStatus, Attempt: parentAttempt,
	}
	createMapTestTask(t, store, parent)
	return source, parent
}

func TestMapIdentityParsingPreservesLegitimateIDsAndGenerationOne(t *testing.T) {
	for _, taskID := range []string{"publish@g2", "foo[bar]", "foo[0]@g1", "foo[0]@green"} {
		if got := PublicTaskID(taskID); got != taskID {
			t.Errorf("PublicTaskID(%q) = %q", taskID, got)
		}
	}
	if got := PublicTaskID("map[7]@g2"); got != "map[7]" {
		t.Fatalf("PublicTaskID(generation two) = %q", got)
	}
	if got := generationTaskID("map[7]", 1); got != "map[7]" {
		t.Fatalf("generation-one task ID = %q", got)
	}
	task := ProjectTaskInstance(models.TaskInstance{
		ID: "run_map[7]", RunID: "run", TaskID: "map[7]", Attempt: 1,
	})
	if task.ID != "run_map[7]" || task.TaskID != "map[7]" {
		t.Fatalf("generation-one projection changed identity: %+v", task)
	}
}

func TestResolveTaskIDDoesNotInterpretOrdinarySuffixesOrBrackets(t *testing.T) {
	store, sched, runID := newMapTestScheduler(t, 0)
	createMapTestTask(t, store, models.TaskInstance{
		ID: runID + "_map", RunID: runID, TaskID: "map", Status: models.TaskRunning, Attempt: 2,
	})

	for _, taskID := range []string{"publish@g2", "foo[bar]"} {
		got, err := sched.ResolveTaskID(runID, taskID)
		if err != nil || got != taskID {
			t.Fatalf("ResolveTaskID(%q) = %q, %v", taskID, got, err)
		}
	}
	got, err := sched.ResolveTaskID(runID, "map[0]")
	if err != nil || got != "map[0]@g2" {
		t.Fatalf("ResolveTaskID(map child) = %q, %v", got, err)
	}
}

func TestMapSetupRecoveryReconcilesCompleteSetFromDurableBinding(t *testing.T) {
	store, sched, runID := newMapTestScheduler(t, 0)
	source, parent := createSourceAndParent(t, store, runID, `["a","b","c"]`, 1, models.TaskPending)
	startedAt := time.Now().UTC()
	setup, created, err := store.EnsureMapSetup(models.MapSetup{
		ParentAttemptID: parent.ID, UpstreamAttemptID: source.ID,
		UpstreamOutput: source.Output, StartedAt: startedAt,
	})
	if err != nil || !created {
		t.Fatalf("EnsureMapSetup() = created %v, err %v", created, err)
	}
	if _, err := sched.lifecycle.StartSetup(parent.ID, setup.StartedAt); err != nil {
		t.Fatalf("StartSetup: %v", err)
	}
	item := "a"
	createMapTestTask(t, store, models.TaskInstance{
		ID: runID + "_map[0]", RunID: runID, TaskID: "map[0]",
		Status: models.TaskPending, ItemValue: &item, Attempt: 1,
	})

	// A newer upstream success must not alter the already-bound expansion.
	createMapTestTask(t, store, models.TaskInstance{
		ID: runID + "_source_2", RunID: runID, TaskID: "source",
		Status: models.TaskSuccess, Output: `["new"]`, Attempt: 2,
	})
	if err := sched.PromotePendingTasks(); err != nil {
		t.Fatalf("PromotePendingTasks recovery: %v", err)
	}
	if err := sched.PromotePendingTasks(); err != nil {
		t.Fatalf("PromotePendingTasks replay: %v", err)
	}

	tasks, err := store.GetLatestTaskAttempts(runID)
	if err != nil {
		t.Fatalf("GetLatestTaskAttempts: %v", err)
	}
	children := map[string]models.TaskInstance{}
	for _, task := range tasks {
		if belongsToMapGeneration(task.TaskID, "map", 1) {
			children[task.TaskID] = task
		}
	}
	if len(children) != 3 {
		t.Fatalf("child count = %d, want 3", len(children))
	}
	for index, wantItem := range []string{"a", "b", "c"} {
		taskID := "map[" + strconv.Itoa(index) + "]"
		child := children[taskID]
		if child.ID != runID+"_"+taskID || child.Status != models.TaskQueued ||
			child.ItemValue == nil || *child.ItemValue != wantItem {
			t.Errorf("child %s = %+v, want queued item %q", taskID, child, wantItem)
		}
	}
	persisted, err := store.GetMapSetup(parent.ID)
	if err != nil || persisted.UpstreamAttemptID != source.ID || persisted.UpstreamOutput != source.Output {
		t.Fatalf("durable setup changed: %+v, %v", persisted, err)
	}
}

func TestPendingMapSetupResumesItsBindingAfterUpstreamChanges(t *testing.T) {
	store, sched, runID := newMapTestScheduler(t, 0)
	source, parent := createSourceAndParent(t, store, runID, `["bound"]`, 1, models.TaskPending)
	if _, _, err := store.EnsureMapSetup(models.MapSetup{
		ParentAttemptID: parent.ID, UpstreamAttemptID: source.ID,
		UpstreamOutput: source.Output, StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("EnsureMapSetup: %v", err)
	}
	createMapTestTask(t, store, models.TaskInstance{
		ID: runID + "_source_2", RunID: runID, TaskID: "source",
		Status: models.TaskFailed, Output: `["wrong"]`, Attempt: 2,
	})

	if err := sched.PromotePendingTasks(); err != nil {
		t.Fatalf("PromotePendingTasks: %v", err)
	}
	child, err := store.GetTaskAttempts(runID, "map[0]")
	if err != nil || len(child) != 1 || child[0].ItemValue == nil ||
		*child[0].ItemValue != "bound" || child[0].Status != models.TaskQueued {
		t.Fatalf("recovered bound child = %+v, %v", child, err)
	}
}

func TestQueuedMapParentIsRecoveredIntoSetup(t *testing.T) {
	store, sched, runID := newMapTestScheduler(t, 0)
	_, parent := createSourceAndParent(t, store, runID, `["a"]`, 1, models.TaskQueued)
	if err := sched.PromotePendingTasks(); err != nil {
		t.Fatalf("PromotePendingTasks: %v", err)
	}
	reloaded, _ := store.GetTaskInstance(parent.ID)
	if reloaded.Status != models.TaskRunning || reloaded.StartedAt == nil {
		t.Fatalf("queued parent was not claimed for setup: %+v", reloaded)
	}
	children, err := store.GetTaskAttempts(runID, "map[0]")
	if err != nil || len(children) != 1 || children[0].Status != models.TaskQueued {
		t.Fatalf("queued-parent children = %+v, %v", children, err)
	}
	if _, err := store.GetMapSetup(parent.ID); err != nil {
		t.Fatalf("queued-parent binding missing: %v", err)
	}
}

func TestMapSetupConflictCancelsChildrenAndAppliesParentRetryPolicy(t *testing.T) {
	store, sched, runID := newMapTestScheduler(t, 1)
	source, parent := createSourceAndParent(t, store, runID, `["expected"]`, 1, models.TaskPending)
	setup, _, err := store.EnsureMapSetup(models.MapSetup{
		ParentAttemptID: parent.ID, UpstreamAttemptID: source.ID,
		UpstreamOutput: source.Output, StartedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("EnsureMapSetup: %v", err)
	}
	if _, err := sched.lifecycle.StartSetup(parent.ID, setup.StartedAt); err != nil {
		t.Fatalf("StartSetup: %v", err)
	}
	wrongItem := "conflict"
	createMapTestTask(t, store, models.TaskInstance{
		ID: runID + "_map[0]", RunID: runID, TaskID: "map[0]",
		Status: models.TaskPending, ItemValue: &wrongItem, Attempt: 1,
	})

	if err := sched.PromotePendingTasks(); err == nil {
		t.Fatal("PromotePendingTasks returned nil for conflicting child")
	}
	reloadedParent, _ := store.GetTaskInstance(parent.ID)
	reloadedChild, _ := store.GetTaskInstance(runID + "_map[0]")
	if reloadedParent.Status != models.TaskUpForRetry {
		t.Fatalf("parent status = %s, want up_for_retry", reloadedParent.Status)
	}
	if reloadedChild.Status != models.TaskCancelled {
		t.Fatalf("child status = %s, want cancelled", reloadedChild.Status)
	}
}

func TestMapGenerationTwoIsolatesStorageAndChildRetryBudget(t *testing.T) {
	store, sched, runID := newMapTestScheduler(t, 1)
	createMapTestTask(t, store, models.TaskInstance{
		ID: runID + "_source", RunID: runID, TaskID: "source",
		Status: models.TaskSuccess, Output: `["a"]`, Attempt: 1,
	})
	createMapTestTask(t, store, models.TaskInstance{
		ID: runID + "_map", RunID: runID, TaskID: "map",
		Status: models.TaskUpForRetry, Attempt: 1,
	})
	createMapTestTask(t, store, models.TaskInstance{
		ID: runID + "_map_2", RunID: runID, TaskID: "map",
		Status: models.TaskPending, Attempt: 2,
	})
	createMapTestTask(t, store, models.TaskInstance{
		ID: runID + "_map[0]", RunID: runID, TaskID: "map[0]",
		Status: models.TaskFailed, Attempt: 1,
	})

	if err := sched.PromotePendingTasks(); err != nil {
		t.Fatalf("PromotePendingTasks: %v", err)
	}
	child, err := store.GetTaskAttempts(runID, "map[0]@g2")
	if err != nil || len(child) != 1 {
		t.Fatalf("generation-two attempts = %d, %v", len(child), err)
	}
	if child[0].ID != runID+"_map[0]@g2" || child[0].Status != models.TaskQueued {
		t.Fatalf("generation-two child = %+v", child[0])
	}
	if err := store.UpdateTaskInstanceStatus(child[0].ID, models.TaskUpForRetry); err != nil {
		t.Fatalf("mark child retryable: %v", err)
	}
	if err := sched.RetryTask(runID, "map[0]"); err != nil {
		t.Fatalf("RetryTask(public child): %v", err)
	}
	child, err = store.GetTaskAttempts(runID, "map[0]@g2")
	if err != nil || len(child) != 2 || child[1].Status != models.TaskQueued {
		t.Fatalf("generation-two retry history = %+v, %v", child, err)
	}
	old, err := store.GetTaskAttempts(runID, "map[0]")
	if err != nil || len(old) != 1 || old[0].Status != models.TaskFailed {
		t.Fatalf("generation-one history changed: %+v, %v", old, err)
	}
	projected := ProjectTaskInstance(child[1])
	if projected.TaskID != "map[0]" || projected.ID != runID+"_map[0]_2" {
		t.Fatalf("projected retry = %+v", projected)
	}
}

func TestMapAggregationRequiresFullCardinalityAndIgnoresObsoleteGeneration(t *testing.T) {
	t.Run("partial set waits for recovery", func(t *testing.T) {
		store, sched, runID := newMapTestScheduler(t, 0)
		source, parent := createSourceAndParent(t, store, runID, `["a","b"]`, 1, models.TaskPending)
		setup, _, err := store.EnsureMapSetup(models.MapSetup{
			ParentAttemptID: parent.ID, UpstreamAttemptID: source.ID,
			UpstreamOutput: source.Output, StartedAt: time.Now().UTC(),
		})
		if err != nil {
			t.Fatalf("EnsureMapSetup: %v", err)
		}
		if _, err := sched.lifecycle.StartSetup(parent.ID, setup.StartedAt); err != nil {
			t.Fatalf("StartSetup: %v", err)
		}
		item := "a"
		createMapTestTask(t, store, models.TaskInstance{
			ID: runID + "_map[0]", RunID: runID, TaskID: "map[0]",
			Status: models.TaskSuccess, ItemValue: &item, Attempt: 1,
		})
		if err := sched.evaluateRunCompletions(); err != nil {
			t.Fatalf("evaluateRunCompletions: %v", err)
		}
		reloaded, _ := store.GetTaskInstance(parent.ID)
		if reloaded.Status != models.TaskRunning {
			t.Fatalf("partial map parent status = %s, want running", reloaded.Status)
		}
	})

	t.Run("obsolete failure does not fail run", func(t *testing.T) {
		store, sched, runID := newMapTestScheduler(t, 1)
		source, _ := createSourceAndParent(t, store, runID, `["a"]`, 1, models.TaskUpForRetry)
		parent := models.TaskInstance{
			ID: runID + "_map_2", RunID: runID, TaskID: "map",
			Status: models.TaskPending, Attempt: 2,
		}
		createMapTestTask(t, store, parent)
		oldItem := "obsolete"
		createMapTestTask(t, store, models.TaskInstance{
			ID: runID + "_map[0]", RunID: runID, TaskID: "map[0]",
			Status: models.TaskFailed, ItemValue: &oldItem, Attempt: 1,
		})
		if err := sched.PromotePendingTasks(); err != nil {
			t.Fatalf("PromotePendingTasks: %v", err)
		}
		current, _ := store.GetTaskAttempts(runID, "map[0]@g2")
		if err := store.UpdateTaskInstanceStatus(current[0].ID, models.TaskFailed); err != nil {
			t.Fatalf("fail first current child attempt: %v", err)
		}
		item := "a"
		createMapTestTask(t, store, models.TaskInstance{
			ID: runID + "_map[0]@g2_2", RunID: runID, TaskID: "map[0]@g2",
			Status: models.TaskSuccess, ItemValue: &item, Attempt: 2,
		})
		if err := sched.evaluateRunCompletions(); err != nil {
			t.Fatalf("evaluateRunCompletions: %v", err)
		}
		run, _ := store.GetDagRun(runID)
		if run.Status != models.RunSuccess {
			t.Fatalf("run status = %s, want success (source %s)", run.Status, source.ID)
		}
	})
}

func TestCancelMapChildrenLockedTargetsExactGeneration(t *testing.T) {
	store, sched, runID := newMapTestScheduler(t, 0)
	oldItem, currentItem := "old", "current"
	createMapTestTask(t, store, models.TaskInstance{
		ID: runID + "_map[0]", RunID: runID, TaskID: "map[0]",
		Status: models.TaskRunning, ItemValue: &oldItem, Attempt: 1,
	})
	createMapTestTask(t, store, models.TaskInstance{
		ID: runID + "_map[0]@g2", RunID: runID, TaskID: "map[0]@g2",
		Status: models.TaskQueued, ItemValue: &currentItem, Attempt: 1,
	})
	sched.orchestrate.Lock()
	err := sched.cancelMapChildrenLocked(runID, "map", 2)
	sched.orchestrate.Unlock()
	if err != nil {
		t.Fatalf("cancelMapChildrenLocked: %v", err)
	}
	old, _ := store.GetTaskInstance(runID + "_map[0]")
	current, _ := store.GetTaskInstance(runID + "_map[0]@g2")
	if old.Status != models.TaskRunning || current.Status != models.TaskCancelled {
		t.Fatalf("statuses after generation-two cancellation: old=%s current=%s", old.Status, current.Status)
	}
}

func TestEmptyMapCompletesDuringRecoverableSetup(t *testing.T) {
	store, sched, runID := newMapTestScheduler(t, 0)
	_, parent := createSourceAndParent(t, store, runID, `[]`, 1, models.TaskPending)
	if err := sched.PromotePendingTasks(); err != nil {
		t.Fatalf("PromotePendingTasks: %v", err)
	}
	reloaded, _ := store.GetTaskInstance(parent.ID)
	if reloaded.Status != models.TaskSuccess {
		t.Fatalf("empty map parent status = %s, want success", reloaded.Status)
	}
	setup, err := store.GetMapSetup(parent.ID)
	if err != nil {
		t.Fatalf("empty map setup was not durable: %v", err)
	}
	var items []string
	if err := json.Unmarshal([]byte(setup.UpstreamOutput), &items); err != nil || len(items) != 0 {
		t.Fatalf("empty map binding = %q, %v", setup.UpstreamOutput, err)
	}
}
