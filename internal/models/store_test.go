package models

import (
	"fmt"
	"testing"
	"time"
)

func TestNewStoreAndInitSchema(t *testing.T) {
	// Use a shared in-memory db for testing so the connection pool shares the same db
	store, err := NewStore("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}
	defer store.Close()

	// Verify the tables exist by querying them
	rows, err := store.db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name='dag_runs'")
	if err != nil {
		t.Fatalf("failed to query sqlite_master: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Error("table dag_runs was not created")
	}

	rows2, err := store.db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name='task_instances'")
	if err != nil {
		t.Fatalf("failed to query sqlite_master: %v", err)
	}
	defer rows2.Close()

	if !rows2.Next() {
		t.Error("table task_instances was not created")
	}
}

func TestStoreQueries(t *testing.T) {
	store, err := NewStore("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}
	defer store.Close()

	run := DagRun{
		ID:          "run_1",
		DAGID:       "my_dag",
		Status:      RunRunning,
		ExecDate:    time.Now(),
		TriggerType: "scheduled",
		CreatedAt:   time.Now(),
	}

	if err := store.CreateDagRun(&run); err != nil {
		t.Fatalf("failed to create dag run: %v", err)
	}

	task := TaskInstance{
		ID:        "task_1",
		RunID:     "run_1",
		TaskID:    "my_task",
		Status:    TaskQueued,
		Attempt:   1,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if err := store.CreateTaskInstance(&task); err != nil {
		t.Fatalf("failed to create task instance: %v", err)
	}

	queuedTasks, err := store.GetQueuedTasks()
	if err != nil {
		t.Fatalf("failed to get queued tasks: %v", err)
	}

	if len(queuedTasks) != 1 || queuedTasks[0].ID != "task_1" {
		t.Errorf("expected 1 queued task with ID 'task_1', got %+v", queuedTasks)
	}

	if err := store.UpdateTaskInstanceStatus("task_1", TaskRunning); err != nil {
		t.Fatalf("failed to update task status: %v", err)
	}

	queuedTasksEmpty, _ := store.GetQueuedTasks()
	if len(queuedTasksEmpty) != 0 {
		t.Errorf("expected 0 queued tasks, got %d", len(queuedTasksEmpty))
	}
}

// TestTaskAttemptDefaultsToOne verifies that the first task instance for a
// task is automatically assigned attempt number 1.
func TestTaskAttemptDefaultsToOne(t *testing.T) {
	store, err := NewStore("file::memory:?cache=shared&mode=memory")
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}
	defer store.Close()

	if err := store.CreateDagRun(&DagRun{
		ID: "run_a1", DAGID: "dag_a", Status: RunRunning,
		ExecDate: time.Now(), TriggerType: "manual", CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	ti := &TaskInstance{
		ID: "run_a1_extract", RunID: "run_a1", TaskID: "extract",
		Status: TaskQueued, Attempt: 1,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := store.CreateTaskInstance(ti); err != nil {
		t.Fatalf("create task instance: %v", err)
	}

	tasks, err := store.GetLatestTaskAttempts("run_a1")
	if err != nil {
		t.Fatalf("GetLatestTaskAttempts: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	if tasks[0].Attempt != 1 {
		t.Errorf("expected attempt=1, got %d", tasks[0].Attempt)
	}
}

// TestCreateNewAttemptOnRetry verifies that retrying a failed task inserts
// a brand-new row (attempt=2) without modifying the original (attempt=1) row.
func TestCreateNewAttemptOnRetry(t *testing.T) {
	store, err := NewStore("file::memory:?cache=shared&mode=memory")
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}
	defer store.Close()

	if err := store.CreateDagRun(&DagRun{
		ID: "run_b1", DAGID: "dag_b", Status: RunRunning,
		ExecDate: time.Now(), TriggerType: "manual", CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	// 1st attempt – fails
	attempt1 := &TaskInstance{
		ID: "run_b1_load_1", RunID: "run_b1", TaskID: "load",
		Status: TaskFailed, Attempt: 1, Output: "exit code 1: connection refused",
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := store.CreateTaskInstance(attempt1); err != nil {
		t.Fatalf("create attempt 1: %v", err)
	}

	// Simulate the retry: create a 2nd attempt row
	newID, err := store.CreateNewTaskAttempt("run_b1", "load")
	if err != nil {
		t.Fatalf("CreateNewTaskAttempt: %v", err)
	}
	if newID == "" {
		t.Fatal("expected a non-empty new task instance ID")
	}

	// The original attempt row must be untouched
	allAttempts, err := store.GetTaskAttempts("run_b1", "load")
	if err != nil {
		t.Fatalf("GetTaskAttempts: %v", err)
	}
	if len(allAttempts) != 2 {
		t.Fatalf("expected 2 attempts, got %d", len(allAttempts))
	}
	if allAttempts[0].Attempt != 1 {
		t.Errorf("first attempt should be 1, got %d", allAttempts[0].Attempt)
	}
	if allAttempts[0].Output != "exit code 1: connection refused" {
		t.Errorf("original attempt output was overwritten: got %q", allAttempts[0].Output)
	}
	if allAttempts[1].Attempt != 2 {
		t.Errorf("second attempt should be 2, got %d", allAttempts[1].Attempt)
	}
	if allAttempts[1].Status != TaskQueued {
		t.Errorf("new attempt should start as queued, got %s", allAttempts[1].Status)
	}
}

// TestGetLatestTaskAttemptsReturnsMostRecent verifies that when multiple
// attempts exist for a task, GetLatestTaskAttempts returns only the most
// recent one per task ID.
func TestGetLatestTaskAttemptsReturnsMostRecent(t *testing.T) {
	store, err := NewStore("file::memory:?cache=shared&mode=memory")
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}
	defer store.Close()

	if err := store.CreateDagRun(&DagRun{
		ID: "run_c1", DAGID: "dag_c", Status: RunRunning,
		ExecDate: time.Now(), TriggerType: "manual", CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	// Insert two tasks, "extract" with 2 attempts and "load" with 1 attempt
	instances := []TaskInstance{
		{ID: "run_c1_extract_1", RunID: "run_c1", TaskID: "extract", Attempt: 1, Status: TaskFailed, Output: "fail log", CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{ID: "run_c1_extract_2", RunID: "run_c1", TaskID: "extract", Attempt: 2, Status: TaskSuccess, Output: "ok", CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{ID: "run_c1_load_1", RunID: "run_c1", TaskID: "load", Attempt: 1, Status: TaskSuccess, Output: "loaded 100 rows", CreatedAt: time.Now(), UpdatedAt: time.Now()},
	}
	for _, ti := range instances {
		tiCopy := ti
		if err := store.CreateTaskInstance(&tiCopy); err != nil {
			t.Fatalf("create task instance %s: %v", ti.ID, err)
		}
	}

	latest, err := store.GetLatestTaskAttempts("run_c1")
	if err != nil {
		t.Fatalf("GetLatestTaskAttempts: %v", err)
	}

	// Should have exactly 2 tasks (extract and load), not 3 rows
	if len(latest) != 2 {
		t.Fatalf("expected 2 latest tasks, got %d", len(latest))
	}

	// Find extract in results and verify it's attempt 2
	for _, task := range latest {
		if task.TaskID == "extract" {
			if task.Attempt != 2 {
				t.Errorf("expected extract to show attempt=2, got %d", task.Attempt)
			}
			if task.Output != "ok" {
				t.Errorf("expected extract output to be 'ok', got %q", task.Output)
			}
		}
	}
}

// TestAppendTaskOutput verifies that AppendTaskOutput accumulates lines
// into the output column without overwriting previous content.
func TestAppendTaskOutput(t *testing.T) {
	store, err := NewStore("file::memory:?cache=shared&mode=memory")
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}
	defer store.Close()

	if err := store.CreateDagRun(&DagRun{
		ID: "run_d1", DAGID: "dag_d", Status: RunRunning,
		ExecDate: time.Now(), TriggerType: "manual", CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	ti := &TaskInstance{
		ID: "run_d1_task1", RunID: "run_d1", TaskID: "task1",
		Status: TaskRunning, Attempt: 1,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := store.CreateTaskInstance(ti); err != nil {
		t.Fatalf("create task instance: %v", err)
	}

	lines := []string{"line one\n", "line two\n", "line three\n"}
	for _, line := range lines {
		if err := store.AppendTaskOutput("run_d1_task1", line); err != nil {
			t.Fatalf("AppendTaskOutput: %v", err)
		}
	}

	result, err := store.GetTaskInstance("run_d1_task1")
	if err != nil {
		t.Fatalf("GetTaskInstance: %v", err)
	}

	want := "line one\nline two\nline three\n"
	if result.Output != want {
		t.Errorf("expected output %q, got %q", want, result.Output)
	}
}

func TestGetDagRunsPaginationAndFiltering(t *testing.T) {
	// Use a new shared in-memory db specifically for this test to avoid state bleeding if running in parallel
	store, err := NewStore("file::memory:?cache=shared&mode=memory")
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}
	defer store.Close()

	// Insert 5 runs for dag_A
	for i := 0; i < 5; i++ {
		run := DagRun{
			ID:          fmt.Sprintf("run_A_%d", i),
			DAGID:       "dag_A",
			Status:      RunSuccess,
			ExecDate:    time.Now(),
			TriggerType: "scheduled",
			CreatedAt:   time.Now().Add(time.Duration(-i) * time.Minute), // earlier ones created later to test ordering
		}
		if err := store.CreateDagRun(&run); err != nil {
			t.Fatalf("failed to create dag run: %v", err)
		}
	}

	// Insert 3 runs for dag_B
	for i := 0; i < 3; i++ {
		run := DagRun{
			ID:          fmt.Sprintf("run_B_%d", i),
			DAGID:       "dag_B",
			Status:      RunSuccess,
			ExecDate:    time.Now(),
			TriggerType: "scheduled",
			CreatedAt:   time.Now().Add(time.Duration(-i) * time.Minute),
		}
		if err := store.CreateDagRun(&run); err != nil {
			t.Fatalf("failed to create dag run: %v", err)
		}
	}

	// Test GetDagRunsCount
	countA, err := store.GetDagRunsCount("dag_A", "", "")
	if err != nil || countA != 5 {
		t.Errorf("expected 5 runs for dag_A, got %d", countA)
	}

	countAll, err := store.GetDagRunsCount("", "", "")
	if err != nil || countAll != 8 {
		t.Errorf("expected 8 total runs, got %d", countAll)
	}

	// Test Pagination for All Runs
	// limit 3, offset 0 -> 3 runs
	runs, err := store.GetDagRuns(3, 0, "", "", "")
	if err != nil || len(runs) != 3 {
		t.Errorf("expected 3 runs with limit 3, got %d", len(runs))
	}

	// Test Pagination with Filtering
	// dag_A, limit 2, offset 2 -> should get 2 runs
	runsA, err := store.GetDagRuns(2, 2, "dag_A", "", "")
	if err != nil || len(runsA) != 2 {
		t.Errorf("expected 2 runs for dag_A with limit 2, got %d", len(runsA))
	}

	// dag_B, limit 5, offset 0 -> should get 3 runs
	runsB, err := store.GetDagRuns(5, 0, "dag_B", "", "")
	if err != nil || len(runsB) != 3 {
		t.Errorf("expected 3 runs for dag_B, got %d", len(runsB))
	}
}

func TestGetSystemStats(t *testing.T) {
	store, err := NewStore("file::memory:?cache=shared&mode=memory")
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}
	defer store.Close()

	now := time.Now()
	recent := now.Add(-1 * time.Hour) // within last 24 h
	old := now.Add(-48 * time.Hour)   // older than 24 h

	runs := []DagRun{
		{ID: "s1", DAGID: "d", Status: RunSuccess, ExecDate: now, TriggerType: "scheduled", CreatedAt: recent},
		{ID: "s2", DAGID: "d", Status: RunSuccess, ExecDate: now, TriggerType: "scheduled", CreatedAt: old},
		{ID: "r1", DAGID: "d", Status: RunRunning, ExecDate: now, TriggerType: "scheduled", CreatedAt: recent},
		{ID: "f1", DAGID: "d", Status: RunFailed, ExecDate: now, TriggerType: "scheduled", CreatedAt: recent},
		{ID: "f2", DAGID: "d", Status: RunFailed, ExecDate: now, TriggerType: "scheduled", CreatedAt: old},
	}
	for _, run := range runs {
		if err := store.CreateDagRun(&run); err != nil {
			t.Fatalf("failed to create run %s: %v", run.ID, err)
		}
	}

	stats, err := store.GetSystemStats()
	if err != nil {
		t.Fatalf("GetSystemStats() returned error: %v", err)
	}

	if stats.TotalRuns != 5 {
		t.Errorf("TotalRuns: got %d, want 5", stats.TotalRuns)
	}
	if stats.ActiveRuns != 1 {
		t.Errorf("ActiveRuns: got %d, want 1", stats.ActiveRuns)
	}
	// only f1 is failed AND within 24 h; f2 is old
	if stats.FailedRuns24h != 1 {
		t.Errorf("FailedRuns24h: got %d, want 1", stats.FailedRuns24h)
	}
}

// ---- Cloud instances table tests ----

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := NewStore("file::memory:?cache=shared&mode=memory")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestCloudInstance_SaveAndList(t *testing.T) {
	s := newTestStore(t)

	inst := CloudInstance{
		ID:           "docker-abc123",
		Provider:     "docker",
		ProviderID:   "container-xyz",
		WorkerID:     "",
		Pools:        `["default"]`,
		InstanceType: "",
		Region:       "",
		Status:       "provisioning",
		CostPerHour:  0,
		CreatedAt:    time.Now().UTC().Truncate(time.Second),
	}

	if err := s.SaveCloudInstance(inst); err != nil {
		t.Fatalf("SaveCloudInstance: %v", err)
	}

	active, err := s.ListActiveCloudInstances()
	if err != nil {
		t.Fatalf("ListActiveCloudInstances: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("expected 1 active instance, got %d", len(active))
	}
	got := active[0]
	if got.ID != inst.ID {
		t.Errorf("ID: got %q, want %q", got.ID, inst.ID)
	}
	if got.Status != "provisioning" {
		t.Errorf("Status: got %q, want provisioning", got.Status)
	}
}

func TestCloudInstance_UpdateStatus(t *testing.T) {
	s := newTestStore(t)

	inst := CloudInstance{
		ID:         "aws-abc",
		Provider:   "aws",
		ProviderID: "i-12345",
		Pools:      `["default"]`,
		Status:     "provisioning",
		CreatedAt:  time.Now().UTC(),
	}
	if err := s.SaveCloudInstance(inst); err != nil {
		t.Fatalf("SaveCloudInstance: %v", err)
	}

	if err := s.UpdateCloudInstanceStatus("aws-abc", "running"); err != nil {
		t.Fatalf("UpdateCloudInstanceStatus: %v", err)
	}

	active, _ := s.ListActiveCloudInstances()
	if len(active) != 1 || active[0].Status != "running" {
		t.Errorf("expected status running after update, got %v", active)
	}
}

func TestCloudInstance_UpdateWorkerID(t *testing.T) {
	s := newTestStore(t)

	inst := CloudInstance{
		ID:         "docker-wid",
		Provider:   "docker",
		ProviderID: "ctr-1",
		Pools:      `["default"]`,
		Status:     "running",
		CreatedAt:  time.Now().UTC(),
	}
	if err := s.SaveCloudInstance(inst); err != nil {
		t.Fatalf("SaveCloudInstance: %v", err)
	}

	if err := s.UpdateCloudInstanceWorkerID("docker-wid", "worker-remote-1"); err != nil {
		t.Fatalf("UpdateCloudInstanceWorkerID: %v", err)
	}

	active, _ := s.ListActiveCloudInstances()
	if len(active) != 1 || active[0].WorkerID != "worker-remote-1" {
		t.Errorf("expected worker ID worker-remote-1, got %v", active)
	}
}

func TestCloudInstance_TerminateExcludesFromList(t *testing.T) {
	s := newTestStore(t)

	for i := 0; i < 3; i++ {
		err := s.SaveCloudInstance(CloudInstance{
			ID:         fmt.Sprintf("inst-%d", i),
			Provider:   "docker",
			ProviderID: fmt.Sprintf("ctr-%d", i),
			Pools:      `["default"]`,
			Status:     "running",
			CreatedAt:  time.Now().UTC(),
		})
		if err != nil {
			t.Fatalf("SaveCloudInstance %d: %v", i, err)
		}
	}

	// Terminate the first instance.
	if err := s.TerminateCloudInstance("inst-0", time.Now().UTC()); err != nil {
		t.Fatalf("TerminateCloudInstance: %v", err)
	}

	active, err := s.ListActiveCloudInstances()
	if err != nil {
		t.Fatalf("ListActiveCloudInstances: %v", err)
	}
	if len(active) != 2 {
		t.Errorf("expected 2 active instances after termination, got %d", len(active))
	}
	for _, a := range active {
		if a.ID == "inst-0" {
			t.Error("terminated instance should not appear in active list")
		}
	}
}

func TestCloudInstance_GetCostSummary(t *testing.T) {
	s := newTestStore(t)

	now := time.Now().UTC()
	oneHourAgo := now.Add(-1 * time.Hour)

	// One running instance at $0.10/hr started 1 hour ago.
	if err := s.SaveCloudInstance(CloudInstance{
		ID:          "cost-1",
		Provider:    "aws",
		ProviderID:  "i-cost-1",
		Pools:       `["default"]`,
		Status:      "running",
		CostPerHour: 0.10,
		CreatedAt:   oneHourAgo,
	}); err != nil {
		t.Fatalf("SaveCloudInstance: %v", err)
	}

	// One terminated instance at $0.50/hr, ran for 30 minutes.
	halfHourAgo := now.Add(-30 * time.Minute)
	if err := s.SaveCloudInstance(CloudInstance{
		ID:           "cost-2",
		Provider:     "aws",
		ProviderID:   "i-cost-2",
		Pools:        `["gpu_workers"]`,
		Status:       "terminated",
		CostPerHour:  0.50,
		CreatedAt:    now.Add(-1 * time.Hour),
		TerminatedAt: &halfHourAgo,
	}); err != nil {
		t.Fatalf("SaveCloudInstance: %v", err)
	}

	summary, err := s.GetCloudCostSummary()
	if err != nil {
		t.Fatalf("GetCloudCostSummary: %v", err)
	}

	if summary.TotalInstances != 2 {
		t.Errorf("expected TotalInstances=2, got %d", summary.TotalInstances)
	}
	if summary.ActiveInstances != 1 {
		t.Errorf("expected ActiveInstances=1, got %d", summary.ActiveInstances)
	}
	// Estimated cost should be positive.
	if summary.EstimatedCostUSD <= 0 {
		t.Errorf("expected positive estimated cost, got %f", summary.EstimatedCostUSD)
	}
}

func TestResetStaleTasks(t *testing.T) {
	s := newTestStore(t)
	now := time.Now()

	// Create a dag run in 'running' state (simulating a run that was interrupted).
	run := DagRun{
		ID: "stale-run-1", DAGID: "my_dag", Status: RunRunning,
		ExecDate: now, TriggerType: "scheduled", CreatedAt: now,
	}
	if err := s.CreateDagRun(&run); err != nil {
		t.Fatalf("CreateDagRun: %v", err)
	}

	// One task running, one queued — both should become failed.
	for _, ti := range []TaskInstance{
		{ID: "t-running", RunID: "stale-run-1", TaskID: "t1", Status: TaskRunning, Attempt: 1, CreatedAt: now, UpdatedAt: now},
		{ID: "t-queued", RunID: "stale-run-1", TaskID: "t2", Status: TaskQueued, Attempt: 1, CreatedAt: now, UpdatedAt: now},
	} {
		if err := s.CreateTaskInstance(&ti); err != nil {
			t.Fatalf("CreateTaskInstance %s: %v", ti.ID, err)
		}
	}

	// Create a task that is already succeeded — it must NOT be touched.
	run2 := DagRun{
		ID: "done-run-1", DAGID: "my_dag", Status: RunSuccess,
		ExecDate: now, TriggerType: "scheduled", CreatedAt: now,
	}
	if err := s.CreateDagRun(&run2); err != nil {
		t.Fatalf("CreateDagRun: %v", err)
	}
	success := TaskInstance{ID: "t-success", RunID: "done-run-1", TaskID: "t3", Status: TaskSuccess, Attempt: 1, CreatedAt: now, UpdatedAt: now}
	if err := s.CreateTaskInstance(&success); err != nil {
		t.Fatalf("CreateTaskInstance t-success: %v", err)
	}

	affected, err := s.ResetStaleTasks()
	if err != nil {
		t.Fatalf("ResetStaleTasks: %v", err)
	}
	if affected != 2 {
		t.Errorf("expected 2 stale tasks reset, got %d", affected)
	}

	// Running and queued tasks should now be failed.
	for _, id := range []string{"t-running", "t-queued"} {
		ti, err := s.GetTaskInstance(id)
		if err != nil {
			t.Fatalf("GetTaskInstance %s: %v", id, err)
		}
		if ti.Status != TaskFailed {
			t.Errorf("task %s: expected status=%s, got %s", id, TaskFailed, ti.Status)
		}
	}

	// Successful task should be untouched.
	ti, err := s.GetTaskInstance("t-success")
	if err != nil {
		t.Fatalf("GetTaskInstance t-success: %v", err)
	}
	if ti.Status != TaskSuccess {
		t.Errorf("t-success: expected status=%s, got %s", TaskSuccess, ti.Status)
	}

	// The interrupted dag run should now be marked failed.
	staleRun, err := s.GetDagRun("stale-run-1")
	if err != nil {
		t.Fatalf("GetDagRun stale-run-1: %v", err)
	}
	if staleRun.Status != RunFailed {
		t.Errorf("stale-run-1: expected status=%s, got %s", RunFailed, staleRun.Status)
	}

	// The completed run should be untouched.
	doneRun, err := s.GetDagRun("done-run-1")
	if err != nil {
		t.Fatalf("GetDagRun done-run-1: %v", err)
	}
	if doneRun.Status != RunSuccess {
		t.Errorf("done-run-1: expected status=%s, got %s", RunSuccess, doneRun.Status)
	}
}
