package scheduler

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/models"
)

func TestSchedulerLoadDAGs(t *testing.T) {
	store, _ := models.NewStore("file::memory:?cache=shared")
	defer store.Close()

	sched := NewScheduler(store)

	// Create a temporary directory for test DAGs
	tmpDir := t.TempDir()
	yamlContent := []byte(`
id: my_test_dag
description: "Test DAG"
schedule: "0 0 * * *"
tasks:
  - id: t1
    type: command
    command: "echo test"
`)
	if err := os.WriteFile(filepath.Join(tmpDir, "test.yaml"), yamlContent, 0644); err != nil {
		t.Fatalf("failed to write test yaml: %v", err)
	}

	if err := sched.LoadDAGs(tmpDir); err != nil {
		t.Fatalf("LoadDAGs failed: %v", err)
	}

	if len(sched.dags) != 1 {
		t.Fatalf("expected 1 DAG to be loaded, got %d", len(sched.dags))
	}

	if _, ok := sched.dags["my_test_dag"]; !ok {
		t.Errorf("expected DAG 'my_test_dag' to be loaded into map")
	}
}

func TestSchedulerTick(t *testing.T) {
	store, _ := models.NewStore("file::memory:?cache=shared")
	defer store.Close()

	sched := NewScheduler(store)

	// Inject a dummy DAG manually
	sched.dags["test_tick_dag"] = &models.DAGDef{
		ID:       "test_tick_dag",
		Schedule: "* * * * *", // Every minute
		Tasks: []models.TaskDef{
			{ID: "t1"},
			{ID: "t2", DependsOn: []string{"t1"}},
		},
	}
	// Artificially set last Exec to 1 minute ago so it triggers immediately
	sched.lastExec["test_tick_dag"] = time.Now().Add(-1 * time.Minute)

	if err := sched.Tick(); err != nil {
		t.Fatalf("Tick failed: %v", err)
	}

	// Verify a DagRun was created
	rows, _ := store.GetQueuedTasks() // This gives queued task instances
	// task t1 should be queued (no dependencies), t2 should be pending
	if len(rows) != 1 {
		t.Fatalf("expected 1 queued task (t1), got %d", len(rows))
	}
	if rows[0].TaskID != "t1" {
		t.Errorf("expected queued task to be t1, got %s", rows[0].TaskID)
	}
}

func TestSchedulerConcurrentAccess(t *testing.T) {
	store, _ := models.NewStore("file::memory:?cache=shared&mode=memory")
	defer store.Close()

	sched := NewScheduler(store)
	tmpDir := t.TempDir()

	// Write a valid initial DAG
	initialYaml := []byte(`
id: concurrent_dag
description: "Test DAG"
schedule: "* * * * *"
tasks:
  - id: t1
    type: command
    command: "echo test"
`)
	os.WriteFile(filepath.Join(tmpDir, "test.yaml"), initialYaml, 0644)
	sched.LoadDAGs(tmpDir)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Spawn 5 reader goroutines
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					dags := sched.GetDAGs()
					_ = len(dags)
					errs := sched.GetDAGErrors()
					_ = len(errs)
					_ = sched.Tick()
					_ = sched.PromotePendingTasks()
					time.Sleep(2 * time.Millisecond)
				}
			}
		}()
	}

	// Writer goroutine simulating hot-reloads
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			// rewrite file
			newYaml := []byte(fmt.Sprintf(`
id: concurrent_dag_%d
description: "Test DAG"
schedule: "* * * * *"
tasks:
  - id: t1
    type: command
    command: "echo test"
`, i))
			os.WriteFile(filepath.Join(tmpDir, "test.yaml"), newYaml, 0644)
			sched.LoadDAGs(tmpDir)
			time.Sleep(10 * time.Millisecond)
		}
		close(stop)
	}()

	wg.Wait()
}

func TestSchedulerTriggerDAG(t *testing.T) {
	store, _ := models.NewStore("file::memory:?cache=shared&mode=memory")
	defer store.Close()

	sched := NewScheduler(store)

	// Inject a dummy DAG manually
	sched.dags["manual_dag"] = &models.DAGDef{
		ID:       "manual_dag",
		Schedule: "* * * * *",
		Tasks: []models.TaskDef{
			{ID: "t1"},
		},
	}

	run, err := sched.TriggerDAG("manual_dag")
	if err != nil {
		t.Fatalf("TriggerDAG failed: %v", err)
	}
	if run == nil {
		t.Fatalf("Expected run to be returned")
	}

	// Verify a DagRun was created
	rows, _ := store.GetQueuedTasks()
	if len(rows) != 1 || rows[0].TaskID != "t1" {
		t.Fatalf("expected 1 queued task (t1)")
	}
}

func TestSchedulerRetryTask(t *testing.T) {
	store, _ := models.NewStore("file::memory:?cache=shared&mode=memory")
	defer store.Close()

	sched := NewScheduler(store)

	now := time.Now()
	runID := "run_retry_test"

	// Seed a failed run
	run := &models.DagRun{
		ID:          runID,
		DAGID:       "some_dag",
		Status:      models.RunFailed,
		ExecDate:    now,
		TriggerType: "scheduled",
		CreatedAt:   now,
	}
	store.CreateDagRun(run)

	// Seed the first attempt (failed)
	ti := &models.TaskInstance{
		ID:        runID + "_t2_1",
		RunID:     runID,
		TaskID:    "t2",
		Status:    models.TaskFailed,
		Output:    "exit code 1: some error",
		Attempt:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}
	store.CreateTaskInstance(ti)

	err := sched.RetryTask(runID, "t2")
	if err != nil {
		t.Fatalf("RetryTask failed: %v", err)
	}

	// Latest attempt should be queued (new attempt row was inserted)
	status, err := store.GetTaskStatus(runID, "t2")
	if err != nil {
		t.Fatalf("GetTaskStatus failed: %v", err)
	}
	if status != models.TaskQueued {
		t.Errorf("Expected latest attempt to be queued, got %v", status)
	}

	// Original failed attempt must still exist with its output intact
	attempts, err := store.GetTaskAttempts(runID, "t2")
	if err != nil {
		t.Fatalf("GetTaskAttempts failed: %v", err)
	}
	if len(attempts) != 2 {
		t.Fatalf("Expected 2 attempts after retry, got %d", len(attempts))
	}
	if attempts[0].Output != "exit code 1: some error" {
		t.Errorf("Original attempt output was overwritten: got %q", attempts[0].Output)
	}
	if attempts[1].Attempt != 2 {
		t.Errorf("New attempt should have attempt=2, got %d", attempts[1].Attempt)
	}

	// Verify Run is back online
	dbRun, _ := store.GetDagRun(runID)
	if dbRun.Status != models.RunRunning {
		t.Fatalf("Expected run to be reverted to running, got %v", dbRun.Status)
	}
}

func TestSchedulerAutoRetry(t *testing.T) {
	store, _ := models.NewStore("file::memory:?cache=shared&mode=memory")
	defer store.Close()

	sched := NewScheduler(store)

	now := time.Now()
	runID := "run_auto_retry"
	dagID := "retry_dag"

	// Add DAG to memory
	sched.dags[dagID] = &models.DAGDef{
		ID:       dagID,
		Schedule: "* * * * *",
		Tasks: []models.TaskDef{
			{
				ID:                "t1",
				Retries:           2,
				RetryDelaySeconds: 1, // 1 second delay for faster tests
			},
		},
	}

	// Seed a running DAG, but the task is FAILED
	run := &models.DagRun{
		ID:          runID,
		DAGID:       dagID,
		Status:      models.RunRunning,
		ExecDate:    now,
		TriggerType: "scheduled",
		CreatedAt:   now,
	}
	store.CreateDagRun(run)

	// Attempt 1: Failed just now
	ti := &models.TaskInstance{
		ID:        runID + "_t1_1",
		RunID:     runID,
		TaskID:    "t1",
		Status:    models.TaskFailed,
		Attempt:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}
	store.CreateTaskInstance(ti)

	// Evaluate immediately: should wait because delay is 1 seconds
	sched.evaluateRunCompletions()

	status, _ := store.GetTaskStatus(runID, "t1")
	if status == models.TaskQueued {
		t.Fatalf("Task should not be queued immediately, delay hasn't passed")
	}

	// Run should still be running
	dbRun, _ := store.GetDagRun(runID)
	if dbRun.Status != models.RunRunning {
		t.Fatalf("Expected run to still be running during delay, got %v", dbRun.Status)
	}

	// Fast forward time by sleeping 1 second
	time.Sleep(1 * time.Second)

	// Evaluate again
	sched.evaluateRunCompletions()

	// Should have queued a new attempt
	status, _ = store.GetTaskStatus(runID, "t1")
	if status != models.TaskQueued {
		t.Fatalf("Expected task to be queued for retry after delay, got %v", status)
	}

	// Simulate Attempt 2 failing
	// At this point evaluateRunCompletions has queued the new attempt via RetryTask.
	// We get the new task instance and mark it as failed.
	attempts, _ := store.GetTaskAttempts(runID, "t1")
	if len(attempts) != 2 {
		t.Fatalf("Expected 2 attempts, got %d", len(attempts))
	}
	// It's currently queued. We mark it as failed and update time.
	store.UpdateTaskInstanceStatus(attempts[1].ID, models.TaskFailed)

	// Evaluate immediately should still not queue 3rd attempt
	sched.evaluateRunCompletions()
	attempts, _ = store.GetTaskAttempts(runID, "t1")
	if len(attempts) != 2 {
		t.Fatalf("Expected still 2 attempts, got %d", len(attempts))
	}

	// Sleep 1 second for Attempt 3 delay
	time.Sleep(1 * time.Second)
	sched.evaluateRunCompletions()

	// Should have queued a 3rd attempt
	attempts, _ = store.GetTaskAttempts(runID, "t1")
	if len(attempts) != 3 {
		t.Fatalf("Expected 3 attempts after 2nd delay, got %d", len(attempts))
	}

	// Simulate Attempt 3 failing
	store.UpdateTaskInstanceStatus(attempts[2].ID, models.TaskFailed)
	time.Sleep(1 * time.Second) // wait for it just in case, though retries = 2

	sched.evaluateRunCompletions()

	// Retries exhausted, should fail the run
	dbRun, _ = store.GetDagRun(runID)
	if dbRun.Status != models.RunFailed {
		t.Fatalf("Expected run to fail after retries exhausted, got %v", dbRun.Status)
	}
}
