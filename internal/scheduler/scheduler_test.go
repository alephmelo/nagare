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
