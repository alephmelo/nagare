package scheduler

import (
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/models"
)

type recordingTaskCanceller struct {
	killed []string
}

func (c *recordingTaskCanceller) KillTask(attemptID string) error {
	c.killed = append(c.killed, attemptID)
	return nil
}

func TestKillDagRunCancelsLatestRetryAttemptWithoutRewritingHistory(t *testing.T) {
	store, err := models.NewStore("file:kill-latest-retry?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	now := time.Now()
	const (
		runID  = "run-kill-latest-retry"
		taskID = "download"
	)
	if err := store.CreateDagRun(&models.DagRun{
		ID:          runID,
		DAGID:       "retrying-dag",
		Status:      models.RunRunning,
		ExecDate:    now,
		TriggerType: "manual",
		CreatedAt:   now,
	}); err != nil {
		t.Fatalf("CreateDagRun: %v", err)
	}

	first := &models.TaskInstance{
		ID:        runID + "_" + taskID + "_1",
		RunID:     runID,
		TaskID:    taskID,
		Status:    models.TaskFailed,
		Output:    "first attempt failed",
		Attempt:   1,
		CreatedAt: now.Add(-time.Minute),
		UpdatedAt: now.Add(-time.Minute),
	}
	latest := &models.TaskInstance{
		ID:        runID + "_" + taskID + "_2",
		RunID:     runID,
		TaskID:    taskID,
		Status:    models.TaskUpForRetry,
		Output:    "waiting for retry delay",
		Attempt:   2,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := store.CreateTaskInstance(first); err != nil {
		t.Fatalf("CreateTaskInstance(first): %v", err)
	}
	if err := store.CreateTaskInstance(latest); err != nil {
		t.Fatalf("CreateTaskInstance(latest): %v", err)
	}

	canceller := &recordingTaskCanceller{}
	if err := NewScheduler(store).KillDagRun(runID, canceller); err != nil {
		t.Fatalf("KillDagRun: %v", err)
	}

	attempts, err := store.GetTaskAttempts(runID, taskID)
	if err != nil {
		t.Fatalf("GetTaskAttempts: %v", err)
	}
	if len(attempts) != 2 {
		t.Fatalf("attempt history length = %d, want 2", len(attempts))
	}
	if attempts[0].Status != models.TaskFailed || attempts[0].Output != "first attempt failed" {
		t.Fatalf("first attempt was rewritten: status=%s output=%q", attempts[0].Status, attempts[0].Output)
	}
	if attempts[1].ID != latest.ID {
		t.Fatalf("current attempt ID = %q, want %q", attempts[1].ID, latest.ID)
	}
	if attempts[1].Status != models.TaskCancelled {
		t.Fatalf("latest retry status = %s, want %s", attempts[1].Status, models.TaskCancelled)
	}
}
