package models

import (
	"path/filepath"
	"testing"
	"time"
)

func TestRetryCurrentTaskAttemptPendingCreatesAndReplaysPendingSuccessor(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "pending-retry.db"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	now := time.Date(2026, time.July, 26, 20, 0, 0, 0, time.UTC)
	if err := store.CreateTaskInstance(&TaskInstance{
		ID: "failed", RunID: "run-1", TaskID: "task-1",
		Status: TaskFailed, Attempt: 1, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("CreateTaskInstance: %v", err)
	}

	first, err := store.RetryCurrentTaskAttemptPending("run-1", "task-1", now.Add(time.Minute))
	if err != nil {
		t.Fatalf("first RetryCurrentTaskAttemptPending: %v", err)
	}
	if !first.Applied || first.Replay {
		t.Fatalf("first result = %#v, want Applied", first)
	}
	if first.Attempt.Status != TaskPending || first.Attempt.Attempt != 2 {
		t.Fatalf("first successor = %#v, want pending attempt 2", first.Attempt)
	}

	replay, err := store.RetryCurrentTaskAttemptPending("run-1", "task-1", now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("replayed RetryCurrentTaskAttemptPending: %v", err)
	}
	if replay.Applied || !replay.Replay {
		t.Fatalf("replayed result = %#v, want Replay", replay)
	}
	if replay.Attempt.ID != first.Attempt.ID || replay.Attempt.Status != TaskPending {
		t.Fatalf("replayed successor = %#v, want original pending %q", replay.Attempt, first.Attempt.ID)
	}
}
