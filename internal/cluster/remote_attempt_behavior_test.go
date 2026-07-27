package cluster_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/cluster"
	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/tasklifecycle"
)

func remoteAttemptFixture(t *testing.T, attempt int, retries int) (*models.Store, *models.DAGDef, *cluster.Coordinator, http.Handler) {
	t.Helper()

	store := newTestStore(t)
	dag := &models.DAGDef{
		ID: "remote_attempt_dag",
		Tasks: []models.TaskDef{{
			ID:      "task",
			Command: "true",
			Pool:    "default",
			Retries: retries,
		}},
	}
	coord := cluster.NewCoordinator(store, func(id string) (*models.DAGDef, bool) {
		return dag, id == dag.ID
	}, 30*time.Second, "")
	coord.Register(cluster.WorkerRegistration{
		WorkerID: "owner",
		Pools:    []string{"default"},
		MaxTasks: 2,
	})

	if err := store.CreateDagRun(&models.DagRun{
		ID:     "run",
		DAGID:  dag.ID,
		Status: models.RunRunning,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := store.CreateTaskInstance(&models.TaskInstance{
		ID:      "attempt",
		RunID:   "run",
		TaskID:  "task",
		Status:  models.TaskQueued,
		Attempt: attempt,
	}); err != nil {
		t.Fatalf("create attempt: %v", err)
	}

	handler := coord.Handler()
	claim := postJSON(t, handler, "/api/workers/poll", cluster.PollRequest{
		WorkerID: "owner",
		Pools:    []string{"default"},
	})
	if claim.Code != http.StatusOK {
		t.Fatalf("poll assignment: got %d: %s", claim.Code, claim.Body.String())
	}
	var assignment cluster.TaskAssignmentDTO
	if err := json.NewDecoder(claim.Body).Decode(&assignment); err != nil {
		t.Fatalf("decode assignment: %v", err)
	}
	if assignment.TaskInstanceID != "attempt" {
		t.Fatalf("claimed attempt %q, want attempt", assignment.TaskInstanceID)
	}

	return store, dag, coord, handler
}

func postRemoteResult(t *testing.T, handler http.Handler, workerID, status string, timedOut bool) int {
	t.Helper()
	response := postJSON(t, handler, "/api/workers/result", cluster.TaskResult{
		TaskInstanceID: "attempt",
		WorkerID:       workerID,
		Status:         status,
		Output:         "worker output",
		TimedOut:       timedOut,
	})
	return response.Code
}

func requireAttemptStatus(t *testing.T, store *models.Store, want models.TaskStatus) {
	t.Helper()
	attempt, err := store.GetTaskInstance("attempt")
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if attempt.Status != want {
		t.Fatalf("attempt status = %q, want %q", attempt.Status, want)
	}
}

func TestCoordinator_RemoteResultPreservesExplicitStatus(t *testing.T) {
	tests := []struct {
		name    string
		attempt int
		retries int
		status  string
		want    models.TaskStatus
	}{
		{
			name:    "cancelled is not treated as ordinary failure",
			attempt: 1,
			retries: 3,
			status:  "cancelled",
			want:    models.TaskCancelled,
		},
		{
			name:    "explicit retry remains retryable even when policy is exhausted",
			attempt: 2,
			retries: 0,
			status:  "up_for_retry",
			want:    models.TaskUpForRetry,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, _, _, handler := remoteAttemptFixture(t, tt.attempt, tt.retries)
			if code := postRemoteResult(t, handler, "owner", tt.status, false); code != http.StatusOK {
				t.Fatalf("post result: got %d, want 200", code)
			}
			requireAttemptStatus(t, store, tt.want)
		})
	}
}

func TestCoordinator_RemoteFailureUsesRetryPolicyCapturedAtClaim(t *testing.T) {
	store, dag, _, handler := remoteAttemptFixture(t, 1, 1)

	// Configuration is mutable, but the assignment's policy was fixed when claimed.
	dag.Tasks[0].Retries = 0

	if code := postRemoteResult(t, handler, "owner", "failed", false); code != http.StatusOK {
		t.Fatalf("post result: got %d, want 200", code)
	}
	requireAttemptStatus(t, store, models.TaskUpForRetry)
}

func TestCoordinator_RemoteTimeoutIsTerminalDespiteRetries(t *testing.T) {
	store, _, _, handler := remoteAttemptFixture(t, 1, 3)
	if code := postRemoteResult(t, handler, "owner", "failed", true); code != http.StatusOK {
		t.Fatalf("post result: got %d, want 200", code)
	}
	requireAttemptStatus(t, store, models.TaskFailed)
}

func TestCoordinator_RemoteResultRequiresClaimOwner(t *testing.T) {
	store, _, coord, handler := remoteAttemptFixture(t, 1, 0)
	coord.Register(cluster.WorkerRegistration{
		WorkerID: "stranger",
		Pools:    []string{"default"},
		MaxTasks: 1,
	})

	if code := postRemoteResult(t, handler, "stranger", "success", false); code < 400 {
		t.Fatalf("mismatched owner result got %d, want rejection", code)
	}
	requireAttemptStatus(t, store, models.TaskRunning)
	if got := coord.WorkerActiveTasks("owner"); got != 1 {
		t.Fatalf("owner active tasks = %d, want 1 after rejected result", got)
	}
}

func TestCoordinator_LateRemoteResultDoesNotOverwriteCancellation(t *testing.T) {
	store, _, coord, handler := remoteAttemptFixture(t, 1, 2)
	lifecycle := tasklifecycle.New(store)
	if disposition, err := lifecycle.CancelCurrent("run", "task", time.Now()); err != nil {
		t.Fatalf("cancel current attempt: %v", err)
	} else if disposition != tasklifecycle.Applied {
		t.Fatalf("cancel disposition = %v, want Applied", disposition)
	}

	if code := postRemoteResult(t, handler, "owner", "failed", false); code != http.StatusOK {
		t.Fatalf("late owned result: got %d, want 200", code)
	}
	requireAttemptStatus(t, store, models.TaskCancelled)
	if got := coord.WorkerActiveTasks("owner"); got != 0 {
		t.Fatalf("owner active tasks = %d, want 0 after accepted late result", got)
	}
}

func TestCoordinator_ReplayedResultCannotReleaseNewerAssignment(t *testing.T) {
	store, dag, coord, handler := remoteAttemptFixture(t, 1, 0)
	if code := postRemoteResult(t, handler, "owner", "success", false); code != http.StatusOK {
		t.Fatalf("first result: got %d, want 200", code)
	}

	dag.Tasks = append(dag.Tasks, models.TaskDef{
		ID: "other", Command: "true", Pool: "default",
	})
	if err := store.CreateTaskInstance(&models.TaskInstance{
		ID: "newer-attempt", RunID: "run", TaskID: "other",
		Status: models.TaskQueued, Attempt: 1,
	}); err != nil {
		t.Fatalf("create newer assignment: %v", err)
	}
	claim := postJSON(t, handler, "/api/workers/poll", cluster.PollRequest{
		WorkerID: "owner", Pools: []string{"default"},
	})
	if claim.Code != http.StatusOK {
		t.Fatalf("poll newer assignment: got %d: %s", claim.Code, claim.Body.String())
	}
	if got := coord.WorkerActiveTasks("owner"); got != 1 {
		t.Fatalf("active tasks after newer claim = %d, want 1", got)
	}

	if code := postRemoteResult(t, handler, "owner", "success", false); code != http.StatusOK {
		t.Fatalf("replayed old result: got %d, want 200", code)
	}
	if got := coord.WorkerActiveTasks("owner"); got != 1 {
		t.Fatalf("replayed old result changed newer accounting: active tasks = %d, want 1", got)
	}
}
