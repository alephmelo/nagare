package worker

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/alephmelo/nagare/internal/logbroker"
	"github.com/alephmelo/nagare/internal/models"
	"github.com/alephmelo/nagare/internal/tasklifecycle"
)

func TestShutdownCancellationRetriesTransientPersistenceFailure(t *testing.T) {
	store, pool, work := newShutdownCancellationPool(t)
	locked := errors.New("database is locked")
	lifecycle := &faultingAttemptLifecycle{
		delegate:     tasklifecycle.New(store),
		cancelErrors: []error{locked, locked},
	}
	pool.lifecycle = lifecycle

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pool.executeTask(ctx, work, 0, "default")

	if lifecycle.cancelCalls != 3 {
		t.Fatalf("CancelAttempt calls = %d, want 3", lifecycle.cancelCalls)
	}
	attempt, err := store.GetTaskInstance(work.ti.ID)
	if err != nil {
		t.Fatalf("GetTaskInstance: %v", err)
	}
	if attempt.Status != models.TaskCancelled {
		t.Fatalf("attempt status = %q, want cancelled", attempt.Status)
	}
	if _, owned := pool.attempts[work.ti.ID]; owned {
		t.Fatal("durably cancelled attempt still owned locally")
	}
	if got := len(work.control.admission); got != 0 {
		t.Fatalf("admission tokens = %d, want 0", got)
	}
	if err := pool.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestShutdownCancellationPermanentFailureRetainsOwnership(t *testing.T) {
	store, pool, work := newShutdownCancellationPool(t)
	permanent := errors.New("persistence unavailable")
	lifecycle := &faultingAttemptLifecycle{
		delegate:     tasklifecycle.New(store),
		cancelErrors: []error{permanent},
	}
	pool.lifecycle = lifecycle

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pool.executeTask(ctx, work, 0, "default")

	if lifecycle.cancelCalls != 1 {
		t.Fatalf("CancelAttempt calls = %d, want 1", lifecycle.cancelCalls)
	}
	attempt, err := store.GetTaskInstance(work.ti.ID)
	if err != nil {
		t.Fatalf("GetTaskInstance: %v", err)
	}
	if attempt.Status != models.TaskRunning {
		t.Fatalf("attempt status = %q, want running", attempt.Status)
	}
	if pool.attempts[work.ti.ID] != work.control {
		t.Fatal("failed cancellation released local attempt ownership")
	}
	if got := len(work.control.admission); got != 1 {
		t.Fatalf("admission tokens = %d, want 1", got)
	}
	if err := pool.Stop(); !errors.Is(err, permanent) {
		t.Fatalf("Stop error = %v, want persistence failure", err)
	}
}

func TestShutdownCancellationExhaustsTransientRetriesBeforeRetainingOwnership(t *testing.T) {
	store, pool, work := newShutdownCancellationPool(t)
	locked := errors.New("database table is locked")
	lifecycle := &faultingAttemptLifecycle{
		delegate: tasklifecycle.New(store),
		cancelErrors: []error{
			locked, locked, locked, locked, locked, locked,
		},
	}
	pool.lifecycle = lifecycle

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pool.executeTask(ctx, work, 0, "default")

	if lifecycle.cancelCalls != lifecyclePersistenceAttempts {
		t.Fatalf("CancelAttempt calls = %d, want %d", lifecycle.cancelCalls, lifecyclePersistenceAttempts)
	}
	attempt, err := store.GetTaskInstance(work.ti.ID)
	if err != nil {
		t.Fatalf("GetTaskInstance: %v", err)
	}
	if attempt.Status != models.TaskRunning {
		t.Fatalf("attempt status = %q, want running", attempt.Status)
	}
	if pool.attempts[work.ti.ID] != work.control || len(work.control.admission) != 1 {
		t.Fatal("exhausted transient cancellation released local ownership")
	}
	if err := pool.Stop(); !errors.Is(err, locked) {
		t.Fatalf("Stop error = %v, want locked persistence failure", err)
	}
}

func newShutdownCancellationPool(t *testing.T) (*models.Store, *Pool, queuedAttempt) {
	t.Helper()
	store, err := models.NewStore(filepath.Join(t.TempDir(), "shutdown.db"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	run := &models.DagRun{ID: "run-1", DAGID: "dag-1", Status: models.RunRunning}
	if err := store.CreateDagRun(run); err != nil {
		t.Fatalf("CreateDagRun: %v", err)
	}
	ti := models.TaskInstance{
		ID: "attempt-1", RunID: run.ID, TaskID: "task-1",
		Status: models.TaskRunning, Attempt: 1,
	}
	if err := store.CreateTaskInstance(&ti); err != nil {
		t.Fatalf("CreateTaskInstance: %v", err)
	}
	dag := &models.DAGDef{
		ID: run.DAGID,
		Tasks: []models.TaskDef{{
			ID: ti.TaskID, Command: "must-not-execute",
		}},
	}
	admission := make(chan struct{}, 1)
	admission <- struct{}{}
	control := &localAttempt{ti: ti, admission: admission}
	pool := &Pool{
		store: store,
		getDAG: func(id string) (*models.DAGDef, bool) {
			return dag, id == dag.ID
		},
		broker:   logbroker.NewBroker(),
		attempts: map[string]*localAttempt{ti.ID: control},
	}
	return store, pool, queuedAttempt{ti: ti, control: control}
}

type faultingAttemptLifecycle struct {
	delegate     attemptLifecycle
	cancelErrors []error
	cancelCalls  int
}

func (l *faultingAttemptLifecycle) Claim(attemptID string, claimedAt time.Time) (tasklifecycle.Disposition, error) {
	return l.delegate.Claim(attemptID, claimedAt)
}

func (l *faultingAttemptLifecycle) Complete(input tasklifecycle.Completion) (tasklifecycle.Disposition, error) {
	return l.delegate.Complete(input)
}

func (l *faultingAttemptLifecycle) CancelAttempt(attemptID string, cancelledAt time.Time) (tasklifecycle.Disposition, error) {
	l.cancelCalls++
	if len(l.cancelErrors) > 0 {
		err := l.cancelErrors[0]
		l.cancelErrors = l.cancelErrors[1:]
		return 0, err
	}
	return l.delegate.CancelAttempt(attemptID, cancelledAt)
}
