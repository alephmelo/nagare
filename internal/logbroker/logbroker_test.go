package logbroker

import (
	"sync"
	"testing"
	"time"
)

func TestPublishAndSubscribe(t *testing.T) {
	b := NewBroker()

	ch, unsub := b.Subscribe("task_1")
	defer unsub()

	b.Publish("task_1", "line one")
	b.Publish("task_1", "line two")

	assertLine(t, ch, "line one")
	assertLine(t, ch, "line two")
}

func TestMultipleSubscribers(t *testing.T) {
	b := NewBroker()

	ch1, unsub1 := b.Subscribe("task_1")
	ch2, unsub2 := b.Subscribe("task_1")
	defer unsub1()
	defer unsub2()

	b.Publish("task_1", "hello")

	assertLine(t, ch1, "hello")
	assertLine(t, ch2, "hello")
}

func TestSubscribeAfterPublish(t *testing.T) {
	b := NewBroker()

	// Publish before anyone subscribes
	b.Publish("task_1", "early line")

	// Late subscriber should receive buffered history
	ch, unsub := b.Subscribe("task_1")
	defer unsub()

	assertLine(t, ch, "early line")
}

func TestUnsubscribe(t *testing.T) {
	b := NewBroker()

	ch, unsub := b.Subscribe("task_1")
	unsub()

	// Channel should be closed after unsubscribe
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected channel to be closed after unsubscribe")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("channel was not closed after unsubscribe")
	}
}

func TestCloseTask(t *testing.T) {
	b := NewBroker()

	ch, unsub := b.Subscribe("task_1")
	defer unsub()

	b.Publish("task_1", "last line")
	b.Close("task_1")

	assertLine(t, ch, "last line")

	// Channel must be closed after Close()
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected channel to be closed after Close()")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("channel was not closed after Close()")
	}
}

func TestCloseTaskLateSubscriber(t *testing.T) {
	b := NewBroker()

	b.Publish("task_1", "line a")
	b.Publish("task_1", "line b")
	b.Close("task_1")

	// Subscriber after Close should still get history and an immediately closed channel
	ch, unsub := b.Subscribe("task_1")
	defer unsub()

	assertLine(t, ch, "line a")
	assertLine(t, ch, "line b")

	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected channel to be closed for late subscriber of closed task")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("channel was not closed for late subscriber")
	}
}

func TestConcurrentPublish(t *testing.T) {
	b := NewBroker()

	ch, unsub := b.Subscribe("task_1")
	defer unsub()

	const numLines = 100
	var wg sync.WaitGroup
	for i := 0; i < numLines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Publish("task_1", "line")
		}()
	}
	wg.Wait()
	b.Close("task_1")

	count := 0
	for range ch {
		count++
	}
	if count != numLines {
		t.Errorf("expected %d lines, got %d", numLines, count)
	}
}

func TestCleanup(t *testing.T) {
	b := NewBroker()

	b.Publish("task_1", "some output")
	b.Close("task_1")
	b.Cleanup("task_1")

	// After cleanup, subscribing returns a channel that is immediately closed
	// (no history, task is gone)
	ch, unsub := b.Subscribe("task_1")
	defer unsub()

	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected immediately closed channel after cleanup")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("channel was not closed after cleanup")
	}
}

// assertLine reads one value from ch with a timeout and asserts it equals want.
func assertLine(t *testing.T, ch <-chan string, want string) {
	t.Helper()
	select {
	case got, ok := <-ch:
		if !ok {
			t.Fatalf("channel closed early, expected line %q", want)
		}
		if got != want {
			t.Errorf("expected line %q, got %q", want, got)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for line %q", want)
	}
}
