package logbroker

import "sync"

// Broker is an in-memory pub/sub hub for task log lines.
// Each task instance gets its own channel of log lines that
// subscribers (e.g. SSE handlers) can read in real time.
type Broker struct {
	mu    sync.Mutex
	tasks map[string]*taskLog
}

type taskLog struct {
	lines       []string
	subscribers []chan string
	closed      bool
	cleaned     bool // set by Cleanup; distinguishes "never seen" from "fully done"
}

// NewBroker creates a ready-to-use Broker.
func NewBroker() *Broker {
	return &Broker{tasks: make(map[string]*taskLog)}
}

// Publish appends a line to the task's history and delivers it to all
// current subscribers. Safe to call concurrently.
func (b *Broker) Publish(taskID, line string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	tl := b.getOrCreate(taskID)
	if tl.closed {
		return
	}
	tl.lines = append(tl.lines, line)
	for _, ch := range tl.subscribers {
		ch <- line
	}
}

// Subscribe returns a channel that will receive all log lines for taskID —
// including lines already published (history replay) — followed by new ones.
// The returned unsubscribe function must be called to release resources; it
// closes the channel. The channel is also closed by Close(taskID).
func (b *Broker) Subscribe(taskID string) (<-chan string, func()) {
	b.mu.Lock()

	tl := b.getOrCreate(taskID)

	// Buffered enough to hold history + a reasonable burst of live lines.
	ch := make(chan string, len(tl.lines)+128)

	// Replay history into the buffered channel immediately.
	for _, line := range tl.lines {
		ch <- line
	}

	// If already closed (or cleaned up), seal the channel now and return a no-op unsub.
	if tl.closed || tl.cleaned {
		b.mu.Unlock()
		close(ch)
		return ch, func() {}
	}

	tl.subscribers = append(tl.subscribers, ch)
	b.mu.Unlock()

	unsub := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		tl2, ok := b.tasks[taskID]
		if !ok {
			return
		}
		for i, sub := range tl2.subscribers {
			if sub == ch {
				tl2.subscribers = append(tl2.subscribers[:i], tl2.subscribers[i+1:]...)
				close(ch)
				break
			}
		}
	}

	return ch, unsub
}

// Close marks the task as finished, closes all subscriber channels, and
// retains the history buffer for late subscribers until Cleanup is called.
func (b *Broker) Close(taskID string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	tl, ok := b.tasks[taskID]
	if !ok || tl.closed {
		return
	}
	tl.closed = true
	for _, ch := range tl.subscribers {
		close(ch)
	}
	tl.subscribers = nil
}

// Cleanup removes all state for taskID. Call this after the final output has
// been persisted to the database to free memory.
func (b *Broker) Cleanup(taskID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if tl, ok := b.tasks[taskID]; ok {
		tl.lines = nil
		tl.cleaned = true
	}
}

// getOrCreate returns the taskLog for taskID, creating it if necessary.
// Caller must hold b.mu.
func (b *Broker) getOrCreate(taskID string) *taskLog {
	tl, ok := b.tasks[taskID]
	if !ok {
		tl = &taskLog{}
		b.tasks[taskID] = tl
	}
	return tl
}
