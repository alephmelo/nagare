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
	mu          sync.Mutex
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
//
// The send to each subscriber channel is non-blocking: if the channel
// is full the line is dropped for that subscriber rather than blocking
// the caller (and holding the lock). A slow SSE consumer will miss
// lines but will never stall backend goroutines.
func (b *Broker) Publish(taskID, line string) {
	tl := b.getOrCreate(taskID)

	tl.mu.Lock()
	defer tl.mu.Unlock()

	if tl.closed {
		return
	}
	tl.lines = append(tl.lines, line)
	for _, ch := range tl.subscribers {
		select {
		case ch <- line:
		default:
			// Subscriber channel full — drop the line for this consumer.
			// The full output is persisted to the DB at task completion.
		}
	}
}

// Subscribe returns a channel that will receive all log lines for taskID —
// including lines already published (history replay) — followed by new ones.
// The returned unsubscribe function must be called to release resources; it
// closes the channel. The channel is also closed by Close(taskID).
func (b *Broker) Subscribe(taskID string) (<-chan string, func()) {
	tl := b.getOrCreate(taskID)

	tl.mu.Lock()

	// Buffered enough to hold history + a reasonable burst of live lines.
	ch := make(chan string, len(tl.lines)+128)

	// Replay history into the buffered channel immediately.
	for _, line := range tl.lines {
		ch <- line
	}

	// If already closed (or cleaned up), seal the channel now and return a no-op unsub.
	if tl.closed || tl.cleaned {
		tl.mu.Unlock()
		close(ch)
		return ch, func() {}
	}

	tl.subscribers = append(tl.subscribers, ch)
	tl.mu.Unlock()

	unsub := func() {
		tl.mu.Lock()
		defer tl.mu.Unlock()
		for i, sub := range tl.subscribers {
			if sub == ch {
				tl.subscribers = append(tl.subscribers[:i], tl.subscribers[i+1:]...)
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
	tl := b.getOrCreate(taskID)

	tl.mu.Lock()
	defer tl.mu.Unlock()

	if tl.closed {
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
	tl := b.getOrCreate(taskID)

	tl.mu.Lock()
	defer tl.mu.Unlock()

	tl.lines = nil
	tl.cleaned = true
}

// getOrCreate returns the taskLog for taskID, creating it if necessary.
func (b *Broker) getOrCreate(taskID string) *taskLog {
	b.mu.Lock()
	tl, ok := b.tasks[taskID]
	if !ok {
		tl = &taskLog{}
		b.tasks[taskID] = tl
	}
	b.mu.Unlock()
	return tl
}
