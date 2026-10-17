package wait

import (
	"sync"
	"time"
)

// Wait is similar to sync.WaitGroup but can wait with a timeout without
// spawning a helper goroutine (the old implementation leaked one goroutine
// for every timed-out wait).
type Wait struct {
	mu    sync.Mutex
	count int
	done  chan struct{}
}

// Add adds delta, which may be negative, to the WaitGroup counter.
func (w *Wait) Add(delta int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if delta < 0 && w.count+delta < 0 {
		panic("wait: negative counter")
	}
	w.count += delta
	if w.count > 0 {
		if w.done == nil {
			w.done = make(chan struct{})
		}
		return
	}
	if w.done != nil {
		close(w.done)
		w.done = nil
	}
}

// Done decrements the WaitGroup counter by one.
func (w *Wait) Done() {
	w.Add(-1)
}

// Wait blocks until the WaitGroup counter is zero.
func (w *Wait) Wait() {
	w.mu.Lock()
	if w.count == 0 {
		w.mu.Unlock()
		return
	}
	done := w.done
	w.mu.Unlock()
	<-done
}

// WaitWithTimeout blocks until the WaitGroup counter is zero or timeout.
// It returns true when the timeout expired.
func (w *Wait) WaitWithTimeout(timeout time.Duration) bool {
	w.mu.Lock()
	if w.count == 0 {
		w.mu.Unlock()
		return false
	}
	done := w.done
	w.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return false
	case <-timer.C:
		return true
	}
}
