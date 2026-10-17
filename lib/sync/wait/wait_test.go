package wait

import (
	"testing"
	"time"
)

func TestWait(t *testing.T) {
	var w Wait
	w.Add(2)
	if w.WaitWithTimeout(5*time.Millisecond) != true {
		t.Fatal("expected a timeout while the counter is positive")
	}
	w.Done()
	if w.WaitWithTimeout(5*time.Millisecond) != true {
		t.Fatal("expected a timeout with one task left")
	}
	w.Done()
	if w.WaitWithTimeout(100*time.Millisecond) != false {
		t.Fatal("expected WaitWithTimeout to return immediately at zero")
	}
	// the Wait must be reusable after it reaches zero
	w.Add(1)
	w.Done()
	w.Wait()
}
