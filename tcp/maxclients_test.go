package tcp

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type blockingHandler struct {
	active   int32
	accepted int32
	maxSeen  int32
	done     chan struct{}
	once     sync.Once
}

func (h *blockingHandler) Handle(ctx context.Context, conn net.Conn) {
	n := atomic.AddInt32(&h.active, 1)
	atomic.AddInt32(&h.accepted, 1)
	for {
		old := atomic.LoadInt32(&h.maxSeen)
		if n <= old || atomic.CompareAndSwapInt32(&h.maxSeen, old, n) {
			break
		}
	}
	<-h.done
	atomic.AddInt32(&h.active, -1)
	_ = conn.Close()
}

func (h *blockingHandler) Close() error {
	h.once.Do(func() { close(h.done) })
	return nil
}

func TestListenAndServeMaxClients(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	handler := &blockingHandler{done: make(chan struct{})}
	closeChan := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		listenAndServe(listener, handler, closeChan, 2)
		close(stopped)
	}()

	var conns []net.Conn
	for i := 0; i < 6; i++ {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			conns = append(conns, c)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&handler.accepted) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)

	if got := atomic.LoadInt32(&handler.accepted); got != 2 {
		t.Fatalf("accepted = %d connections, want 2", got)
	}
	if got := atomic.LoadInt32(&handler.maxSeen); got > 2 {
		t.Fatalf("max simultaneous clients = %d, want <= 2", got)
	}

	for _, c := range conns {
		_ = c.Close()
	}
	close(closeChan)
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("listenAndServe did not stop")
	}
}
