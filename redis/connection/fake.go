package connection

import (
	"fmt"
	"github.com/hdt3213/godis/lib/logger"
	"io"
	"sync"
)

// FakeConn implements redis.Connection for test.
// Master/test goroutines may write while a parser goroutine reads on another
// goroutine, so every access is guarded by the mutex.
type FakeConn struct {
	Connection
	buf    []byte
	offset int
	waitOn chan struct{}
	closed bool
	mu     sync.Mutex
}

func NewFakeConn() *FakeConn {
	c := &FakeConn{}
	return c
}

// Write writes data to buffer
func (c *FakeConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, io.EOF
	}
	c.buf = append(c.buf, b...)
	waitOn := c.waitOn
	c.waitOn = nil
	c.mu.Unlock()
	if waitOn != nil {
		logger.Debug(fmt.Sprintf("notify %p", waitOn))
		close(waitOn)
	}
	return len(b), nil
}

func (c *FakeConn) notify() {
	c.mu.Lock()
	waitOn := c.waitOn
	c.waitOn = nil
	c.mu.Unlock()
	if waitOn != nil {
		logger.Debug(fmt.Sprintf("notify %p", waitOn))
		close(waitOn)
	}
}

// Read reads data from buffer; it blocks until data is written or the
// connection is closed
func (c *FakeConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		n := copy(p, c.buf[c.offset:])
		if n > 0 {
			c.offset += n
			return n, nil
		}
		if c.closed {
			return 0, io.EOF
		}
		if c.waitOn == nil {
			c.waitOn = make(chan struct{})
		}
		waitOn := c.waitOn
		logger.Debug(fmt.Sprintf("wait on %p", waitOn))
		c.mu.Unlock()
		<-waitOn
		c.mu.Lock()
	}
}

// Clean resets the buffer
func (c *FakeConn) Clean() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.waitOn != nil {
		close(c.waitOn)
		c.waitOn = nil
	}
	c.buf = nil
	c.offset = 0
}

// Bytes returns a copy of written data
func (c *FakeConn) Bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	res := make([]byte, len(c.buf))
	copy(res, c.buf)
	return res
}

func (c *FakeConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.notify()
	return nil
}

func (c *FakeConn) RemoteAddr() string {
	return ""
}
