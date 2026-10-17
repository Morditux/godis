package connection

import (
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestFakeConn(t *testing.T) *FakeConn {
	t.Helper()
	return NewFakeConn()
}

func TestFakeConnWriteRead(t *testing.T) {
	c := newTestFakeConn(t)
	n, err := c.Write([]byte("abcd"))
	if err != nil || n != 4 {
		t.Errorf("write returned %d, %v", n, err)
	}
	n, err = c.Write([]byte("ef"))
	if err != nil || n != 2 {
		t.Errorf("write returned %d, %v", n, err)
	}
	if string(c.Bytes()) != "abcdef" {
		t.Errorf("expected abcdef, got %s", string(c.Bytes()))
	}

	// read advances the offset
	buf := make([]byte, 3)
	n, err = c.Read(buf)
	if err != nil || n != 3 || string(buf) != "abc" {
		t.Errorf("read returned %d, %v, %s", n, err, string(buf))
	}
	n, err = c.Read(buf)
	if err != nil || n != 3 || string(buf) != "def" {
		t.Errorf("read returned %d, %v, %s", n, err, string(buf))
	}

	c.Clean()
	if len(c.Bytes()) != 0 {
		t.Errorf("expected empty buffer after clean, got %d bytes", len(c.Bytes()))
	}
}

func TestFakeConnReadBlocksUntilWrite(t *testing.T) {
	c := newTestFakeConn(t)
	type readResult struct {
		data []byte
		err  error
	}
	results := make(chan readResult, 1)
	go func() {
		buf := make([]byte, 5)
		n, err := c.Read(buf)
		results <- readResult{data: buf[:n], err: err}
	}()

	// give the reader time to enter its wait state; correctness does not
	// depend on this sleep: the read loop re-checks buffered data on wake
	time.Sleep(50 * time.Millisecond)
	if _, err := c.Write([]byte("hello")); err != nil {
		t.Errorf("write failed: %v", err)
	}

	select {
	case r := <-results:
		if r.err != nil {
			t.Errorf("read error: %v", r.err)
		}
		if string(r.data) != "hello" {
			t.Errorf("expected hello, got %s", string(r.data))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read did not wake up after write")
	}
}

func TestFakeConnReadEOFAfterClose(t *testing.T) {
	c := newTestFakeConn(t)
	results := make(chan error, 1)
	go func() {
		buf := make([]byte, 1)
		_, err := c.Read(buf)
		results <- err
	}()
	time.Sleep(50 * time.Millisecond)
	_ = c.Close()

	select {
	case err := <-results:
		if !errors.Is(err, io.EOF) {
			t.Errorf("expected io.EOF after close, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read did not wake up after close")
	}
}

func TestFakeConnWriteAfterClose(t *testing.T) {
	c := newTestFakeConn(t)
	_ = c.Close()
	n, err := c.Write([]byte("x"))
	if !errors.Is(err, io.EOF) || n != 0 {
		t.Errorf("expected 0, io.EOF, got %d, %v", n, err)
	}
}

func TestFakeConnConcurrentReaders(t *testing.T) {
	// one writer, two sequential readers on different goroutines; race
	// detector should not report anything
	c := NewFakeConn()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 4)
		if _, err := c.Read(buf); err != nil {
			t.Errorf("reader 1: %v", err)
		}
	}()
	time.Sleep(30 * time.Millisecond)
	_, _ = c.Write([]byte("abcd"))
	wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 4)
		if _, err := c.Read(buf); err != nil {
			t.Errorf("reader 2: %v", err)
		}
	}()
	time.Sleep(30 * time.Millisecond)
	_, _ = c.Write([]byte("wxyz"))
	wg.Wait()
}

func TestConnChannelState(t *testing.T) {
	c := newTestFakeConn(t)
	if c.SubsCount() != 0 {
		t.Errorf("expected empty subs, got %d", c.SubsCount())
	}
	c.Subscribe("a")
	c.Subscribe("b")
	if c.SubsCount() != 2 {
		t.Errorf("expected 2 subs, got %d", c.SubsCount())
	}
	channels := c.GetChannels()
	if len(channels) != 2 || !contains(channels, "a") || !contains(channels, "b") {
		t.Errorf("unexpected channels: %v", channels)
	}
	c.UnSubscribe("a")
	if c.SubsCount() != 1 {
		t.Errorf("expected 1 sub, got %d", c.SubsCount())
	}
	// unsubscribing a channel not subscribed to is a no-op
	c.UnSubscribe("unknown")
	if c.SubsCount() != 1 {
		t.Errorf("expected 1 sub, got %d", c.SubsCount())
	}
}

func contains(list []string, target string) bool {
	for _, item := range list {
		if item == target {
			return true
		}
	}
	return false
}

func TestConnMultiState(t *testing.T) {
	c := newTestFakeConn(t)
	if c.InMultiState() {
		t.Error("connection should not be in multi state initially")
	}
	c.SetMultiState(true)
	if !c.InMultiState() {
		t.Error("connection should be in multi state")
	}
	c.EnqueueCmd([][]byte{[]byte("get"), []byte("a")})
	c.EnqueueCmd([][]byte{[]byte("get"), []byte("b")})
	if len(c.GetQueuedCmdLine()) != 2 {
		t.Errorf("expected 2 queued cmds, got %d", len(c.GetQueuedCmdLine()))
	}
	c.AddTxError(errors.New("syntax error"))
	if len(c.GetTxErrors()) != 1 {
		t.Errorf("expected 1 tx error, got %d", len(c.GetTxErrors()))
	}
	c.ClearQueuedCmds()
	if len(c.GetQueuedCmdLine()) != 0 {
		t.Errorf("expected 0 queued cmds after clear, got %d", len(c.GetQueuedCmdLine()))
	}
	c.SetMultiState(false)
	if c.InMultiState() {
		t.Error("connection should not be in multi state after discard")
	}
	// watching should be cleared when leaving multi state
	if len(c.GetWatching()) != 0 {
		t.Errorf("expected empty watching after discard, got %d", len(c.GetWatching()))
	}
}

func TestConnWatching(t *testing.T) {
	c := newTestFakeConn(t)
	c.SetWatching("k1", 3)
	if got := c.GetWatching()["k1"]; got != 3 {
		t.Errorf("expected version 3, got %d", got)
	}
	c.SetWatching("k1", 5)
	if got := c.GetWatching()["k1"]; got != 5 {
		t.Errorf("expected version 5, got %d", got)
	}
}

func TestConnBasicState(t *testing.T) {
	c := newTestFakeConn(t)
	if c.GetPassword() != "" {
		t.Error("expected empty password")
	}
	c.SetPassword("secret")
	if c.GetPassword() != "secret" {
		t.Errorf("expected secret, got %s", c.GetPassword())
	}

	if c.GetDBIndex() != 0 {
		t.Errorf("expected db 0, got %d", c.GetDBIndex())
	}
	c.SelectDB(3)
	if c.GetDBIndex() != 3 {
		t.Errorf("expected db 3, got %d", c.GetDBIndex())
	}

	if c.IsSlave() || c.IsMaster() {
		t.Error("connection should have no role initially")
	}
	c.SetSlave()
	if !c.IsSlave() {
		t.Error("expected slave role")
	}
	c.SetMaster()
	if !c.IsMaster() {
		t.Error("expected master role")
	}
}

func TestConnCloseResetStateAndIdempotent(t *testing.T) {
	// net.Pipe provides a real net.Conn without any network dependency
	local, remote := net.Pipe()
	defer func() { _ = remote.Close() }()

	c := NewConn(local)
	c.SelectDB(5)
	c.Subscribe("a")
	c.SetSlave()
	c.SetMultiState(true)
	c.EnqueueCmd([][]byte{[]byte("get"), []byte("a")})

	if err := c.Close(); err != nil {
		t.Errorf("close failed: %v", err)
	}
	// second close must be a no-op, not a panic and not a double cleanup
	if err := c.Close(); err != nil {
		t.Errorf("second close failed: %v", err)
	}

	if c.GetDBIndex() != 0 {
		t.Errorf("expected db reset to 0, got %d", c.GetDBIndex())
	}
	if c.SubsCount() != 0 {
		t.Errorf("expected subs reset, got %d", c.SubsCount())
	}
	if c.IsSlave() {
		t.Error("expected role reset")
	}
	if c.InMultiState() {
		t.Error("expected multi state reset")
	}
	if len(c.GetQueuedCmdLine()) != 0 {
		t.Errorf("expected queued cmds reset, got %d", len(c.GetQueuedCmdLine()))
	}

	// the piped peer should see the connection shutting down
	buf := make([]byte, 1)
	_ = remote.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := remote.Read(buf); err == nil {
		t.Error("expected read error on the closed piped peer")
	}
}

func TestFakeConnRemoteAddrEmpty(t *testing.T) {
	c := newTestFakeConn(t)
	if c.RemoteAddr() != "" {
		t.Errorf("expected empty remote addr, got %s", c.RemoteAddr())
	}
	if strings.Contains(c.Name(), "tcp") {
		t.Errorf("fake conn name should be empty, got %s", c.Name())
	}
}
