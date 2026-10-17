package gnet

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/hdt3213/godis/database"
)

func TestListenAndServe(t *testing.T) {
	var err error
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Error(err)
		return
	}
	addr := listener.Addr().String()
	db := database.NewStandaloneServer()
	server := NewGnetServer(db)
	go server.Run(addr)
	time.Sleep(2 * time.Second)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = conn.Write([]byte("PING\r\n"))
	if err != nil {
		t.Error(err)
		return
	}
	bufReader := bufio.NewReader(conn)
	line, _, err := bufReader.ReadLine()
	if err != nil {
		t.Error(err)
		return
	}
	if string(line) != "+PONG" {
		t.Error("get wrong response")
		return
	}
	conn.Close()
	server.Close()
}

func newTestGnetServer(t *testing.T) (addr string, stop func()) {
	t.Helper()
	// probe a free port, then release it for the server to bind
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	addr = listener.Addr().String()
	_ = listener.Close()
	db := database.NewStandaloneServer()
	server := NewGnetServer(db)
	go func() { _ = server.Run(addr) }() // Run returns an error on Stop, harmless here
	return addr, func() {
		server.Close()
	}
}

func dialWithRetry(t *testing.T, addr string) net.Conn {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			return conn
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial %s failed: %v", addr, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestGnetPipeline checks that several command lines delivered in one segment
// are all executed in a single OnTraffic event (drain loop) with batched replies
func TestGnetPipeline(t *testing.T) {
	addr, stop := newTestGnetServer(t)
	defer stop()
	conn := dialWithRetry(t, addr)
	defer conn.Close()

	// set a b / get a / ping, all pipelined and RESP-encoded
	pipeline := "*3\r\n$3\r\nset\r\n$1\r\na\r\n$1\r\nb\r\n" +
		"*2\r\n$3\r\nget\r\n$1\r\na\r\n" +
		"*1\r\n$4\r\nping\r\n"
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte(pipeline)); err != nil {
		t.Fatal(err)
	}
	all, err := func() (string, error) {
		buf := make([]byte, 512)
		n, err := conn.Read(buf)
		return string(buf[:n]), err
	}()
	if err != nil {
		t.Fatalf("read reply batch failed: %v", err)
	}
	for _, expected := range []string{"+OK", "$1\r\nb\r\n", "+PONG"} {
		if !strings.Contains(all, expected) {
			t.Errorf("missing %q in reply batch %q", expected, all)
		}
	}
}

// TestGnetPartialCommandWaits checks that a command crossing several segments
// does not close the connection with a spurious protocol error: the connection
// must wait for the rest, then complete the command when bytes arrive
func TestGnetPartialCommandWaits(t *testing.T) {
	addr, stop := newTestGnetServer(t)
	defer stop()
	conn := dialWithRetry(t, addr)
	defer conn.Close()

	if _, err := conn.Write([]byte("*2\r\n$3\r\nget\r\n")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond * 100) // Give the server time to treat the incomplete command

	if _, err := conn.Write([]byte("$1\r\nk\r\n")); err != nil {
		t.Fatalf("connection was likely closed while waiting for the rest of the command: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read reply failed: %v", err)
	}
	if string(buf[:n]) != "$-1\r\n" {
		t.Fatalf("expected null bulk (key not set), got %q", buf[:n])
	}
}
