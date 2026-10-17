package client

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
	"github.com/hdt3213/godis/redis/protocol/asserts"
	"strconv"
	"testing"
)

func requireRedis(t *testing.T) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", "127.0.0.1:6379", 200*time.Millisecond)
	if err != nil {
		t.Skipf("redis is not available on 127.0.0.1:6379: %v", err)
	}
	_ = conn.Close()
}

func TestClient(t *testing.T) {
	requireRedis(t)
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})
	client, err := MakeClient("localhost:6379")
	if err != nil {
		t.Error(err)
	}
	client.Start()

	result := client.Send([][]byte{
		[]byte("PING"),
	})
	if statusRet, ok := result.(*protocol.StatusReply); ok {
		if statusRet.Status != "PONG" {
			t.Error("`ping` failed, result: " + statusRet.Status)
		}
	}

	result = client.Send([][]byte{
		[]byte("SET"),
		[]byte("a"),
		[]byte("a"),
	})
	if statusRet, ok := result.(*protocol.StatusReply); ok {
		if statusRet.Status != "OK" {
			t.Error("`set` failed, result: " + statusRet.Status)
		}
	}

	result = client.Send([][]byte{
		[]byte("GET"),
		[]byte("a"),
	})
	if bulkRet, ok := result.(*protocol.BulkReply); ok {
		if string(bulkRet.Arg) != "a" {
			t.Error("`get` failed, result: " + string(bulkRet.Arg))
		}
	}

	result = client.Send([][]byte{
		[]byte("DEL"),
		[]byte("a"),
	})
	if intRet, ok := result.(*protocol.IntReply); ok {
		if intRet.Code != 1 {
			t.Error("`del` failed, result: " + strconv.FormatInt(intRet.Code, 10))
		}
	}

	client.doHeartbeat() // random do heartbeat
	result = client.Send([][]byte{
		[]byte("GET"),
		[]byte("a"),
	})
	if _, ok := result.(*protocol.NullBulkReply); !ok {
		t.Error("`get` failed, result: " + string(result.ToBytes()))
	}

	result = client.Send([][]byte{
		[]byte("DEL"),
		[]byte("arr"),
	})

	result = client.Send([][]byte{
		[]byte("RPUSH"),
		[]byte("arr"),
		[]byte("1"),
		[]byte("2"),
		[]byte("c"),
	})
	if intRet, ok := result.(*protocol.IntReply); ok {
		if intRet.Code != 3 {
			t.Error("`rpush` failed, result: " + strconv.FormatInt(intRet.Code, 10))
		}
	}

	result = client.Send([][]byte{
		[]byte("LRANGE"),
		[]byte("arr"),
		[]byte("0"),
		[]byte("-1"),
	})
	if multiBulkRet, ok := result.(*protocol.MultiBulkReply); ok {
		if len(multiBulkRet.Args) != 3 ||
			string(multiBulkRet.Args[0]) != "1" ||
			string(multiBulkRet.Args[1]) != "2" ||
			string(multiBulkRet.Args[2]) != "c" {
			t.Error("`lrange` failed, result: " + string(multiBulkRet.ToBytes()))
		}
	}

	client.Close()
	ret := client.Send(utils.ToCmdLine("ping"))
	asserts.AssertErrReply(t, ret, "client closed")
}

func TestReconnect(t *testing.T) {
	requireRedis(t)
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})
	client, err := MakeClient("localhost:6379")
	if err != nil {
		t.Error(err)
	}
	client.Start()

	_ = client.conn.Close()
	time.Sleep(time.Second) // wait for reconnecting
	success := false
	for i := 0; i < 3; i++ {
		result := client.Send([][]byte{
			[]byte("PING"),
		})
		if bytes.Equal(result.ToBytes(), []byte("+PONG\r\n")) {
			success = true
			break
		}
	}
	if !success {
		t.Error("reconnect error")
	}
}

// fakeRedis reads one multi-bulk request then either replies +OK or drops the
// connection (first request only), to exercise the reconnect path without a
// real redis instance
type fakeRedis struct {
	dropped int32 // dropped must be set to 1 for the first served request
}

func (f *fakeRedis) serve(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		args, err := readFakeRequest(reader)
		if err != nil {
			_ = conn.Close()
			return
		}
		if len(args) > 0 && atomic.CompareAndSwapInt32(&f.dropped, 0, 1) {
			_ = conn.Close()
			return
		}
		if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
			_ = conn.Close()
			return
		}
	}
}

func readFakeRequest(reader *bufio.Reader) ([][]byte, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	line = bytes.TrimSuffix(line, []byte(protocol.CRLF))
	if len(line) == 0 || line[0] != '*' {
		return nil, errors.New("fake redis: require multi bulk protocol")
	}
	n, err := strconv.Atoi(string(line[1:]))
	if err != nil || n < 0 {
		return nil, errors.New("fake redis: illegal array header")
	}
	args := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		l, err := reader.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		l = bytes.TrimSuffix(l, []byte(protocol.CRLF))
		if len(l) < 2 || l[0] != '$' {
			return nil, errors.New("fake redis: illegal bulk string header")
		}
		strLen, err := strconv.Atoi(string(l[1:]))
		if err != nil || strLen < -1 {
			return nil, errors.New("fake redis: illegal bulk string length")
		}
		body := make([]byte, strLen+2)
		if _, err := io.ReadFull(reader, body); err != nil {
			return nil, err
		}
		args = append(args, body[:len(body)-2])
	}
	return args, nil
}

func startFakeRedis(t *testing.T) (addr string, serving *fakeRedis) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serving = &fakeRedis{}
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go serving.serve(conn)
		}
	}()
	t.Cleanup(func() { _ = l.Close() })
	return l.Addr().String(), serving
}

// TestClientReconnectOnDrop sends a request to a server that closes the
// connection without replying: the pending request must fail with an error
// and the client must recover on its own
func TestClientReconnectOnDrop(t *testing.T) {
	addr, _ := startFakeRedis(t)
	client, err := MakeClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	client.Start()
	defer client.Close()

	// the first request triggers the drop
	result := client.Send(utils.ToCmdLine("SET", "a", "1"))
	asserts.AssertErrReply(t, result, "request failed connection closed")

	// afterwards the client must reconnect automatically
	reconnected := false
	for i := 0; i < 10 && !reconnected; i++ {
		time.Sleep(200 * time.Millisecond)
		if bytes.Equal(client.Send(utils.ToCmdLine("PING")).ToBytes(), []byte("+OK\r\n")) {
			reconnected = true
			break
		}
	}
	if !reconnected {
		t.Error("client failed to reconnect after drop")
	}
}

// TestClientCloseIdempotent makes sure a second Close does not panic
// (a duplicate close on pendingReqs/waitingReqs used to crash)
func TestClientCloseIdempotent(t *testing.T) {
	addr, _ := startFakeRedis(t)
	client, err := MakeClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	client.Start()
	client.Close()
	client.Close() // no panic expected
	asserts.AssertErrReply(t, client.Send(utils.ToCmdLine("ping")), "client closed")
}
