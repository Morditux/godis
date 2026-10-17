package parser

import (
	"bytes"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
	"io"
	"net"
	"runtime"
	"testing"
	"time"
)

func TestParseStream(t *testing.T) {
	replies := []redis.Reply{
		protocol.MakeIntReply(1),
		protocol.MakeStatusReply("OK"),
		protocol.MakeErrReply("ERR unknown"),
		protocol.MakeBulkReply([]byte("a\r\nb")), // test binary safe
		protocol.MakeNullBulkReply(),
		protocol.MakeMultiBulkReply([][]byte{
			[]byte("a"),
			[]byte("\r\n"),
		}),
		protocol.MakeEmptyMultiBulkReply(),
	}
	reqs := bytes.Buffer{}
	for _, re := range replies {
		reqs.Write(re.ToBytes())
	}
	reqs.Write([]byte("set a a" + protocol.CRLF)) // test text protocol
	expected := make([]redis.Reply, len(replies))
	copy(expected, replies)
	expected = append(expected, protocol.MakeMultiBulkReply([][]byte{
		[]byte("set"), []byte("a"), []byte("a"),
	}))

	ch := ParseStream(bytes.NewReader(reqs.Bytes()))
	i := 0
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF {
				return
			}
			t.Error(payload.Err)
			return
		}
		if payload.Data == nil {
			t.Error("empty data")
			return
		}
		exp := expected[i]
		i++
		if !utils.BytesEquals(exp.ToBytes(), payload.Data.ToBytes()) {
			t.Error("parse failed: " + string(exp.ToBytes()))
		}
	}
}

func TestParseOne(t *testing.T) {
	replies := []redis.Reply{
		protocol.MakeIntReply(1),
		protocol.MakeStatusReply("OK"),
		protocol.MakeErrReply("ERR unknown"),
		protocol.MakeBulkReply([]byte("a\r\nb")), // test binary safe
		protocol.MakeNullBulkReply(),
		protocol.MakeMultiBulkReply([][]byte{
			[]byte("a"),
			[]byte("\r\n"),
		}),
		protocol.MakeEmptyMultiBulkReply(),
	}
	for _, re := range replies {
		result, err := ParseOne(re.ToBytes())
		if err != nil {
			t.Error(err)
			continue
		}
		if !utils.BytesEquals(result.ToBytes(), re.ToBytes()) {
			t.Error("parse failed: " + string(re.ToBytes()))
		}
	}
}

// TestParseOneOnMultiPayload makes sure ParseOne terminates correctly on data
// holding more than one payload and does not leak the internal goroutine
func TestParseOneOnMultiPayload(t *testing.T) {
	data := protocol.MakeStatusReply("OK").ToBytes()
	data = append(data, protocol.MakeIntReply(42).ToBytes()...)
	data = append(data, protocol.MakeNullBulkReply().ToBytes()...)

	result, err := ParseOne(data)
	if err != nil {
		t.Fatal(err)
	}
	if !utils.BytesEquals(result.ToBytes(), protocol.MakeStatusReply("OK").ToBytes()) {
		t.Fatal("ParseOne should return the first payload")
	}
}

// TestParseStreamWithStop checks that closing the stop channel terminates the
// parse goroutine and closes the payload channel, so that `for range` consumers
// are always released
func TestParseStreamWithStop(t *testing.T) {
	connR, connW := net.Pipe()
	defer func() {
		_ = connW.Close()
	}()
	stop := make(chan struct{})
	ch := ParseStreamWithStop(connR, stop)
	before := runtime.NumGoroutine()

	// write one payload into the pipe and receive it
	if _, err := connW.Write(protocol.MakeStatusReply("OK").ToBytes()); err != nil {
		t.Fatal(err)
	}
	first := <-ch
	if first.Err != nil || first.Data == nil {
		t.Fatalf("unexpected first payload: %v", first)
	}

	// stop while the parser is waiting for new data; a blocked read is not
	// interrupted by stop, so close the pipe too: the parser notices the error,
	// gives up delivering and exits, closing the channel
	close(stop)
	_ = connR.Close()

	// the channel must be closed so that `for range` consumers finish
	deadline := time.Now().Add(2 * time.Second)
	closedCh := false
	for time.Now().Before(deadline) {
		select {
		case payload := <-ch:
			if payload != nil {
				t.Fatalf("expected closed channel with nil payload, got %v", payload)
			}
			closedCh = true
		default:
			time.Sleep(20 * time.Millisecond)
		}
		if closedCh {
			break
		}
	}
	if !closedCh {
		t.Fatal("payload channel must be closed after the parser stopped")
	}

	// once the channel is closed the parse goroutine has returned from its
	// main body; make sure it fully terminated (NumGoroutine settles)
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("parse goroutine leaked: before=%d, now=%d", before, runtime.NumGoroutine())
}

func TestTruncatedCommandIsAnError(t *testing.T) {
	// a clean EOF is io.EOF, but a command cut in the middle must not be
	// mistaken for a normal end of stream (e.g. during AOF replay)
	if _, err := ParseOne([]byte("*2\r\n$3\r\nSET\r\n")); err == nil {
		t.Fatal("truncated multi-bulk command was accepted as a clean EOF")
	}
}
