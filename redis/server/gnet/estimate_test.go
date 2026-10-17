package gnet

import (
	"testing"
)

func TestScanCommandComplete(t *testing.T) {
	cmd := "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"
	size, complete, err := scanCommand([]byte(cmd))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !complete {
		t.Fatalf("expected complete command")
	}
	if size != len(cmd) {
		t.Fatalf("expected size %d, got %d", len(cmd), size)
	}
}

func TestScanCommandIncomplete(t *testing.T) {
	cases := map[string][]byte{
		"missing bulk body and crlf": []byte("*1\r\n$10\r\nabc"),
		"missing body tail":          []byte("*1\r\n$3\r\nSET\r"),
		"missing crlf":               []byte("*1\r\n$3\r\nSET"),
		"missing count crlf":         []byte("*1\r"),
		"missing type byte":          []byte("*2\r\n$1\r\na\r\n"),
		"inline without crlf":        []byte("PING"),
	}
	for name, data := range cases {
		_, complete, err := scanCommand(data)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", name, err)
		}
		if complete {
			t.Errorf("%s: expected incomplete", name)
		}
	}
}

func TestScanCommandInline(t *testing.T) {
	size, complete, err := scanCommand([]byte("PING\r\n"))
	if err != nil || !complete {
		t.Fatalf("expected complete inline command, got complete=%v err=%v", complete, err)
	}
	if size != len("PING\r\n") {
		t.Fatalf("expected size %d, got %d", len("PING\r\n"), size)
	}
}

func TestScanCommandNullBulk(t *testing.T) {
	// *-1: null array, only the header is consumed
	size, complete, err := scanCommand([]byte("*-1\r\n"))
	if err != nil || !complete {
		t.Fatalf("expected complete null array, got complete=%v err=%v", complete, err)
	}
	if size != len("*-1\r\n") {
		t.Fatalf("expected size %d, got %d", len("*-1\r\n"), size)
	}

	// null bulk string as the only argument
	cmd := "*1\r\n$-1\r\n"
	size, complete, err = scanCommand([]byte(cmd))
	if err != nil || !complete {
		t.Fatalf("expected complete null bulk, got complete=%v err=%v", complete, err)
	}
	if size != len(cmd) {
		t.Fatalf("expected size %d, got %d", len(cmd), size)
	}
}

func TestScanCommandPipelined(t *testing.T) {
	// only scan the first command; the rest stays for the next round
	first := "*1\r\n$1\r\na\r\n"
	data := first + "*1\r\n$1\r\nb\r\n"
	size, complete, err := scanCommand([]byte(data))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !complete || size != len(first) {
		t.Fatalf("expected complete size %d, got size=%d complete=%v", len(first), size, complete)
	}
}

func TestScanCommandMalformed(t *testing.T) {
	cases := map[string][]byte{
		"unsupported arg type": []byte("*1\r\n+hello\r\n"),
		"bad integer":          []byte("*abc\r\n"),
		"empty integer":        []byte("*\r\n"),
		"just error marker":    []byte("*1\r\n$-\r\n"),
		"crlf not paired":      []byte("*1\r\nX$1\r\na\r\n"),
	}
	for name, data := range cases {
		_, complete, err := scanCommand(data)
		if err == nil {
			t.Errorf("%s: expected protocol error, got complete=%v", name, complete)
		}
	}
}
