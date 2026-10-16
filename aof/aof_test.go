package aof

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/datastruct/list"
	"github.com/hdt3213/godis/datastruct/set"
	SortedSet "github.com/hdt3213/godis/datastruct/sortedset"
	dbiface "github.com/hdt3213/godis/interface/database"
	redisiface "github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
	rdbcore "github.com/hdt3213/rdb/core"
)

// fakeEngine records commands executed on it and satisfies database.DBEngine
type fakeEngine struct {
	mu    sync.Mutex
	execd [][]string
}

var _ dbiface.DBEngine = (*fakeEngine)(nil)

func (e *fakeEngine) record(cmdLine [][]byte) {
	rendered := make([]string, len(cmdLine))
	for i, arg := range cmdLine {
		rendered[i] = string(arg)
	}
	e.mu.Lock()
	e.execd = append(e.execd, rendered)
	e.mu.Unlock()
}

func (e *fakeEngine) snapshot() [][]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([][]string, len(e.execd))
	copy(out, e.execd)
	return out
}

func (e *fakeEngine) Exec(conn redisiface.Connection, cmdLine [][]byte) redisiface.Reply {
	e.record(cmdLine)
	return protocol.MakeOkReply()
}

func (e *fakeEngine) ExecWithLock(conn redisiface.Connection, cmdLine [][]byte) redisiface.Reply {
	e.record(cmdLine)
	return protocol.MakeOkReply()
}

func (e *fakeEngine) ExecMulti(conn redisiface.Connection, watching map[string]uint32, cmdLines []CmdLine) redisiface.Reply {
	return protocol.MakeOkReply()
}

func (e *fakeEngine) LoadRDB(dec *rdbcore.Decoder) error {
	// always claim "no valid rdb preamble" so loading falls back to RESP parsing
	return protocol.MakeErrReply("no rdb preamble in fake engine")
}

func (e *fakeEngine) GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine {
	return nil
}

func (e *fakeEngine) ForEach(dbIndex int, cb func(key string, data *dbiface.DataEntity, expiration *time.Time) bool) {
}

func (e *fakeEngine) RWLocks(dbIndex int, writeKeys []string, readKeys []string) {}

func (e *fakeEngine) RWUnLocks(dbIndex int, writeKeys []string, readKeys []string) {}

func (e *fakeEngine) GetDBSize(dbIndex int) (int, int) { return 0, 0 }

func (e *fakeEngine) GetEntity(dbIndex int, key string) (*dbiface.DataEntity, bool) {
	return nil, false
}

func (e *fakeEngine) GetExpiration(dbIndex int, key string) *time.Time { return nil }

func (e *fakeEngine) SetKeyInsertedCallback(cb dbiface.KeyEventCallback) {}

func (e *fakeEngine) SetKeyDeletedCallback(cb dbiface.KeyEventCallback) {}

func (e *fakeEngine) AfterClientClose(c redisiface.Connection) {}

func (e *fakeEngine) Close() {}

func respEncode(args ...string) string {
	var sb strings.Builder
	sb.WriteString("*" + stringify(len(args)) + "\r\n")
	for _, arg := range args {
		sb.WriteString("$" + stringify(len(arg)) + "\r\n" + arg + "\r\n")
	}
	return sb.String()
}

func stringify(n int) string {
	return strconv.Itoa(n)
}

// recordingListener implements aof.Listener
type recordingListener struct {
	mu    sync.Mutex
	calls [][]string
}

func (l *recordingListener) Callback(cmdLines []CmdLine) {
	rendered := make([]string, 0, len(cmdLines)*3)
	for _, line := range cmdLines {
		for _, arg := range line {
			rendered = append(rendered, string(arg))
		}
	}
	l.mu.Lock()
	l.calls = append(l.calls, rendered)
	l.mu.Unlock()
}

func (l *recordingListener) callCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.calls)
}

func (l *recordingListener) lastCall() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.calls) == 0 {
		return nil
	}
	return l.calls[len(l.calls)-1]
}

func makeTestPersister(t *testing.T, engine *fakeEngine, load bool, fsync string) *Persister {
	t.Helper()
	filename := filepath.Join(t.TempDir(), "aof.log")
	persister, err := NewPersister(engine, filename, load, fsync, nil)
	if err != nil {
		t.Fatalf("new persister: %v", err)
	}
	t.Cleanup(func() { persister.Close() })
	return persister
}

func waitAofContains(t *testing.T, p *Persister, substring string) bool {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(p.aofFilename)
		if err == nil && strings.Contains(string(data), substring) {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

func waitFor(t *testing.T, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(message)
}

func TestSaveCmdLineWritesProtocol(t *testing.T) {
	engine := &fakeEngine{}
	persister := makeTestPersister(t, engine, false, FsyncNo)

	persister.SaveCmdLine(0, utils.ToCmdLine("SET", "k", "v"))
	persister.SaveCmdLine(1, utils.ToCmdLine("SET", "k2", "v2"))

	if !waitAofContains(t, persister, "$3\r\nSET\r\n") {
		t.Error("expected SET command in aof file")
	}
	// switching dbIndex writes a SELECT line first
	if !waitAofContains(t, persister, "$6\r\nSELECT\r\n$1\r\n1\r\n") {
		t.Error("expected SELECT 1 transition in aof file")
	}
}

func TestSaveCmdLineAlwaysFsync(t *testing.T) {
	engine := &fakeEngine{}
	persister := makeTestPersister(t, engine, false, FsyncAlways)

	// always mode writes synchronously: the entry is visible right away
	persister.SaveCmdLine(0, utils.ToCmdLine("SET", "k", "v"))
	data, err := os.ReadFile(persister.aofFilename)
	if err != nil {
		t.Fatalf("read aof: %v", err)
	}
	if !strings.Contains(string(data), "$3\r\nSET\r\n") {
		t.Errorf("expected synchronous write, got %q", string(data))
	}
}

func TestListenerCallback(t *testing.T) {
	l := &recordingListener{}
	persister := makeTestPersister(t, &fakeEngine{}, false, FsyncNo)

	persister.AddListener(l)
	persister.SaveCmdLine(0, utils.ToCmdLine("SET", "k", "v"))
	waitFor(t, func() bool { return l.callCount() > 0 }, "listener did not receive callback")

	// buffer contains previous commands then the submitted one
	last := l.lastCall()
	if len(last) < 3 || last[len(last)-3] != "SET" || last[len(last)-2] != "k" || last[len(last)-1] != "v" {
		t.Errorf("unexpected callback buffer: %v", last)
	}

	// removed listener stops receiving callbacks
	persister.RemoveListener(l)
	before := l.callCount()
	persister.SaveCmdLine(0, utils.ToCmdLine("SET", "k2", "v2"))
	time.Sleep(100 * time.Millisecond)
	if l.callCount() != before {
		t.Error("removed listener still received callbacks")
	}
}

func TestLoadAofReplaysCommands(t *testing.T) {
	engine := &fakeEngine{}
	filename := filepath.Join(t.TempDir(), "load.log")
	content := respEncode("SET", "k", "v") +
		respEncode("SELECT", "1") +
		respEncode("SET", "k2", "v2")
	if err := os.WriteFile(filename, []byte(content), 0o600); err != nil {
		t.Fatalf("write aof fixture: %v", err)
	}

	// build the persister around the very same file (avoid calling
	// t.TempDir() again: each call makes a fresh directory)
	persister, err := NewPersister(engine, filename, true, FsyncNo, nil)
	if err != nil {
		t.Fatalf("new persister: %v", err)
	}
	t.Cleanup(func() { persister.Close() })

	// SELECT is replayed too (engine returns OK) and advances currentDB
	waitFor(t, func() bool { return len(engine.snapshot()) == 3 }, "expected 3 loaded commands")

	got := engine.snapshot()
	expect := [][]string{
		{"SET", "k", "v"},
		{"SELECT", "1"},
		{"SET", "k2", "v2"},
	}
	for i, want := range expect {
		if strings.Join(got[i], " ") != strings.Join(want, " ") {
			t.Errorf("command %d: expected %v, got %v", i, want, got[i])
		}
	}
	if persister.currentDB != 1 {
		t.Errorf("expected currentDB 1 after SELECT replay, got %d", persister.currentDB)
	}
}

func TestLoadAofMissingFileIsTolerated(t *testing.T) {
	engine := &fakeEngine{}
	// the file does not exist: LoadAof must be a silent no-op
	persister := makeTestPersister(t, engine, true, FsyncNo)
	if persister.currentDB != 0 {
		t.Errorf("expected currentDB 0, got %d", persister.currentDB)
	}
	if n := len(engine.snapshot()); n != 0 {
		t.Errorf("expected no replayed commands, got %d", n)
	}
}

func TestCloseIdempotent(t *testing.T) {
	engine := &fakeEngine{}
	filename := filepath.Join(t.TempDir(), "close.log")
	persister, err := NewPersister(engine, filename, false, FsyncNo, nil)
	if err != nil {
		t.Fatalf("new persister: %v", err)
	}
	persister.SaveCmdLine(0, utils.ToCmdLine("SET", "k", "v"))
	persister.Close()
	// double close must not panic (guarded by closeOnce)
	persister.Close()
	// submissions after close land in the cancelled stream: no panic allowed
	persister.SaveCmdLine(0, utils.ToCmdLine("SET", "late", "v"))
	time.Sleep(50 * time.Millisecond)
}

func TestEntityToCmdString(t *testing.T) {
	c := EntityToCmd("key", &dbiface.DataEntity{Data: []byte("value")})
	if c == nil {
		t.Fatal("expected SET command for string entity")
	}
	if string(c.ToBytes()) != respEncode("SET", "key", "value") {
		t.Errorf("unexpected SET encoding: %q", string(c.ToBytes()))
	}
}

func TestEntityToCmdNilAndUnsupported(t *testing.T) {
	if EntityToCmd("key", nil) != nil {
		t.Error("expected nil command for nil entity")
	}
	if EntityToCmd("key", &dbiface.DataEntity{Data: 42}) != nil {
		t.Error("expected nil command for unsupported entity type")
	}
}

func TestEntityToCmdList(t *testing.T) {
	l := list.Make([]byte("a"), []byte("b"))
	c := EntityToCmd("key", &dbiface.DataEntity{Data: l})
	if !strings.Contains(string(c.ToBytes()), "RPUSH") {
		t.Errorf("expected RPUSH command, got %q", string(c.ToBytes()))
	}
}

func TestEntityToCmdSet(t *testing.T) {
	s := set.Make("m1", "m2")
	c := EntityToCmd("key", &dbiface.DataEntity{Data: s})
	if !strings.Contains(string(c.ToBytes()), "SADD") {
		t.Errorf("expected SADD command, got %q", string(c.ToBytes()))
	}
}

func TestEntityToCmdHash(t *testing.T) {
	h := dict.MakeSimple[[]byte]()
	h.Put("field", []byte("value"))
	c := EntityToCmd("key", &dbiface.DataEntity{Data: h})
	if !strings.Contains(string(c.ToBytes()), "HMSET") {
		t.Errorf("expected HMSET command, got %q", string(c.ToBytes()))
	}
}

func TestEntityToCmdZSet(t *testing.T) {
	z := SortedSet.Make()
	z.Add("a", 1.5)
	c := EntityToCmd("key", &dbiface.DataEntity{Data: z})
	if !strings.Contains(string(c.ToBytes()), "ZADD") {
		t.Errorf("expected ZADD command, got %q", string(c.ToBytes()))
	}
}

func TestMakeExpireCmd(t *testing.T) {
	expireAt := time.Unix(1700000000, 123456789)
	c := MakeExpireCmd("key", expireAt)
	encoded := string(c.ToBytes())
	if !strings.Contains(encoded, "PEXPIREAT") {
		t.Errorf("expected PEXPIREAT command, got %q", encoded)
	}
	if !strings.Contains(encoded, "key") {
		t.Errorf("expected key in command, got %q", encoded)
	}
}
