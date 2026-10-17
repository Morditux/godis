package database

import (
	"strconv"
	"testing"
	"time"

	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol"
)

func execCompatCmd(t *testing.T, db *DB, c redis.Connection, args ...string) redis.Reply {
	t.Helper()
	return db.Exec(c, utils.ToCmdLine(args...))
}

func parseScanReply(t *testing.T, reply redis.Reply) (int, [][]byte) {
	t.Helper()
	raw, ok := reply.(*protocol.MultiRawReply)
	if !ok || len(raw.Replies) != 2 {
		t.Fatalf("invalid scan reply: %T %q", reply, string(reply.ToBytes()))
	}
	cursorReply, ok := raw.Replies[0].(*protocol.BulkReply)
	if !ok {
		t.Fatalf("scan cursor is not a bulk reply: %T", raw.Replies[0])
	}
	cursor, err := strconv.Atoi(string(cursorReply.Arg))
	if err != nil {
		t.Fatalf("invalid cursor: %v", err)
	}
	items, ok := raw.Replies[1].(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("scan items are not a multi-bulk reply: %T", raw.Replies[1])
	}
	return cursor, items.Args
}

func TestHSetMultipleFields(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()
	defer c.Close()

	if got := string(execCompatCmd(t, db, c, "HSET", "h", "f1", "v1", "f2", "v2").ToBytes()); got != ":2\r\n" {
		t.Fatalf("HSET two new fields = %q, want :2", got)
	}
	if got := string(execCompatCmd(t, db, c, "HSET", "h", "f1", "new", "f3", "v3").ToBytes()); got != ":1\r\n" {
		t.Fatalf("HSET update+add = %q, want :1", got)
	}
	if got := string(execCompatCmd(t, db, c, "HGET", "h", "f1").ToBytes()); got != "$3\r\nnew\r\n" {
		t.Fatalf("HGET f1 = %q", got)
	}
	if reply := execCompatCmd(t, db, c, "HSET", "h", "f1", "v1", "f2"); !protocol.IsErrorReply(reply) {
		t.Fatalf("odd HSET should fail, got %q", string(reply.ToBytes()))
	}
}

func TestHScanReturnsValuesAndPaginates(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()
	defer c.Close()

	execCompatCmd(t, db, c, "HSET", "h", "a", "1", "b", "2", "c", "3")
	seen := make(map[string]string)
	cursor := 0
	for pages := 0; pages < 10; pages++ {
		next, items := parseScanReply(t, execCompatCmd(t, db, c, "HSCAN", "h", strconv.Itoa(cursor), "COUNT", "1"))
		if len(items)%2 != 0 {
			t.Fatalf("HSCAN must return field/value pairs, got %d items", len(items))
		}
		for i := 0; i < len(items); i += 2 {
			seen[string(items[i])] = string(items[i+1])
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if len(seen) != 3 || seen["a"] != "1" || seen["b"] != "2" || seen["c"] != "3" {
		t.Fatalf("HSCAN did not return all field/value pairs: %v", seen)
	}

	// missing key: ["0", []]
	next, items := parseScanReply(t, execCompatCmd(t, db, c, "HSCAN", "missing", "0"))
	if next != 0 || len(items) != 0 {
		t.Fatalf("HSCAN missing = %d/%d, want 0/0", next, len(items))
	}
}

func TestSScanAndZScanPagination(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()
	defer c.Close()

	execCompatCmd(t, db, c, "SADD", "s", "a", "b", "c", "d", "e")
	seen := make(map[string]bool)
	cursor := 0
	for pages := 0; pages < 10; pages++ {
		next, items := parseScanReply(t, execCompatCmd(t, db, c, "SSCAN", "s", strconv.Itoa(cursor), "COUNT", "2"))
		if len(items) > 2 {
			t.Fatalf("SSCAN COUNT 2 returned %d items", len(items))
		}
		for _, item := range items {
			seen[string(item)] = true
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if len(seen) != 5 {
		t.Fatalf("SSCAN returned %d/5 members", len(seen))
	}

	execCompatCmd(t, db, c, "ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d")
	scores := make(map[string]string)
	cursor = 0
	for pages := 0; pages < 10; pages++ {
		next, items := parseScanReply(t, execCompatCmd(t, db, c, "ZSCAN", "z", strconv.Itoa(cursor), "COUNT", "2"))
		if len(items)%2 != 0 {
			t.Fatalf("ZSCAN must return member/score pairs, got %d items", len(items))
		}
		for i := 0; i < len(items); i += 2 {
			scores[string(items[i])] = string(items[i+1])
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if len(scores) != 4 || scores["a"] != "1" || scores["d"] != "4" {
		t.Fatalf("ZSCAN returned unexpected data: %v", scores)
	}
}

func TestSetOptions(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()
	defer c.Close()

	// KEEPTTL preserves the deadline
	execCompatCmd(t, db, c, "SET", "k", "v1", "EX", "100")
	execCompatCmd(t, db, c, "SET", "k", "v2", "KEEPTTL")
	if got := string(execCompatCmd(t, db, c, "GET", "k").ToBytes()); got != "$2\r\nv2\r\n" {
		t.Fatalf("GET after KEEPTTL = %q", got)
	}
	if ttl := string(execCompatCmd(t, db, c, "TTL", "k").ToBytes()); ttl == ":-1\r\n" || ttl == ":-2\r\n" {
		t.Fatalf("KEEPTTL lost the ttl: %s", ttl)
	}
	// a normal SET clears it
	execCompatCmd(t, db, c, "SET", "k", "v3")
	if got := string(execCompatCmd(t, db, c, "TTL", "k").ToBytes()); got != ":-1\r\n" {
		t.Fatalf("SET did not clear ttl: %s", got)
	}

	// GET returns the old value
	if got := string(execCompatCmd(t, db, c, "SET", "k", "v4", "GET").ToBytes()); got != "$2\r\nv3\r\n" {
		t.Fatalf("SET GET returned %q, want old value v3", got)
	}
	if got := string(execCompatCmd(t, db, c, "SET", "brand-new", "v", "GET").ToBytes()); got != "$-1\r\n" {
		t.Fatalf("SET GET on missing key = %q, want nil", got)
	}
	if got := string(execCompatCmd(t, db, c, "SET", "k", "other", "NX", "GET").ToBytes()); got != "$2\r\nv4\r\n" {
		t.Fatalf("SET NX GET on existing key = %q, want old value", got)
	}
	if got := string(execCompatCmd(t, db, c, "GET", "k").ToBytes()); got != "$2\r\nv4\r\n" {
		t.Fatalf("SET NX modified the key: %q", got)
	}

	// GET aborts on a non-string target
	execCompatCmd(t, db, c, "RPUSH", "list", "a")
	if reply := execCompatCmd(t, db, c, "SET", "list", "x", "GET"); !protocol.IsErrorReply(reply) {
		t.Fatalf("SET GET on a list should fail, got %q", string(reply.ToBytes()))
	}
	if got := string(execCompatCmd(t, db, c, "LRANGE", "list", "0", "-1").ToBytes()); got != "*1\r\n$1\r\na\r\n" {
		t.Fatalf("SET GET modified the list: %q", got)
	}

	// absolute expiry modes
	future := time.Now().Add(100 * time.Second)
	execCompatCmd(t, db, c, "SET", "exat", "v", "EXAT", strconv.FormatInt(future.Unix(), 10))
	if ttl := string(execCompatCmd(t, db, c, "TTL", "exat").ToBytes()); ttl == ":-1\r\n" || ttl == ":-2\r\n" {
		t.Fatalf("SET EXAT did not set a ttl: %s", ttl)
	}
	execCompatCmd(t, db, c, "SET", "pxat", "v", "PXAT", strconv.FormatInt(future.UnixMilli(), 10))
	if ttl := string(execCompatCmd(t, db, c, "TTL", "pxat").ToBytes()); ttl == ":-1\r\n" || ttl == ":-2\r\n" {
		t.Fatalf("SET PXAT did not set a ttl: %s", ttl)
	}
	execCompatCmd(t, db, c, "SET", "past", "v", "EXAT", "1")
	if got := string(execCompatCmd(t, db, c, "EXISTS", "past").ToBytes()); got != ":0\r\n" {
		t.Fatalf("SET EXAT in the past left the key alive: %s", got)
	}

	// incompatible options
	if reply := execCompatCmd(t, db, c, "SET", "k", "v", "KEEPTTL", "EX", "10"); !protocol.IsErrorReply(reply) {
		t.Fatalf("KEEPTTL+EX should fail, got %q", string(reply.ToBytes()))
	}
}

func TestZAddOptions(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()
	defer c.Close()

	if got := string(execCompatCmd(t, db, c, "ZADD", "z", "NX", "1", "a").ToBytes()); got != ":1\r\n" {
		t.Fatalf("ZADD NX new = %q, want :1", got)
	}
	if got := string(execCompatCmd(t, db, c, "ZADD", "z", "NX", "2", "a").ToBytes()); got != ":0\r\n" {
		t.Fatalf("ZADD NX existing = %q, want :0", got)
	}
	if got := string(execCompatCmd(t, db, c, "ZSCORE", "z", "a").ToBytes()); got != "$1\r\n1\r\n" {
		t.Fatalf("ZADD NX updated the score: %q", got)
	}

	if got := string(execCompatCmd(t, db, c, "ZADD", "z", "XX", "3", "missing").ToBytes()); got != ":0\r\n" {
		t.Fatalf("ZADD XX missing = %q, want :0", got)
	}
	if got := string(execCompatCmd(t, db, c, "EXISTS", "z").ToBytes()); got != ":1\r\n" {
		t.Fatalf("zset missing: %q", got)
	}

	execCompatCmd(t, db, c, "ZADD", "z", "GT", "5", "a") // 1 -> 5 (added count 0)
	if got := string(execCompatCmd(t, db, c, "ZSCORE", "z", "a").ToBytes()); got != "$1\r\n5\r\n" {
		t.Fatalf("ZADD GT = %q", got)
	}
	execCompatCmd(t, db, c, "ZADD", "z", "GT", "3", "a") // not greater, ignored
	if got := string(execCompatCmd(t, db, c, "ZSCORE", "z", "a").ToBytes()); got != "$1\r\n5\r\n" {
		t.Fatalf("ZADD GT lowered the score: %q", got)
	}
	execCompatCmd(t, db, c, "ZADD", "z", "LT", "2", "a")
	if got := string(execCompatCmd(t, db, c, "ZSCORE", "z", "a").ToBytes()); got != "$1\r\n2\r\n" {
		t.Fatalf("ZADD LT = %q", got)
	}
	if got := string(execCompatCmd(t, db, c, "ZADD", "z", "CH", "2", "a").ToBytes()); got != ":0\r\n" {
		t.Fatalf("ZADD CH same score = %q, want :0", got)
	}
	if got := string(execCompatCmd(t, db, c, "ZADD", "z", "CH", "9", "a").ToBytes()); got != ":1\r\n" {
		t.Fatalf("ZADD CH update = %q, want :1", got)
	}

	if got := string(execCompatCmd(t, db, c, "ZADD", "z", "INCR", "1", "a").ToBytes()); got != "$2\r\n10\r\n" {
		t.Fatalf("ZADD INCR = %q, want 10", got)
	}
	if got := string(execCompatCmd(t, db, c, "ZADD", "z", "INCR", "2", "b").ToBytes()); got != "$1\r\n2\r\n" {
		t.Fatalf("ZADD INCR new member = %q, want 2", got)
	}
	if got := string(execCompatCmd(t, db, c, "ZADD", "z", "NX", "INCR", "1", "a").ToBytes()); got != "$-1\r\n" {
		t.Fatalf("ZADD NX INCR existing = %q, want nil", got)
	}
	if reply := execCompatCmd(t, db, c, "ZADD", "z", "INCR", "1", "a", "2", "b"); !protocol.IsErrorReply(reply) {
		t.Fatalf("ZADD INCR with multiple pairs should fail, got %q", string(reply.ToBytes()))
	}
	if reply := execCompatCmd(t, db, c, "ZADD", "z", "NX", "XX", "1", "a"); !protocol.IsErrorReply(reply) {
		t.Fatalf("ZADD NX XX should fail, got %q", string(reply.ToBytes()))
	}
}

func TestExpireOptions(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()
	defer c.Close()

	execCompatCmd(t, db, c, "SET", "k", "v")
	if got := string(execCompatCmd(t, db, c, "EXPIRE", "k", "100", "NX").ToBytes()); got != ":1\r\n" {
		t.Fatalf("EXPIRE NX without ttl = %q, want :1", got)
	}
	if got := string(execCompatCmd(t, db, c, "EXPIRE", "k", "200", "NX").ToBytes()); got != ":0\r\n" {
		t.Fatalf("EXPIRE NX with ttl = %q, want :0", got)
	}
	if got := string(execCompatCmd(t, db, c, "EXPIRE", "k", "200", "XX").ToBytes()); got != ":1\r\n" {
		t.Fatalf("EXPIRE XX = %q, want :1", got)
	}
	if got := string(execCompatCmd(t, db, c, "EXPIRE", "k", "100", "GT").ToBytes()); got != ":0\r\n" {
		t.Fatalf("EXPIRE GT lower = %q, want :0", got)
	}
	if got := string(execCompatCmd(t, db, c, "EXPIRE", "k", "300", "GT").ToBytes()); got != ":1\r\n" {
		t.Fatalf("EXPIRE GT higher = %q, want :1", got)
	}
	if got := string(execCompatCmd(t, db, c, "EXPIRE", "k", "100", "LT").ToBytes()); got != ":1\r\n" {
		t.Fatalf("EXPIRE LT lower = %q, want :1", got)
	}
	if got := string(execCompatCmd(t, db, c, "EXPIRE", "missing", "100", "NX").ToBytes()); got != ":0\r\n" {
		t.Fatalf("EXPIRE NX missing = %q, want :0", got)
	}
	if reply := execCompatCmd(t, db, c, "EXPIRE", "k", "10", "NX", "XX"); !protocol.IsErrorReply(reply) {
		t.Fatalf("EXPIRE NX XX should fail, got %q", string(reply.ToBytes()))
	}
	if reply := execCompatCmd(t, db, c, "EXPIRE", "k", "10", "GT", "LT"); !protocol.IsErrorReply(reply) {
		t.Fatalf("EXPIRE GT LT should fail, got %q", string(reply.ToBytes()))
	}
}

func TestSetNXAndXXRespectExpiration(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()
	defer c.Close()

	execCompatCmd(t, db, c, "SET", "nx-key", "old", "PX", "1")
	execCompatCmd(t, db, c, "SET", "xx-key", "old", "PX", "1")
	time.Sleep(10 * time.Millisecond)

	// an expired key is absent for NX: the SET must succeed and clear the
	// stale deadline
	if got := string(execCompatCmd(t, db, c, "SET", "nx-key", "new", "NX").ToBytes()); got != "+OK\r\n" {
		t.Fatalf("SET NX on expired key = %q, want OK", got)
	}
	if got := string(execCompatCmd(t, db, c, "GET", "nx-key").ToBytes()); got != "$3\r\nnew\r\n" {
		t.Fatalf("SET NX value = %q", got)
	}
	if got := string(execCompatCmd(t, db, c, "TTL", "nx-key").ToBytes()); got != ":-1\r\n" {
		t.Fatalf("stale ttl survived SET NX: %s", got)
	}

	// XX must fail on an expired key
	if got := string(execCompatCmd(t, db, c, "SET", "xx-key", "new", "XX").ToBytes()); got != "$-1\r\n" {
		t.Fatalf("SET XX on expired key = %q, want nil", got)
	}
	if got := string(execCompatCmd(t, db, c, "EXISTS", "xx-key").ToBytes()); got != ":0\r\n" {
		t.Fatalf("SET XX resurrected the expired key")
	}
}

func TestServerCloseIdempotent(t *testing.T) {
	oldProperties := config.Properties
	defer func() { config.Properties = oldProperties }()
	config.Properties = &config.ServerProperties{
		Databases: 4,
		Dir:       t.TempDir(),
	}
	server := NewStandaloneServer()
	server.Close()
	server.Close() // must not panic or double-close the AOF/replication state
}
