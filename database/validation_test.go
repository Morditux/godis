package database

import (
	"testing"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol"
)

func runValidationCmd(t *testing.T, db *DB, c redis.Connection, args ...string) redis.Reply {
	t.Helper()
	return db.Exec(c, utils.ToCmdLine(args...))
}

func assertReplyErr(t *testing.T, reply redis.Reply) {
	t.Helper()
	if !protocol.IsErrorReply(reply) {
		t.Fatalf("expected an error reply, got %q", string(reply.ToBytes()))
	}
}

func assertReplyBytes(t *testing.T, reply redis.Reply, want string) {
	t.Helper()
	if got := string(reply.ToBytes()); got != want {
		t.Fatalf("reply = %q, want %q", got, want)
	}
}

func TestInvalidCommandArgumentsDoNotPanic(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()
	defer c.Close()

	// LPOP/RPOP negative count used to panic in make([]byte, count)
	runValidationCmd(t, db, c, "RPUSH", "list", "a")
	assertReplyErr(t, runValidationCmd(t, db, c, "LPOP", "list", "-1"))
	assertReplyErr(t, runValidationCmd(t, db, c, "RPOP", "list", "-1"))
	assertReplyBytes(t, runValidationCmd(t, db, c, "LPOP", "list", "0"), "*0\r\n")
	// validation also applies when the key does not exist
	assertReplyErr(t, runValidationCmd(t, db, c, "LPOP", "missing-list", "-1"))
	assertReplyBytes(t, runValidationCmd(t, db, c, "LPOP", "missing-list"), "$-1\r\n")
	assertReplyBytes(t, runValidationCmd(t, db, c, "LPOP", "missing-list", "2"), "*0\r\n")

	// SETRANGE/SETBIT/GETBIT offsets
	assertReplyErr(t, runValidationCmd(t, db, c, "SETRANGE", "str", "-1", "x"))
	assertReplyErr(t, runValidationCmd(t, db, c, "SETBIT", "bits", "-1", "1"))
	assertReplyErr(t, runValidationCmd(t, db, c, "SETBIT", "bits", "999999999999999", "1"))
	assertReplyErr(t, runValidationCmd(t, db, c, "GETBIT", "bits", "-1"))

	// BITCOUNT accepts either no range or start+end; a lone start is a syntax
	// error, not an index-out-of-range panic.
	runValidationCmd(t, db, c, "SET", "bits", "abc")
	assertReplyErr(t, runValidationCmd(t, db, c, "BITCOUNT", "bits", "0"))
	assertReplyBytes(t, runValidationCmd(t, db, c, "BITCOUNT", "bits", "0", "1"), ":6\r\n")
	// BITPOS with only start is valid
	assertReplyBytes(t, runValidationCmd(t, db, c, "BITPOS", "bits", "1", "0"), ":0\r\n")

	// SCAN option parsing used to panic when the option had no value
	assertReplyErr(t, runValidationCmd(t, db, c, "SCAN", "0", "COUNT"))
	assertReplyErr(t, runValidationCmd(t, db, c, "SCAN", "0", "MATCH"))
	assertReplyErr(t, runValidationCmd(t, db, c, "SCAN", "0", "COUNT", "0"))
	runValidationCmd(t, db, c, "SADD", "set", "a")
	assertReplyErr(t, runValidationCmd(t, db, c, "SSCAN", "set", "0", "COUNT"))
	runValidationCmd(t, db, c, "HSET", "hash", "f", "v")
	assertReplyErr(t, runValidationCmd(t, db, c, "HSCAN", "hash", "0", "MATCH"))
	runValidationCmd(t, db, c, "ZADD", "zset", "1", "m")
	assertReplyErr(t, runValidationCmd(t, db, c, "ZSCAN", "zset", "0", "COUNT"))
}

func TestIntegerOverflow(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()
	defer c.Close()

	runValidationCmd(t, db, c, "SET", "max", "9223372036854775807")
	assertReplyErr(t, runValidationCmd(t, db, c, "INCR", "max"))
	assertReplyErr(t, runValidationCmd(t, db, c, "INCRBY", "max", "1"))
	assertReplyBytes(t, runValidationCmd(t, db, c, "GET", "max"), "$19\r\n9223372036854775807\r\n")

	runValidationCmd(t, db, c, "SET", "min", "-9223372036854775808")
	assertReplyErr(t, runValidationCmd(t, db, c, "DECR", "min"))
	assertReplyErr(t, runValidationCmd(t, db, c, "DECRBY", "min", "9223372036854775807"))
	assertReplyErr(t, runValidationCmd(t, db, c, "DECRBY", "missing", "-9223372036854775808"))
}

func TestFloatSpecialValuesRejected(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()
	defer c.Close()

	runValidationCmd(t, db, c, "SET", "f", "1")
	assertReplyErr(t, runValidationCmd(t, db, c, "INCRBYFLOAT", "f", "nan"))
	assertReplyErr(t, runValidationCmd(t, db, c, "INCRBYFLOAT", "f", "inf"))
	assertReplyBytes(t, runValidationCmd(t, db, c, "GET", "f"), "$1\r\n1\r\n")

	assertReplyErr(t, runValidationCmd(t, db, c, "HINCRBYFLOAT", "h", "field", "nan"))
	assertReplyBytes(t, runValidationCmd(t, db, c, "EXISTS", "h"), ":0\r\n")

	assertReplyErr(t, runValidationCmd(t, db, c, "ZADD", "z", "nan", "m"))
	assertReplyErr(t, runValidationCmd(t, db, c, "ZINCRBY", "z", "nan", "m"))
	runValidationCmd(t, db, c, "ZADD", "z", "+inf", "m")
	assertReplyErr(t, runValidationCmd(t, db, c, "ZINCRBY", "z", "-inf", "m"))
}

func TestTTLOverflowRejected(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()
	defer c.Close()

	runValidationCmd(t, db, c, "SET", "k", "v")
	assertReplyErr(t, runValidationCmd(t, db, c, "SET", "k", "v", "EX", "9223372036854775807"))
	assertReplyErr(t, runValidationCmd(t, db, c, "SET", "k", "v", "PX", "9223372036854775807"))
	assertReplyErr(t, runValidationCmd(t, db, c, "EXPIRE", "k", "9223372036854775807"))
	assertReplyErr(t, runValidationCmd(t, db, c, "PEXPIRE", "k", "9223372036854775807"))
	assertReplyErr(t, runValidationCmd(t, db, c, "EXPIREAT", "k", "9223372036854775807"))
	assertReplyErr(t, runValidationCmd(t, db, c, "PEXPIREAT", "k", "9223372036854775807"))
}

func TestRenameSameKeyKeepsValue(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()
	defer c.Close()

	runValidationCmd(t, db, c, "SET", "same", "value")
	assertReplyBytes(t, runValidationCmd(t, db, c, "RENAME", "same", "same"), "+OK\r\n")
	assertReplyBytes(t, runValidationCmd(t, db, c, "GET", "same"), "$5\r\nvalue\r\n")
}

func TestCopyIsDeepAndInheritsTTL(t *testing.T) {
	mdb := NewStandaloneServer()
	defer mdb.Close()
	c := connection.NewFakeConn()
	defer c.Close()

	// list copy must not share the underlying container
	mdb.Exec(c, utils.ToCmdLine("RPUSH", "src-list", "a"))
	assertReplyBytes(t, mdb.Exec(c, utils.ToCmdLine("COPY", "src-list", "dst-list")), ":1\r\n")
	mdb.Exec(c, utils.ToCmdLine("RPUSH", "dst-list", "b"))
	assertReplyBytes(t, mdb.Exec(c, utils.ToCmdLine("LRANGE", "src-list", "0", "-1")), "*1\r\n$1\r\na\r\n")
	assertReplyBytes(t, mdb.Exec(c, utils.ToCmdLine("LRANGE", "dst-list", "0", "-1")), "*2\r\n$1\r\na\r\n$1\r\nb\r\n")

	// source ttl is copied to the destination
	mdb.Exec(c, utils.ToCmdLine("SET", "src-ttl", "v"))
	mdb.Exec(c, utils.ToCmdLine("EXPIRE", "src-ttl", "100"))
	assertReplyBytes(t, mdb.Exec(c, utils.ToCmdLine("COPY", "src-ttl", "dst-ttl")), ":1\r\n")
	ttlReply := mdb.Exec(c, utils.ToCmdLine("TTL", "dst-ttl"))
	if string(ttlReply.ToBytes()) == ":-1\r\n" || string(ttlReply.ToBytes()) == ":-2\r\n" {
		t.Fatalf("COPY did not inherit the source ttl: %q", string(ttlReply.ToBytes()))
	}

	// replacing a destination with a non-volatile source clears its old ttl
	mdb.Exec(c, utils.ToCmdLine("SET", "dst-old", "old"))
	mdb.Exec(c, utils.ToCmdLine("EXPIRE", "dst-old", "100"))
	mdb.Exec(c, utils.ToCmdLine("SET", "src-plain", "new"))
	assertReplyBytes(t, mdb.Exec(c, utils.ToCmdLine("COPY", "src-plain", "dst-old", "REPLACE")), ":1\r\n")
	assertReplyBytes(t, mdb.Exec(c, utils.ToCmdLine("TTL", "dst-old")), ":-1\r\n")
}
