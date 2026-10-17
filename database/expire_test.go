package database

import (
	"sync"
	"testing"
	"time"

	idatabase "github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol"
)

func execExpireTestCmd(db *DB, c redis.Connection, args ...string) redis.Reply {
	return db.Exec(c, utils.ToCmdLine(args...))
}

// TestConcurrentGetOnExpiredKey is the regression test for the crash where two
// GETs of an expired key removed it from the data dict while holding only the
// shared lock, producing "fatal error: concurrent map read and map write".
func TestConcurrentGetOnExpiredKey(t *testing.T) {
	db := makeTestDB()
	setupConn := connection.NewFakeConn()
	defer setupConn.Close()
	if r := execExpireTestCmd(db, setupConn, "SET", "race-key", "v", "PX", "1"); protocol.IsErrorReply(r) {
		t.Fatalf("SET failed: %s", string(r.ToBytes()))
	}
	time.Sleep(10 * time.Millisecond)

	const workers = 16
	const iterations = 300
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := connection.NewFakeConn()
			defer c.Close()
			for j := 0; j < iterations; j++ {
				reply := execExpireTestCmd(db, c, "GET", "race-key")
				if _, ok := reply.(*protocol.NullBulkReply); !ok {
					t.Errorf("GET of an expired key returned %s, want null bulk", string(reply.ToBytes()))
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestExpireInPastDeletesKey(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()
	defer c.Close()

	if r := execExpireTestCmd(db, c, "SET", "k", "v"); protocol.IsErrorReply(r) {
		t.Fatalf("SET failed: %s", string(r.ToBytes()))
	}
	if r := execExpireTestCmd(db, c, "EXPIRE", "k", "-1"); protocol.IsErrorReply(r) {
		t.Fatalf("EXPIRE failed: %s", string(r.ToBytes()))
	}
	if r := execExpireTestCmd(db, c, "EXISTS", "k"); string(r.ToBytes()) != ":0\r\n" {
		t.Fatalf("EXISTS after a past EXPIRE = %q, want :0", string(r.ToBytes()))
	}
	if r := execExpireTestCmd(db, c, "TTL", "k"); string(r.ToBytes()) != ":-2\r\n" {
		t.Fatalf("TTL after a past EXPIRE = %q, want :-2", string(r.ToBytes()))
	}
}

// TestPutEntityClearsExpiredTTL ensures a write that re-creates an expired key
// does not inherit the stale deadline and get deleted by the time wheel.
func TestPutEntityClearsExpiredTTL(t *testing.T) {
	db := makeTestDB()

	// simulate a key whose deadline passed but whose wheel task has not fired
	db.data.Put("k", &idatabase.DataEntity{Data: []byte("old")})
	db.ttlMap.Put("k", time.Now().Add(-time.Hour))
	db.PutEntity("k", &idatabase.DataEntity{Data: []byte("new")})
	if _, ok := db.ttlMap.Get("k"); ok {
		t.Fatal("PutEntity kept an expired ttl for the re-created key")
	}

	// a valid ttl must survive an update
	db.ttlMap.Put("k", time.Now().Add(time.Hour))
	db.PutEntity("k", &idatabase.DataEntity{Data: []byte("newer")})
	if _, ok := db.ttlMap.Get("k"); !ok {
		t.Fatal("PutEntity dropped a valid ttl")
	}
}

func TestForEachSkipsExpiredKeys(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()
	defer c.Close()

	if r := execExpireTestCmd(db, c, "SET", "live", "1"); protocol.IsErrorReply(r) {
		t.Fatalf("SET live failed: %s", string(r.ToBytes()))
	}
	if r := execExpireTestCmd(db, c, "SET", "dead", "1", "PX", "1"); protocol.IsErrorReply(r) {
		t.Fatalf("SET dead failed: %s", string(r.ToBytes()))
	}
	time.Sleep(10 * time.Millisecond)

	seen := make(map[string]bool)
	db.ForEach(func(key string, _ *idatabase.DataEntity, _ *time.Time) bool {
		seen[key] = true
		return true
	})
	if seen["dead"] {
		t.Fatal("ForEach returned an expired key")
	}
	if !seen["live"] {
		t.Fatal("ForEach did not return the live key")
	}
}
