package database

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol"
)

func newReplicationTestServer(t *testing.T) *Server {
	t.Helper()
	oldProperties := config.Properties
	config.Properties = &config.ServerProperties{
		Databases:   4,
		Dir:         t.TempDir(),
		ReplTimeout: 1,
	}
	t.Cleanup(func() { config.Properties = oldProperties })
	server := NewStandaloneServer()
	t.Cleanup(server.Close)
	return server
}

func TestPSyncWithoutAOF(t *testing.T) {
	server := newReplicationTestServer(t)
	conn := connection.NewFakeConn()
	reply := server.Exec(conn, utils.ToCmdLine("PSYNC", "?", "-1"))
	if !protocol.IsErrorReply(reply) {
		t.Fatalf("PSYNC without AOF = %q, want an error", string(reply.ToBytes()))
	}
	if !strings.Contains(string(reply.ToBytes()), "appendonly") {
		t.Fatalf("PSYNC error should mention appendonly, got %q", string(reply.ToBytes()))
	}
}

func TestMasterBacklogOffsetGuard(t *testing.T) {
	server := newReplicationTestServer(t)
	slaveConn := connection.NewFakeConn()
	slave := newSlaveClient(slaveConn)

	server.masterStatus.mu.Lock()
	server.masterStatus.backlog = &replBacklog{
		buf:           make([]byte, 10),
		beginOffset:   5,
		currentOffset: 15,
	}
	server.masterStatus.slaveMap[slaveConn] = slave
	server.masterStatus.onlineSlaves[slave] = struct{}{}
	server.masterStatus.mu.Unlock()

	// offset 0 is before beginOffset 5: the guard must disconnect the slave
	// instead of slicing the backlog with a negative index.
	server.masterSendUpdatesToSlave()

	server.masterStatus.mu.RLock()
	_, online := server.masterStatus.onlineSlaves[slave]
	server.masterStatus.mu.RUnlock()
	if online {
		t.Fatal("slave with an out-of-range backlog offset was not disconnected")
	}
}

func TestMasterCronPrunesStaleSlave(t *testing.T) {
	server := newReplicationTestServer(t)
	slaveConn := connection.NewFakeConn()
	slave := newSlaveClient(slaveConn)
	slave.lastAckTime = time.Now().Add(-time.Minute)

	server.masterStatus.mu.Lock()
	server.masterStatus.slaveMap[slaveConn] = slave
	server.masterStatus.onlineSlaves[slave] = struct{}{}
	server.masterStatus.mu.Unlock()

	server.masterCron()

	server.masterStatus.mu.RLock()
	_, online := server.masterStatus.onlineSlaves[slave]
	_, known := server.masterStatus.slaveMap[slaveConn]
	server.masterStatus.mu.RUnlock()
	if online || known {
		t.Fatal("slave that stopped ACKing was not pruned")
	}
}

func TestLoadDBPreservesCallbacks(t *testing.T) {
	server := newReplicationTestServer(t)
	var inserted, deleted int32
	server.SetKeyInsertedCallback(func(dbIndex int, key string, entity *database.DataEntity) {
		atomic.AddInt32(&inserted, 1)
	})
	server.SetKeyDeletedCallback(func(dbIndex int, key string, entity *database.DataEntity) {
		atomic.AddInt32(&deleted, 1)
	})

	// full sync / FLUSHDB replaces the DB object; callbacks must survive
	server.loadDB(0, makeDB())
	db := server.mustSelectDB(0)
	db.PutEntity("callback-key", &database.DataEntity{Data: []byte("v")})
	db.Remove("callback-key")

	if got := atomic.LoadInt32(&inserted); got != 1 {
		t.Fatalf("insert callback called %d times, want 1", got)
	}
	if got := atomic.LoadInt32(&deleted); got != 1 {
		t.Fatalf("delete callback called %d times, want 1", got)
	}
}

func TestWaitCountsAckedReplicas(t *testing.T) {
	server := newReplicationTestServer(t)
	first := newSlaveClient(connection.NewFakeConn())
	second := newSlaveClient(connection.NewFakeConn())
	first.ackOffset = 100
	second.ackOffset = 50

	server.masterStatus.mu.Lock()
	server.masterStatus.backlog = &replBacklog{currentOffset: 100}
	server.masterStatus.onlineSlaves[first] = struct{}{}
	server.masterStatus.onlineSlaves[second] = struct{}{}
	server.masterStatus.mu.Unlock()

	// only the first replica has reached offset 100; wait for two with a
	// short timeout and return the actual count
	reply := server.execWait(nil, [][]byte{[]byte("2"), []byte("40")})
	if got := string(reply.ToBytes()); got != ":1\r\n" {
		t.Fatalf("WAIT = %q, want :1", got)
	}
}

func TestWaitWakesWhenAckArrives(t *testing.T) {
	server := newReplicationTestServer(t)
	slave := newSlaveClient(connection.NewFakeConn())
	server.masterStatus.mu.Lock()
	server.masterStatus.backlog = &replBacklog{currentOffset: 10}
	server.masterStatus.onlineSlaves[slave] = struct{}{}
	server.masterStatus.mu.Unlock()

	result := make(chan string, 1)
	go func() {
		result <- string(server.execWait(nil, [][]byte{[]byte("1"), []byte("1000")}).ToBytes())
	}()
	time.Sleep(20 * time.Millisecond)
	slave.mu.Lock()
	slave.ackOffset = 10
	slave.mu.Unlock()

	select {
	case got := <-result:
		if got != ":1\r\n" {
			t.Fatalf("WAIT = %q, want :1", got)
		}
	case <-time.After(time.Second):
		t.Fatal("WAIT did not wake up after the replica acknowledged")
	}
}

func TestAckDoesNotRewindSendOffset(t *testing.T) {
	server := newReplicationTestServer(t)
	conn := connection.NewFakeConn()
	slave := newSlaveClient(conn)
	slave.sendOffset = 100
	slave.ackOffset = 50
	server.masterStatus.mu.Lock()
	server.masterStatus.slaveMap[conn] = slave
	server.masterStatus.mu.Unlock()

	reply := server.execReplConf(conn, [][]byte{[]byte("ACK"), []byte("60")})
	if _, ok := reply.(*protocol.NoReply); !ok {
		t.Fatalf("REPLCONF ACK = %T %q, want NoReply", reply, reply.ToBytes())
	}
	slave.mu.Lock()
	sendOffset, ackOffset := slave.sendOffset, slave.ackOffset
	slave.mu.Unlock()
	if sendOffset != 100 || ackOffset != 60 {
		t.Fatalf("offsets after ACK = send:%d ack:%d, want send:100 ack:60", sendOffset, ackOffset)
	}
}

func TestWaitStopsWhenClientCloses(t *testing.T) {
	server := newReplicationTestServer(t)
	conn := connection.NewFakeConn()
	server.masterStatus.mu.Lock()
	server.masterStatus.backlog = &replBacklog{currentOffset: 100}
	server.masterStatus.mu.Unlock()

	result := make(chan string, 1)
	go func() {
		result <- string(server.execWait(conn, [][]byte{[]byte("1"), []byte("0")}).ToBytes())
	}()
	time.Sleep(20 * time.Millisecond)
	_ = conn.Close()

	select {
	case got := <-result:
		if got != ":0\r\n" {
			t.Fatalf("WAIT = %q, want :0", got)
		}
	case <-time.After(time.Second):
		t.Fatal("WAIT kept blocking after the client connection was closed")
	}
}

type blockingReplicaConn struct {
	*connection.FakeConn
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingReplicaConn) Write(b []byte) (int, error) {
	c.once.Do(func() { close(c.started) })
	<-c.release
	return c.FakeConn.Write(b)
}

func TestNotifySlavesDoesNotBlockOnSlowReplica(t *testing.T) {
	server := newReplicationTestServer(t)
	blocking := &blockingReplicaConn{
		FakeConn: connection.NewFakeConn(),
		started:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	slave := newSlaveClient(blocking)
	server.masterStatus.mu.Lock()
	server.masterStatus.backlog = &replBacklog{}
	server.masterStatus.backlog.appendBytes([]byte("*1\r\n$4\r\nping\r\n"))
	server.masterStatus.mu.Unlock()

	// start at offset 0 so the sender has backlog data to write
	server.setSlaveOnline(slave, 0)
	select {
	case <-blocking.started:
	case <-time.After(time.Second):
		t.Fatal("replica sender did not start")
	}

	// the sender is now blocked inside Write; the AOF path must not wait for it
	done := make(chan struct{})
	go func() {
		server.notifySlaves()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("notifySlaves blocked on a slow replica")
	}

	close(blocking.release)
	server.removeSlave(slave)
}
