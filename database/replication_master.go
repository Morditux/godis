package database

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/atomic"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

const (
	// replWriteTimeout bounds a single backlog write to a replica. Without it
	// a slow replica blocks the AOF writer (which forwards updates) forever.
	replWriteTimeout = 5 * time.Second
	// replRDBWriteTimeout bounds the full-sync RDB transfer; it runs in its own
	// goroutine but must not hang forever.
	replRDBWriteTimeout = 5 * time.Minute
)

type writeDeadliner interface {
	SetWriteDeadline(time.Time) error
}

func setReplWriteDeadline(conn redis.Connection, timeout time.Duration) {
	if conn == nil {
		return
	}
	dl, ok := conn.(writeDeadliner)
	if !ok {
		return
	}
	if timeout > 0 {
		_ = dl.SetWriteDeadline(time.Now().Add(timeout))
		return
	}
	_ = dl.SetWriteDeadline(time.Time{})
}

// newTempRDBPath reserves a unique path for an RDB file. The placeholder is
// removed so GenerateRDBForReplication can atomically rename its own temp
// file into place (and so the empty file does not leak on every save).
func newTempRDBPath() (string, error) {
	// create the placeholder in the configured dir: GenerateRDBForReplication
	// creates its temp file in the same directory, which keeps the final rename
	// on the same filesystem.
	f, err := os.CreateTemp(config.GetTmpDir(), "*.rdb")
	if err != nil {
		return "", err
	}
	name := f.Name()
	if err := f.Close(); err != nil {
		_ = os.Remove(name)
		return "", err
	}
	if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
		return "", err
	}
	return name, nil
}

/*
`execPSync` receives `slaveof` command. Depending on the status of rdb, there are two branches:

Branch A: rdb is not ready
1. Master adds the client into `server.masterStatus.waitSlaves`.
2. If there is no rdb generation in progress, calls `saveForReplication` to start.
3. `persister.GenerateRDBForReplication` generates rdb file and sends following updates after rdb to parameter `listener`
4. After the rdb generation is done, `saveForReplication` calls `masterFullReSyncWithSlave` to send rdb to `waitSlaves`

Branch B: rdb is ready
runs `masterTryPartialSyncWithSlave` if cannot go partial sync, go to `masterFullReSyncWithSlave`
*/

const (
	bgSaveIdle = uint8(iota)
	bgSaveRunning
	bgSaveFinish
)

const (
	slaveCapacityNone = 0
	slaveCapacityEOF  = 1 << iota
	slaveCapacityPsync2
)

// slaveClient stores slave status in the view of master
type slaveClient struct {
	conn redis.Connection
	// mu guards sendOffset/ackOffset/lastAckTime. sendOffset is the last byte
	// handed to the socket; ackOffset is the last byte the replica confirmed.
	// Keeping them separate avoids replaying data whenever an ACK lags.
	mu           sync.Mutex
	sendOffset   int64
	ackOffset    int64
	lastAckTime  time.Time
	announceIp   string
	announcePort int
	capacity     uint8

	// sendCh wakes this replica's sender; the backlog itself stays shared.
	sendCh     chan struct{}
	doneCh     chan struct{}
	doneOnce   sync.Once
	senderOnce sync.Once
}

func newSlaveClient(conn redis.Connection) *slaveClient {
	return &slaveClient{
		conn:   conn,
		sendCh: make(chan struct{}, 1),
		doneCh: make(chan struct{}),
	}
}

func (slave *slaveClient) notify() {
	if slave == nil || slave.sendCh == nil {
		return
	}
	select {
	case slave.sendCh <- struct{}{}:
	default:
	}
}

func (slave *slaveClient) stopSender() {
	if slave == nil || slave.doneCh == nil {
		return
	}
	slave.doneOnce.Do(func() { close(slave.doneCh) })
}

// aofListener is currently only responsible for updating the backlog
type replBacklog struct {
	buf           []byte
	beginOffset   int64
	currentOffset int64
}

func (backlog *replBacklog) appendBytes(bin []byte) {
	backlog.buf = append(backlog.buf, bin...)
	backlog.currentOffset += int64(len(bin))
}

func (backlog *replBacklog) getSnapshot() ([]byte, int64) {
	return backlog.buf[:], backlog.currentOffset
}

func (backlog *replBacklog) getSnapshotAfter(beginOffset int64) ([]byte, int64) {
	beg := beginOffset - backlog.beginOffset
	return backlog.buf[beg:], backlog.currentOffset
}

func (backlog *replBacklog) isValidOffset(offset int64) bool {
	return offset >= backlog.beginOffset && offset < backlog.currentOffset
}

// ackNotifier wakes WAIT clients when replication state changes. The channel
// is replaced under its own mutex, so WAIT can snapshot it before checking the
// ACK counters without a lost-wakeup race.
type ackNotifier struct {
	mu sync.Mutex
	ch chan struct{}
}

func (n *ackNotifier) notify() {
	n.mu.Lock()
	if n.ch != nil {
		close(n.ch)
	}
	n.ch = make(chan struct{})
	n.mu.Unlock()
}

func (n *ackNotifier) waitChan() <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.ch == nil {
		n.ch = make(chan struct{})
	}
	return n.ch
}

type masterStatus struct {
	mu           sync.RWMutex
	ackNotifier  ackNotifier
	replId       string
	backlog      *replBacklog
	slaveMap     map[redis.Connection]*slaveClient
	waitSlaves   map[*slaveClient]struct{}
	onlineSlaves map[*slaveClient]struct{}
	bgSaveState  uint8
	rdbFilename  string
	aofListener  *replAofListener
	rewriting    atomic.Boolean
}

// bgSaveForReplication does bg-save and send rdb to waiting slaves
func (server *Server) bgSaveForReplication() {
	go func() {
		defer func() {
			if e := recover(); e != nil {
				logger.Errorf("panic: %v", e)
			}
		}()
		if err := server.saveForReplication(); err != nil {
			logger.Errorf("save for replication error: %v", err)
		}
	}()

}

// saveForReplication does bg-save and send rdb to waiting slaves
func (server *Server) saveForReplication() error {
	if server.persister == nil {
		return errors.New("replication requires appendonly yes")
	}
	rdbFilename, err := newTempRDBPath()
	if err != nil {
		return fmt.Errorf("create temp rdb failed: %v", err)
	}
	server.masterStatus.mu.Lock()
	server.masterStatus.bgSaveState = bgSaveRunning
	server.masterStatus.rdbFilename = rdbFilename // todo: can reuse config.Properties.RDBFilename?
	aofListener := &replAofListener{
		mdb:     server,
		backlog: server.masterStatus.backlog,
	}
	server.masterStatus.aofListener = aofListener
	server.masterStatus.mu.Unlock()

	err = server.persister.GenerateRDBForReplication(rdbFilename, aofListener, nil)
	if err != nil {
		// do not leave bgSaveState=RUNNING forever: waitSlaves would never be
		// served and every later PSYNC would wait for a save that already failed.
		server.masterStatus.mu.Lock()
		if server.masterStatus.aofListener == aofListener {
			server.masterStatus.aofListener = nil
		}
		server.masterStatus.bgSaveState = bgSaveIdle
		server.masterStatus.mu.Unlock()
		return err
	}
	aofListener.readyToSend.Set(true)

	// change bgSaveState and get waitSlaves for sending
	waitSlaves := make(map[*slaveClient]struct{})
	server.masterStatus.mu.Lock()
	server.masterStatus.bgSaveState = bgSaveFinish
	for slave := range server.masterStatus.waitSlaves {
		waitSlaves[slave] = struct{}{}
	}
	server.masterStatus.waitSlaves = nil
	server.masterStatus.mu.Unlock()

	// send rdb to waiting slaves in parallel: one slow replica must not delay
	// the full sync of the others
	var wg sync.WaitGroup
	for slave := range waitSlaves {
		wg.Add(1)
		go func(slave *slaveClient) {
			defer wg.Done()
			defer func() {
				if err := recover(); err != nil {
					logger.Errorf("masterFullReSyncWithSlave panic: %v", err)
					server.removeSlave(slave)
				}
			}()
			if err := server.masterFullReSyncWithSlave(slave); err != nil {
				server.removeSlave(slave)
				logger.Errorf("masterFullReSyncWithSlave error: %v", err)
			}
		}(slave)
	}
	wg.Wait()
	return nil
}

func (server *Server) rewriteRDB() error {
	if server.persister == nil {
		return errors.New("replication requires appendonly yes")
	}
	rdbFilename, err := newTempRDBPath()
	if err != nil {
		return fmt.Errorf("create temp rdb failed: %v", err)
	}
	newBacklog := &replBacklog{}
	aofListener := &replAofListener{
		backlog: newBacklog,
		mdb:     server,
	}
	hook := func() {
		// pausing aof first, then lock masterStatus.
		// use the same order as replAofListener to avoid dead lock
		server.masterStatus.mu.Lock()
		defer server.masterStatus.mu.Unlock()
		newBacklog.beginOffset = server.masterStatus.backlog.currentOffset
	}
	err = server.persister.GenerateRDBForReplication(rdbFilename, aofListener, hook)
	if err != nil { // wait rdb result
		return err
	}
	server.masterStatus.mu.Lock()
	server.masterStatus.rdbFilename = rdbFilename
	server.masterStatus.backlog = newBacklog
	server.persister.RemoveListener(server.masterStatus.aofListener)
	server.masterStatus.aofListener = aofListener
	server.masterStatus.mu.Unlock()
	// setting readyToSend=true must happen after the new backlog is really
	// ready (that means masterStatus.mu.Unlock above); reading it from the
	// aof goroutine races otherwise, so it stays an atomic flag
	aofListener.readyToSend.Set(true)
	return nil
}

// masterFullReSyncWithSlave send replication header, rdb file and all backlogs to slave
func (server *Server) masterFullReSyncWithSlave(slave *slaveClient) error {
	setReplWriteDeadline(slave.conn, replRDBWriteTimeout)
	defer setReplWriteDeadline(slave.conn, 0)

	server.masterStatus.mu.RLock()
	replId := server.masterStatus.replId
	rdbFilename := server.masterStatus.rdbFilename
	beginOffset := server.masterStatus.backlog.beginOffset
	server.masterStatus.mu.RUnlock()
	if rdbFilename == "" {
		return errors.New("no rdb file available for replication")
	}

	// write replication header
	header := "+FULLRESYNC " + replId + " " +
		strconv.FormatInt(beginOffset, 10) + protocol.CRLF
	_, err := slave.conn.Write([]byte(header))
	if err != nil {
		return fmt.Errorf("write replication header to slave failed: %v", err)
	}
	// send rdb
	rdbFile, err := os.Open(rdbFilename)
	if err != nil {
		return fmt.Errorf("open rdb file %s for replication error: %v", rdbFilename, err)
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	rdbInfo, err := rdbFile.Stat()
	if err != nil {
		return fmt.Errorf("stat rdb file %s for replication error: %v", rdbFilename, err)
	}
	rdbHeader := "$" + strconv.FormatInt(rdbInfo.Size(), 10) + protocol.CRLF
	_, err = slave.conn.Write([]byte(rdbHeader))
	if err != nil {
		return fmt.Errorf("write rdb header to slave failed: %v", err)
	}
	_, err = io.Copy(slave.conn, rdbFile)
	if err != nil {
		return fmt.Errorf("write rdb file to slave failed: %v", err)
	}

	// send backlog
	server.masterStatus.mu.RLock()
	backlog, currentOffset := server.masterStatus.backlog.getSnapshot()
	server.masterStatus.mu.RUnlock()
	_, err = slave.conn.Write(backlog)
	if err != nil {
		return fmt.Errorf("full resync write backlog to slave failed: %v", err)
	}

	// set slave as online
	server.setSlaveOnline(slave, currentOffset)
	return nil
}

var cannotPartialSync = errors.New("cannot do partial sync")

func (server *Server) masterTryPartialSyncWithSlave(slave *slaveClient, replId string, slaveOffset int64) error {
	setReplWriteDeadline(slave.conn, replWriteTimeout)
	defer setReplWriteDeadline(slave.conn, 0)

	server.masterStatus.mu.RLock()
	currentReplId := server.masterStatus.replId
	if replId != currentReplId {
		server.masterStatus.mu.RUnlock()
		return cannotPartialSync
	}
	if !server.masterStatus.backlog.isValidOffset(slaveOffset) {
		server.masterStatus.mu.RUnlock()
		return cannotPartialSync
	}
	backlog, currentOffset := server.masterStatus.backlog.getSnapshotAfter(slaveOffset)
	server.masterStatus.mu.RUnlock()

	// send replication header
	header := "+CONTINUE " + currentReplId + protocol.CRLF
	_, err := slave.conn.Write([]byte(header))
	if err != nil {
		return fmt.Errorf("write replication header to slave failed: %v", err)
	}
	// send backlog
	_, err = slave.conn.Write(backlog)
	if err != nil {
		return fmt.Errorf("partial resync write backlog to slave failed: %v", err)
	}

	// set slave online
	server.setSlaveOnline(slave, currentOffset)
	return nil
}

// notifySlaves wakes the per-replica senders without blocking the AOF writer.
func (server *Server) notifySlaves() {
	server.masterStatus.mu.RLock()
	defer server.masterStatus.mu.RUnlock()
	for slave := range server.masterStatus.onlineSlaves {
		slave.notify() // non-blocking, buffered channel
	}
}

// slaveSender is the only goroutine writing replication updates to one slave.
func (server *Server) slaveSender(slave *slaveClient) {
	for {
		select {
		case <-slave.doneCh:
			return
		case <-server.closeChan:
			return
		case <-slave.sendCh:
			if err := server.sendUpdatesToSlave(slave); err != nil {
				logger.Errorf("send updates backlog to slave failed: %v", err)
				server.removeSlave(slave)
				return
			}
		}
	}
}

// sendUpdatesToSlave sends the backlog range this particular slave has not
// received yet. It must only be called by that slave's sender goroutine.
func (server *Server) sendUpdatesToSlave(slave *slaveClient) error {
	server.masterStatus.mu.RLock()
	if _, online := server.masterStatus.onlineSlaves[slave]; !online {
		server.masterStatus.mu.RUnlock()
		return nil
	}
	backlog := server.masterStatus.backlog
	if backlog == nil {
		server.masterStatus.mu.RUnlock()
		return nil
	}
	beginOffset := backlog.beginOffset
	backlogBytes, currentOffset := backlog.getSnapshot()
	slave.mu.Lock()
	slaveBeginOffset := slave.sendOffset - beginOffset
	slave.mu.Unlock()
	server.masterStatus.mu.RUnlock()

	if slaveBeginOffset < 0 || slaveBeginOffset > int64(len(backlogBytes)) {
		return fmt.Errorf("slave backlog offset %d out of range [0,%d]", slaveBeginOffset, len(backlogBytes))
	}
	if slaveBeginOffset == int64(len(backlogBytes)) {
		return nil
	}
	setReplWriteDeadline(slave.conn, replWriteTimeout)
	_, err := slave.conn.Write(backlogBytes[slaveBeginOffset:])
	setReplWriteDeadline(slave.conn, 0)
	if err != nil {
		return err
	}
	slave.mu.Lock()
	if slave.sendOffset < currentOffset {
		slave.sendOffset = currentOffset
	}
	slave.mu.Unlock()
	return nil
}

// masterSendUpdatesToSlave performs one synchronous flush for all online
// slaves. Production uses the per-slave senders; this helper is kept for
// tests and explicit flushes.
func (server *Server) masterSendUpdatesToSlave() error {
	server.masterStatus.mu.RLock()
	slaves := make([]*slaveClient, 0, len(server.masterStatus.onlineSlaves))
	for slave := range server.masterStatus.onlineSlaves {
		slaves = append(slaves, slave)
	}
	server.masterStatus.mu.RUnlock()
	for _, slave := range slaves {
		if err := server.sendUpdatesToSlave(slave); err != nil {
			logger.Errorf("send updates backlog to slave failed: %v", err)
			server.removeSlave(slave)
		}
	}
	return nil
}

// countAckedSlaves returns how many online replicas have acknowledged at
// least target offset.
func (server *Server) countAckedSlaves(target int64) int {
	server.masterStatus.mu.RLock()
	defer server.masterStatus.mu.RUnlock()
	count := 0
	for slave := range server.masterStatus.onlineSlaves {
		slave.mu.Lock()
		acked := slave.ackOffset
		slave.mu.Unlock()
		if acked >= target {
			count++
		}
	}
	return count
}

// execWait implements WAIT numreplicas timeout. It waits until enough online
// replicas acknowledged the master's current offset, or the timeout expires.
func (server *Server) execWait(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeArgNumErrReply("wait")
	}
	numReplicas, err := strconv.Atoi(string(args[0]))
	if err != nil || numReplicas < 0 {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	timeoutMs, err := strconv.Atoi(string(args[1]))
	if err != nil || timeoutMs < 0 {
		return protocol.MakeErrReply("ERR timeout is not an integer or out of range")
	}
	if server.persister != nil {
		// include writes that are still queued in the AOF writer
		server.persister.WaitForAof()
	}
	server.masterStatus.mu.RLock()
	target := int64(0)
	if server.masterStatus.backlog != nil {
		target = server.masterStatus.backlog.currentOffset
	}
	server.masterStatus.mu.RUnlock()

	if numReplicas == 0 {
		return protocol.MakeIntReply(0)
	}
	var clientDone <-chan struct{}
	if c != nil {
		if notifier, ok := c.(interface{ Done() <-chan struct{} }); ok {
			clientDone = notifier.Done()
		}
	}
	var timer *time.Timer
	var timeoutCh <-chan time.Time
	if timeoutMs > 0 {
		maxTimeoutMs := int64(^uint64(0)>>1) / int64(time.Millisecond)
		if int64(timeoutMs) > maxTimeoutMs {
			timeoutMs = int(maxTimeoutMs)
		}
		timer = time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
		timeoutCh = timer.C
		defer timer.Stop()
	}

	for {
		// snapshot the wake channel before reading the counters: an ACK that
		// lands between the count and the select closes this channel, so the
		// wakeup cannot be lost.
		waitCh := server.masterStatus.ackNotifier.waitChan()
		acked := server.countAckedSlaves(target)
		if acked >= numReplicas {
			return protocol.MakeIntReply(int64(acked))
		}
		if c != nil {
			if checker, ok := c.(interface{ IsClosed() bool }); ok && checker.IsClosed() {
				return protocol.MakeIntReply(int64(acked))
			}
		}
		if timeoutCh != nil {
			select {
			case <-waitCh:
			case <-timeoutCh:
				return protocol.MakeIntReply(int64(server.countAckedSlaves(target)))
			case <-server.closeChan:
				return protocol.MakeIntReply(int64(acked))
			case <-clientDone:
				return protocol.MakeIntReply(int64(acked))
			}
		} else {
			select {
			case <-waitCh:
			case <-server.closeChan:
				return protocol.MakeIntReply(int64(acked))
			case <-clientDone:
				return protocol.MakeIntReply(int64(acked))
			}
		}
	}
}

func (server *Server) execPSync(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeArgNumErrReply("psync")
	}
	if server.persister == nil {
		return protocol.MakeErrReply("ERR replication requires appendonly yes")
	}
	replId := string(args[0])
	replOffset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	server.masterStatus.mu.Lock()
	defer server.masterStatus.mu.Unlock()
	slave := server.masterStatus.slaveMap[c]
	if slave == nil {
		slave = newSlaveClient(c)
		c.SetSlave()
		server.masterStatus.slaveMap[c] = slave
	}
	if server.masterStatus.bgSaveState == bgSaveIdle {
		server.masterStatus.waitSlaves[slave] = struct{}{}
		server.bgSaveForReplication()
	} else if server.masterStatus.bgSaveState == bgSaveRunning {
		server.masterStatus.waitSlaves[slave] = struct{}{}
	} else if server.masterStatus.bgSaveState == bgSaveFinish {
		go func() {
			defer func() {
				if e := recover(); e != nil {
					logger.Errorf("panic: %v", e)
				}
			}()
			err := server.masterTryPartialSyncWithSlave(slave, replId, replOffset)
			if err == nil {
				return
			}
			if err != cannotPartialSync {
				server.removeSlave(slave)
				logger.Errorf("masterTryPartialSyncWithSlave error: %v", err)
				return
			}
			// assert err == cannotPartialSync
			if err := server.masterFullReSyncWithSlave(slave); err != nil {
				server.removeSlave(slave)
				logger.Errorf("masterFullReSyncWithSlave error: %v", err)
				return
			}
		}()
	}
	return &protocol.NoReply{}
}

func (server *Server) execReplConf(c redis.Connection, args [][]byte) redis.Reply {
	if len(args)%2 != 0 {
		return protocol.MakeSyntaxErrReply()
	}
	server.masterStatus.mu.RLock()
	slave := server.masterStatus.slaveMap[c]
	server.masterStatus.mu.RUnlock()
	for i := 0; i < len(args); i += 2 {
		key := strings.ToLower(string(args[i]))
		value := string(args[i+1])
		switch key {
		case "ack":
			offset, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			if slave == nil {
				// ACK from a connection that never performed PSYNC
				return protocol.MakeErrReply("ERR no PSYNC session for this connection")
			}
			slave.mu.Lock()
			if offset < 0 {
				offset = 0
			}
			if offset > slave.sendOffset {
				offset = slave.sendOffset
			}
			if offset > slave.ackOffset {
				slave.ackOffset = offset
			}
			slave.lastAckTime = time.Now()
			slave.mu.Unlock()
			server.masterStatus.ackNotifier.notify()
			return &protocol.NoReply{}
		}
	}
	return protocol.MakeOkReply()
}

func (server *Server) removeSlave(slave *slaveClient) {
	slave.stopSender()
	server.masterStatus.mu.Lock()
	_ = slave.conn.Close()
	delete(server.masterStatus.slaveMap, slave.conn)
	delete(server.masterStatus.waitSlaves, slave)
	delete(server.masterStatus.onlineSlaves, slave)
	logger.Info("disconnect with slave " + slave.conn.Name())
	server.masterStatus.mu.Unlock()
	server.masterStatus.ackNotifier.notify()
}

func (server *Server) setSlaveOnline(slave *slaveClient, currentOffset int64) {
	server.masterStatus.mu.Lock()
	slave.mu.Lock()
	slave.sendOffset = currentOffset
	// do not count a freshly synced replica as ACKed until it confirms the
	// stream; WAIT would otherwise return before the data is durable on it.
	slave.ackOffset = 0
	slave.lastAckTime = time.Now()
	slave.mu.Unlock()
	if slave.sendCh == nil {
		slave.sendCh = make(chan struct{}, 1)
	}
	if slave.doneCh == nil {
		slave.doneCh = make(chan struct{})
	}
	server.masterStatus.onlineSlaves[slave] = struct{}{}
	startSender := false
	slave.senderOnce.Do(func() { startSender = true })
	server.masterStatus.mu.Unlock()
	if startSender {
		go server.slaveSender(slave)
	}
	slave.notify()
	server.masterStatus.ackNotifier.notify()
}

var pingBytes = protocol.MakeMultiBulkReply(utils.ToCmdLine("ping")).ToBytes()

const maxBacklogSize = 10 * 1024 * 1024 // 10MB

func (server *Server) masterCron() {
	server.masterStatus.mu.Lock()
	if len(server.masterStatus.slaveMap) == 0 { // no slaves, do nothing
		server.masterStatus.mu.Unlock()
		return
	}
	if server.masterStatus.bgSaveState == bgSaveFinish {
		server.masterStatus.backlog.appendBytes(pingBytes)
	}
	backlogSize := len(server.masterStatus.backlog.buf)
	replTimeout := 60 * time.Second
	if config.Properties.ReplTimeout > 0 {
		replTimeout = time.Duration(config.Properties.ReplTimeout) * time.Second
	}
	var stale []*slaveClient
	for slave := range server.masterStatus.onlineSlaves {
		slave.mu.Lock()
		lastAck := slave.lastAckTime
		slave.mu.Unlock()
		if !lastAck.IsZero() && time.Since(lastAck) > replTimeout {
			stale = append(stale, slave)
		}
	}
	server.masterStatus.mu.Unlock()
	for _, slave := range stale {
		logger.Warn("disconnect stale slave " + slave.conn.Name())
		server.removeSlave(slave)
	}
	server.notifySlaves()
	if backlogSize > maxBacklogSize && !server.masterStatus.rewriting.Get() {
		go func() {
			server.masterStatus.rewriting.Set(true)
			defer server.masterStatus.rewriting.Set(false)
			if err := server.rewriteRDB(); err != nil {
				server.masterStatus.rewriting.Set(false)
				logger.Errorf("rewrite error: %v", err)
			}
		}()
	}
}

// replAofListener is an implementation for aof.Listener
type replAofListener struct {
	mdb         *Server
	backlog     *replBacklog // may NOT be mdb.masterStatus.backlog
	readyToSend atomic.Boolean
}

func (listener *replAofListener) Callback(cmdLines []CmdLine) {
	listener.mdb.masterStatus.mu.Lock()
	for _, cmdLine := range cmdLines {
		reply := protocol.MakeMultiBulkReply(cmdLine)
		listener.backlog.appendBytes(reply.ToBytes())
	}
	listener.mdb.masterStatus.mu.Unlock()
	// listener could receive updates generated during rdb saving in progress
	// Do not wake the senders before the rdb is finished.
	if listener.readyToSend.Get() {
		listener.mdb.notifySlaves()
	}
}

func (server *Server) initMasterStatus() {
	server.masterStatus = &masterStatus{
		mu:           sync.RWMutex{},
		replId:       utils.RandHexString(40),
		backlog:      &replBacklog{},
		slaveMap:     make(map[redis.Connection]*slaveClient),
		waitSlaves:   make(map[*slaveClient]struct{}),
		onlineSlaves: make(map[*slaveClient]struct{}),
		bgSaveState:  bgSaveIdle,
		rdbFilename:  "",
		ackNotifier:  ackNotifier{ch: make(chan struct{})},
	}
}

func (server *Server) stopMaster() {
	server.masterStatus.mu.Lock()
	defer server.masterStatus.mu.Unlock()

	// disconnect with slave
	for _, slave := range server.masterStatus.slaveMap {
		slave.stopSender()
		_ = slave.conn.Close()
		delete(server.masterStatus.slaveMap, slave.conn)
		delete(server.masterStatus.waitSlaves, slave)
		delete(server.masterStatus.onlineSlaves, slave)
	}

	// clean master status
	if server.persister != nil {
		server.persister.RemoveListener(server.masterStatus.aofListener)
	}
	_ = os.Remove(server.masterStatus.rdbFilename)
	server.masterStatus.rdbFilename = ""
	server.masterStatus.replId = ""
	server.masterStatus.backlog = &replBacklog{}
	server.masterStatus.slaveMap = make(map[redis.Connection]*slaveClient)
	server.masterStatus.waitSlaves = make(map[*slaveClient]struct{})
	server.masterStatus.onlineSlaves = make(map[*slaveClient]struct{})
	server.masterStatus.bgSaveState = bgSaveIdle
	server.masterStatus.ackNotifier.notify()
}

// execWaitCommand is only metadata: Server.Exec intercepts WAIT before routing
// it to a DB because the command needs the replication state.
func execWaitCommand(db *DB, args [][]byte) redis.Reply {
	return protocol.MakeErrReply("ERR WAIT must be executed by the server")
}

func init() {
	registerCommand("Wait", execWaitCommand, noPrepare, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 0, 0, 0)
}
