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
	// mu guards offset/lastAckTime: the REPLCONF ACK handler updates them
	// concurrently with masterSendUpdatesToSlave, which must not hold
	// masterStatus.mu while writing to slave connections
	mu           sync.Mutex
	offset       int64
	lastAckTime  time.Time
	announceIp   string
	announcePort int
	capacity     uint8
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

type masterStatus struct {
	mu           sync.RWMutex
	replId       string
	backlog      *replBacklog
	slaveMap     map[redis.Connection]*slaveClient
	waitSlaves   map[*slaveClient]struct{}
	onlineSlaves map[*slaveClient]struct{}
	bgSaveState  uint8
	rdbFilename  string
	aofListener  *replAofListener
	rewriting    atomic.Boolean
	// sendMu serializes backlog writes to replicas: masterCron and the AOF
	// listener can both trigger masterSendUpdatesToSlave, and interleaving two
	// RESP writes on the same connection would corrupt the stream.
	sendMu sync.Mutex
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

	// send rdb to waiting slaves
	for slave := range waitSlaves {
		err = server.masterFullReSyncWithSlave(slave)
		if err != nil {
			server.removeSlave(slave)
			logger.Errorf("masterFullReSyncWithSlave error: %v", err)
			continue
		}
	}
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

// masterSendUpdatesToSlave only sends data to online slaves after bgSave is finished
// if bgSave is running, updates will be sent after the saving finished
func (server *Server) masterSendUpdatesToSlave() error {
	server.masterStatus.sendMu.Lock()
	defer server.masterStatus.sendMu.Unlock()

	onlineSlaves := make(map[*slaveClient]struct{})
	server.masterStatus.mu.RLock()
	beginOffset := server.masterStatus.backlog.beginOffset
	backlog, currentOffset := server.masterStatus.backlog.getSnapshot()
	for slave := range server.masterStatus.onlineSlaves {
		onlineSlaves[slave] = struct{}{}
	}
	server.masterStatus.mu.RUnlock()
	for slave := range onlineSlaves {
		slave.mu.Lock()
		slaveBeginOffset := slave.offset - beginOffset
		slave.mu.Unlock()
		if slaveBeginOffset < 0 || slaveBeginOffset > int64(len(backlog)) {
			// the backlog was rotated while this slave was behind: a partial
			// stream cannot be built, force a full resync on its next PSYNC.
			logger.Errorf("slave backlog offset %d out of range [0,%d], disconnecting", slaveBeginOffset, len(backlog))
			server.removeSlave(slave)
			continue
		}
		if slaveBeginOffset == int64(len(backlog)) {
			continue
		}
		setReplWriteDeadline(slave.conn, replWriteTimeout)
		_, err := slave.conn.Write(backlog[slaveBeginOffset:])
		setReplWriteDeadline(slave.conn, 0)
		if err != nil {
			logger.Errorf("send updates backlog to slave failed: %v", err)
			server.removeSlave(slave)
			continue
		}
		slave.mu.Lock()
		slave.offset = currentOffset
		slave.mu.Unlock()
	}
	return nil
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
		slave = &slaveClient{
			conn: c,
		}
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
			slave.offset = offset
			slave.lastAckTime = time.Now()
			slave.mu.Unlock()
			return &protocol.NoReply{}
		}
	}
	return protocol.MakeOkReply()
}

func (server *Server) removeSlave(slave *slaveClient) {
	server.masterStatus.mu.Lock()
	defer server.masterStatus.mu.Unlock()
	_ = slave.conn.Close()
	delete(server.masterStatus.slaveMap, slave.conn)
	delete(server.masterStatus.waitSlaves, slave)
	delete(server.masterStatus.onlineSlaves, slave)
	logger.Info("disconnect with slave " + slave.conn.Name())
}

func (server *Server) setSlaveOnline(slave *slaveClient, currentOffset int64) {
	server.masterStatus.mu.Lock()
	defer server.masterStatus.mu.Unlock()
	slave.mu.Lock()
	slave.offset = currentOffset
	slave.lastAckTime = time.Now()
	slave.mu.Unlock()
	server.masterStatus.onlineSlaves[slave] = struct{}{}
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
	if err := server.masterSendUpdatesToSlave(); err != nil {
		logger.Errorf("masterSendUpdatesToSlave error: %v", err)
	}
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
	// Do not send updates to slave before rdb saving is finished
	if listener.readyToSend.Get() {
		if err := listener.mdb.masterSendUpdatesToSlave(); err != nil {
			logger.Errorf("masterSendUpdatesToSlave after receive aof error: %v", err)
		}
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
	}
}

func (server *Server) stopMaster() {
	server.masterStatus.mu.Lock()
	defer server.masterStatus.mu.Unlock()

	// disconnect with slave
	for _, slave := range server.masterStatus.slaveMap {
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
}
