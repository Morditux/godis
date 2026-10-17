package aof

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	rdb "github.com/hdt3213/rdb/core"

	"github.com/hdt3213/godis/config"

	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 20
)

const (
	// FsyncAlways do fsync for every command
	FsyncAlways = "always"
	// FsyncEverySec do fsync every second
	FsyncEverySec = "everysec"
	// FsyncNo lets operating system decides when to do fsync
	FsyncNo = "no"
)

type payload struct {
	cmdLine CmdLine
	dbIndex int
	wg      *sync.WaitGroup
}

// Listener will be called-back after receiving a aof payload
// with a listener we can forward the updates to slave nodes etc.
type Listener interface {
	// Callback will be called-back after receiving a aof payload
	Callback([]CmdLine)
}

// Persister receive msgs from channel and write to AOF file
type Persister struct {
	ctx        context.Context
	cancel     context.CancelFunc
	db         database.DBEngine
	tmpDBMaker func() database.DBEngine
	// aofChan is the channel to receive aof payload(listenCmd will send payload to this channel)
	aofChan chan *payload
	// aofFile is the file handler of aof file
	aofFile *os.File
	// aofFilename is the path of aof file
	aofFilename string
	// aofFsync is the strategy of fsync
	aofFsync string
	// aof goroutine will send msg to main goroutine through this channel when aof tasks finished and ready to shut down
	aofFinished chan struct{}
	// pause aof for start/finish aof rewrite progress
	pausingAof sync.Mutex
	// rewriteMu serializes AOF rewrite and RDB generation; TryLock is used
	// to reject a second concurrent rewrite instead of corrupting both files.
	rewriteMu sync.Mutex
	currentDB int
	listeners map[Listener]struct{}
	// reuse cmdLine buffer
	buffer []CmdLine
	// closeOnce guards double Close
	closeOnce sync.Once
}

// NewPersister creates a new aof.Persister
func NewPersister(db database.DBEngine, filename string, load bool, fsync string, tmpDBMaker func() database.DBEngine) (*Persister, error) {
	persister := &Persister{}
	persister.aofFilename = filename
	persister.aofFsync = strings.ToLower(fsync)
	persister.db = db
	persister.tmpDBMaker = tmpDBMaker
	persister.currentDB = 0
	// load aof file if needed
	if load {
		if err := persister.LoadAof(0); err != nil {
			return nil, fmt.Errorf("load aof %s: %w", filename, err)
		}
	}
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	persister.aofFile = aofFile
	persister.aofChan = make(chan *payload, aofQueueSize)
	persister.aofFinished = make(chan struct{})
	persister.listeners = make(map[Listener]struct{})
	// create ctx BEFORE starting the goroutine: listenCmd reads ctx.Done()
	ctx, cancel := context.WithCancel(context.Background())
	persister.ctx = ctx
	persister.cancel = cancel
	// start aof goroutine to write aof file in background and fsync periodically if needed (see fsyncEverySecond)
	go func() {
		persister.listenCmd()
	}()
	// fsync every second if needed
	if persister.aofFsync == FsyncEverySec {
		persister.fsyncEverySecond()
	}
	return persister, nil
}

// AddListener adds a listener to aof callback, so aof payloads are
// forwarded to it after being written
func (persister *Persister) AddListener(listener Listener) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	persister.listeners[listener] = struct{}{}
}

// RemoveListener removes a listener from aof handler, so we can close the listener
func (persister *Persister) RemoveListener(listener Listener) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	delete(persister.listeners, listener)
}

// SaveCmdLine send command to aof goroutine through channel
func (persister *Persister) SaveCmdLine(dbIndex int, cmdLine CmdLine) {
	// aofChan will be set as nil temporarily during load aof see Persister.LoadAof
	if persister.aofChan == nil {
		return
	}

	if persister.aofFsync == FsyncAlways {
		p := &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
		persister.writeAof(p)
		return
	}

	persister.aofChan <- &payload{
		cmdLine: cmdLine,
		dbIndex: dbIndex,
	}

}

// listenCmd listen aof channel and write into file.
// It stops when persister.ctx is canceled, after draining remaining payloads.
func (persister *Persister) listenCmd() {
	defer func() {
		persister.aofFinished <- struct{}{}
	}()
	for {
		select {
		case <-persister.ctx.Done():
			// drain remaining buffered payloads and exit;
			// commands submitted afterwards are lost:
			// within this shutdown window, request execution should already
			// have stopped (invokers must stop enqueuing before closing)
			for {
				select {
				case p, ok := <-persister.aofChan:
					if !ok {
						return
					}
					persister.writeAof(p)
				default:
					return
				}
			}
		case p := <-persister.aofChan:
			persister.writeAof(p)
		}
	}
}

func (persister *Persister) writeAof(p *payload) {
	persister.buffer = persister.buffer[:0] // reuse underlying array
	persister.pausingAof.Lock()             // prevent other goroutines from pausing aof
	defer persister.pausingAof.Unlock()
	if persister.aofFile == nil {
		// the live file is temporarily unavailable after a failed reopen;
		// keep the server alive but make the data-loss condition visible.
		logger.Error("aof file is not open, dropping command")
		return
	}
	// ensure aof is in the right database
	if p.dbIndex != persister.currentDB {
		// select db
		selectCmd := utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))
		persister.buffer = append(persister.buffer, selectCmd)
		data := protocol.MakeMultiBulkReply(selectCmd).ToBytes()
		_, err := persister.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
			return // skip this command
		}
		persister.currentDB = p.dbIndex
	}
	// save command
	data := protocol.MakeMultiBulkReply(p.cmdLine).ToBytes()
	persister.buffer = append(persister.buffer, p.cmdLine)
	_, err := persister.aofFile.Write(data)
	if err != nil {
		logger.Warn(err)
	}
	for listener := range persister.listeners {
		listener.Callback(persister.buffer)
	}
	if persister.aofFsync == FsyncAlways {
		_ = persister.aofFile.Sync()
	}
}

// LoadAof read aof file, can only be used before Persister.listenCmd started.
// A malformed file or a command that cannot be replayed is an error: silently
// keeping a partially applied AOF would hide data loss.
func (persister *Persister) LoadAof(maxBytes int) error {
	// persister.db.Exec may call persister.AddAof
	// delete aofChan to prevent loaded commands back into aofChan
	aofChan := persister.aofChan
	persister.aofChan = nil
	defer func(aofChan chan *payload) {
		persister.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(persister.aofFilename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open aof: %w", err)
	}
	defer file.Close()

	// load rdb preamble if needed
	decoder := rdb.NewDecoder(file)
	err = persister.db.LoadRDB(decoder)
	if err != nil {
		// no rdb preamble
		if _, seekErr := file.Seek(0, io.SeekStart); seekErr != nil {
			return fmt.Errorf("rewind aof: %w", seekErr)
		}
	} else {
		// has rdb preamble; GetReadCount includes the trailing checksum
		readCount := decoder.GetReadCount()
		if _, seekErr := file.Seek(int64(readCount), io.SeekStart); seekErr != nil {
			return fmt.Errorf("seek aof preamble: %w", seekErr)
		}
		maxBytes -= readCount
	}
	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}
	ch := parser.ParseStream(reader)
	fakeConn := connection.NewFakeConn() // only used for save dbIndex
	for payload := range ch {
		if payload.Err != nil {
			if errors.Is(payload.Err, io.EOF) {
				break
			}
			return fmt.Errorf("parse aof: %w", payload.Err)
		}
		if payload.Data == nil {
			return errors.New("invalid aof payload: empty command")
		}
		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			return errors.New("invalid aof payload: expected multi-bulk protocol")
		}
		if len(r.Args) == 0 {
			return errors.New("invalid aof payload: empty command")
		}
		ret := persister.db.Exec(fakeConn, r.Args)
		if protocol.IsErrorReply(ret) {
			return fmt.Errorf("exec aof command %q: %s", strings.ToLower(string(r.Args[0])), string(ret.ToBytes()))
		}
		if strings.EqualFold(string(r.Args[0]), "select") {
			if len(r.Args) < 2 {
				return errors.New("invalid select command in aof")
			}
			dbIndex, err := strconv.Atoi(string(r.Args[1]))
			if err != nil {
				return fmt.Errorf("invalid select index in aof: %w", err)
			}
			persister.currentDB = dbIndex
		}
	}
	return nil
}

// Fsync flushes aof file to disk
func (persister *Persister) Fsync() {
	persister.pausingAof.Lock()
	if persister.aofFile == nil {
		persister.pausingAof.Unlock()
		return
	}
	if err := persister.aofFile.Sync(); err != nil {
		logger.Errorf("fsync failed: %v", err)
	}
	persister.pausingAof.Unlock()
}

// Close gracefully stops aof persistence procedure; calling it repeatedly is safe
func (persister *Persister) Close() {
	if persister == nil {
		return
	}
	persister.closeOnce.Do(func() {
		// wait for a running rewrite/RDB generation to finish before closing
		// the file it may still be using
		persister.rewriteMu.Lock()
		defer persister.rewriteMu.Unlock()
		if persister.cancel != nil {
			persister.cancel()
		}
		if persister.aofFinished != nil {
			<-persister.aofFinished // wait for aof finished
		}
		if persister.aofFile != nil {
			err := persister.aofFile.Close()
			if err != nil {
				logger.Warn(err)
			}
		}
	})
}

// fsyncEverySecond fsync aof file every second
func (persister *Persister) fsyncEverySecond() {
	ticker := time.NewTicker(time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				persister.Fsync()
			case <-persister.ctx.Done():
				return
			}
		}
	}()
}

func (persister *Persister) generateAof(ctx *RewriteCtx) error {
	// rewrite aof tmpFile
	tmpFile := ctx.tmpFile
	// load aof tmpFile
	tmpAof := persister.newRewriteHandler()
	if err := tmpAof.LoadAof(int(ctx.fileSize)); err != nil {
		return err
	}
	for i := 0; i < config.Properties.Databases; i++ {
		// select db
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(i))).ToBytes()
		_, err := tmpFile.Write(data)
		if err != nil {
			return err
		}
		// dump db
		var writeErr error
		tmpAof.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			cmd := EntityToCmd(key, entity)
			if cmd != nil {
				if _, err := tmpFile.Write(cmd.ToBytes()); err != nil {
					writeErr = err
					return false
				}
			}
			if expiration != nil {
				cmd := MakeExpireCmd(key, *expiration)
				if cmd != nil {
					if _, err := tmpFile.Write(cmd.ToBytes()); err != nil {
						writeErr = err
						return false
					}
				}
			}
			return true
		})
		if writeErr != nil {
			return writeErr
		}
	}
	return nil
}
