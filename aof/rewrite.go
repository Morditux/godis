package aof

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

// ErrRewriteInProgress is returned when a rewrite/RDB generation is already
// running. It prevents two AOF rewrites from renaming different temp files
// over the same live file.
var ErrRewriteInProgress = errors.New("ERR background append only file rewriting already in progress")

func (persister *Persister) newRewriteHandler() *Persister {
	h := &Persister{}
	h.aofFilename = persister.aofFilename
	h.db = persister.tmpDBMaker()
	return h
}

// RewriteCtx holds context of an AOF rewriting procedure
type RewriteCtx struct {
	tmpFile  *os.File // tmpFile is the file handler of aof tmpFile
	fileSize int64
	dbIdx    int // selected db index when startRewrite
}

// cleanupRewriteCtx closes and removes a temp file that was not committed.
func cleanupRewriteCtx(ctx *RewriteCtx) {
	if ctx == nil || ctx.tmpFile == nil {
		return
	}
	name := ctx.tmpFile.Name()
	_ = ctx.tmpFile.Close()
	_ = os.Remove(name)
}

// Rewrite carries out AOF rewrite. It rejects a concurrent rewrite instead of
// letting two rewrites race on the live AOF file.
func (persister *Persister) Rewrite() error {
	if !persister.rewriteMu.TryLock() {
		return ErrRewriteInProgress
	}
	defer persister.rewriteMu.Unlock()
	return persister.rewriteLocked()
}

// StartBackgroundRewrite starts an asynchronous rewrite. The single-flight
// decision is made synchronously so the caller can report ErrRewriteInProgress
// to the client; errors happening in the goroutine are logged.
func (persister *Persister) StartBackgroundRewrite() error {
	if !persister.rewriteMu.TryLock() {
		return ErrRewriteInProgress
	}
	go func() {
		defer persister.rewriteMu.Unlock()
		defer func() {
			if e := recover(); e != nil {
				logger.Errorf("background aof rewrite panic: %v", e)
			}
		}()
		if err := persister.rewriteLocked(); err != nil {
			logger.Errorf("background aof rewrite failed: %v", err)
		}
	}()
	return nil
}

func (persister *Persister) rewriteLocked() error {
	ctx, err := persister.StartRewrite()
	if err != nil {
		return err
	}
	if err := persister.DoRewrite(ctx); err != nil {
		cleanupRewriteCtx(ctx)
		return err
	}
	if err := persister.FinishRewrite(ctx); err != nil {
		cleanupRewriteCtx(ctx)
		return err
	}
	return nil
}

// DoRewrite actually rewrite aof file
// makes DoRewrite public for testing only, please use Rewrite instead
func (persister *Persister) DoRewrite(ctx *RewriteCtx) (err error) {
	// start rewrite
	if !config.Properties.AofUseRdbPreamble {
		logger.Info("generate aof preamble")
		err = persister.generateAof(ctx)
	} else {
		logger.Info("generate rdb preamble")
		err = persister.generateRDB(ctx)
	}
	return err
}

// StartRewrite prepares rewrite procedure
func (persister *Persister) StartRewrite() (*RewriteCtx, error) {
	// pausing aof
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	if persister.aofFile == nil {
		return nil, errors.New("aof file is not open")
	}

	err := persister.aofFile.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, err
	}

	// get current aof file size
	fileInfo, err := os.Stat(persister.aofFilename)
	if err != nil {
		logger.Errorf("stat aof file failed: %v", err)
		return nil, err
	}
	filesize := fileInfo.Size()

	// create the temp file next to the live AOF: the final rename must stay on
	// the same filesystem, otherwise it fails with EXDEV.
	dir := filepath.Dir(persister.aofFilename)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	file, err := os.CreateTemp(dir, "*.aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, err
	}
	return &RewriteCtx{
		tmpFile:  file,
		fileSize: filesize,
		dbIdx:    persister.currentDB,
	}, nil
}

func (persister *Persister) reopenAofFile() error {
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		persister.aofFile = nil
		return err
	}
	persister.aofFile = aofFile
	return nil
}

// FinishRewrite replaces the live AOF with the rewritten temp file. On any
// failure the current AOF is left in place and the temp file is removed.
func (persister *Persister) FinishRewrite(ctx *RewriteCtx) error {
	if ctx == nil || ctx.tmpFile == nil {
		return errors.New("invalid rewrite context")
	}
	persister.pausingAof.Lock() // pausing aof
	defer persister.pausingAof.Unlock()

	tmpFile := ctx.tmpFile
	tmpName := tmpFile.Name()
	committed := false
	defer func() {
		if !committed {
			_ = tmpFile.Close()
			_ = os.Remove(tmpName)
		}
	}()

	src, err := os.Open(persister.aofFilename)
	if err != nil {
		return fmt.Errorf("open aof file %q: %w", persister.aofFilename, err)
	}
	defer func() {
		if src != nil {
			_ = src.Close()
		}
	}()
	if _, err = src.Seek(ctx.fileSize, 0); err != nil {
		return fmt.Errorf("seek aof file: %w", err)
	}
	// sync tmpFile's db index with the online aofFile
	data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(ctx.dbIdx))).ToBytes()
	if _, err = tmpFile.Write(data); err != nil {
		return fmt.Errorf("write rewrite buffer: %w", err)
	}
	if _, err = io.Copy(tmpFile, src); err != nil {
		return fmt.Errorf("copy aof tail: %w", err)
	}
	if err = tmpFile.Sync(); err != nil {
		return fmt.Errorf("sync rewritten aof: %w", err)
	}
	if err = tmpFile.Close(); err != nil {
		return fmt.Errorf("close rewritten aof: %w", err)
	}
	// close src before rename so the operation also works on Windows
	if err = src.Close(); err != nil {
		return fmt.Errorf("close old aof: %w", err)
	}
	src = nil

	if err = persister.aofFile.Sync(); err != nil {
		return fmt.Errorf("sync current aof: %w", err)
	}
	if err = persister.aofFile.Close(); err != nil {
		_ = persister.reopenAofFile()
		return fmt.Errorf("close current aof: %w", err)
	}
	if err = os.Rename(tmpName, persister.aofFilename); err != nil {
		_ = persister.reopenAofFile()
		return fmt.Errorf("replace aof file: %w", err)
	}
	committed = true
	if err = persister.reopenAofFile(); err != nil {
		return fmt.Errorf("reopen rewritten aof: %w", err)
	}

	// resume the file with the currently selected database
	data = protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(persister.currentDB))).ToBytes()
	if _, err = persister.aofFile.Write(data); err != nil {
		return fmt.Errorf("write select to rewritten aof: %w", err)
	}
	return nil
}
