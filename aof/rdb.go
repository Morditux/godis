package aof

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/datastruct/dict"
	List "github.com/hdt3213/godis/datastruct/list"
	"github.com/hdt3213/godis/datastruct/set"
	SortedSet "github.com/hdt3213/godis/datastruct/sortedset"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/lib/logger"
	rdb "github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/model"
)

// GenerateRDB generates an RDB file from the persisted AOF snapshot.
func (persister *Persister) GenerateRDB(rdbFilename string) error {
	persister.rewriteMu.Lock()
	defer persister.rewriteMu.Unlock()
	return persister.generateRDBFile(rdbFilename, nil, nil)
}

// GenerateRDBForReplication asynchronously generates rdb file from aof file and returns a channel to receive following data
// parameter listener would receive following updates of rdb
// parameter hook allows you to do something during aof pausing
func (persister *Persister) GenerateRDBForReplication(rdbFilename string, listener Listener, hook func()) error {
	persister.rewriteMu.Lock()
	defer persister.rewriteMu.Unlock()
	return persister.generateRDBFile(rdbFilename, listener, hook)
}

func (persister *Persister) generateRDBFile(rdbFilename string, listener Listener, hook func()) error {
	if rdbFilename == "" {
		return errors.New("empty rdb filename")
	}
	dir := filepath.Dir(rdbFilename)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	ctx, err := persister.startGenerateRDB(listener, hook, dir)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			if listener != nil {
				persister.RemoveListener(listener)
			}
			cleanupRewriteCtx(ctx)
		}
	}()
	if err := persister.generateRDB(ctx); err != nil {
		return err
	}
	if err := ctx.tmpFile.Sync(); err != nil {
		return err
	}
	if err := ctx.tmpFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(ctx.tmpFile.Name(), rdbFilename); err != nil {
		return err
	}
	committed = true
	return nil
}

// RDBProvider supplies point-in-time database snapshots for RDB encoding.
// Implementations must return independent copies of the entities.
type RDBProvider interface {
	DBNum() int
	Snapshot(dbIndex int) []*database.RDBEntry
}

// engineRDBProvider adapts a DBEngine (used for AOF replay) to RDBProvider.
type engineRDBProvider struct {
	db database.DBEngine
}

func (p engineRDBProvider) DBNum() int {
	return config.Properties.Databases
}

func (p engineRDBProvider) Snapshot(dbIndex int) []*database.RDBEntry {
	entries := make([]*database.RDBEntry, 0)
	p.db.ForEach(dbIndex, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
		entries = append(entries, &database.RDBEntry{
			Key:        key,
			Entity:     entity,
			Expiration: expiration,
		})
		return true
	})
	return entries
}

// GenerateRDBFromDB writes an RDB snapshot directly from a live database. It
// is used by SAVE/BGSAVE and does not require AOF to be enabled.
func GenerateRDBFromDB(provider RDBProvider, rdbFilename string) error {
	if rdbFilename == "" {
		return errors.New("empty rdb filename")
	}
	if dir := filepath.Dir(rdbFilename); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	// create the temp file next to the destination so the final rename stays
	// on the same filesystem (rename across mounts is not atomic and may fail)
	file, err := os.CreateTemp(filepath.Dir(rdbFilename), "*.rdb")
	if err != nil {
		return err
	}
	tmpName := file.Name()
	committed := false
	defer func() {
		if !committed {
			_ = file.Close()
			_ = os.Remove(tmpName)
		}
	}()
	if err := encodeRDBProvider(provider, file); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, rdbFilename); err != nil {
		return err
	}
	committed = true
	return nil
}

func (persister *Persister) startGenerateRDB(newListener Listener, hook func(), tmpDir string) (*RewriteCtx, error) {
	persister.pausingAof.Lock() // pausing aof
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
	// create the temp file in the destination directory so the final rename
	// stays on the same filesystem
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return nil, err
	}
	file, err := os.CreateTemp(tmpDir, "*.rdb")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, err
	}
	if newListener != nil {
		persister.listeners[newListener] = struct{}{}
	}
	if hook != nil {
		hook()
	}
	return &RewriteCtx{
		tmpFile:  file,
		fileSize: filesize,
	}, nil
}

// generateRDB generates rdb file from aof file
func (persister *Persister) generateRDB(ctx *RewriteCtx) error {
	// load aof tmpFile
	tmpHandler := persister.newRewriteHandler()
	if err := tmpHandler.LoadAof(int(ctx.fileSize)); err != nil {
		return err
	}
	return encodeRDB(tmpHandler.db, ctx.tmpFile)
}

func encodeRDB(db database.DBEngine, file *os.File) error {
	return encodeRDBProvider(engineRDBProvider{db: db}, file)
}

func encodeRDBProvider(provider RDBProvider, file *os.File) error {
	encoder := rdb.NewEncoder(file).EnableCompress()
	err := encoder.WriteHeader()
	if err != nil {
		return err
	}
	auxMap := map[string]string{
		"redis-ver":    "6.0.0",
		"redis-bits":   "64",
		"aof-preamble": "0",
		"ctime":        strconv.FormatInt(time.Now().Unix(), 10),
	}

	// change aof preamble
	if config.Properties.AofUseRdbPreamble {
		auxMap["aof-preamble"] = "1"
	}

	for k, v := range auxMap {
		if err := encoder.WriteAux(k, v); err != nil {
			return err
		}
	}

	for i := 0; i < provider.DBNum(); i++ {
		entries := provider.Snapshot(i)
		if len(entries) == 0 {
			continue
		}
		ttlCount := 0
		for _, entry := range entries {
			if entry.Expiration != nil {
				ttlCount++
			}
		}
		if err := encoder.WriteDBHeader(uint(i), uint64(len(entries)), uint64(ttlCount)); err != nil {
			return err
		}
		for _, entry := range entries {
			var opts []interface{}
			if entry.Expiration != nil {
				opts = append(opts, rdb.WithTTL(uint64(entry.Expiration.UnixNano()/1e6)))
			}
			switch obj := entry.Entity.Data.(type) {
			case []byte:
				err = encoder.WriteStringObject(entry.Key, obj, opts...)
			case List.List[[]byte]:
				vals := make([][]byte, 0, obj.Len())
				obj.ForEach(func(i int, v []byte) bool {
					vals = append(vals, v)
					return true
				})
				err = encoder.WriteListObject(entry.Key, vals, opts...)
			case *set.Set:
				vals := make([][]byte, 0, obj.Len())
				obj.ForEach(func(m string) bool {
					vals = append(vals, []byte(m))
					return true
				})
				err = encoder.WriteSetObject(entry.Key, vals, opts...)
			case dict.Dict[[]byte]:
				hash := make(map[string][]byte)
				obj.ForEach(func(key string, val []byte) bool {
					hash[key] = val
					return true
				})
				err = encoder.WriteHashMapObject(entry.Key, hash, opts...)
			case *SortedSet.SortedSet:
				var zEntries []*model.ZSetEntry
				obj.ForEachByRank(int64(0), obj.Len(), true, func(element *SortedSet.Element) bool {
					zEntries = append(zEntries, &model.ZSetEntry{
						Member: element.Member,
						Score:  element.Score,
					})
					return true
				})
				err = encoder.WriteZSetObject(entry.Key, zEntries, opts...)
			}
			if err != nil {
				return err
			}
		}
	}
	return encoder.WriteEnd()
}
