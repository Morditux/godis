package database

import (
	"github.com/hdt3213/godis/aof"
	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/datastruct/list"
	"github.com/hdt3213/godis/datastruct/set"
	"github.com/hdt3213/godis/datastruct/sortedset"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/lib/wildcard"
	"github.com/hdt3213/godis/redis/protocol"
	"math"
	"strconv"
	"strings"
	"time"
)

// execDel removes a key from db
func execDel(db *DB, args [][]byte) redis.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}

	deleted := db.Removes(keys...)
	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("del", args...))
	}
	return protocol.MakeIntReply(int64(deleted))
}

func undoDel(db *DB, args [][]byte) []CmdLine {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	return rollbackGivenKeys(db, keys...)
}

// execExists checks if given key is existed in db
func execExists(db *DB, args [][]byte) redis.Reply {
	result := int64(0)
	for _, arg := range args {
		key := string(arg)
		_, exists := db.GetEntity(key)
		if exists {
			result++
		}
	}
	return protocol.MakeIntReply(result)
}

// execFlushDB removes all data in current db
// deprecated, use Server.flushDB
func execFlushDB(db *DB, args [][]byte) redis.Reply {
	db.Flush()
	db.addAof(utils.ToCmdLine3("flushdb", args...))
	return &protocol.OkReply{}
}

// returns the type of entity, including: string, list, hash, set and zset
func typeOf(entity *database.DataEntity) string {
	switch entity.Data.(type) {
	case []byte:
		return "string"
	case list.List[[]byte]:
		return "list"
	case dict.Dict[[]byte]:
		return "hash"
	case *set.Set:
		return "set"
	case *sortedset.SortedSet:
		return "zset"
	}
	return ""
}

func getType(db *DB, key string) string {
	entity, exists := db.GetEntity(key)
	if !exists {
		return "none"
	}
	return typeOf(entity)
}

// getTypeLocked reads the entity with the dict's own read lock. SCAN runs
// without per-key locks, so it must not use the lock-free GetEntity helper.
func getTypeLocked(db *DB, key string) string {
	entity, exists := db.data.Get(key)
	if !exists || db.IsExpired(key) {
		return "none"
	}
	return typeOf(entity)
}

// execType returns the type of entity, including: string, list, hash, set and zset
func execType(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	result := getType(db, key)
	if len(result) > 0 {
		return protocol.MakeStatusReply(result)
	} else {
		return &protocol.UnknownErrReply{}
	}
}

func prepareRename(args [][]byte) ([]string, []string) {
	src := string(args[0])
	dest := string(args[1])
	// RENAME removes src and overwrites dest: both need the write lock.
	return []string{src, dest}, nil
}

// execRename a key
func execRename(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}
	src := string(args[0])
	dest := string(args[1])

	entity, ok := db.GetEntity(src)
	if !ok {
		return protocol.MakeErrReply("no such key")
	}
	if src == dest {
		// RENAME key key is a no-op in Redis, not a deletion.
		return &protocol.OkReply{}
	}
	rawTTL, hasTTL := db.ttlMap.Get(src)
	db.PutEntity(dest, entity)
	db.Remove(src)
	db.Persist(dest) // destination keeps the source ttl, not its own
	if hasTTL {
		db.Expire(dest, rawTTL)
	}
	db.addAof(utils.ToCmdLine3("rename", args...))
	return &protocol.OkReply{}
}

func undoRename(db *DB, args [][]byte) []CmdLine {
	src := string(args[0])
	dest := string(args[1])
	return rollbackGivenKeys(db, src, dest)
}

// execRenameNx a key, only if the new key does not exist
func execRenameNx(db *DB, args [][]byte) redis.Reply {
	src := string(args[0])
	dest := string(args[1])

	_, ok := db.GetEntity(dest)
	if ok {
		return protocol.MakeIntReply(0)
	}

	entity, ok := db.GetEntity(src)
	if !ok {
		return protocol.MakeErrReply("no such key")
	}
	rawTTL, hasTTL := db.ttlMap.Get(src)
	db.Removes(src, dest) // clean src and dest with their ttl
	db.PutEntity(dest, entity)
	if hasTTL {
		db.Persist(src) // clean src and dest with their ttl
		db.Persist(dest)
		db.Expire(dest, rawTTL)
	}
	db.addAof(utils.ToCmdLine3("renamenx", args...))
	return protocol.MakeIntReply(1)
}

// execExpire sets a key's time to live in seconds
func execExpire(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	if ttlArg > maxTTLSeconds {
		return protocol.MakeErrReply(invalidExpireErr("expire"))
	}

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	expireAt := time.Now()
	if ttlArg > 0 {
		expireAt = expireAt.Add(time.Duration(ttlArg) * time.Second)
	}
	db.Expire(key, expireAt)
	db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return protocol.MakeIntReply(1)
}

// execExpireAt sets a key's expiration in unix timestamp
func execExpireAt(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	if raw > maxUnixSeconds || raw < minUnixSeconds {
		return protocol.MakeErrReply(invalidExpireErr("expireat"))
	}
	expireAt := time.Unix(raw, 0)

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	db.Expire(key, expireAt)
	db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return protocol.MakeIntReply(1)
}

// execExpireTime returns the absolute Unix expiration timestamp in seconds at which the given key will expire.
func execExpireTime(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(-2)
	}

	raw, exists := db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(-1)
	}
	return protocol.MakeIntReply(raw.Unix())
}

// execPExpire sets a key's time to live in milliseconds
func execPExpire(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	if ttlArg > maxTTLMillis {
		return protocol.MakeErrReply(invalidExpireErr("pexpire"))
	}

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	expireAt := time.Now()
	if ttlArg > 0 {
		expireAt = expireAt.Add(time.Duration(ttlArg) * time.Millisecond)
	}
	db.Expire(key, expireAt)
	db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return protocol.MakeIntReply(1)
}

// execPExpireAt sets a key's expiration in unix timestamp specified in milliseconds
func execPExpireAt(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	if raw > maxUnixMillis || raw < minUnixMillis {
		return protocol.MakeErrReply(invalidExpireErr("pexpireat"))
	}
	expireAt := time.Unix(0, raw*int64(time.Millisecond))

	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	db.Expire(key, expireAt)

	db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return protocol.MakeIntReply(1)
}

// execPExpireTime returns the absolute Unix expiration timestamp in milliseconds at which the given key will expire.
func execPExpireTime(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(-2)
	}

	raw, exists := db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(-1)
	}
	return protocol.MakeIntReply(raw.UnixMilli())
}

// execTTL returns a key's time to live in seconds
func execTTL(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(-2)
	}

	raw, exists := db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(-1)
	}
	ttl := raw.Sub(time.Now()).Seconds()
	return protocol.MakeIntReply(int64(math.Round(ttl)))
}

// execPTTL returns a key's time to live in milliseconds
func execPTTL(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(-2)
	}

	raw, exists := db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(-1)
	}
	ttl := raw.Sub(time.Now()).Milliseconds()
	return protocol.MakeIntReply(int64(math.Round(float64(ttl))))
}

// execPersist removes expiration from a key
func execPersist(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	_, exists = db.ttlMap.Get(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	db.Persist(key)
	db.addAof(utils.ToCmdLine3("persist", args...))
	return protocol.MakeIntReply(1)
}

// execKeys returns all keys matching the given pattern
func execKeys(db *DB, args [][]byte) redis.Reply {
	pattern, err := wildcard.CompilePattern(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR illegal wildcard")
	}
	result := make([][]byte, 0)
	db.data.ForEach(func(key string, val *database.DataEntity) bool {
		if !pattern.IsMatch(key) {
			return true
		}
		if !db.IsExpired(key) {
			result = append(result, []byte(key))
		}
		return true
	})
	return protocol.MakeMultiBulkReply(result)
}

func toTTLCmd(db *DB, key string) *protocol.MultiBulkReply {
	raw, exists := db.ttlMap.Get(key)
	if !exists {
		// has no TTL
		return protocol.MakeMultiBulkReply(utils.ToCmdLine("PERSIST", key))
	}
	timestamp := strconv.FormatInt(raw.UnixNano()/1000/1000, 10)
	return protocol.MakeMultiBulkReply(utils.ToCmdLine("PEXPIREAT", key, timestamp))
}

func undoExpire(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	return []CmdLine{
		toTTLCmd(db, key).Args,
	}
}

// cloneEntity returns a deep copy of entity. COPY must not share the
// underlying slice/container with the source key: mutating one key through
// APPEND/LPUSH/HSET used to mutate the other.
func cloneEntity(entity *database.DataEntity) *database.DataEntity {
	switch data := entity.Data.(type) {
	case []byte:
		return &database.DataEntity{Data: append([]byte(nil), data...)}
	case list.List[[]byte]:
		cloned := list.NewQuickList[[]byte]()
		data.ForEach(func(_ int, v []byte) bool {
			cloned.Add(append([]byte(nil), v...))
			return true
		})
		return &database.DataEntity{Data: cloned}
	case dict.Dict[[]byte]:
		cloned := dict.MakeSimple[[]byte]()
		data.ForEach(func(field string, v []byte) bool {
			cloned.Put(field, append([]byte(nil), v...))
			return true
		})
		return &database.DataEntity{Data: cloned}
	case *set.Set:
		cloned := set.Make()
		data.ForEach(func(member string) bool {
			cloned.Add(member)
			return true
		})
		return &database.DataEntity{Data: cloned}
	case *sortedset.SortedSet:
		cloned := sortedset.Make()
		data.ForEachByRank(0, data.Len(), false, func(element *sortedset.Element) bool {
			cloned.Add(element.Member, element.Score)
			return true
		})
		return &database.DataEntity{Data: cloned}
	default:
		return entity
	}
}

// execCopy usage: COPY source destination [DB destination-db] [REPLACE]
// This command copies the value stored at the source key to the destination key.
func execCopy(mdb *Server, conn redis.Connection, args [][]byte) redis.Reply {
	dbIndex := conn.GetDBIndex()
	db := mdb.mustSelectDB(dbIndex) // Current DB
	replaceFlag := false
	srcKey := string(args[0])
	destKey := string(args[1])

	// Parse options
	if len(args) > 2 {
		for i := 2; i < len(args); i++ {
			arg := strings.ToLower(string(args[i]))
			if arg == "db" {
				if i+1 >= len(args) {
					return &protocol.SyntaxErrReply{}
				}
				idx, err := strconv.Atoi(string(args[i+1]))
				if err != nil {
					return &protocol.SyntaxErrReply{}
				}
				if idx >= len(mdb.dbSet) || idx < 0 {
					return protocol.MakeErrReply("ERR DB index is out of range")
				}
				dbIndex = idx
				i++
			} else if arg == "replace" {
				replaceFlag = true
			} else {
				return &protocol.SyntaxErrReply{}
			}
		}
	}

	if srcKey == destKey && dbIndex == conn.GetDBIndex() {
		return protocol.MakeErrReply("ERR source and destination objects are the same")
	}

	srcDBIndex := conn.GetDBIndex()
	srcDB := db
	destDB := mdb.mustSelectDB(dbIndex)
	// Lock source and destination. When they live in different databases,
	// acquire the lower database index first to keep a global order.
	if srcDBIndex == dbIndex {
		srcDB.RWLocks([]string{destKey}, []string{srcKey})
		defer srcDB.RWUnLocks([]string{destKey}, []string{srcKey})
	} else if srcDBIndex < dbIndex {
		srcDB.RWLocks(nil, []string{srcKey})
		defer srcDB.RWUnLocks(nil, []string{srcKey})
		destDB.RWLocks([]string{destKey}, nil)
		defer destDB.RWUnLocks([]string{destKey}, nil)
	} else {
		destDB.RWLocks([]string{destKey}, nil)
		defer destDB.RWUnLocks([]string{destKey}, nil)
		srcDB.RWLocks(nil, []string{srcKey})
		defer srcDB.RWUnLocks(nil, []string{srcKey})
	}

	// source key does not exist
	src, exists := db.GetEntity(srcKey)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	if _, exists = destDB.GetEntity(destKey); exists != false {
		// If destKey exists and there is no "replace" option
		if replaceFlag == false {
			return protocol.MakeIntReply(0)
		}
	}

	destDB.PutEntity(destKey, cloneEntity(src))
	raw, exists := db.ttlMap.Get(srcKey)
	destDB.Persist(destKey) // destination inherits the source ttl, not its own
	if exists {
		destDB.Expire(destKey, raw)
	}
	mdb.AddAof(conn.GetDBIndex(), utils.ToCmdLine3("copy", args...))
	return protocol.MakeIntReply(1)
}

// execScan return the result of the scan
func execScan(db *DB, args [][]byte) redis.Reply {
	pattern, count, scanType, errReply := parseScanOptions(args, 1, true)
	if errReply != nil {
		return errReply
	}
	cursor, err := strconv.Atoi(string(args[0]))
	if err != nil || cursor < 0 {
		return protocol.MakeErrReply("ERR invalid cursor")
	}
	keysReply, nextCursor := db.data.DictScan(cursor, count, pattern)
	if nextCursor < 0 {
		return protocol.MakeErrReply("Invalid argument")
	}

	if len(scanType) != 0 {
		// filter in place, O(n) instead of O(n^2) element moves
		n := 0
		for _, key := range keysReply {
			if getTypeLocked(db, string(key)) == scanType {
				keysReply[n] = key
				n++
			}
		}
		keysReply = keysReply[:n]
	}
	result := make([]redis.Reply, 2)
	result[0] = protocol.MakeBulkReply([]byte(strconv.FormatInt(int64(nextCursor), 10)))
	result[1] = protocol.MakeMultiBulkReply(keysReply)

	return protocol.MakeMultiRawReply(result)
}

func init() {
	registerCommand("Del", execDel, writeAllKeys, undoDel, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, -1, 1)
	registerCommand("Expire", execExpire, writeFirstKey, undoExpire, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("ExpireAt", execExpireAt, writeFirstKey, undoExpire, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("ExpireTime", execExpireTime, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("PExpire", execPExpire, writeFirstKey, undoExpire, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("PExpireAt", execPExpireAt, writeFirstKey, undoExpire, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("PExpireTime", execPExpireTime, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("TTL", execTTL, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagRandom, redisFlagFast}, 1, 1, 1)
	registerCommand("PTTL", execPTTL, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagRandom, redisFlagFast}, 1, 1, 1)
	registerCommand("Persist", execPersist, writeFirstKey, undoExpire, 2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("Exists", execExists, readAllKeys, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("Type", execType, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("Rename", execRename, prepareRename, undoRename, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("RenameNx", execRenameNx, prepareRename, undoRename, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("Keys", execKeys, noPrepare, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, 1, 1)
	registerCommand("Scan", execScan, noPrepare, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, 1, 1)
}
