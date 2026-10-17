package dict

import (
	"github.com/hdt3213/godis/lib/wildcard"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ConcurrentDict is thread safe map using sharding lock
type ConcurrentDict[V any] struct {
	table      []*shard[V]
	count      int32
	shardCount int
}

type shard[V any] struct {
	m     map[string]V
	mutex sync.RWMutex
}

func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	}
	return n + 1
}

// MakeConcurrent creates ConcurrentDict with the given shard count
func MakeConcurrent[V any](shardCount int) *ConcurrentDict[V] {
	if shardCount == 1 {
		table := []*shard[V]{
			{
				m: make(map[string]V),
			},
		}
		return &ConcurrentDict[V]{
			count:      0,
			table:      table,
			shardCount: shardCount,
		}
	}
	shardCount = computeCapacity(shardCount)
	table := make([]*shard[V], shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &shard[V]{
			m: make(map[string]V),
		}
	}
	d := &ConcurrentDict[V]{
		count:      0,
		table:      table,
		shardCount: shardCount,
	}
	return d
}

const prime32 = uint32(16777619)

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (dict *ConcurrentDict[V]) spread(key string) uint32 {
	if dict == nil {
		panic("dict is nil")
	}
	if len(dict.table) == 1 {
		return 0
	}
	hashCode := fnv32(key)
	tableSize := uint32(len(dict.table))
	return (tableSize - 1) & hashCode
}

func (dict *ConcurrentDict[V]) getShard(index uint32) *shard[V] {
	if dict == nil {
		panic("dict is nil")
	}
	return dict.table[index]
}

// Get returns the binding value and whether the key is exist
func (dict *ConcurrentDict[V]) Get(key string) (val V, exists bool) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	val, exists = s.m[key]
	return
}

func (dict *ConcurrentDict[V]) GetWithLock(key string) (val V, exists bool) {
	if dict == nil {
		panic("dict is nil")
	}
	/* see PutWithLock */
	index := dict.spread(key)
	s := dict.getShard(index)

	val, exists = s.m[key]
	return
}

// Len returns the number of dict
func (dict *ConcurrentDict[V]) Len() int {
	if dict == nil {
		panic("dict is nil")
	}
	return int(atomic.LoadInt32(&dict.count))
}

// Put puts key value into dict and returns the number of new inserted key-value pair
func (dict *ConcurrentDict[V]) Put(key string, val V) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok { // update
		s.m[key] = val
		return 0
	}
	s.m[key] = val
	dict.addCount()
	return 1
}

// PutWithLock puts with the assuming that the lock key is lock by the invoker
func (dict *ConcurrentDict[V]) PutWithLock(key string, val V) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	/* it is not a strict concurrent safety, depends on the invoker's locking strategy
	two invoker can put different hash of key at the same time
	but the invoker who holds the lock of the key will check whether related keys was changed, then put
	if one invoker put the key, the other put will do duplicated insert, then cause count upset
	so the optimistic approach is, check first, if not exists, not to put and return 0, the invoker should
	retry after a while */
	index := dict.spread(key)
	s := dict.getShard(index)

	/* no multiple inserts:
	if the field was not exists, err: can't put the related value into result
	if the field was updated by another invoker, may cause unpredictable behavior */
	if _, ok := s.m[key]; ok { // update
		s.m[key] = val
		return 0
	}
	s.m[key] = val
	dict.addCount()
	return 1
}

// PutIfAbsent puts value if the key is not exists and returns the number of updated key-value
func (dict *ConcurrentDict[V]) PutIfAbsent(key string, val V) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		return 0
	}
	s.m[key] = val
	dict.addCount()
	return 1
}

func (dict *ConcurrentDict[V]) PutIfAbsentWithLock(key string, val V) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)

	if _, ok := s.m[key]; ok {
		return 0
	}
	s.m[key] = val
	dict.addCount()
	return 1
}

// PutIfExists puts value if the key is existed and returns the number of inserted key-value
func (dict *ConcurrentDict[V]) PutIfExists(key string, val V) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	return 0
}

func (dict *ConcurrentDict[V]) PutIfExistsWithLock(key string, val V) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	return 0
}

// Remove removes the key and return the number of deleted key-value
func (dict *ConcurrentDict[V]) Remove(key string) (val V, result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if val, ok := s.m[key]; ok {
		delete(s.m, key)
		dict.decreaseCount()
		return val, 1
	}
	return val, 0
}

func (dict *ConcurrentDict[V]) RemoveWithLock(key string) (val V, result int) {
	if dict == nil {
		panic("dict is nil")
	}
	index := dict.spread(key)
	s := dict.getShard(index)

	if val, ok := s.m[key]; ok {
		delete(s.m, key)
		dict.decreaseCount()
		return val, 1
	}
	return val, 0
}

func (dict *ConcurrentDict[V]) addCount() int32 {
	return atomic.AddInt32(&dict.count, 1)
}

func (dict *ConcurrentDict[V]) decreaseCount() int32 {
	return atomic.AddInt32(&dict.count, -1)
}

// ForEach traversal the dict
// it may not visit new entry inserted during traversal
func (dict *ConcurrentDict[V]) ForEach(consumer Consumer[V]) {
	if dict == nil {
		panic("dict is nil")
	}

	for _, s := range dict.table {
		s.mutex.RLock()
		f := func() bool {
			defer s.mutex.RUnlock()
			for key, value := range s.m {
				continues := consumer(key, value)
				if !continues {
					return false
				}
			}
			return true
		}
		if !f() {
			break
		}
	}
}

// Keys returns all keys in dict
func (dict *ConcurrentDict[V]) Keys() []string {
	keys := make([]string, dict.Len())
	i := 0
	dict.ForEach(func(key string, val V) bool {
		if i < len(keys) {
			keys[i] = key
			i++
		} else {
			keys = append(keys, key)
		}
		return true
	})
	return keys
}

// RandomKey returns a key randomly
func (shard *shard[V]) RandomKey() string {
	if shard == nil {
		panic("shard is nil")
	}
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	for key := range shard.m {
		return key
	}
	return ""
}

// RandomKeys randomly returns keys of the given number, may contain duplicated key
func (dict *ConcurrentDict[V]) RandomKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}
	shardCount := len(dict.table)

	result := make([]string, limit)
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < limit; {
		s := dict.getShard(uint32(nR.Intn(shardCount)))
		if s == nil {
			continue
		}
		key := s.RandomKey()
		if key != "" {
			result[i] = key
			i++
		}
	}
	return result
}

// RandomDistinctKeys randomly returns keys of the given number, won't contain duplicated key
func (dict *ConcurrentDict[V]) RandomDistinctKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}

	shardCount := len(dict.table)
	result := make(map[string]struct{})
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(result) < limit {
		shardIndex := uint32(nR.Intn(shardCount))
		s := dict.getShard(shardIndex)
		if s == nil {
			continue
		}
		key := s.RandomKey()
		if key != "" {
			if _, exists := result[key]; !exists {
				result[key] = struct{}{}
			}
		}
	}
	arr := make([]string, limit)
	i := 0
	for k := range result {
		arr[i] = k
		i++
	}
	return arr
}

// Clear removes all keys in dict
func (dict *ConcurrentDict[V]) Clear() {
	*dict = *MakeConcurrent[V](dict.shardCount)
}

func (dict *ConcurrentDict[V]) toLockIndices(keys []string, reverse bool) []uint32 {
	indexMap := make(map[uint32]struct{})
	for _, key := range keys {
		index := dict.spread(key)
		indexMap[index] = struct{}{}
	}
	indices := make([]uint32, 0, len(indexMap))
	for index := range indexMap {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(i, j int) bool {
		if !reverse {
			return indices[i] < indices[j]
		}
		return indices[i] > indices[j]
	})
	return indices
}

// concatKeys merges write and read keys into a fresh slice. Appending into
// writeKeys directly would write into its backing array's free capacity,
// which may be shared with other slices held by the caller.
func concatKeys(writeKeys []string, readKeys []string) []string {
	keys := make([]string, 0, len(writeKeys)+len(readKeys))
	keys = append(keys, writeKeys...)
	keys = append(keys, readKeys...)
	return keys
}

// RWLocks locks write keys and read keys together. allow duplicate keys
func (dict *ConcurrentDict[V]) RWLocks(writeKeys []string, readKeys []string) {
	keys := concatKeys(writeKeys, readKeys)
	indices := dict.toLockIndices(keys, false)
	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		idx := dict.spread(wKey)
		writeIndexSet[idx] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := &dict.table[index].mutex
		if w {
			mu.Lock()
		} else {
			mu.RLock()
		}
	}
}

// RWUnLocks unlocks write keys and read keys together. allow duplicate keys
func (dict *ConcurrentDict[V]) RWUnLocks(writeKeys []string, readKeys []string) {
	keys := concatKeys(writeKeys, readKeys)
	indices := dict.toLockIndices(keys, true)
	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		idx := dict.spread(wKey)
		writeIndexSet[idx] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := &dict.table[index].mutex
		if w {
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
	}
}

func stringsToBytes(strSlice []string) [][]byte {
	byteSlice := make([][]byte, len(strSlice))
	for i, str := range strSlice {
		byteSlice[i] = []byte(str)
	}
	return byteSlice
}

// DictScan scans the dict keys matching the pattern, values are not included.
// cursor is the shard index to continue from; 0 means the scan is completed
func (dict *ConcurrentDict[V]) DictScan(cursor int, count int, pattern string) ([][]byte, int) {
	size := dict.Len()
	result := make([][]byte, 0)

	if pattern == "*" && count >= size {
		return stringsToBytes(dict.Keys()), 0
	}

	matchKey, err := wildcard.CompilePattern(pattern)
	if err != nil {
		return result, -1
	}

	shardCount := len(dict.table)
	shardIndex := cursor

	for shardIndex < shardCount {
		shard := dict.table[shardIndex]
		shard.mutex.RLock()
		if len(result)+len(shard.m) > count && shardIndex > cursor {
			shard.mutex.RUnlock()
			return result, shardIndex
		}

		for key := range shard.m {
			if pattern == "*" || matchKey.IsMatch(key) {
				result = append(result, []byte(key))
			}
		}
		shard.mutex.RUnlock()
		shardIndex++
	}

	return result, 0
}
