package dict

import (
	"github.com/hdt3213/godis/lib/wildcard"
)

// SimpleDict wraps a map, it is not thread safe
type SimpleDict[V any] struct {
	m map[string]V
}

// MakeSimple makes a new map
func MakeSimple[V any]() *SimpleDict[V] {
	return &SimpleDict[V]{
		m: make(map[string]V),
	}
}

// Get returns the binding value and whether the key is exist
func (dict *SimpleDict[V]) Get(key string) (val V, exists bool) {
	val, ok := dict.m[key]
	return val, ok
}

// Len returns the number of dict
func (dict *SimpleDict[V]) Len() int {
	if dict.m == nil {
		panic("m is nil")
	}
	return len(dict.m)
}

// Put puts key value into dict and returns the number of new inserted key-value
func (dict *SimpleDict[V]) Put(key string, val V) (result int) {
	_, existed := dict.m[key]
	dict.m[key] = val
	if existed {
		return 0
	}
	return 1
}

// PutIfAbsent puts value if the key is not exists and returns the number of updated key-value
func (dict *SimpleDict[V]) PutIfAbsent(key string, val V) (result int) {
	_, existed := dict.m[key]
	if existed {
		return 0
	}
	dict.m[key] = val
	return 1
}

// PutIfExists puts value if the key exists and returns the number of inserted key-value
func (dict *SimpleDict[V]) PutIfExists(key string, val V) (result int) {
	_, existed := dict.m[key]
	if !existed {
		return 0
	}
	dict.m[key] = val
	return 1
}

// Remove removes the key and return the number of deleted key-value
func (dict *SimpleDict[V]) Remove(key string) (val V, result int) {
	val, existed := dict.m[key]
	if existed {
		delete(dict.m, key)
		return val, 1
	}
	return val, 0
}

// Keys returns all the keys in the dict
func (dict *SimpleDict[V]) Keys() []string {
	result := make([]string, len(dict.m))
	i := 0
	for k := range dict.m {
		result[i] = k
		i++
	}
	return result
}

// ForEach traversal the dict
func (dict *SimpleDict[V]) ForEach(consumer Consumer[V]) {
	for k, v := range dict.m {
		if !consumer(k, v) {
			break
		}
	}
}

// RandomKeys randomly returns keys of the given number, may contain duplicated key
func (dict *SimpleDict[V]) RandomKeys(limit int) []string {
	result := make([]string, limit)
	for i := 0; i < limit; i++ {
		for k := range dict.m {
			result[i] = k
			break
		}
	}
	return result
}

// RandomDistinctKeys randomly returns keys of the given number, won't contain duplicated key
func (dict *SimpleDict[V]) RandomDistinctKeys(limit int) []string {
	size := limit
	if size > len(dict.m) {
		size = len(dict.m)
	}
	result := make([]string, size)
	i := 0
	for k := range dict.m {
		if i == size {
			break
		}
		result[i] = k
		i++
	}
	return result
}

// Clear removes all keys in dict
func (dict *SimpleDict[V]) Clear() {
	*dict = *MakeSimple[V]()
}

// DictScan traverses the whole dict (SimpleDict has no cursor concept) and
// returns the keys matching the pattern; values are not included
func (dict *SimpleDict[V]) DictScan(cursor int, count int, pattern string) ([][]byte, int) {
	result := make([][]byte, 0)
	matchKey, err := wildcard.CompilePattern(pattern)
	if err != nil {
		return result, -1
	}
	for k := range dict.m {
		if pattern == "*" || matchKey.IsMatch(k) {
			result = append(result, []byte(k))
		}
	}
	return result, 0
}
