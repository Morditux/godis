package dict

// Consumer is used to traversal dict, if it returns false the traversal will be break
type Consumer[V any] func(key string, val V) bool

// Dict is interface of a key-value data structure.
// DictScan scans the dict keys matching the pattern and returns them;
// values are not included in the result
type Dict[V any] interface {
	Get(key string) (val V, exists bool)
	Len() int
	Put(key string, val V) (result int)
	PutIfAbsent(key string, val V) (result int)
	PutIfExists(key string, val V) (result int)
	Remove(key string) (val V, result int)
	ForEach(consumer Consumer[V])
	Keys() []string
	RandomKeys(limit int) []string
	RandomDistinctKeys(limit int) []string
	Clear()
	DictScan(cursor int, count int, pattern string) ([][]byte, int)
}
