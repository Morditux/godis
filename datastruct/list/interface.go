package list

// Expected check whether given item is equals to expected value
type Expected[T any] func(a T) bool

// Consumer traverses list.
// It receives index and value as params, returns true to continue traversal, while returns false to break
type Consumer[T any] func(i int, v T) bool

// List is a generic list interface
type List[T any] interface {
	Add(val T)
	Get(index int) (val T)
	Set(index int, val T)
	Insert(index int, val T)
	Remove(index int) (val T)
	RemoveLast() (val T)
	RemoveAllByVal(expected Expected[T]) int
	RemoveByVal(expected Expected[T], count int) int
	ReverseRemoveByVal(expected Expected[T], count int) int
	Len() int
	ForEach(consumer Consumer[T])
	Contains(expected Expected[T]) bool
	Range(start int, stop int) []T
}
