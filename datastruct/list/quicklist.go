package list

import "container/list"

// pageSize must be even
const pageSize = 1024

// QuickList is a linked list of page (which type is []T)
// QuickList has better performance than LinkedList of Add, Range and memory usage
type QuickList[T any] struct {
	data *list.List // list of []T
	size int
}

// iterator of QuickList, move between [-1, ql.Len()]
type iterator[T any] struct {
	node   *list.Element
	offset int
	ql     *QuickList[T]
}

func NewQuickList[T any]() *QuickList[T] {
	l := &QuickList[T]{
		data: list.New(),
	}
	return l
}

// Add adds value to the tail
func (ql *QuickList[T]) Add(val T) {
	ql.size++
	if ql.data.Len() == 0 { // empty list
		page := make([]T, 0, pageSize)
		page = append(page, val)
		ql.data.PushBack(page)
		return
	}
	// assert list.data.Back() != nil
	backNode := ql.data.Back()
	backPage := backNode.Value.([]T)
	if len(backPage) == cap(backPage) { // full page, create new page
		page := make([]T, 0, pageSize)
		page = append(page, val)
		ql.data.PushBack(page)
		return
	}
	// append into page
	backPage = append(backPage, val)
	backNode.Value = backPage
}

// find returns page and in-page-offset of given index
func (ql *QuickList[T]) find(index int) *iterator[T] {
	if ql == nil {
		panic("list is nil")
	}
	if index < 0 || index >= ql.size {
		panic("index out of bound")
	}
	var n *list.Element
	var page []T
	var pageBeg int
	if index < ql.size/2 {
		// search from front
		n = ql.data.Front()
		pageBeg = 0
		for {
			// assert: n != nil
			page = n.Value.([]T)
			if pageBeg+len(page) > index {
				break
			}
			pageBeg += len(page)
			n = n.Next()
		}
	} else {
		// search from back
		n = ql.data.Back()
		pageBeg = ql.size
		for {
			page = n.Value.([]T)
			pageBeg -= len(page)
			if pageBeg <= index {
				break
			}
			n = n.Prev()
		}
	}
	pageOffset := index - pageBeg
	return &iterator[T]{
		node:   n,
		offset: pageOffset,
		ql:     ql,
	}
}

func (iter *iterator[T]) get() T {
	return iter.page()[iter.offset]
}

func (iter *iterator[T]) page() []T {
	return iter.node.Value.([]T)
}

// next returns whether iter is in bound
func (iter *iterator[T]) next() bool {
	page := iter.page()
	if iter.offset < len(page)-1 {
		iter.offset++
		return true
	}
	// move to next page
	if iter.node == iter.ql.data.Back() {
		// already at last node
		iter.offset = len(page)
		return false
	}
	iter.offset = 0
	iter.node = iter.node.Next()
	return true
}

// prev returns whether iter is in bound
func (iter *iterator[T]) prev() bool {
	if iter.offset > 0 {
		iter.offset--
		return true
	}
	// move to prev page
	if iter.node == iter.ql.data.Front() {
		// already at first page
		iter.offset = -1
		return false
	}
	iter.node = iter.node.Prev()
	prevPage := iter.node.Value.([]T)
	iter.offset = len(prevPage) - 1
	return true
}

func (iter *iterator[T]) atEnd() bool {
	if iter.ql.data.Len() == 0 {
		return true
	}
	if iter.node != iter.ql.data.Back() {
		return false
	}
	page := iter.page()
	return iter.offset == len(page)
}

func (iter *iterator[T]) atBegin() bool {
	if iter.ql.data.Len() == 0 {
		return true
	}
	if iter.node != iter.ql.data.Front() {
		return false
	}
	return iter.offset == -1
}

// Get returns value at the given index
func (ql *QuickList[T]) Get(index int) (val T) {
	iter := ql.find(index)
	return iter.get()
}

func (iter *iterator[T]) set(val T) {
	page := iter.page()
	page[iter.offset] = val
}

// Set updates value at the given index, the index should between [0, list.size]
func (ql *QuickList[T]) Set(index int, val T) {
	iter := ql.find(index)
	iter.set(val)
}

func (ql *QuickList[T]) Insert(index int, val T) {
	if index == ql.size { // insert at
		ql.Add(val)
		return
	}
	iter := ql.find(index)
	page := iter.node.Value.([]T)
	if len(page) < pageSize {
		// insert into not full page
		page = append(page[:iter.offset+1], page[iter.offset:]...)
		page[iter.offset] = val
		iter.node.Value = page
		ql.size++
		return
	}
	// insert into a full page may cause memory copy, so we split a full page into two half pages
	var nextPage []T
	nextPage = append(nextPage, page[pageSize/2:]...) // pageSize must be even
	page = page[:pageSize/2]
	if iter.offset < len(page) {
		page = append(page[:iter.offset+1], page[iter.offset:]...)
		page[iter.offset] = val
	} else {
		i := iter.offset - pageSize/2
		nextPage = append(nextPage[:i+1], nextPage[i:]...)
		nextPage[i] = val
	}
	// store current page and next page
	iter.node.Value = page
	ql.data.InsertAfter(nextPage, iter.node)
	ql.size++
}

func (iter *iterator[T]) remove() T {
	page := iter.page()
	val := page[iter.offset]
	page = append(page[:iter.offset], page[iter.offset+1:]...)
	if len(page) > 0 {
		// page is not empty, update iter.offset only
		iter.node.Value = page
		if iter.offset == len(page) {
			// removed page[-1], node should move to next page
			if iter.node != iter.ql.data.Back() {
				iter.node = iter.node.Next()
				iter.offset = 0
			}
			// else: assert iter.atEnd() == true
		}
	} else {
		// page is empty, update iter.node and iter.offset
		if iter.node == iter.ql.data.Back() {
			// move iter so iter.atEnd() can be true
			if prevNode := iter.node.Prev(); prevNode != nil {
				iter.ql.data.Remove(iter.node)
				iter.node = prevNode
				iter.offset = len(prevNode.Value.([]T))
			} else {
				// removed last element, ql is empty now
				iter.ql.data.Remove(iter.node)
				iter.node = nil
				iter.offset = 0
			}
		} else {
			nextNode := iter.node.Next()
			iter.ql.data.Remove(iter.node)
			iter.node = nextNode
			iter.offset = 0
		}
	}
	iter.ql.size--
	return val
}

// Remove removes value at the given index
func (ql *QuickList[T]) Remove(index int) T {
	iter := ql.find(index)
	return iter.remove()
}

// Len returns the number of elements in list
func (ql *QuickList[T]) Len() int {
	return ql.size
}

// RemoveLast removes the last element and returns its value;
// it returns the zero value of T when the list is empty
func (ql *QuickList[T]) RemoveLast() (val T) {
	if ql.Len() == 0 {
		return val
	}
	ql.size--
	lastNode := ql.data.Back()
	lastPage := lastNode.Value.([]T)
	if len(lastPage) == 1 {
		ql.data.Remove(lastNode)
		return lastPage[0]
	}
	val = lastPage[len(lastPage)-1]
	lastPage = lastPage[:len(lastPage)-1]
	lastNode.Value = lastPage
	return val
}

// RemoveAllByVal removes all elements with the given val
func (ql *QuickList[T]) RemoveAllByVal(expected Expected[T]) int {
	iter := ql.find(0)
	removed := 0
	for !iter.atEnd() {
		if expected(iter.get()) {
			iter.remove()
			removed++
		} else {
			iter.next()
		}
	}
	return removed
}

// RemoveByVal removes at most `count` values of the specified value in this list
// scan from left to right
func (ql *QuickList[T]) RemoveByVal(expected Expected[T], count int) int {
	if ql.size == 0 {
		return 0
	}
	iter := ql.find(0)
	removed := 0
	for !iter.atEnd() {
		if expected(iter.get()) {
			iter.remove()
			removed++
			if removed == count {
				break
			}
		} else {
			iter.next()
		}
	}
	return removed
}

func (ql *QuickList[T]) ReverseRemoveByVal(expected Expected[T], count int) int {
	if ql.size == 0 {
		return 0
	}
	iter := ql.find(ql.size - 1)
	removed := 0
	for !iter.atBegin() {
		if expected(iter.get()) {
			iter.remove()
			removed++
			if removed == count {
				break
			}
		}
		iter.prev()
	}
	return removed
}

// ForEach visits each element in the list
// if the consumer returns false, the loop will be break
func (ql *QuickList[T]) ForEach(consumer Consumer[T]) {
	if ql == nil {
		panic("list is nil")
	}
	if ql.Len() == 0 {
		return
	}
	iter := ql.find(0)
	i := 0
	for {
		goNext := consumer(i, iter.get())
		if !goNext {
			break
		}
		i++
		if !iter.next() {
			break
		}
	}
}

func (ql *QuickList[T]) Contains(expected Expected[T]) bool {
	contains := false
	ql.ForEach(func(i int, actual T) bool {
		if expected(actual) {
			contains = true
			return false
		}
		return true
	})
	return contains
}

// Range returns elements which index within [start, stop)
func (ql *QuickList[T]) Range(start int, stop int) []T {
	if start < 0 || start >= ql.Len() {
		panic("`start` out of range")
	}
	if stop < start || stop > ql.Len() {
		panic("`stop` out of range")
	}
	sliceSize := stop - start
	slice := make([]T, 0, sliceSize)
	iter := ql.find(start)
	i := 0
	for i < sliceSize {
		slice = append(slice, iter.get())
		iter.next()
		i++
	}
	return slice
}
