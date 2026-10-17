package set

import (
	"sort"

	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/lib/wildcard"
)

// Set is a set of elements based on hash table
type Set struct {
	dict dict.Dict[struct{}]
}

// Make creates a new set
func Make(members ...string) *Set {
	set := &Set{
		dict: dict.MakeSimple[struct{}](),
	}
	for _, member := range members {
		set.Add(member)
	}
	return set
}

// MakeConcurrentSafe creates a new set which is concurrent safe
func MakeConcurrentSafe(members ...string) *Set {
	set := &Set{
		dict: dict.MakeConcurrent[struct{}](1),
	}
	for _, member := range members {
		set.Add(member)
	}
	return set
}

// Add adds member into set
func (set *Set) Add(val string) int {
	return set.dict.Put(val, struct{}{})
}

// Remove removes member from set
func (set *Set) Remove(val string) int {
	_, ret := set.dict.Remove(val)
	return ret
}

// Has returns true if the val exists in the set
func (set *Set) Has(val string) bool {
	if set == nil || set.dict == nil {
		return false
	}
	_, exists := set.dict.Get(val)
	return exists
}

// Len returns number of members in the set
func (set *Set) Len() int {
	if set == nil || set.dict == nil {
		return 0
	}
	return set.dict.Len()
}

// ToSlice convert set to []string
func (set *Set) ToSlice() []string {
	slice := make([]string, set.Len())
	i := 0
	set.dict.ForEach(func(key string, val struct{}) bool {
		if i < len(slice) {
			slice[i] = key
		} else {
			// set extended during traversal
			slice = append(slice, key)
		}
		i++
		return true
	})
	return slice
}

// ForEach visits each member in the set
func (set *Set) ForEach(consumer func(member string) bool) {
	if set == nil || set.dict == nil {
		return
	}
	set.dict.ForEach(func(key string, val struct{}) bool {
		return consumer(key)
	})
}

// ShallowCopy copies all members to another set
func (set *Set) ShallowCopy() *Set {
	result := Make()
	set.ForEach(func(member string) bool {
		result.Add(member)
		return true
	})
	return result
}

// Intersect intersects two sets
func Intersect(sets ...*Set) *Set {
	result := Make()
	if len(sets) == 0 {
		return result
	}

	countMap := make(map[string]int)
	for _, set := range sets {
		set.ForEach(func(member string) bool {
			countMap[member]++
			return true
		})
	}
	for k, v := range countMap {
		if v == len(sets) {
			result.Add(k)
		}
	}
	return result
}

// Union adds two sets
func Union(sets ...*Set) *Set {
	result := Make()
	for _, set := range sets {
		set.ForEach(func(member string) bool {
			result.Add(member)
			return true
		})
	}
	return result
}

// Diff subtracts two sets
func Diff(sets ...*Set) *Set {
	if len(sets) == 0 {
		return Make()
	}
	result := sets[0].ShallowCopy()
	for i := 1; i < len(sets); i++ {
		sets[i].ForEach(func(member string) bool {
			result.Remove(member)
			return true
		})
		if result.Len() == 0 {
			break
		}
	}
	return result
}

// RandomMembers randomly returns keys of the given number, may contain duplicated key
func (set *Set) RandomMembers(limit int) []string {
	if set == nil || set.dict == nil {
		return nil
	}
	return set.dict.RandomKeys(limit)
}

// RandomDistinctMembers randomly returns keys of the given number, won't contain duplicated key
func (set *Set) RandomDistinctMembers(limit int) []string {
	return set.dict.RandomDistinctKeys(limit)
}

// Scan set with cursor and pattern. The cursor is the index of the next
// member in a sorted snapshot of the set, so a full iteration makes progress
// even though the underlying dict is unordered.
func (set *Set) SetScan(cursor int, count int, pattern string) ([][]byte, int) {
	if set == nil {
		return nil, 0
	}
	if cursor < 0 {
		return nil, -1
	}
	if count <= 0 {
		count = 10
	}
	var matchKey *wildcard.Pattern
	var err error
	if pattern != "*" {
		matchKey, err = wildcard.CompilePattern(pattern)
		if err != nil {
			return nil, -1
		}
	}
	members := set.ToSlice()
	sort.Strings(members)
	if cursor >= len(members) {
		return nil, 0
	}
	result := make([][]byte, 0, count)
	i := cursor
	scanned := 0
	for i < len(members) && scanned < count {
		member := members[i]
		i++
		scanned++
		if matchKey != nil && !matchKey.IsMatch(member) {
			continue
		}
		result = append(result, []byte(member))
	}
	next := 0
	if i < len(members) {
		next = i
	}
	return result, next
}
