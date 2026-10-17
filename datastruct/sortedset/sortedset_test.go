package sortedset

import (
	"testing"

	"github.com/hdt3213/godis/lib/utils"
)

func TestSortedSet_PopMin(t *testing.T) {
	var set = Make()
	set.Add("s1", 1)
	set.Add("s2", 2)
	set.Add("s3", 3)
	set.Add("s4", 4)

	var results = set.PopMin(2)
	if results[0].Member != "s1" || results[1].Member != "s2" {
		t.Fail()
	}
}

func TestSetScan(t *testing.T) {
	set := Make()
	size := 10
	for i := 0; i < size; i++ {
		str := "a" + utils.RandString(5)
		set.Add(str, float64(i))
	}
	keys, nextCursor := set.ZSetScan(0, size, "*")
	if len(keys) != size*2 {
		t.Errorf("expect %d keys, actual: %d", size*2, len(keys))
		return
	}
	if nextCursor != 0 {
		t.Errorf("expect 0, actual: %d", nextCursor)
		return
	}
	for i := 0; i < size; i++ {
		str := "b" + utils.RandString(5)
		set.Add(str, float64(i+size))
	}
	keys, _ = set.ZSetScan(0, size*2, "a*")
	if len(keys) != size*2 {
		t.Errorf("expect %d keys, actual: %d", size*2, len(keys))
		return
	}
}

func TestZSetScanInvalidation(t *testing.T) {
	set := Make()
	set.Add("a", 1)
	set.Add("b", 2)
	if keys, _ := set.ZSetScan(0, 10, "*"); len(keys) != 4 {
		t.Fatalf("initial scan = %d items, want 4", len(keys))
	}
	set.Remove("a")
	set.Add("c", 3)
	keys, _ := set.ZSetScan(0, 10, "*")
	if len(keys) != 4 || string(keys[0]) != "b" || string(keys[2]) != "c" {
		t.Fatalf("scan after mutation = %q, want [b 2 c 3]", keys)
	}
}
