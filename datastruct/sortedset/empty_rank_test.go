package sortedset

import "testing"

// TestForEachByRankEmpty covers the panic that used to crash BGREWRITEAOF and
// ZCOUNT when a sorted set had no members left.
func TestForEachByRankEmpty(t *testing.T) {
	z := Make()
	z.ForEachByRank(0, 0, false, func(*Element) bool {
		t.Fatal("consumer called on an empty sorted set")
		return true
	})
	z.ForEachByRank(0, 10, true, func(*Element) bool {
		t.Fatal("consumer called on an empty sorted set")
		return true
	})
	if got := z.RangeByRank(0, 0, false); len(got) != 0 {
		t.Fatalf("RangeByRank on empty set returned %d elements", len(got))
	}
	min, err := ParseScoreBorder("-inf")
	if err != nil {
		t.Fatal(err)
	}
	max, err := ParseScoreBorder("+inf")
	if err != nil {
		t.Fatal(err)
	}
	if got := z.RangeCount(min, max); got != 0 {
		t.Fatalf("RangeCount on empty set = %d, want 0", got)
	}
}

func TestForEachByRankOutOfRangeIsClamped(t *testing.T) {
	z := Make()
	z.Add("a", 1)
	z.Add("b", 2)
	var seen []string
	z.ForEachByRank(1, 100, false, func(e *Element) bool {
		seen = append(seen, e.Member)
		return true
	})
	if len(seen) != 1 || seen[0] != "b" {
		t.Fatalf("ForEachByRank clamp = %v, want [b]", seen)
	}
	if got := z.RangeByRank(5, 10, false); len(got) != 0 {
		t.Fatalf("RangeByRank out of range returned %d elements", len(got))
	}
	if got := z.RangeByRank(1, 0, false); len(got) != 0 {
		t.Fatalf("RangeByRank with stop<start returned %d elements", len(got))
	}
}
