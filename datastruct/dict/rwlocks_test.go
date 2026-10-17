package dict

import (
	"testing"
)

// TestRWLocksDoesNotMutateWriteKeys is a regression test for a latent bug:
// RWLocks used to build its key list with `append(writeKeys, readKeys...)`,
// which wrote into the spare capacity of the writeKeys backing array that may
// still be referenced by the caller (e.g. slices later extended by append).
func TestRWLocksDoesNotMutateWriteKeys(t *testing.T) {
	dict := MakeConcurrent[int](16)
	writeKeys := make([]string, 1, 4)
	writeKeys[0] = "key-a"
	readKeys := []string{"key-b", "key-c"}

	dict.RWLocks(writeKeys, readKeys)
	// view the hidden capacity area of the caller's slice
	exported := writeKeys[:cap(writeKeys)]
	if exported[1] != "" || exported[2] != "" || exported[3] != "" {
		t.Fatalf("RWLocks wrote into the caller's backing array: %q", exported)
	}

	dict.RWUnLocks(writeKeys, readKeys)
	exported = writeKeys[:cap(writeKeys)]
	if exported[1] != "" || exported[2] != "" || exported[3] != "" {
		t.Fatalf("RWUnLocks wrote into the caller's backing array: %q", exported)
	}
}
