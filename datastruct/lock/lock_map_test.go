package lock

import (
	"testing"
)

// TestRWLocksDoesNotMutateWriteKeys regarded as regression test for a latent
// bug: RWLocks used to build its key list with `append(writeKeys, readKeys...)`,
// which wrote into the spare capacity of the writeKeys backing array that may
// still be referenced by the caller.
func TestRWLocksDoesNotMutateWriteKeys(t *testing.T) {
	locks := Make(16)
	writeKeys := make([]string, 1, 4)
	writeKeys[0] = "key-a"
	readKeys := []string{"key-b", "key-c"}

	locks.RWLocks(writeKeys, readKeys)
	exported := writeKeys[:cap(writeKeys)]
	if exported[1] != "" || exported[2] != "" || exported[3] != "" {
		t.Fatalf("RWLocks wrote into the caller's backing array: %q", exported)
	}

	defer func() {
		locks.RWUnLocks(writeKeys, readKeys)
		exported = writeKeys[:cap(writeKeys)]
		for _, item := range exported[len(writeKeys):] {
			if item == "key-b" || item == "key-c" {
				t.Errorf("RWUnLocks wrote into the caller's backing array: %q", exported)
			}
		}
	}()
}

func TestLocksAndRUnLocks(t *testing.T) {
	locks := Make(16)
	locks.Locks("k1", "k1", "k2")
	locks.UnLocks("k2", "k1")
	locks.RLocks("k3")
	locks.RUnLocks("k3")
}
