package database

import (
	"fmt"
	"sync"
	"testing"

	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

// TestVersionMonotonicUnderConcurrency checks that concurrent writes of the
// same key increment its version exactly once per write. Bumping versions
// outside the RWLocks region (a non-atomic read-modify-write) could lose
// increments or move the version backward, which could make WATCH consider
// a concurrently modified key as unchanged.
func TestVersionMonotonicUnderConcurrency(t *testing.T) {
	db := makeTestDB()
	const goroutines = 8
	const writesPerGoroutine = 200
	total := goroutines * writesPerGoroutine

	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			<-start
			for i := 0; i < writesPerGoroutine; i++ {
				cmdLine := utils.ToCmdLine("set", "key", fmt.Sprintf("v-%d-%d", g, i))
				result := db.Exec(nil, cmdLine)
				if protocol.IsErrorReply(result) {
					t.Errorf("set failed unexpectedly: %v", result)
					return
				}
			}
		}(g)
	}
	close(start)
	wg.Wait()

	if got := db.GetVersion("key"); got != uint32(total) {
		t.Fatalf("expected version %d after %d concurrent writes, got %d", total, total, got)
	}
}

// TestWatchDetectsConcurrentChange formulates the user-visible effect of the
// version bug: a WATCHed key must be reported as changed when it is written
// concurrently (the exact check performed by execMulti through isWatchingChanged,
// replayed here without the full transaction).
func TestWatchDetectsConcurrentChange(t *testing.T) {
	db := makeTestDB()
	cmdLine := utils.ToCmdLine("set", "key", "v0")
	result := db.Exec(nil, cmdLine)
	if protocol.IsErrorReply(result) {
		t.Fatalf("set failed: %v", result)
	}
	watchingVersion := db.GetVersion("key")

	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			<-start
			for i := 0; i < 100; i++ {
				db.Exec(nil, utils.ToCmdLine("set", "key", fmt.Sprintf("v-%d-%d", g, i)))
			}
		}(g)
	}
	close(start)
	wg.Wait()

	watching := map[string]uint32{"key": watchingVersion}
	if !isWatchingChanged(db, watching) {
		t.Fatal("WATCH missed modifications of a concurrently written key")
	}
}
