package aof

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	dbiface "github.com/hdt3213/godis/interface/database"
)

func makeRewritePersister(t *testing.T) *Persister {
	t.Helper()
	filename := filepath.Join(t.TempDir(), "rewrite.log")
	persister, err := NewPersister(&fakeEngine{}, filename, false, FsyncNo, func() dbiface.DBEngine {
		return &fakeEngine{}
	})
	if err != nil {
		t.Fatalf("new persister: %v", err)
	}
	t.Cleanup(func() { persister.Close() })
	return persister
}

func TestRewriteSingleFlight(t *testing.T) {
	persister := makeRewritePersister(t)

	persister.rewriteMu.Lock()
	if err := persister.Rewrite(); !errors.Is(err, ErrRewriteInProgress) {
		t.Fatalf("Rewrite while locked = %v, want ErrRewriteInProgress", err)
	}
	if err := persister.StartBackgroundRewrite(); !errors.Is(err, ErrRewriteInProgress) {
		t.Fatalf("StartBackgroundRewrite while locked = %v, want ErrRewriteInProgress", err)
	}
	persister.rewriteMu.Unlock()

	if err := persister.Rewrite(); err != nil {
		t.Fatalf("Rewrite after unlock failed: %v", err)
	}
	// the lock must be released after a successful rewrite
	if err := persister.Rewrite(); err != nil {
		t.Fatalf("second Rewrite failed: %v", err)
	}
}

func TestGenerateAofPropagatesWriteError(t *testing.T) {
	persister := makeRewritePersister(t)
	file, err := os.CreateTemp(t.TempDir(), "*.aof")
	if err != nil {
		t.Fatal(err)
	}
	ctx := &RewriteCtx{tmpFile: file, fileSize: 0}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := persister.DoRewrite(ctx); err == nil {
		t.Fatal("DoRewrite should return the write error")
	}
}

func TestFinishRewriteRemovesTempOnError(t *testing.T) {
	persister := makeRewritePersister(t)
	ctx, err := persister.StartRewrite()
	if err != nil {
		t.Fatalf("StartRewrite: %v", err)
	}
	tmpName := ctx.tmpFile.Name()
	// make the live AOF unreadable so FinishRewrite fails after creating tmp
	if err := os.Remove(persister.aofFilename); err != nil {
		t.Fatal(err)
	}
	if err := persister.FinishRewrite(ctx); err == nil {
		t.Fatal("FinishRewrite should fail when the live AOF is missing")
	}
	if _, err := os.Stat(tmpName); !os.IsNotExist(err) {
		t.Fatalf("temp file %s was not removed: %v", tmpName, err)
	}
}
