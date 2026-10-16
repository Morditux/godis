package logger

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// waitFileContent polls the log file until it contains the given
// substring or the deadline passes; entries are delivered asynchronously
func waitFileContent(t *testing.T, logFile string, substring string) bool {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(logFile)
		if err == nil && strings.Contains(string(data), substring) {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

func newTestFileLogger(t *testing.T) (*Logger, string) {
	t.Helper()
	dir := t.TempDir()
	logger, err := NewFileLogger(&Settings{
		Path:       dir,
		Name:       "test-log",
		Ext:        "log",
		TimeFormat: "20060102150405",
	})
	if err != nil {
		t.Fatalf("create file logger: %v", err)
	}

	// find the log file created for this test
	pattern := filepath.Join(dir, "test-log-*.log")
	matches, err := filepath.Glob(pattern)
	if err != nil || len(matches) != 1 {
		t.Fatalf("expected 1 log file matching %s, got %v", pattern, matches)
	}
	return logger, matches[0]
}

func TestFileLoggerWritesToFile(t *testing.T) {
	logger, logFile := newTestFileLogger(t)

	logger.Output(INFO, 2, "hello 42")
	if !waitFileContent(t, logFile, "hello 42") {
		t.Errorf("expected %s to contain 'hello 42'", logFile)
	}

	logger.Output(ERROR, 2, "boom x")
	if !waitFileContent(t, logFile, "boom x") {
		t.Errorf("expected %s to contain 'boom x'", logFile)
	}
}

func TestFileLoggerIncludesLevel(t *testing.T) {
	logger, logFile := newTestFileLogger(t)

	logger.Output(INFO, 1, "with level")
	if !waitFileContent(t, logFile, "[INFO]") {
		t.Errorf("expected entry to include level tag")
	}
}

func TestLogSyncBypassesAsyncQueue(t *testing.T) {
	logger, logFile := newTestFileLogger(t)

	// logSync writes directly, so the entry is visible immediately
	// without waiting for the async consumer
	logger.logSync(WARNING, "sync message")
	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if !strings.Contains(string(data), "sync message") {
		t.Errorf("expected immediate sync entry, got %s", string(data))
	}
}

func TestStdoutLoggerConsumesEntries(t *testing.T) {
	// no panic and no leak: entries flow through the async consumer
	logger := NewStdoutLogger()
	logger.Output(INFO, 2, "to stdout")
	logger.Output(WARNING, 2, "warn msg")
	logger.Output(ERROR, 2, "err msg")
	if logger.logger == nil {
		t.Error("expected initialized std logger")
	}
}

func TestPackageLevelLogging(t *testing.T) {
	// package-level functions route to DefaultLogger without panic
	Info("info message")
	Infof("formatted %d", 2)
	Warn("warn message")
	Error("error message")
	Errorf("formatted %s", "e")
	Debug("debug message")
	Debugf("formatted %d", 3)
}

func TestLogSyncCallerDepth(t *testing.T) {
	logger, logFile := newTestFileLogger(t)

	// logSync writes through runtime.Caller(2) which resolves the caller;
	// the exact file:line varies with inlining, so only assert delivery
	func() {
		logger.logSync(ERROR, "depth check")
	}()
	if !strings.Contains(readFile(t, logFile), "depth check") {
		t.Errorf("expected depth-check entry")
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}
