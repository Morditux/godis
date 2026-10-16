package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// Settings stores config for Logger
type Settings struct {
	Path       string `yaml:"path"`
	Name       string `yaml:"name"`
	Ext        string `yaml:"ext"`
	TimeFormat string `yaml:"time-format"`
}

type LogLevel int

// Output levels
const (
	DEBUG LogLevel = iota
	INFO
	WARNING
	ERROR
	FATAL
)

const (
	flags              = log.LstdFlags
	defaultCallerDepth = 2
	bufferSize         = 1e5
)

type logEntry struct {
	msg   string
	level LogLevel
}

var (
	levelFlags = []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
)

// ILogger defines the methods that any logger should implement
type ILogger interface {
	Output(level LogLevel, callerDepth int, msg string)
}

// Logger is Logger
type Logger struct {
	logFile   *os.File
	logger    *log.Logger
	entryChan chan *logEntry
	entryPool *sync.Pool
}

var DefaultLogger ILogger = NewStdoutLogger()

// NewStdoutLogger creates a logger which print msg to stdout
func NewStdoutLogger() *Logger {
	logger := &Logger{
		logFile:   nil,
		logger:    log.New(os.Stdout, "", flags),
		entryChan: make(chan *logEntry, bufferSize),
		entryPool: &sync.Pool{
			New: func() interface{} {
				return &logEntry{}
			},
		},
	}
	go func() {
		for e := range logger.entryChan {
			_ = logger.logger.Output(0, e.msg) // msg includes call stack, no need for calldepth
			logger.entryPool.Put(e)
		}
	}()
	return logger
}

// NewFileLogger creates a logger which print msg to stdout and log file
func NewFileLogger(settings *Settings) (*Logger, error) {
	fileName := fmt.Sprintf("%s-%s.%s",
		settings.Name,
		time.Now().Format(settings.TimeFormat),
		settings.Ext)
	logFile, err := mustOpen(fileName, settings.Path)
	if err != nil {
		return nil, fmt.Errorf("logging.Join err: %s", err)
	}
	mw := io.MultiWriter(os.Stdout, logFile)
	logger := &Logger{
		logFile:   logFile,
		logger:    log.New(mw, "", flags),
		entryChan: make(chan *logEntry, bufferSize),
		entryPool: &sync.Pool{
			New: func() interface{} {
				return &logEntry{}
			},
		},
	}
	go func() {
		for e := range logger.entryChan {
			logFilename := fmt.Sprintf("%s-%s.%s",
				settings.Name,
				time.Now().Format(settings.TimeFormat),
				settings.Ext)
			if path.Join(settings.Path, logFilename) != logger.logFile.Name() {
				next, err := mustOpen(logFilename, settings.Path)
				if err != nil {
					// never stop the service or drop the entry because of
					// a rotation failure: keep writing to the current file
					_ = logger.logger.Output(0, "[ERROR][logger] open log "+logFilename+" failed: "+err.Error())
				} else {
					_ = logger.logFile.Close() // release the previous file
					logger.logFile = next
					logger.logger = log.New(io.MultiWriter(os.Stdout, next), "", flags)
				}
			}
			_ = logger.logger.Output(0, e.msg) // msg includes call stack, no need for calldepth
			logger.entryPool.Put(e)
		}
	}()
	return logger, nil
}

// Setup initializes DefaultLogger
func Setup(settings *Settings) {
	logger, err := NewFileLogger(settings)
	if err != nil {
		panic(err)
	}
	DefaultLogger = logger
}

// Output sends a msg to logger
func (logger *Logger) Output(level LogLevel, callerDepth int, msg string) {
	var formattedMsg string
	_, file, line, ok := runtime.Caller(callerDepth)
	if ok {
		formattedMsg = fmt.Sprintf("[%s][%s:%d] %s", levelFlags[level], filepath.Base(file), line, msg)
	} else {
		formattedMsg = fmt.Sprintf("[%s] %s", levelFlags[level], msg)
	}
	entry := logger.entryPool.Get().(*logEntry)
	entry.msg = formattedMsg
	entry.level = level
	logger.entryChan <- entry
}

// logSync writes the formatted message directly, bypassing the async queue.
// callerDepth=2 refers to the caller of logSync
func (logger *Logger) logSync(level LogLevel, msg string) {
	var formattedMsg string
	_, file, line, ok := runtime.Caller(2)
	if ok {
		formattedMsg = fmt.Sprintf("[%s][%s:%d] %s", levelFlags[level], filepath.Base(file), line, msg)
	} else {
		formattedMsg = fmt.Sprintf("[%s] %s", levelFlags[level], msg)
	}
	_ = logger.logger.Output(0, formattedMsg)
}

// Debug logs debug message through DefaultLogger
func Debug(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(DEBUG, defaultCallerDepth, msg)
}

// Debugf logs debug message through DefaultLogger
func Debugf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	DefaultLogger.Output(DEBUG, defaultCallerDepth, msg)
}

// Info logs message through DefaultLogger
func Info(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(INFO, defaultCallerDepth, msg)
}

// Infof logs message through DefaultLogger
func Infof(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	DefaultLogger.Output(INFO, defaultCallerDepth, msg)
}

// Warn logs warning message through DefaultLogger
func Warn(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(WARNING, defaultCallerDepth, msg)
}

// Error logs error message through DefaultLogger
func Error(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.Output(ERROR, defaultCallerDepth, msg)
}

// Errorf logs error message through DefaultLogger
func Errorf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	DefaultLogger.Output(ERROR, defaultCallerDepth, msg)
}

// Fatal prints the error message synchronously (OS exit would drop entries
// still buffered in the async consumer) then stops the program
func Fatal(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	if fl, ok := DefaultLogger.(*Logger); ok {
		fl.logSync(FATAL, msg)
	} else {
		DefaultLogger.Output(FATAL, defaultCallerDepth, msg)
	}
	os.Exit(1)
}
