package tcp

/**
 * A tcp server
 */

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hdt3213/godis/interface/tcp"
	"github.com/hdt3213/godis/lib/logger"
)

// Config stores tcp server properties
type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

// ClientCounter Record the number of clients in the current Godis server
// ClientCounter must be accessed atomically (see LoadClientCounter)
var ClientCounter int32

// LoadClientCounter reads the number of clients connected to all tcp servers atomically.
// Database INFO reads it through a std-server-installed hook, avoiding an import cycle.
func LoadClientCounter() int32 {
	return atomic.LoadInt32(&ClientCounter)
}

// ListenAndServeWithSignal binds port and handle requests, blocking until receive stop signal
func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{})
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	//cfg.Address = listener.Addr().String()
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
	listenAndServe(listener, handler, closeChan, cfg.MaxConnect)
	return nil
}

// ListenAndServe binds port and handle requests, blocking until close
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	listenAndServe(listener, handler, closeChan, 0)
}

// listenAndServe accepts connections until the listener is closed. maxConnect
// limits the number of simultaneous clients; 0 means unlimited.
func listenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}, maxConnect uint32) {
	// listen signal
	errCh := make(chan error, 1)
	go func() {
		select {
		case <-closeChan:
			logger.Info("get exit signal")
		case er := <-errCh:
			logger.Info(fmt.Sprintf("accept error: %s", er.Error()))
		}
		logger.Info("shutting down...")
		_ = listener.Close() // listener.Accept() will return err immediately
		_ = handler.Close()  // close connections
	}()

	limit := int32(0)
	if maxConnect > 0 {
		if maxConnect > 1<<31-1 {
			limit = 1<<31 - 1
		} else {
			limit = int32(maxConnect)
		}
	}
	var active int32
	ctx := context.Background()
	var waitDone sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			// learn from net/http/serve.go#Serve()
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				logger.Infof("accept occurs temporary error: %v, retry in 5ms", err)
				time.Sleep(5 * time.Millisecond)
				continue
			}
			errCh <- err
			break
		}
		// handle
		// logger.Info("accept link")
		if limit > 0 && atomic.LoadInt32(&active) >= limit {
			logger.Warn("max clients reached, refusing connection")
			_ = conn.Close()
			continue
		}
		atomic.AddInt32(&active, 1)
		atomic.AddInt32(&ClientCounter, 1)
		waitDone.Add(1)
		go func() {
			defer func() {
				waitDone.Done()
				atomic.AddInt32(&active, -1)
				atomic.AddInt32(&ClientCounter, -1)
			}()
			handler.Handle(ctx, conn)
		}()
	}
	waitDone.Wait()
}
