package gnet

import (
	"context"
	"sync"
	"sync/atomic"

	idatabase "github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"

	"github.com/hdt3213/godis/database"
	"github.com/panjf2000/gnet/v2"
)

type GnetServer struct {
	gnet.BuiltinEventEngine
	booted    chan struct{} // closed by OnBoot; Close waits for it (happens-before edge for eng)
	eng       gnet.Engine
	connected int32
	db        idatabase.DB
	stopOnce  sync.Once
}

func NewGnetServer(db idatabase.DB) *GnetServer {
	s := &GnetServer{
		db:     db,
		booted: make(chan struct{}),
	}
	// expose this engine's client count to the INFO command; the gnet engine
	// does not use the tcp package so it reports its own counter
	database.ClientCountFunc = func() int64 {
		return int64(atomic.LoadInt32(&s.connected))
	}
	return s
}

func (s *GnetServer) Run(listenAddr string) error {
	return gnet.Run(s, "tcp://"+listenAddr, gnet.WithMulticore(true))
}

func (s *GnetServer) OnBoot(eng gnet.Engine) (action gnet.Action) {
	s.eng = eng
	close(s.booted)
	return
}

func (s *GnetServer) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	client := connection.NewConn(c)
	c.SetContext(client)
	atomic.AddInt32(&s.connected, 1)
	return
}

func (s *GnetServer) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	if err != nil {
		logger.Infof("error occurred on connection=%s, %v\n", c.RemoteAddr().String(), err)
	}
	atomic.AddInt32(&s.connected, -1)
	conn := c.Context().(redis.Connection)
	s.db.AfterClientClose(conn)
	return
}

func (s *GnetServer) OnTraffic(c gnet.Conn) (action gnet.Action) {
	conn := c.Context().(redis.Connection)
	cmdLine, err := parser.ParseV2(c)
	if err != nil {
		logger.Infof("parse command line failed: %v", err)
		return gnet.Close
	}
	if len(cmdLine) == 0 {
		return gnet.None
	}
	result := s.db.Exec(conn, cmdLine)
	buffer := result.ToBytes()
	if len(buffer) > 0 {
		c.Write(buffer)
	}
	return gnet.None
}

// Close stops the engine; it first waits for the boot to complete because
// gnet Engine.Stop is not safe to call while run() is still initializing
func (s *GnetServer) Close() {
	s.stopOnce.Do(func() {
		<-s.booted
		s.eng.Stop(context.Background())
	})
}
