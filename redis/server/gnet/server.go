package gnet

import (
	"context"
	"math"
	"sync"
	"sync/atomic"

	"github.com/valyala/bytebufferpool"

	"github.com/hdt3213/godis/config"
	idatabase "github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"

	"github.com/hdt3213/godis/database"
	"github.com/panjf2000/gnet/v2"
)

// gnetMaxBatchPerEvent caps the commands executed per OnTraffic call so the
// event-loop stays responsive to other connections. With very large pipelines
// the rest is processed on subsequent events (gnet keeps delivering events
// as long as data is buffered).
const gnetMaxBatchPerEvent = 64

type GnetServer struct {
	gnet.BuiltinEventEngine
	booted     chan struct{} // closed by OnBoot; Close waits for it (happens-before edge for eng)
	eng        gnet.Engine
	connected  int32
	maxClients int32
	db         idatabase.DB
	stopOnce   sync.Once
}

func NewGnetServer(db idatabase.DB) *GnetServer {
	s := &GnetServer{
		db:     db,
		booted: make(chan struct{}),
	}
	if config.Properties.MaxClients > 0 {
		maxClients := config.Properties.MaxClients
		if maxClients > math.MaxInt32 {
			maxClients = math.MaxInt32
		}
		s.maxClients = int32(maxClients)
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
	for {
		current := atomic.LoadInt32(&s.connected)
		if s.maxClients > 0 && current >= s.maxClients {
			logger.Warn("max clients reached, refusing connection")
			return nil, gnet.Close
		}
		if atomic.CompareAndSwapInt32(&s.connected, current, current+1) {
			break
		}
	}
	client := connection.NewConn(c)
	c.SetContext(client)
	return
}

func (s *GnetServer) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	if err != nil {
		logger.Infof("error occurred on connection=%s, %v\n", c.RemoteAddr().String(), err)
	}
	// connections refused by OnOpen have no context and were never counted
	conn, ok := c.Context().(redis.Connection)
	if !ok || conn == nil {
		return
	}
	atomic.AddInt32(&s.connected, -1)
	s.db.AfterClientClose(conn)
	return
}

// OnTraffic parses and executes every command complete enough to be parsed
// without blocking, batching all replies into a single write per event.
// This is what makes pipelining work in gnet mode: without the drain loop
// only one command per network event was executed, which throttled
// redis-benchmark pipelines to the RTT.
func (s *GnetServer) OnTraffic(c gnet.Conn) (action gnet.Action) {
	conn, ok := c.Context().(redis.Connection)
	if !ok || conn == nil {
		return gnet.Close
	}
	out := bytebufferpool.Get()
	defer bytebufferpool.Put(out)
	protocolErr := func(err error) gnet.Action {
		logger.Infof("parse command line failed: %v", err)
		out.Write(protocol.MakeErrReply("ERR Protocol error: " + err.Error()).ToBytes())
		if out.Len() > 0 {
			_, _ = c.Write(out.Bytes())
		}
		return gnet.Close
	}
	for i := 0; i < gnetMaxBatchPerEvent; i++ {
		_, complete, err := estimateCommandLength(c)
		if err != nil {
			return protocolErr(err)
		}
		if !complete {
			// no consumption here: leftover bytes remain valid for the next event
			break
		}
		cmdLine, err := parser.ParseV2(c)
		if err != nil {
			return protocolErr(err)
		}
		if len(cmdLine) == 0 {
			// null array like *-1: nothing to execute
			continue
		}
		result := s.db.Exec(conn, cmdLine)
		if result != nil {
			out.Write(result.ToBytes())
		}
	}
	if out.Len() > 0 {
		if _, err := c.Write(out.Bytes()); err != nil {
			return gnet.Close
		}
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
