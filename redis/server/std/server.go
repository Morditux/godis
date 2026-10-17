package std

/*
 * A tcp.Handler implements redis protocol
 */

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/hdt3213/godis/cluster"
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/database"
	idatabase "github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/atomic"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
	"github.com/hdt3213/godis/tcp"
	"github.com/valyala/bytebufferpool"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

// handleFlushThreshold caps the size of batched replies before they are
// flushed to the client, so huge pipelined bursts don't accumulate
// unbounded amounts of buffered bytes.
const handleFlushThreshold = 32 << 10 // 32 KiB

// Handler implements tcp.Handler and serves as a redis server
type Handler struct {
	activeConn sync.Map // *client -> placeholder
	db         idatabase.DB
	closing    atomic.Boolean // refusing new client and new request

	mu        sync.Mutex
	closed    bool
	connWG    sync.WaitGroup
	closeOnce sync.Once
}

// MakeHandler creates a Handler instance
func MakeHandler() *Handler {
	// expose the tcp client counter to the INFO command without making
	// the database package depend on the transport layer
	database.ClientCountFunc = func() int64 {
		return int64(tcp.LoadClientCounter())
	}
	var db idatabase.DB
	if config.Properties.ClusterEnable {
		db = cluster.MakeCluster()
	} else {
		db = database.NewStandaloneServer()
	}
	return &Handler{
		db: db,
	}
}

func Serve(addr string, handler *Handler) error {
	maxConnect := uint32(0)
	if config.Properties.MaxClients > 0 {
		maxConnect = uint32(config.Properties.MaxClients)
	}
	return tcp.ListenAndServeWithSignal(&tcp.Config{
		Address:    addr,
		MaxConnect: maxConnect,
	}, handler)
}

func (h *Handler) closeClient(client *connection.Connection) {
	// only the first caller owns the cleanup: Handler.Close and the command
	// loop can race on the same client
	if _, loaded := h.activeConn.LoadAndDelete(client); !loaded {
		return
	}
	h.db.AfterClientClose(client)
	_ = client.Close()
}

// Handle receives and executes redis commands
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// closing handler refuse new connection
		_ = conn.Close()
		return
	}

	client := connection.NewConn(conn)
	// registering the connection and the WaitGroup Add must be atomic with
	// respect to Close: otherwise Close could miss the client and wait forever.
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		_ = client.Close()
		return
	}
	h.activeConn.Store(client, struct{}{})
	h.connWG.Add(1)
	h.mu.Unlock()
	defer h.connWG.Done()

	ch := parser.ParseStream(conn)
	// pipeline pipelining aware batching: replies are accumulated and flushed
	// whenever nothing else is queued in the parse channel, so pipelined
	// command bursts produce one write syscall per burst instead of one per
	// command; sequential (ping-pong) clients are flushed immediately
	out := bytebufferpool.Get()
	defer bytebufferpool.Put(out)
	flush := func() bool {
		if out.Len() == 0 {
			return true
		}
		if _, err := client.Write(out.Bytes()); err != nil {
			h.closeClient(client)
			logger.Info("connection closed: " + client.RemoteAddr())
			return false
		}
		out.Reset()
		return true
	}
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// connection closed, flush pending replies first
				if !flush() {
					return
				}
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			// Protocol error (including an oversized RESP frame): send the
			// error, flush it, then close. The parser stops after a protocol
			// error, so continuing would desynchronize the stream and leak the
			// client tracking entry.
			logger.Infof("protocol error from %s: %v", client.RemoteAddr(), payload.Err)
			out.Write(protocol.MakeErrReply(payload.Err.Error()).ToBytes())
			if flush() {
				h.closeClient(client)
			}
			return
		}
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		result := h.db.Exec(client, r.Args)
		if result != nil {
			out.Write(result.ToBytes())
		} else {
			out.Write(unknownErrReplyBytes)
		}
		if len(ch) == 0 || out.Len() >= handleFlushThreshold {
			// nothing else queued in the parser: flush now to keep the
			// latency of sequential clients unchanged, or protect memory
			if !flush() {
				return
			}
		}
	}
}

// Close stops handler
func (h *Handler) Close() error {
	h.closeOnce.Do(func() {
		logger.Info("handler shutting down...")
		h.mu.Lock()
		h.closed = true
		h.closing.Set(true)
		h.mu.Unlock()

		var wg sync.WaitGroup
		h.activeConn.Range(func(key interface{}, val interface{}) bool {
			client := key.(*connection.Connection)
			wg.Add(1)
			go func() {
				defer wg.Done()
				h.closeClient(client)
			}()
			return true
		})
		wg.Wait()
		// wait for command loops to observe the closed connections before the
		// database/AOF is torn down
		h.connWG.Wait()
		h.db.Close()
	})
	return nil
}
