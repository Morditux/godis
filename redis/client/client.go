package client

import (
	"errors"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/wait"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	created = iota
	running
	closed
)

// Client is a pipeline mode redis client
type Client struct {
	conn        net.Conn
	pendingReqs chan *request // wait to send
	waitingReqs chan *request // waiting response
	ticker      *time.Ticker
	addr        string

	status int32
	// doneCh is closed in Close; senders block in select on pendingReqs/doneCh
	// to avoid sending on a closed channel
	doneCh chan struct{}
	// connMu guards conn and waitingReqs against concurrent reconnect
	connMu  sync.Mutex
	working *sync.WaitGroup // its counter presents unfinished requests(pending and waiting)
}

// request is a message sends to redis server
type request struct {
	id        uint64
	args      [][]byte
	reply     redis.Reply
	heartbeat bool
	waiting   *wait.Wait
	err       error
}

const (
	chanSize = 256
	maxWait  = 3 * time.Second
)

// MakeClient creates a new client
func MakeClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		addr:        addr,
		conn:        conn,
		pendingReqs: make(chan *request, chanSize),
		waitingReqs: make(chan *request, chanSize),
		doneCh:      make(chan struct{}),
		working:     &sync.WaitGroup{},
	}, nil
}

func (client *Client) RemoteAddress() string {
	return client.addr
}

// Start starts asynchronous goroutines
func (client *Client) Start() {
	client.ticker = time.NewTicker(10 * time.Second)
	go client.handleWrite()
	go client.handleRead()
	go client.heartbeat()
	if atomic.CompareAndSwapInt32(&client.status, created, running) {
		return
	}
	atomic.StoreInt32(&client.status, running)
}

// Close stops asynchronous goroutines and close connection;
// calling Close more than once is safe
func (client *Client) Close() {
	if !atomic.CompareAndSwapInt32(&client.status, running, closed) {
		return
	}
	if client.ticker != nil {
		client.ticker.Stop()
	}
	close(client.doneCh) // all pendingReqs senders quit with a "client closed" error
	// stop new request
	close(client.pendingReqs)

	// wait stop process
	client.working.Wait()

	// clean: waitingReqs is left unclosed on purpose, as its senders are all
	// finished after working.Wait() and requests inside are simply garbage.
	// conn may be nil while reconnect() is re-establishing the connection.
	client.connMu.Lock()
	conn := client.conn
	client.conn = nil
	client.connMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (client *Client) reconnect() {
	logger.Info("reconnect with: " + client.addr)
	// Part 1: under connMu, close the old conn and fail every request
	// that is waiting for a response. doRequest is guarded by connMu as well,
	// so no one can send to the closed waitingReqs channel.
	client.connMu.Lock()
	if atomic.LoadInt32(&client.status) != running {
		client.connMu.Unlock()
		return
	}
	_ = client.conn.Close() // ignore possible errors from repeated closes
	close(client.waitingReqs)
	for req := range client.waitingReqs {
		req.err = errors.New("connection closed")
		req.waiting.Done()
	}
	client.waitingReqs = make(chan *request, chanSize)
	client.conn = nil
	client.connMu.Unlock()

	// Part 2: dial with retries, without holding connMu
	var conn net.Conn
	for i := 0; i < 3; i++ {
		var err error
		conn, err = net.Dial("tcp", client.addr)
		if err != nil {
			logger.Error("reconnect error: " + err.Error())
			time.Sleep(time.Second)
			continue
		} else {
			break
		}
	}
	if conn == nil { // reach max retry, abort
		client.Close()
		return
	}

	// Part 3: install the new connection
	client.connMu.Lock()
	if atomic.LoadInt32(&client.status) != running {
		client.connMu.Unlock()
		_ = conn.Close()
		return
	}
	client.conn = conn
	client.connMu.Unlock()
	// restart handle read
	go client.handleRead()
}

func (client *Client) heartbeat() {
	if client.ticker == nil {
		return
	}
	for {
		select {
		case <-client.ticker.C:
			if atomic.LoadInt32(&client.status) != running {
				return
			}
			client.doHeartbeat()
		case <-client.doneCh:
			return
		}
	}
}

func (client *Client) handleWrite() {
	for req := range client.pendingReqs {
		client.doRequest(req)
	}
}

// Send sends a request to redis server
func (client *Client) Send(args [][]byte) redis.Reply {
	if atomic.LoadInt32(&client.status) != running {
		return protocol.MakeErrReply("client closed")
	}
	req := &request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	req.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	select {
	case client.pendingReqs <- req:
	case <-client.doneCh:
		return protocol.MakeErrReply("client closed")
	}
	timeout := req.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return protocol.MakeErrReply("server time out")
	}
	if req.err != nil {
		return protocol.MakeErrReply("request failed " + req.err.Error())
	}
	return req.reply
}

func (client *Client) doHeartbeat() {
	request := &request{
		args:      [][]byte{[]byte("PING")},
		heartbeat: true,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	select {
	case client.pendingReqs <- request:
	case <-client.doneCh:
		return
	}
	request.waiting.WaitWithTimeout(maxWait)
}

func (client *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}
	re := protocol.MakeMultiBulkReply(req.args)
	bytes := re.ToBytes()
	// connMu guards conn/writing and waitingReqs sending: the same lock is
	// taken during reconnect, so no request can be sent to a closed waitingReqs
	client.connMu.Lock()
	defer client.connMu.Unlock()
	if client.conn == nil { // the connection is down and being re-established
		req.err = errors.New("connection closed")
		req.waiting.Done()
		return
	}
	var err error
	for i := 0; i < 3; i++ { // only retry, waiting for handleRead
		_, err = client.conn.Write(bytes)
		if err == nil ||
			(!strings.Contains(err.Error(), "timeout") && // only retry timeout
				!strings.Contains(err.Error(), "deadline exceeded")) {
			break
		}
	}
	if err == nil {
		client.waitingReqs <- req
	} else {
		req.err = err
		req.waiting.Done()
	}
}

func (client *Client) finishRequest(reply redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Error(err)
		}
	}()
	request := <-client.waitingReqs
	if request == nil {
		return
	}
	request.reply = reply
	if request.waiting != nil {
		request.waiting.Done()
	}
}

func (client *Client) handleRead() {
	// read conn under connMu: Close/reconnect swap it concurrently;
	// the snapshot stays valid until the connection dies
	client.connMu.Lock()
	conn := client.conn
	client.connMu.Unlock()
	if conn == nil {
		// connection was closed while starting
		return
	}
	stop := make(chan struct{})
	ch := parser.ParseStreamWithStop(conn, stop)
	defer close(stop)
	for payload := range ch {
		if payload.Err != nil {
			status := atomic.LoadInt32(&client.status)
			if status == closed {
				return
			}
			client.reconnect()
			return
		}
		client.finishRequest(payload.Data)
	}
}
