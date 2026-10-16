package connection

import (
	"net"
	"sync"
	"time"

	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/sync/wait"
)

const (
	// flagSlave means this a connection with slave
	flagSlave = uint64(1 << iota)
	// flagSlave means this a connection with master
	flagMaster
	// flagMulti means this connection is within a transaction
	flagMulti
)

// Connection represents a connection with a redis-cli
type Connection struct {
	conn net.Conn

	// wait until finish sending data, used for graceful shutdown
	sendingData wait.Wait

	// guards every mutable state below; connection handlers may close
	// the connection from another goroutine while commands are executing
	mu    sync.Mutex
	flags uint64
	// closed marks the connection as unusable, set by Close under mu
	closed bool

	// subscribing channels
	subs map[string]bool

	// password may be changed by CONFIG command during runtime, so store the password
	password string

	// queued commands for `multi`
	queue    [][][]byte
	watching map[string]uint32
	txErrors []error

	// selected db
	selectedDB int
}

var connPool = sync.Pool{
	New: func() interface{} {
		return &Connection{}
	},
}

// RemoteAddr returns the remote network address
func (c *Connection) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

// Close disconnect with the client. Calling it repeatedly is safe.
func (c *Connection) Close() error {
	c.sendingData.WaitWithTimeout(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		// Handler.Close and Handle's closeClient may both close the
		// connection; resetting state twice would corrupt a recycled one
		return nil
	}
	c.closed = true
	if c.conn != nil { // may be a fake conn for tests
		_ = c.conn.Close()
	}
	c.subs = nil
	c.password = ""
	c.queue = nil
	c.watching = nil
	c.txErrors = nil
	c.selectedDB = 0
	// reset role/multi flags as well, otherwise they leak into a recycled connection
	c.flags = 0
	connPool.Put(c)
	return nil
}

// NewConn creates Connection instance
func NewConn(conn net.Conn) *Connection {
	c, ok := connPool.Get().(*Connection)
	if !ok {
		logger.Error("connection pool make wrong type")
		return &Connection{
			conn: conn,
		}
	}
	c.conn = conn
	c.closed = false
	return c
}

// Write sends response to client over tcp connection
func (c *Connection) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	c.sendingData.Add(1)
	defer func() {
		c.sendingData.Done()
	}()

	return c.conn.Write(b)
}

func (c *Connection) Name() string {
	if c.conn != nil {
		return c.conn.RemoteAddr().String()
	}
	return ""
}

// Subscribe add current connection into subscribers of the given channel
func (c *Connection) Subscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subs == nil {
		c.subs = make(map[string]bool)
	}
	c.subs[channel] = true
}

// UnSubscribe removes current connection into subscribers of the given channel
func (c *Connection) UnSubscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.subs) == 0 {
		return
	}
	delete(c.subs, channel)
}

// SubsCount returns the number of subscribing channels
func (c *Connection) SubsCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.subs)
}

// GetChannels returns all subscribing channels
func (c *Connection) GetChannels() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.subs == nil {
		return make([]string, 0)
	}
	channels := make([]string, len(c.subs))
	i := 0
	for channel := range c.subs {
		channels[i] = channel
		i++
	}
	return channels
}

// SetPassword stores password for authentication
func (c *Connection) SetPassword(password string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.password = password
}

// GetPassword get password for authentication
func (c *Connection) GetPassword() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.password
}

// InMultiState tells is connection in an uncommitted transaction
func (c *Connection) InMultiState() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flags&flagMulti > 0
}

// SetMultiState sets transaction flag
func (c *Connection) SetMultiState(state bool) {
	c.mu.Lock()
	if !state { // reset data when cancel multi
		c.watching = nil
		c.queue = nil
		c.flags &= ^flagMulti // clean multi flag
		c.mu.Unlock()
		return
	}
	c.flags |= flagMulti
	c.mu.Unlock()
}

// GetQueuedCmdLine returns queued commands of current transaction
func (c *Connection) GetQueuedCmdLine() [][][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.queue
}

// EnqueueCmd  enqueues command of current transaction
func (c *Connection) EnqueueCmd(cmdLine [][]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queue = append(c.queue, cmdLine)
}

// AddTxError stores syntax error within transaction
func (c *Connection) AddTxError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.txErrors = append(c.txErrors, err)
}

// GetTxErrors returns syntax error within transaction
func (c *Connection) GetTxErrors() []error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.txErrors
}

// ClearQueuedCmds clears queued commands of current transaction
func (c *Connection) ClearQueuedCmds() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queue = nil
}

// GetWatching returns watching keys and their version code when started watching
func (c *Connection) GetWatching() map[string]uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.watching == nil {
		c.watching = make(map[string]uint32)
	}
	return c.watching
}

// SetWatching adds a key to the watched keys with its version.
// Mutating the map returned by GetWatching after the call would race with
// Close resetting the connection state, hence mutate through setters
func (c *Connection) SetWatching(key string, version uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.watching == nil {
		c.watching = make(map[string]uint32)
	}
	c.watching[key] = version
}

// GetDBIndex returns selected db
func (c *Connection) GetDBIndex() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.selectedDB
}

// SelectDB selects a database
func (c *Connection) SelectDB(dbNum int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.selectedDB = dbNum
}

func (c *Connection) SetSlave() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.flags |= flagSlave
}

func (c *Connection) IsSlave() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flags&flagSlave > 0
}

// SetMaster marks c as a connection with master
func (c *Connection) SetMaster() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.flags |= flagMaster
}

func (c *Connection) IsMaster() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flags&flagMaster > 0
}
