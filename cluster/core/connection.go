package core

import (
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/pool"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/client"
	"github.com/hdt3213/godis/redis/parser"
	"github.com/hdt3213/godis/redis/protocol"
)

// ConnectionFactory manages connection with peer nodes in cluster
type ConnectionFactory interface {
	BorrowPeerClient(peerAddr string) (peerClient, error)
	ReturnPeerClient(peerClient peerClient) error
	NewStream(peerAddr string, cmdLine CmdLine) (peerStream, error)
	Close() error
}

// peerClient represents a
type peerClient interface {
	RemoteAddress() string
	Send(args [][]byte) redis.Reply
}

type peerStream interface {
	Stream() <-chan *parser.Payload
	Close() error
}

type defaultClientFactory struct {
	nodeConnections dict.Dict[*pool.Pool] // map[string]*pool.Pool
	// createMu serializes pool creation: BorrowPeerClient checks-then-acts
	// on nodeConnections, so two borrows of the same peer could each create
	// a pool, with the loser and its clients leaking
	createMu sync.Mutex
}

var connectionPoolConfig = pool.Config{
	MaxIdle:   1,
	MaxActive: 16,
}

func NewFactory() ConnectionFactory {
	return &defaultClientFactory{
		nodeConnections: dict.MakeConcurrent[*pool.Pool](1),
	}
}

// NewPeerClient creats a new client, no need to return this client
func (factory *defaultClientFactory) NewPeerClient(peerAddr string) (peerClient, error) {
	c, err := client.MakeClient(peerAddr)
	if err != nil {
		return nil, err
	}
	c.Start()
	// all peers of cluster should use the same password
	if config.Properties.RequirePass != "" {
		authResp := c.Send(utils.ToCmdLine("AUTH", config.Properties.RequirePass))
		if !protocol.IsOKReply(authResp) {
			return nil, fmt.Errorf("auth failed, resp: %s", string(authResp.ToBytes()))
		}
	}
	return c, nil
}

// BorrowPeerClient borrows the connection pool for the peer and takes a client from it
func (factory *defaultClientFactory) BorrowPeerClient(peerAddr string) (peerClient, error) {
	// hold createMu around creation to keep the check-then-act atomic;
	// the fast path (pool exists) only takes it briefly
	factory.createMu.Lock()
	connectionPool, ok := factory.nodeConnections.Get(peerAddr)
	if !ok {
		creator := func() (interface{}, error) {
			return factory.NewPeerClient(peerAddr)
		}
		finalizer := func(x interface{}) {
			logger.Debug("destroy client")
			// must assert pointer type: `x.(client.Client)` never matched
			// since the pool stores `*client.Client`, leaking every connection
			cli, ok := x.(*client.Client)
			if !ok {
				return
			}
			cli.Close()
		}
		connectionPool = pool.New(creator, finalizer, connectionPoolConfig)
		factory.nodeConnections.Put(peerAddr, connectionPool)
	}
	factory.createMu.Unlock()
	raw, err := connectionPool.Get()
	if err != nil {
		return nil, err
	}
	conn, ok := raw.(*client.Client)
	if !ok {
		return nil, errors.New("connection pool make wrong type")
	}
	return conn, nil
}

func (cluster *Cluster) BorrowLeaderClient() (peerClient, error) {
	leaderAddr := cluster.raftNode.GetLeaderRedisAddress()
	return cluster.connections.BorrowPeerClient(leaderAddr)
}

// ReturnPeerClient returns client to pool
func (factory *defaultClientFactory) ReturnPeerClient(peerClient peerClient) error {
	connectionPool, ok := factory.nodeConnections.Get(peerClient.RemoteAddress())
	if !ok {
		return errors.New("connection pool not found")
	}
	connectionPool.Put(peerClient)
	return nil
}

type tcpStream struct {
	conn net.Conn
	ch   <-chan *parser.Payload
}

func (s *tcpStream) Stream() <-chan *parser.Payload {
	return s.ch
}

func (s *tcpStream) Close() error {
	return s.conn.Close()
}

func (factory *defaultClientFactory) NewStream(peerAddr string, cmdLine CmdLine) (peerStream, error) {
	// todo: reuse connection
	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		return nil, fmt.Errorf("connect with %s failed: %v", peerAddr, err)
	}
	ch := parser.ParseStream(conn)
	send2node := func(cmdLine CmdLine) redis.Reply {
		req := protocol.MakeMultiBulkReply(cmdLine)
		_, err := conn.Write(req.ToBytes())
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		resp := <-ch
		if resp.Err != nil {
			return protocol.MakeErrReply(resp.Err.Error())
		}
		return resp.Data
	}
	if config.Properties.RequirePass != "" {
		authResp := send2node(utils.ToCmdLine("AUTH", config.Properties.RequirePass))
		if !protocol.IsOKReply(authResp) {
			return nil, fmt.Errorf("auth failed, resp: %s", string(authResp.ToBytes()))
		}
	}
	req := protocol.MakeMultiBulkReply(cmdLine)
	_, err = conn.Write(req.ToBytes())
	if err != nil {
		return nil, protocol.MakeErrReply("send cmdLine failed: " + err.Error())
	}
	return &tcpStream{
		conn: conn,
		ch:   ch,
	}, nil
}

func newDefaultClientFactory() *defaultClientFactory {
	return &defaultClientFactory{
		nodeConnections: dict.MakeConcurrent[*pool.Pool](1),
	}
}

func (factory *defaultClientFactory) Close() error {
	factory.nodeConnections.ForEach(func(key string, val *pool.Pool) bool {
		val.Close()
		return true
	})
	return nil
}
