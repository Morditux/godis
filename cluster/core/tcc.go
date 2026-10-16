package core

import (
	"strings"
	"sync"
	"time"

	"github.com/hdt3213/godis/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/timewheel"
	"github.com/hdt3213/godis/redis/protocol"
)

// transaction info will be deleted after transactionTTL since commit
const transactionTTL = time.Minute

type TransactionManager struct {
	txs map[string]*TCC
	mu  sync.RWMutex
}

type TCC struct {
	// mu guards the fields below: a slow prepare may still be running while
	// commit/rollback arrive from a retrying client or another node
	mu          sync.Mutex
	realCmdLine CmdLine
	undoLogs    []CmdLine
	writeKeys   []string
	readKeys    []string
	hasLock     bool
}

func newTransactionManager() *TransactionManager {
	return &TransactionManager{
		txs: make(map[string]*TCC),
	}
}

func init() {
	RegisterCmd("prepare", execPrepare)
	RegisterCmd("commit", execCommit)
	RegisterCmd("rollback", execRollback)

}

// execPrepare executes prepare command
// commandline: prepare txid realCmd realArgs...
// execPrepare will check transaction preconditions, lock related keys and prepare undo logs
func execPrepare(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 3 {
		return protocol.MakeArgNumErrReply("prepare")
	}
	txId := string(cmdLine[1])
	realCmdLine := cmdLine[2:]

	// create transaction
	cluster.transactions.mu.Lock()
	tx := cluster.transactions.txs[txId]
	if tx != nil {
		cluster.transactions.mu.Unlock()
		return protocol.MakeErrReply("transaction existed")
	}
	tx = &TCC{}
	cluster.transactions.txs[txId] = tx
	cluster.transactions.mu.Unlock()

	// lock keys and generate undo logs before publishing the transaction state
	writeKeys, readKeys := database.GetRelatedKeys(realCmdLine)
	cluster.db.RWLocks(0, writeKeys, readKeys)
	undoLogs := cluster.db.GetUndoLogs(0, realCmdLine)
	tx.mu.Lock()
	tx.writeKeys = writeKeys
	tx.readKeys = readKeys
	tx.undoLogs = undoLogs
	tx.realCmdLine = realCmdLine
	tx.hasLock = true
	tx.mu.Unlock()

	// execute prepare func
	prepareFuncsMu.RLock()
	prepareFunc := prepareFuncs[strings.ToLower(string(realCmdLine[0]))]
	prepareFuncsMu.RUnlock()
	var result redis.Reply
	if prepareFunc != nil {
		result = prepareFunc(cluster, c, realCmdLine)
	} else {
		result = protocol.MakeOkReply()
	}
	if protocol.IsErrorReply(result) {
		// prepare for rollback
		cluster.db.RWUnLocks(0, writeKeys, readKeys)
		tx.mu.Lock()
		tx.hasLock = false
		tx.mu.Unlock()
	}
	return result
}

func execCommit(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeArgNumErrReply("commit")
	}
	txId := string(cmdLine[1])

	cluster.transactions.mu.Lock()
	tx := cluster.transactions.txs[txId]
	cluster.transactions.mu.Unlock()
	if tx == nil {
		return protocol.MakeErrReply("transaction not found")
	}

	// snapshot the transaction state
	tx.mu.Lock()
	realCmdLine := tx.realCmdLine
	writeKeys := tx.writeKeys
	readKeys := tx.readKeys
	locked := tx.hasLock
	tx.mu.Unlock()
	if realCmdLine == nil {
		// the transaction is not fully prepared yet
		return protocol.MakeErrReply("transaction not prepared")
	}

	resp := cluster.db.ExecWithLock(c, realCmdLine)

	// unlock regardless of result, but only once
	if locked {
		cluster.db.RWUnLocks(0, writeKeys, readKeys)
		tx.mu.Lock()
		tx.hasLock = false
		tx.mu.Unlock()
	}

	if protocol.IsErrorReply(resp) {
		// do not delete transaction, waiting rollback
		return resp
	}

	// delete transaction after deadline
	timewheel.At(time.Now().Add(transactionTTL), txId, func() {
		cluster.transactions.mu.Lock()
		delete(cluster.transactions.txs, txId)
		cluster.transactions.mu.Unlock()
	})
	return resp
}

func execRollback(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeArgNumErrReply("rollback")
	}
	txId := string(cmdLine[1])

	// get transaction
	cluster.transactions.mu.Lock()
	tx := cluster.transactions.txs[txId]
	cluster.transactions.mu.Unlock()
	if tx == nil {
		return protocol.MakeErrReply("transaction not found")
	}

	// snapshot the transaction state
	tx.mu.Lock()
	writeKeys := tx.writeKeys
	readKeys := tx.readKeys
	undoLogs := tx.undoLogs
	locked := tx.hasLock
	if !locked {
		cluster.db.RWLocks(0, writeKeys, readKeys)
		tx.hasLock = true
	}
	tx.mu.Unlock()

	// rollback
	for i := len(undoLogs) - 1; i >= 0; i-- {
		cmdline := undoLogs[i]
		cluster.db.ExecWithLock(c, cmdline)
	}
	cluster.db.RWUnLocks(0, writeKeys, readKeys)
	tx.mu.Lock()
	tx.hasLock = false
	tx.mu.Unlock()

	// delete transaction
	cluster.transactions.mu.Lock()
	delete(cluster.transactions.txs, txId)
	cluster.transactions.mu.Unlock()

	return protocol.MakeOkReply()
}

var (
	prepareFuncsMu sync.RWMutex // guards prepareFuncs, registration happens in init or at startup
	prepareFuncs   = make(map[string]CmdFunc)
)

// RegisterCmd add tcc preparing validator
// prepareFunc will be executed within lock
// If prepareFunc returns an error reply, the transaction should be rolledback-
func RegisterPrepareFunc(name string, fn CmdFunc) {
	prepareFuncsMu.Lock()
	defer prepareFuncsMu.Unlock()
	name = strings.ToLower(name)
	prepareFuncs[name] = fn
}
