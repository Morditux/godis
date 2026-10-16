package commands

import (
	"testing"

	"github.com/hdt3213/godis/cluster/core"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol/asserts"
)

func TestRename(t *testing.T) {
	id1 := "1"
	id2 := "2"
	nodes := core.MakeTestCluster([]string{id1, id2})
	node1 := nodes[id1]
	c := connection.NewFakeConn()
	core.RegisterDefaultCmd("set")
	core.RegisterDefaultCmd("get")
	node1.Exec(c, utils.ToCmdLine("set", "1", "1"))

	// 1, 2 will be routed to node1 and node2, see MakeTestCluster
	res := execRename(node1, c, utils.ToCmdLine("rename", "1", "2"))
	asserts.AssertStatusReply(t, res, "OK")

	res = node1.Exec(c, utils.ToCmdLine("get", "2"))
	asserts.AssertBulkReply(t, res, "1")
}

func TestRenameNx(t *testing.T) {
	id1 := "1"
	id2 := "2"
	nodes := core.MakeTestCluster([]string{id1, id2})
	node1 := nodes[id1]
	c := connection.NewFakeConn()
	core.RegisterDefaultCmd("set")
	core.RegisterDefaultCmd("get")
	node1.Exec(c, utils.ToCmdLine("set", "1", "1"))

	// 1, 2 will be routed to node1 and node2, see MakeTestCluster
	res := execRenameNx(node1, c, utils.ToCmdLine("rename", "1", "2"))
	asserts.AssertIntReply(t, res, 1)

	res = node1.Exec(c, utils.ToCmdLine("get", "2"))
	asserts.AssertBulkReply(t, res, "1")

	node1.Exec(c, utils.ToCmdLine("set", "3", "3"))
	res = execRenameNx(node1, c, utils.ToCmdLine("rename", "2", "3"))
	asserts.AssertIntReply(t, res, 0)
}

// keys "2", "4" and "6" are all routed to node1 (see MakeTestCluster),
// so the same-node branch of execRename is covered:
// the command must be rewritten to `*_` and executed on the hosting node
// (a wrong implementation re-routes through Exec and recurses forever)
func TestRenameSameNode(t *testing.T) {
	id1 := "1"
	id2 := "2"
	nodes := core.MakeTestCluster([]string{id1, id2})
	node1 := nodes[id1]
	c := connection.NewFakeConn()
	core.RegisterDefaultCmd("set")
	core.RegisterDefaultCmd("get")
	node1.Exec(c, utils.ToCmdLine("set", "2", "v2"))

	res := node1.Exec(c, utils.ToCmdLine("rename", "2", "4"))
	asserts.AssertStatusReply(t, res, "OK")
	// data was moved
	asserts.AssertNullBulk(t, node1.Exec(c, utils.ToCmdLine("get", "2")))
	asserts.AssertBulkReply(t, node1.Exec(c, utils.ToCmdLine("get", "4")), "v2")
}

func TestRenameNxSameNode(t *testing.T) {
	id1 := "1"
	id2 := "2"
	nodes := core.MakeTestCluster([]string{id1, id2})
	node1 := nodes[id1]
	c := connection.NewFakeConn()
	core.RegisterDefaultCmd("set")
	core.RegisterDefaultCmd("get")
	node1.Exec(c, utils.ToCmdLine("set", "2", "v2"))
	node1.Exec(c, utils.ToCmdLine("set", "4", "v4"))

	// target exists, RENAMENX should fail without modifying data
	res := node1.Exec(c, utils.ToCmdLine("renamenx", "2", "4"))
	asserts.AssertIntReply(t, res, 0)
	asserts.AssertBulkReply(t, node1.Exec(c, utils.ToCmdLine("get", "2")), "v2")
	asserts.AssertBulkReply(t, node1.Exec(c, utils.ToCmdLine("get", "4")), "v4")

	// target does not exist
	res = node1.Exec(c, utils.ToCmdLine("renamenx", "2", "6"))
	asserts.AssertIntReply(t, res, 1)
	asserts.AssertNullBulk(t, node1.Exec(c, utils.ToCmdLine("get", "2")))
	asserts.AssertBulkReply(t, node1.Exec(c, utils.ToCmdLine("get", "6")), "v2")
}
