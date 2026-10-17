package database

import (
	"testing"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/connection"
	"github.com/hdt3213/godis/redis/protocol"
)

func execEmptyTestCmd(t *testing.T, db *DB, c redis.Connection, args ...string) redis.Reply {
	t.Helper()
	return db.Exec(c, utils.ToCmdLine(args...))
}

// TestEmptyContainersAreRemoved covers the lifecycle bug where removing the
// last member of a sorted set/set/list left an empty container behind. That
// made EXISTS return 1, broke ZCOUNT (empty range panic) and could crash
// BGREWRITEAOF's zSetToCmd/ForEachByRank path.
func TestEmptyContainersAreRemoved(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()
	defer c.Close()

	steps := []struct {
		name  string
		cmds  [][]string
		check string
		want  string
	}{
		{
			name:  "zrem last member",
			cmds:  [][]string{{"ZADD", "z1", "1", "a"}, {"ZREM", "z1", "a"}},
			check: "z1", want: "0",
		},
		{
			name:  "zremrangebyscore all",
			cmds:  [][]string{{"ZADD", "z2", "1", "a", "2", "b"}, {"ZREMRANGEBYSCORE", "z2", "-inf", "+inf"}},
			check: "z2", want: "0",
		},
		{
			name:  "zremrangebyrank all",
			cmds:  [][]string{{"ZADD", "z3", "1", "a", "2", "b"}, {"ZREMRANGEBYRANK", "z3", "0", "-1"}},
			check: "z3", want: "0",
		},
		{
			name:  "zremrangebylex all",
			cmds:  [][]string{{"ZADD", "z4", "1", "a", "2", "b"}, {"ZREMRANGEBYLEX", "z4", "-", "+"}},
			check: "z4", want: "0",
		},
		{
			name:  "zpopmin last member",
			cmds:  [][]string{{"ZADD", "z5", "1", "a"}, {"ZPOPMIN", "z5"}},
			check: "z5", want: "0",
		},
		{
			name:  "spop last member",
			cmds:  [][]string{{"SADD", "s1", "a"}, {"SPOP", "s1"}},
			check: "s1", want: "0",
		},
		{
			name:  "ltrim everything",
			cmds:  [][]string{{"RPUSH", "l1", "a", "b"}, {"LTRIM", "l1", "1", "0"}},
			check: "l1", want: "0",
		},
		{
			name:  "sinterstore missing source clears dest",
			cmds:  [][]string{{"SADD", "dest1", "x"}, {"SADD", "src1", "a"}, {"SINTERSTORE", "dest1", "src1", "missing"}},
			check: "dest1", want: "0",
		},
		{
			name:  "sunionstore empty result clears dest",
			cmds:  [][]string{{"SADD", "dest2", "x"}, {"SUNIONSTORE", "dest2", "missing"}},
			check: "dest2", want: "0",
		},
		{
			name:  "sdiffstore empty result clears dest",
			cmds:  [][]string{{"SADD", "dest3", "x"}, {"SDIFFSTORE", "dest3", "missing"}},
			check: "dest3", want: "0",
		},
	}

	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			for _, cmd := range step.cmds {
				reply := execEmptyTestCmd(t, db, c, cmd...)
				if protocol.IsErrorReply(reply) {
					t.Fatalf("%v failed: %s", cmd, string(reply.ToBytes()))
				}
			}
			reply := execEmptyTestCmd(t, db, c, "EXISTS", step.check)
			if got := string(reply.ToBytes()); got != ":"+step.want+"\r\n" {
				t.Fatalf("EXISTS %s = %q, want :%s", step.check, got, step.want)
			}
		})
	}
}
