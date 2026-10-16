package pubsub

import (
	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/datastruct/list"
	"github.com/hdt3213/godis/datastruct/lock"
	"github.com/hdt3213/godis/interface/redis"
)

// Hub stores all subscribe relations
type Hub struct {
	// channel -> list(*Client)
	subs dict.Dict[*list.LinkedList[redis.Connection]]
	// lock channel
	subsLocker *lock.Locks
}

// MakeHub creates new hub
func MakeHub() *Hub {
	return &Hub{
		subs:       dict.MakeConcurrent[*list.LinkedList[redis.Connection]](4),
		subsLocker: lock.Make(16),
	}
}
