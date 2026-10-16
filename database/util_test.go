package database

import (
	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/interface/database"
	"time"
)

func makeTestDB() *DB {
	return &DB{
		data:       dict.MakeConcurrent[*database.DataEntity](dataDictSize),
		versionMap: dict.MakeConcurrent[uint32](dataDictSize),
		ttlMap:     dict.MakeConcurrent[time.Time](ttlDictSize),
		addAof:     func(line CmdLine) {},
	}
}
