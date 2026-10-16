# Godis

![license](https://img.shields.io/github/license/Morditux/godis)
[![Build Status](https://github.com/Morditux/godis/actions/workflows/coverall.yml/badge.svg?query=branch%3Afork_fixes)](https://github.com/Morditux/godis/actions?query=branch%3Afork_fixes)
[![Go Reference](https://pkg.go.dev/badge/github.com/hdt3213/godis.svg)](https://pkg.go.dev/github.com/hdt3213/godis)
<br>
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge-flat.svg)](https://github.com/avelino/awesome-go)

[中文版 (upstream)](https://github.com/hdt3213/godis/blob/master/README_CN.md)

`Godis` is a golang implementation of Redis Server, which intents to provide an example of writing a high concurrent
middleware using golang.

This is a hardened fork of [hdt3213/godis](https://github.com/HDT3213/godis). **About this fork:**

- **Race free**: data races fixed across the server core, cluster/TCC, AOF, Pub/Sub, connections and logger. The full
  test suite runs with `go test -race ./...` in CI on every push.
- **Leak free & safer shutdown**: goroutine leak fixes, idempotent `Close` on AOF/persister/connections, synchronized
  gnet server boot and graceful time-wheel stop.
- **Bug fixes**: cluster & Raft metadata management, replication, persistence and command edge cases.
- **Go generics for core data structures**: `dict.Dict[V]`, `list.List[T]` and the hash set now store concrete types,
  removing `interface{}` conversions from hot paths (database entities, hashes, lists, Pub/Sub hubs, cluster pools…).
- **More tests**: new suites for AOF (replay, fsync policies, listeners), Pub/Sub, connection state machine and logger;
  `INFO` now reports the real `godis_git_sha1` / `godis_git_dirty` and `blocked_clients`.
- **Repo hygiene**: gofmt & `go vet` clean, dependencies updated (raft-boltdb v2 with Go 1.24), CI enabled on this
  repository.

Key Features (inherited from upstream):

- Support string, list, hash, set, sorted set, bitmap
- Concurrent Core for better performance
- TTL
- Publish/Subscribe
- GEO
- AOF and AOF Rewrite
- RDB read and write
- Multi Database and `SELECT` command
- Transaction is **Atomic** and Isolated. If any errors are encountered during execution, godis will rollback the executed commands
- Replication
- Server-side Cluster which is transparent to client. You can connect to any node in the cluster to access all data in the cluster.
  - Cluster metadata management based on Raft. Support dynamic expansion, rebalancing and failover.
  - `MSET`, `MSETNX`, `DEL`, `Rename`, `RenameNX` command is supported and atomically executed in cluster mode, allow over multi node.
  - `MULTI` Commands Transaction is supported within slot in cluster mode

If you could read Chinese, you can find more details in [the author's blog](https://www.cnblogs.com/Finley/category/1598973.html).

## Get Started

### Build from source

Requires Go 1.24+:

```bash
git clone https://github.com/Morditux/godis.git
cd godis
go build -o godis
```

### Run

The program will try to read config file path from environment variable `CONFIG`.

If environment variable is not set, then the program try to read `redis.conf` in the working directory.

```bash
CONFIG=example.conf ./godis
```

You could use redis-cli or other redis client to connect godis server, which listens on 0.0.0.0:6399 on default mode.

### Cluster mode

We provide node1.conf and node2.conf for demonstration. use following command line to start a two-node-cluster:

```bash
CONFIG=node1.conf ./godis &
CONFIG=node2.conf ./godis &
```

Connect to a node in the cluster to access all data in the cluster:

```cmd
redis-cli -p 6399
```

Please refer to [example.conf](./example.conf) for cluster configuration.

## Supported Commands

See: [commands.md](./commands.md)

## Tests

Run the full test suite with the race detector (same as CI):

```bash
go test -race -count=1 ./...
```

Only the two replication tests in `database` (`TestReplicationSlaveSide`, `TestReplicationFailover`) need a real Redis
server listening on `127.0.0.1:6379`:

```bash
docker run -d --name godis-redis-test -p 6379:6379 redis:latest redis-server --save '' --appendonly no
```

## Benchmark

Report from the upstream project (redis-benchmark against a single godis node):

Environment:

Go version：1.23
System: MacOS Monterey 12.5 M2 Air

```
PING_INLINE: 179211.45 requests per second, p50=1.031 msec                    
PING_MBULK: 173611.12 requests per second, p50=1.071 msec                    
SET: 158478.61 requests per second, p50=1.535 msec                    
GET: 156985.86 requests per second, p50=1.127 msec                    
INCR: 164473.69 requests per second, p50=1.063 msec                    
LPUSH: 151285.92 requests per second, p50=1.079 msec                    
RPUSH: 176678.45 requests per second, p50=1.023 msec                    
LPOP: 177619.89 requests per second, p50=1.039 msec                    
RPOP: 172413.80 requests per second, p50=1.039 msec                    
SADD: 159489.64 requests per second, p50=1.047 msec                    
HSET: 175131.36 requests per second, p50=1.031 msec                    
SPOP: 170648.45 requests per second, p50=1.031 msec                    
ZADD: 165289.25 requests per second, p50=1.039 msec                    
ZPOPMIN: 185528.77 requests per second, p50=0.999 msec                    
LPUSH (needed to benchmark LRANGE): 172117.05 requests per second, p50=1.055 msec                    
LRANGE_100 (first 100 elements): 46511.62 requests per second, p50=4.063 msec                   
LRANGE_300 (first 300 elements): 21217.91 requests per second, p50=9.311 msec                     
LRANGE_500 (first 500 elements): 13331.56 requests per second, p50=14.407 msec                    
LRANGE_600 (first 600 elements): 11153.25 requests per second, p50=17.007 msec                    
MSET (10 keys): 88417.33 requests per second, p50=3.687 msec  
```

## Read My Code

If you want to read the code in this repository, here is a simple guidance.

- project root: the entry point (`main.go` builds the standalone binary)
- config: config parser
- interface: interface definitions (`redis`, `database`, `tcp`)
- lib: utils - logger, pool, semaphore, id generator, geohash, consistent hash, wildcard

The core packages:

- tcp: transport interfaces shared by std `net` and gnet servers
- redis:
    - connection: connection representation and command properties
    - parser: RESP parser
    - protocol: protocol types and replies (with `asserts` helpers for tests)
    - server/std and server/gnet: two server drivers
    - client: a RESP client used by the cluster
- datastruct: self-contained data structures
    - bitmap: bitmap utilities
    - dict: generic concurrent hash map (`dict.Dict[V]`)
    - list: generic list (`list.List[T]` linked list and quick list)
    - lock: hashed key locks for thread safety
    - set: hash set built on the generic dict
    - sortedset: sorted set based on skiplist
- database: the storage engine core
    - server.go: a standalone redis server with multiple databases
    - database.go: data structure and base functions of a single database
    - router.go: the command table
    - keys.go / string.go / list.go / hash.go / set.go / sortedset.go / geo.go: command handlers
    - transaction.go: local transaction (MULTI/EXEC/WATCH)
    - tx_utils.go: undo utilities for transaction rollback
    - persistence.go: TTL, keys loading, RDB
    - replication_master.go / replication_slave.go: replication
    - slowlog.go: slow log
    - systemcmd.go: authentication, INFO and other system commands
    - cluster_helper.go: helpers shared with cluster mode
- pubsub: publish/subscribe (channels and sharded hub)
- aof: AOF persistence, fsync policies, rewrite and command marshalling
- cluster:
    - cluster.go: entrance of cluster mode
    - core: node connections, cron jobs, node/replica managers and the try-commit-catch (TCC) distributed transaction
    - raft: Raft based cluster metadata management (topology, rebalancing, failover)
    - commands: atomic multi-node commands (`DEL`, `MSET`, `RENAME`, TCC helpers)

# License

This project inherits the [GPL license](./LICENSE) of [hdt3213/godis](https://github.com/HDT3213/godis/blob/master/LICENSE).
