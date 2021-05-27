package shardctrler

import "sort"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (c *Config) Balance() {
	var gids []int
	for k,_ := range c.Groups {
		gids = append(gids, k)
	}
	if len(gids) == 0 {
		for i := 0; i < NShards; i++ {
			c.Shards[i] = 0
		}
	} else {
		sort.Slice(gids, func(i int, j int) bool {
			return gids[i] < gids[j]
		})
		for i := 0; i < NShards; i++ {
			c.Shards[i] = gids[i%len(gids)]
		}
	}
}

func (c *Config) Copy() *Config {
	conf := Config{
		Num:    c.Num,
		Shards: [NShards]int{},
		Groups: map[int][]string{},
	}
	for idx, gid := range c.Shards {
		conf.Shards[idx] = gid
	}
	for k,v := range c.Groups {
		conf.Groups[k] = v
	}
	return &conf
}

const (
	OK = "OK"
	ERR = "ERR"
)

type Err string

type JoinArgs struct {
	ClientID int
	SerialID int
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	ClientID int
	SerialID int
	GIDs []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	ClientID int
	SerialID int
	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
