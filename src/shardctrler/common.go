package shardctrler

import (
	"log"
	"time"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:add
// Join(servers) --  a set of groups (gid -> server-list mapping).
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

type OpType string

const (
	OpJoin  OpType = "OpJoin"
	OpLeave OpType = "OpLeave"
	OpMove  OpType = "OpMove"
	OpQuery OpType = "OpQuery"
)

type Op CommandArgs

type OpReply struct {
	Err    Err
	Config Config // for Query special
}

type OpContext struct {
	SeqNum int64
	Reply  OpReply
}

type IdxAndTerm struct {
	index int
	term  int
}

type Err string

const (
	OK             Err = "OK"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
)

/*************** RPC args and reply ***************/
type CommandArgs struct { // put, get, append
	OpType   OpType
	ClientId int64
	SeqNum   int64

	// for Join
	Servers map[int][]string // new GID -> servers mappings

	// for Leave
	GIDs []int

	// for Move
	Shard int
	GID   int

	// for Query
	Num int // desired config number
}

type CommandReply struct {
	//WrongLeader bool
	Err    Err
	Config Config // for Query special
}

const (
	client_retry_time time.Duration = time.Duration(1) * time.Millisecond
	cmd_timeout       time.Duration = time.Duration(2) * time.Second
	gap_time          time.Duration = time.Duration(5) * time.Millisecond
	//snapshot_gap_time time.Duration = time.Duration(10) * time.Millisecond
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
