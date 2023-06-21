package shardkv

import (
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

type OpType string

const (
	OpGet    OpType = "Get"
	OpPut    OpType = "Put"
	OpAppend OpType = "Append"
)

type Op CommandArgs

type OpReply CommandReply

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
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrOutDated    = "ErrOutDated"
	ErrNotReady    = "ErrNotReady"
)

/*************** RPC args and reply ***************/

type CommandArgs struct { // put, get, append
	Key      string
	Value    string
	OpType   OpType
	ClientId int64
	SeqNum   int64
}

type CommandReply struct {
	Err   Err
	Value string
}

const (
	client_retry_time time.Duration = time.Duration(1) * time.Millisecond
	cmd_timeout       time.Duration = time.Duration(2) * time.Second
	gap_time          time.Duration = time.Duration(5) * time.Millisecond
	snapshot_gap_time time.Duration = time.Duration(10) * time.Millisecond
)
