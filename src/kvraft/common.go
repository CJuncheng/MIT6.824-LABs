package kvraft

import (
	"log"
	"time"
)

type OpType string

const (
	OpGet    OpType = "Get"
	OpPut    OpType = "Put"
	OpAppend OpType = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   OpType
	Key      string
	Value    string
	ClientId int64
	SeqNum   int64
}

type OpReply struct {
	Err   Err
	Value string
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
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
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

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
