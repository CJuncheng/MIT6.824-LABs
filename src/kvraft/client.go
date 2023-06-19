package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

// https://cloud.tencent.com/developer/article/2134849
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu       sync.Mutex
	leaderId int
	clientId int64
	seqNum   int64 // Client request sequence number, is incremental
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.seqNum = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Comand", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

func (ck *Clerk) sendComand(key string, value string, opType OpType) string { // Get(), Put(), Append()
	ck.mu.Lock()
	i := ck.leaderId
	DPrintf("Client last leader id is %v", i)
	ck.seqNum += 1
	args := CommandArgs{
		Key:      key,
		Value:    value,
		OpType:   opType,
		ClientId: ck.clientId,
		SeqNum:   ck.seqNum,
	}
	ck.mu.Unlock()

	t := time.Now()
	for time.Since(t).Seconds() < 10 {
		reply := CommandReply{}
		ok := ck.servers[i].Call("KVServer.Command", &args, &reply)
		if !ok {
			i = (i + 1) % len(ck.servers)
			continue
		}
		if reply.Err == OK {
			// success
			ck.leaderId = i
			return reply.Value
		} else if reply.Err == ErrNoKey {
			// empty key
			return ""
		}
		// reply.Err == ErrWrongLeader
		i = (i + 1) % len(ck.servers)
		time.Sleep(client_retry_time)
	}
	return "" // 10s not reply
}

func (ck *Clerk) Get(key string) string {
	return ck.sendComand(key, "", OpGet)
}

func (ck *Clerk) Put(key string, value string) {
	ck.sendComand(key, value, OpPut)
}

func (ck *Clerk) Append(key string, value string) {
	ck.sendComand(key, value, OpAppend)
}
