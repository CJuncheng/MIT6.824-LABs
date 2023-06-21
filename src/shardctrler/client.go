package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.seqNum = 0

	return ck
}

func (ck *Clerk) sendComand(args CommandArgs) CommandReply {

	ck.mu.Lock()
	ck.seqNum += 1
	args.ClientId = ck.clientId
	args.SeqNum = ck.seqNum

	i := ck.leaderId
	DPrintf("Client last leader id is %v", i)
	ck.mu.Unlock()

	t := time.Now()
	for time.Since(t).Seconds() < 10 {
		reply := CommandReply{}
		ok := ck.servers[i].Call("ShardCtrler.Command", &args, &reply)
		if !ok {
			i = (i + 1) % len(ck.servers)
			time.Sleep(client_retry_time)
			continue
		}
		if reply.Err == OK {
			// success
			ck.leaderId = i
			return reply
		}

		// reply.Err == ErrWrongLeader
		i = (i + 1) % len(ck.servers)
		time.Sleep(client_retry_time)
	}
	return CommandReply{} // 10s not reply

}

func (ck *Clerk) Query(num int) Config {
	args := CommandArgs{
		OpType: OpQuery,
		Num:    num,
	}
	reply := ck.sendComand(args)
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := CommandArgs{
		OpType:  OpJoin,
		Servers: servers,
	}
	ck.sendComand(args)
}

func (ck *Clerk) Leave(gids []int) {
	args := CommandArgs{
		OpType: OpLeave,
		GIDs:   gids,
	}
	ck.sendComand(args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := CommandArgs{
		OpType: OpMove,
		Shard:  shard,
		GID:    gid,
	}
	ck.sendComand(args)
}
