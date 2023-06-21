package shardkv

import (
	"sync"
	"time"
)

const (
	CofigurationMonitorTimeout time.Duration = time.Duration(50) * time.Microsecond
	MigrationMonitorTimeout    time.Duration = time.Duration(50) * time.Microsecond
	GCMonitorTimeout           time.Duration = time.Duration(50) * time.Microsecond
	CheckEntryMonitorTimeout   time.Duration = time.Duration(100) * time.Microsecond
)

func (kv *ShardKV) monitor(action func(), timeout time.Duration) {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			action()
		}
		time.Sleep(timeout)
	}
}

/*************** Configuration Update ***************/

func (kv *ShardKV) configurationAction() {
	canPerformNextConfig := true
	kv.mu.Lock()
	for _, shard := range kv.shards {
		if shard.Status != Serving {
			canPerformNextConfig = false
			break
		}
	}
	currConfigNum := kv.currConfig.Num
	kv.mu.Unlock()

	if canPerformNextConfig {
		nextConfig := kv.sc.Query(currConfigNum + 1)
		if nextConfig.Num == currConfigNum+1 {
			cmd := Command{Configuration, nextConfig}
			kv.Execute(cmd, &OpReply{})
		}
	}
}

/*************** Shard Migration ***************/
type PullShardArgs struct {
	ConfigNum int
	ShardIds  []int
}

type PullShardReply struct {
	Err       Err
	ConfigNum int
	Shards    map[int]*Shard // database; shardId
}

func (kv *ShardKV) migrationAction() {
	kv.mu.Lock()
	gid2shardIds := kv.getShardIdsByStatus(Pulling)
	//gid2shardIds := kv.getShardIDsByStatus(Pulling, &kv.lastConfig)
	var wg sync.WaitGroup
	for gid, shardIds := range gid2shardIds {
		wg.Add(1)
		go func(servers []string, configNum int, shardIds []int) {
			defer wg.Done()
			args := PullShardArgs{
				ConfigNum: configNum,
				ShardIds:  shardIds,
			}
			for _, server := range servers {
				var reply PullShardReply
				srv := kv.makeEnd(server)
				if srv.Call("ShardKV.PullShardsData", &args, &reply) && reply.Err == OK {
					cmd := Command{InsertShards, reply} // Insert shards in current config after pulling
					kv.Execute(cmd, &OpReply{})
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currConfig.Num, shardIds)
	}
	kv.mu.Unlock()
	//Debug(dServer, "G%+v {S%+v} migrationAction wait", kv.gid, kv.me)
	wg.Wait()
	//Debug(dServer, "G%+v {S%+v} migrationAction done", kv.gid, kv.me)
}

func (kv *ShardKV) PullShardsData(args *PullShardArgs, reply *PullShardReply) {
	//defer Debug(dServer, "G%+v {S%+v} PullShardsData: args: %+v reply: %+v", kv.gid, kv.me, args, reply)
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	if kv.currConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		kv.mu.Unlock()
		kv.configurationAction()
		return
	}

	reply.Shards = make(map[int]*Shard)
	for _, sharId := range args.ShardIds {
		reply.Shards[sharId] = kv.shards[sharId].deepCopy()
	}

	reply.ConfigNum, reply.Err = args.ConfigNum, OK
	kv.mu.Unlock()
}

/*************** Shard Garbage Collection ***************/

type DeleteShardArgs PullShardArgs
type DeleteShardReply PullShardReply

func (kv *ShardKV) gcAction() {
	kv.mu.Lock()
	gid2shardIds := kv.getShardIdsByStatus(GCing)
	var wg sync.WaitGroup
	for gid, shardIds := range gid2shardIds {
		wg.Add(1)
		go func(servers []string, configNum int, shardIds []int) {
			defer wg.Done()
			args := DeleteShardArgs{
				ConfigNum: configNum,
				ShardIds:  shardIds,
			}
			for _, server := range servers {
				var reply DeleteShardReply
				srv := kv.makeEnd(server)
				if srv.Call("ShardKV.DeleteShardsData", &args, &reply) && reply.Err == OK {
					cmd := Command{DeleteShards, args}
					kv.Execute(cmd, &OpReply{})
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currConfig.Num, shardIds)
	}
	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) DeleteShardsData(args *DeleteShardArgs, reply *DeleteShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	if kv.currConfig.Num > args.ConfigNum {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	var opReply OpReply
	cmd := Command{DeleteShards, *args} // Insert shards in current config after pulling
	kv.Execute(cmd, &opReply)
	reply.Err = opReply.Err

}

/*************** Empty Entry Check ***************/

func (kv *ShardKV) checkEntryInCurrentTermAction() {
	if !kv.rf.HasLogInCurrentTerm() {
		cmd := Command{EmptyEntry, nil}
		kv.Execute(cmd, &OpReply{})
	}
}
