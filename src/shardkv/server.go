package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type ShardKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	makeEnd func(string) *labrpc.ClientEnd
	gid     int
	//ctrlers      []*labrpc.ClientEnd
	sc           *shardctrler.Clerk
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	shards map[int]*Shard // database; shardId

	opReplyChans map[IdxAndTerm]chan OpReply
	lastApplied  int
	lastSnapshot int

	lastConfig shardctrler.Config
	currConfig shardctrler.Config
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
} // Your code here, if desired.

/*************** Shard db ***************/
type ShardStatus uint8

const (
	Serving ShardStatus = iota
	Pulling
	BePulling
	GCing
)

type Shard struct {
	KV            map[string]string
	Status        ShardStatus
	LastOpContext map[int64]OpContext // key client id; vale: CmdContext
}

func NewShard(status ShardStatus) *Shard {
	return &Shard{make(map[string]string), status, make(map[int64]OpContext)}
}

func (shard *Shard) Get(key string) (string, Err) {
	if value, ok := shard.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (shard *Shard) Put(key string, value string) Err {
	shard.KV[key] = value
	return OK
}

func (shard *Shard) Append(key string, value string) Err {
	if value_ori, ok := shard.KV[key]; ok {
		shard.KV[key] = value_ori + value
		return OK
	}
	shard.KV[key] = value
	return OK
}

func (shard *Shard) deepCopy() *Shard {
	newShard := NewShard(Serving)
	for k, v := range shard.KV {
		newShard.KV[k] = v
	}
	for id, context := range shard.LastOpContext {
		newShard.LastOpContext[id] = context
	}
	return newShard
}

func (kv *ShardKV) Opt(op *Op, shardId int) (string, Err) {
	shard := kv.shards[shardId]

	switch op.OpType {
	case OpGet:
		return shard.Get(op.Key)
	case OpPut:
		err := shard.Put(op.Key, op.Value)
		return "", err
	case OpAppend:
		err := shard.Append(op.Key, op.Value)
		return "", err
	default:
		return "", OK
	}
}

func (kv *ShardKV) getShardIdsByStatus(status ShardStatus) map[int][]int {
	gid2shardIds := make(map[int][]int)
	for shardId, _ := range kv.shards {
		if kv.shards[shardId].Status == status {
			gid := kv.lastConfig.Shards[shardId]
			if _, ok := gid2shardIds[gid]; !ok {
				arr := [1]int{shardId}
				gid2shardIds[gid] = arr[:]
			} else {
				gid2shardIds[gid] = append(gid2shardIds[gid], shardId)
			}
		}
	}
	return gid2shardIds
}

/*************** Command def ***************/
type Command struct {
	CmdType CommandType
	Data    interface{}
}

func (command Command) String() string {
	return fmt.Sprintf("{Type:%v,Data:%v}", command.CmdType, command.Data)
}

type CommandType uint8

const (
	Operation CommandType = iota
	Configuration
	InsertShards
	DeleteShards
	EmptyEntry
)

/*************** Command Execute ***************/
func (kv *ShardKV) Execute(cmd Command, reply *OpReply) {
	// 不持有锁以提高吞吐量
	// 当 KVServer 持有锁进行快照时，底层 raft 仍然可以提交 raft 日志
	index, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Value, reply.Err = "", ErrWrongLeader
		return
	}

	kv.mu.Lock()
	idxAndTerm := IdxAndTerm{index, term}
	ch := make(chan OpReply, 1)
	kv.opReplyChans[idxAndTerm] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.opReplyChans, idxAndTerm)
		kv.mu.Unlock()
		close(ch)
	}()

	t := time.NewTimer(cmd_timeout)
	defer t.Stop()

	for {
		kv.mu.Lock()
		select {
		case opReply := <-ch:
			reply.Value, reply.Err = opReply.Value, opReply.Err
			kv.mu.Unlock()
			return
		case <-t.C:
		priority:
			for {
				select {
				case opReply := <-ch:
					reply.Value, reply.Err = opReply.Value, opReply.Err
					kv.mu.Unlock()
					return
				default:
					break priority
				}
			}
			reply.Value, reply.Err = "", ErrTimeout
			kv.mu.Unlock()
			return
		default:
			kv.mu.Unlock()
			time.Sleep(gap_time)
		}
	}
}

/*************** Command RPC handler ***************/

func (kv *ShardKV) canServer(shardId int) bool { // The raft group can provide read/write service
	return kv.currConfig.Shards[shardId] == kv.gid && (kv.shards[shardId].Status == Serving || kv.shards[shardId].Status == GCing)
}

func (kv *ShardKV) Command(args *CommandArgs, reply *CommandReply) {
	kv.mu.Lock()
	shardId := key2shard(args.Key)
	if !kv.canServer(shardId) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	opContext, ok := kv.shards[shardId].LastOpContext[args.ClientId]
	if ok && args.SeqNum <= opContext.SeqNum && args.OpType != OpGet { // duplicate request
		reply.Value, reply.Err = opContext.Reply.Value, opContext.Reply.Err
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	// New Operation Command
	op := Op{
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		OpType:   args.OpType,
		Key:      args.Key,
		Value:    args.Value,
	}
	cmd := Command{Operation, op}

	var opReply OpReply
	kv.Execute(cmd, &opReply)
	reply.Value, reply.Err = opReply.Value, opReply.Err
}

/*************** Apply ***************/
// Get, Put, Append Operation
func (kv *ShardKV) applyOperation(msg *raft.ApplyMsg, op *Op) *OpReply {
	var opReply OpReply
	shardId := key2shard(op.Key)
	if kv.canServer(shardId) {
		opContext, ok := kv.shards[shardId].LastOpContext[op.ClientId]
		if ok && op.SeqNum <= opContext.SeqNum && op.OpType != OpGet {
			opReply = opContext.Reply
			return &opReply
		} else {
			opReply.Value, opReply.Err = kv.Opt(op, shardId)
			kv.shards[shardId].LastOpContext[op.ClientId] = OpContext{
				SeqNum: op.SeqNum,
				Reply:  opReply,
			}
			return &opReply
		}
	}
	return &OpReply{ErrWrongGroup, ""}
}

// Configuration Update
func (kv *ShardKV) updateShardStatus(nextConfig *shardctrler.Config) {
	if nextConfig.Num == 1 {
		shardIds := kv.getAllShards(nextConfig)
		for _, shardId := range shardIds {
			kv.shards[shardId] = NewShard(Serving)
		}
		return
	}

	nextShardIds := kv.getAllShards(nextConfig)
	currShardIds := kv.getAllShards(&kv.currConfig)

	// loss shard
	for _, currShardId := range currShardIds {
		if nextConfig.Shards[currShardId] != kv.gid {
			kv.shards[currShardId].Status = BePulling
		}
	}

	// get shard
	for _, nextShardId := range nextShardIds {
		if kv.currConfig.Shards[nextShardId] != kv.gid {
			kv.shards[nextShardId] = NewShard(Pulling)
		}
	}
}

func (kv *ShardKV) getAllShards(nextConfig *shardctrler.Config) []int {
	var shardIds []int
	for shardId, gid := range nextConfig.Shards {
		if gid == kv.gid {
			shardIds = append(shardIds, shardId)
		}
	}
	return shardIds
}

func (kv *ShardKV) applyConfiguration(nextConfig *shardctrler.Config) *OpReply {
	if nextConfig.Num == kv.currConfig.Num+1 {
		kv.updateShardStatus(nextConfig)
		kv.lastConfig = shardctrler.Config{Num: kv.currConfig.Num, Shards: kv.currConfig.Shards, Groups: shardctrler.DeepCopy(kv.currConfig.Groups)}
		kv.currConfig = shardctrler.Config{Num: nextConfig.Num, Shards: nextConfig.Shards, Groups: shardctrler.DeepCopy(nextConfig.Groups)}
		return &OpReply{OK, ""}
	}
	return &OpReply{ErrOutDated, ""}
}

// Shard Migration

func (kv *ShardKV) applyInsertShards(insertShardsInfo *PullShardReply) *OpReply {
	//Debug(dServer, "G%+v {S%+v} before applyInsertShards: %+v", kv.gid, kv.me, kv.shards)
	if insertShardsInfo.ConfigNum == kv.currConfig.Num {
		for shardId, shardData := range insertShardsInfo.Shards {
			if kv.shards[shardId].Status == Pulling {
				kv.shards[shardId] = shardData.deepCopy()
				kv.shards[shardId].Status = GCing
			} else {
				//Debug(dWarn, "G%+v {S%+v} shard %d is not Pulling: %+v", kv.gid, kv.me, shardId, kv.shards[shardId])
				break
			}
		}
		//Debug(dServer, "G%+v {S%+v} after applyInsertShards: %+v", kv.gid, kv.me, kv.shards)
		return &OpReply{OK, ""}
	}
	return &OpReply{ErrOutDated, ""}
}

// Delete shards
func (kv *ShardKV) applyDeleteShards(deleteShardsInfo *DeleteShardArgs) *OpReply {
	if deleteShardsInfo.ConfigNum == kv.currConfig.Num {
		for _, shardId := range deleteShardsInfo.ShardIds {
			shard := kv.shards[shardId]
			if shard.Status == GCing {
				shard.Status = Serving
			} else if shard.Status == BePulling {
				kv.shards[shardId] = NewShard(Serving)
			} else {
				break
			}
		}
		return &OpReply{OK, ""}
	}
	return &OpReply{OK, ""}
}

// Empty entry
func (kv *ShardKV) applyEmptyEntry() *OpReply {
	return &OpReply{OK, ""}
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.SnapshotValid {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) { // follower persiter snapshot
					kv.readPersist(msg.Snapshot)
					kv.lastApplied = msg.SnapshotIndex
				}
				kv.mu.Unlock()
			} else if msg.CommandValid {
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex

				var opReply OpReply
				cmd := msg.Command.(Command)

				switch cmd.CmdType {
				case Operation:
					op := cmd.Data.(Op)
					opReply = *kv.applyOperation(&msg, &op)
				case Configuration:
					nextConfig := cmd.Data.(shardctrler.Config)
					opReply = *kv.applyConfiguration(&nextConfig)
				case InsertShards:
					insertShardsInfo := cmd.Data.(PullShardReply)
					opReply = *kv.applyInsertShards(&insertShardsInfo)
				case DeleteShards:
					deleteShardsInfo := cmd.Data.(DeleteShardArgs)
					opReply = *kv.applyDeleteShards(&deleteShardsInfo)
				case EmptyEntry:
					opReply = *kv.applyEmptyEntry()
				}

				term, isLeader := kv.rf.GetState()

				if !isLeader || term != msg.CommandTerm {
					kv.mu.Unlock()
					continue
				}

				idxAndTerm := IdxAndTerm{
					index: msg.CommandIndex,
					term:  term,
				}
				ch, ok := kv.opReplyChans[idxAndTerm]
				if ok {
					select {
					case ch <- opReply:
					case <-time.After(10 * time.Millisecond):
					}
				}
				kv.mu.Unlock()
			} else {
				// ...
			}
		default:
			time.Sleep(gap_time)
		}
	}
}

/*************** Snapshot ***************/
func (kv *ShardKV) readPersist(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var shards map[int]*Shard
	var lastConfig, currConfig shardctrler.Config

	if d.Decode(&shards) != nil || d.Decode(&lastConfig) != nil || d.Decode(&currConfig) != nil {
		log.Fatal("Failed to read persist!\n")
		return
	}
	kv.shards = shards
	kv.lastConfig = lastConfig
	kv.currConfig = currConfig
}

func (kv *ShardKV) isNeedSnapshot() bool {
	for _, shard := range kv.shards {
		if shard.Status == BePulling {
			return false
		}
	}
	if kv.maxraftstate != -1 {
		threshold := int(0.8 * float32(kv.maxraftstate))
		if kv.maxraftstate != -1 && kv.rf.RaftPersistSize() > threshold || kv.lastApplied > kv.lastSnapshot+3 {
			return true
		}
	}
	return false
}

func (kv *ShardKV) takeSnapshot(commandIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.shards) != nil || e.Encode(kv.lastConfig) != nil || e.Encode(kv.currConfig) != nil {
		return
	}
	snapshot := w.Bytes()
	kv.rf.Snapshot(commandIndex, snapshot)
}

func (kv *ShardKV) snapshoter() {
	for !kv.killed() {
		kv.mu.Lock()

		if kv.isNeedSnapshot() {
			kv.takeSnapshot(kv.lastApplied)
			// update
			kv.lastSnapshot = kv.lastApplied
		}
		kv.mu.Unlock()
		time.Sleep(snapshot_gap_time)
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(PullShardReply{})
	labgob.Register(PullShardArgs{})
	labgob.Register(DeleteShardArgs{})
	labgob.Register(DeleteShardReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.makeEnd = make_end
	kv.gid = gid

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.dead = 0
	kv.applyCh = make(chan raft.ApplyMsg, 5)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sc = shardctrler.MakeClerk(ctrlers)

	kv.shards = make(map[int]*Shard)
	kv.opReplyChans = make(map[IdxAndTerm]chan OpReply)

	kv.lastApplied = 0
	kv.lastSnapshot = 0

	// initialize from snapshot persisted before a crash
	kv.readPersist(persister.ReadSnapshot())

	// goroutines
	go kv.applier()    // 将提交的日志应用到 stateMachine
	go kv.snapshoter() // leader snapshot --> 同步到其他fallower

	go kv.monitor(kv.configurationAction, CofigurationMonitorTimeout)
	go kv.monitor(kv.migrationAction, MigrationMonitorTimeout)
	go kv.monitor(kv.gcAction, GCMonitorTimeout)
	go kv.monitor(kv.checkEntryInCurrentTermAction, CheckEntryMonitorTimeout)

	return kv
}
