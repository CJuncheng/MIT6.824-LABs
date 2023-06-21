package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	db            *DB // kv databse to store data
	opReplyChans  map[IdxAndTerm]chan OpReply
	lastOpContext map[int64]OpContext // key client id; vale: CmdContext
	lastApplied   int
	lastSnapshot  int
}

/*************** KV database ***************/

type DB struct {
	KvMap map[string]string
}

func (db *DB) Get(key string) (string, Err) {
	if value, ok := db.KvMap[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}
func (db *DB) Put(key string, value string) Err {
	db.KvMap[key] = value
	return OK
}
func (db *DB) Append(key string, value string) Err {
	if value_ori, ok := db.KvMap[key]; ok {
		db.KvMap[key] = value_ori + value
		return OK
	}
	db.KvMap[key] = value
	return OK
}

func (kv *KVServer) Opt(op Op) (string, Err) {
	switch op.OpType {
	case OpGet:
		return kv.db.Get(op.Key)
	case OpPut:
		err := kv.db.Put(op.Key, op.Value)
		return "", err
	case OpAppend:
		err := kv.db.Append(op.Key, op.Value)
		return "", err
	default:
		return "", OK
	}
}

/*************** RPC handler ***************/
// Command RPC handler; Put(), Get(), Append() handler
func (kv *KVServer) Command(args *CommandArgs, reply *CommandReply) {
	kv.mu.Lock()
	cmdContext, ok := kv.lastOpContext[args.ClientId]
	if ok && args.SeqNum <= cmdContext.SeqNum && args.OpType != OpGet {
		reply.Value, reply.Err = cmdContext.Reply.Value, cmdContext.Reply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	cmd := Op{
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		OpType:   args.OpType,
		Key:      args.Key,
		Value:    args.Value,
	}
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

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

/*************** Apply ***************/
func (kv *KVServer) applier() {
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
				cmd := msg.Command.(Op)

				opContext, ok := kv.lastOpContext[cmd.ClientId]
				if ok && cmd.SeqNum <= opContext.SeqNum && cmd.OpType != OpGet {
					opReply = opContext.Reply
				} else {
					opReply.Value, opReply.Err = kv.Opt(cmd)
					kv.lastOpContext[cmd.ClientId] = OpContext{
						SeqNum: cmd.SeqNum,
						Reply:  opReply,
					}
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

			}
		default:
			time.Sleep(gap_time)
		}
	}
}

/*************** Snapshot ***************/
func (kv *KVServer) readPersist(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var db DB
	var lastOpContext map[int64]OpContext
	if d.Decode(&db) != nil || d.Decode(&lastOpContext) != nil {
		log.Fatal("Failed to read persist!\n")
		return
	}
	kv.db = &db
	kv.lastOpContext = lastOpContext
}

func (kv *KVServer) snapshoter() {
	for !kv.killed() {
		kv.mu.Lock()
		threshold := int(0.8 * float32(kv.maxraftstate))
		if kv.maxraftstate != -1 && kv.rf.RaftPersistSize() > threshold && kv.lastApplied > kv.lastSnapshot+3 {
			//kv.takeSnapshot(kv.lastApplied)
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			if e.Encode(*kv.db) != nil || e.Encode(kv.lastOpContext) != nil {
				return
			}
			snapshot := w.Bytes()
			kv.rf.Snapshot(kv.lastApplied, snapshot)

			// update
			kv.lastSnapshot = kv.lastApplied
		}
		kv.mu.Unlock()
		time.Sleep(snapshot_gap_time)
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate // The max bytes of log persiter, not include snapshot

	// You may need initialization code here.

	kv.dead = 0
	kv.applyCh = make(chan raft.ApplyMsg, 5)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh) // create a raft service
	kv.db = &DB{make(map[string]string)}
	kv.opReplyChans = make(map[IdxAndTerm]chan OpReply)
	kv.lastOpContext = make(map[int64]OpContext)
	kv.lastApplied = 0
	kv.lastSnapshot = 0

	// initialize from snapshot persisted before a crash
	kv.readPersist(persister.ReadSnapshot())

	// goroutines
	go kv.applier()
	go kv.snapshoter() // leader snapshot --> 同步到其他fallower

	return kv
}
