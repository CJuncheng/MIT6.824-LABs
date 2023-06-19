package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	// Your data here.

	configs *Configuration // indexed by config num

	opReplyChans  map[IdxAndTerm]chan OpReply
	lastOpContext map[int64]OpContext // key client id; vale: CmdContext
	lastApplied   int
}

/*************** Configuration Operation***************/

type Configuration struct {
	configs []Config // indexed by config num
}

func newConfiguration() *Configuration {
	cfgs := Configuration{make([]Config, 1)}
	cfgs.configs[0] = Config{
		Num:    0,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}
	return &cfgs
}

func DeepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}

func groupToShards(config Config) map[int][]int {
	g2s := make(map[int][]int)
	for gid := range config.Groups {
		g2s[gid] = make([]int, 0)
	}
	for shardId, gid := range config.Shards {
		g2s[gid] = append(g2s[gid], shardId)
	}
	return g2s
}

func getMinNumShardByGid(g2s map[int][]int) int {
	// 不固定顺序的话，可能会导致两次的config不同
	gids := make([]int, 0)
	for key := range g2s {
		gids = append(gids, key)
	}
	sort.Ints(gids) //g2s 的 key(group id) 排序

	min, idx := NShards+1, -1
	for _, gid := range gids {
		if gid != 0 && len(g2s[gid]) < min {
			min = len(g2s[gid])
			idx = gid
		}
	}
	return idx // group id

}

func getMaxNumShardByGid(g2s map[int][]int) int {
	// GID = 0 是无效配置，一开始所有分片分配给GID=0
	if shards, ok := g2s[0]; ok && len(shards) > 0 {
		return 0
	}

	// 不固定顺序的话，可能会导致两次的config不同
	gids := make([]int, 0)
	for key := range g2s {
		gids = append(gids, key)
	}
	sort.Ints(gids) //g2s 的 key(group id) 排序

	max, idx := -1, -1
	for _, gid := range gids {
		if gid != 0 && len(g2s[gid]) > max {
			max = len(g2s[gid])
			idx = gid
		}
	}
	return idx // group id
}

func (cfgr *Configuration) join(groups map[int][]string) Err {
	lastConfig := cfgr.configs[len(cfgr.configs)-1]
	newConfig := Config{len(cfgr.configs), lastConfig.Shards, DeepCopy(lastConfig.Groups)}

	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}

	// load balance
	g2s := groupToShards(newConfig)
	for {
		mxGid, miGid := getMaxNumShardByGid(g2s), getMinNumShardByGid(g2s)
		if mxGid != 0 && len(g2s[mxGid])-len(g2s[miGid]) <= 1 {
			break
		}
		g2s[miGid] = append(g2s[miGid], g2s[mxGid][0])
		g2s[mxGid] = g2s[mxGid][1:]
	}

	// update shards
	var newShards [NShards]int
	for gid, shards := range g2s {
		for _, shardId := range shards {
			newShards[shardId] = gid
		}
	}
	newConfig.Shards = newShards

	cfgr.configs = append(cfgr.configs, newConfig)
	return OK
}

func (cfgr *Configuration) leave(gids []int) Err {
	lastConfig := cfgr.configs[len(cfgr.configs)-1]
	newConfig := Config{len(cfgr.configs), lastConfig.Shards, DeepCopy(lastConfig.Groups)}

	g2s := groupToShards(newConfig)
	deletedShards := make([]int, 0)
	for _, gid := range gids {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shards, ok := g2s[gid]; ok {
			deletedShards = append(deletedShards, shards...)
			delete(g2s, gid)
		}
	}

	var newShards [NShards]int
	if len(newConfig.Groups) > 0 {
		// load balance
		for _, shardId := range deletedShards {
			miGid := getMinNumShardByGid(g2s)
			g2s[miGid] = append(g2s[miGid], shardId)
		}

		// update shards
		for gid, shards := range g2s {
			for _, shardId := range shards {
				newShards[shardId] = gid
			}
		}
	}
	newConfig.Shards = newShards

	cfgr.configs = append(cfgr.configs, newConfig)

	return OK
}

func (cfgr *Configuration) move(shard int, gid int) Err {
	lastConfig := cfgr.configs[len(cfgr.configs)-1]
	newConfig := Config{len(cfgr.configs), lastConfig.Shards, DeepCopy(lastConfig.Groups)}
	newConfig.Shards[shard] = gid
	cfgr.configs = append(cfgr.configs, newConfig)
	return OK
}

func (cfgr *Configuration) query(num int) (Config, Err) {
	// 如果该数字为 -1 或大于已知的最大配置数字，则 shardctrler 应回复最新配置。
	if num < 0 || num >= len(cfgr.configs) {
		return cfgr.configs[len(cfgr.configs)-1], OK
	}
	return cfgr.configs[num], OK
}

func (cfgr *Configuration) Opt(op Op) (Config, Err) {
	switch op.OpType {
	case OpJoin:
		err := cfgr.join(op.Servers)
		return Config{}, err
	case OpLeave:
		err := cfgr.leave(op.GIDs)
		return Config{}, err
	case OpMove:
		err := cfgr.move(op.Shard, op.GID)
		return Config{}, err
	case OpQuery:
		config, err := cfgr.query(op.Num)
		return config, err
	default:
		return Config{}, OK
	}
}

func (cfg *Config) DeepCopy() Config {
	ret := Config{
		Num:    cfg.Num,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}

	for k, v := range cfg.Groups {
		ret.Groups[k] = v
	}
	for i := range cfg.Shards {
		ret.Shards[i] = cfg.Shards[i]
	}
	return ret
}

/*************** RPC handler ***************/
func (sc *ShardCtrler) Command(args *CommandArgs, reply *CommandReply) {
	sc.mu.Lock()
	opContext, ok := sc.lastOpContext[args.ClientId]
	if ok && args.SeqNum <= opContext.SeqNum && args.OpType != OpQuery { // 幂等
		reply.Config, reply.Err = opContext.Reply.Config, opContext.Reply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	cmd := Op{
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		OpType:   args.OpType,
		Servers:  args.Servers,
		GIDs:     args.GIDs,
		Shard:    args.Shard,
		GID:      args.GID,
		Num:      args.Num,
	}
	index, term, isLeader := sc.rf.Start(cmd)
	if !isLeader {
		reply.Config, reply.Err = Config{}, ErrWrongLeader
		return
	}

	sc.mu.Lock()
	idxAndTerm := IdxAndTerm{index, term}
	ch := make(chan OpReply, 1)
	sc.opReplyChans[idxAndTerm] = ch
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		delete(sc.opReplyChans, idxAndTerm)
		sc.mu.Unlock()
		close(ch)
	}()

	t := time.NewTimer(cmd_timeout)
	defer t.Stop()

	for {
		sc.mu.Lock()
		select {
		case opReply := <-ch:
			reply.Config, reply.Err = opReply.Config, opReply.Err
			sc.mu.Unlock()
			return
		case <-t.C:
		priority:
			for {
				select {
				case opReply := <-ch:
					reply.Config, reply.Err = opReply.Config, opReply.Err
					sc.mu.Unlock()
					return
				default:
					break priority
				}
			}
			reply.Config, reply.Err = Config{}, ErrTimeout
			sc.mu.Unlock()
			return
		default:
			sc.mu.Unlock()
			time.Sleep(gap_time)
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

/*************** Apply ***************/
func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		select {
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				sc.mu.Lock()
				if msg.CommandIndex <= sc.lastApplied {
					sc.mu.Unlock()
					continue
				}
				sc.lastApplied = msg.CommandIndex

				var opReply OpReply
				cmd := msg.Command.(Op)

				opContext, ok := sc.lastOpContext[cmd.ClientId]
				if ok && cmd.SeqNum <= opContext.SeqNum && cmd.OpType != OpQuery { //重复请求
					opReply = opContext.Reply
				} else {
					opReply.Config, opReply.Err = sc.configs.Opt(cmd)
					sc.lastOpContext[cmd.ClientId] = OpContext{
						SeqNum: cmd.SeqNum,
						Reply:  opReply,
					}
				}

				term, isLeader := sc.rf.GetState()

				if !isLeader || term != msg.CommandTerm {
					sc.mu.Unlock()
					continue
				}

				idxAndTerm := IdxAndTerm{
					index: msg.CommandIndex,
					term:  term,
				}
				ch, ok := sc.opReplyChans[idxAndTerm]
				if ok {
					select {
					case ch <- opReply:
					case <-time.After(10 * time.Millisecond):
					}
				}
				sc.mu.Unlock()
			} else {

			}
		default:
			time.Sleep(gap_time)
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.configs = newConfiguration()
	sc.opReplyChans = make(map[IdxAndTerm]chan OpReply)
	sc.lastOpContext = make(map[int64]OpContext)
	sc.lastApplied = 0

	go sc.applier()

	return sc
}
