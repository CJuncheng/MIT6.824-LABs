# MIT6.824-LABs(6.5840)
<h2 align="left">目录</h2>
<!--[toc]-->
- [MIT6.824-LABs(6.5840)](#mit6824-labs65840)
	- [Lab 1: MapReduce](#lab-1-mapreduce)
		- [MapReduce结构](#mapreduce结构)
		- [Coordinator](#coordinator)
		- [Worker](#worker)
	- [Lab 2: Raft](#lab-2-raft)
		- [Raft 主体](#raft-主体)
		- [Leader Election](#leader-election)
		- [Append Entries](#append-entries)
		- [Persistence](#persistence)
		- [Install Snapshot](#install-snapshot)
		- [被上层服务调用的API](#被上层服务调用的api)
	- [Lab 3: Fault-tolerant Key/Value Service](#lab-3-fault-tolerant-keyvalue-service)
		- [Client](#client)
		- [Servers](#servers)
	- [Lab 4: Sharded Key/Value Service](#lab-4-sharded-keyvalue-service)
		- [ShardCtrler](#shardctrler)
		- [ShardKV](#shardkv)
			- [客户端Clerk](#客户端clerk)
			- [服务端Server](#服务端server)
				- [ShardKV 主体](#shardkv-主体)
				- [日志类型](#日志类型)
				- [日志应用](#日志应用)
				- [读写服务](#读写服务)
				- [配置更新](#配置更新)
				- [分片迁移](#分片迁移)
				- [分片清理](#分片清理)
				- [空日志检测](#空日志检测)


MIT 6.824(Spring 2022, 6.5840)的四个实验实现。

## Lab 1: MapReduce
MIT6.824 MapReduce Lab 实现代码见：https://github.com/CJuncheng/MIT6.824-Labs。主要参考：https://github.com/s09g/mapreduce-go(Spring 2020)。

### MapReduce结构
1 . Task 结构体定义

Task 对应四个状态：
```go
/** state value
 * 0 : map
 * 1 : reduce
 * 2 : wait
 * 3 : exit, nothing to do
 */
const (
	Map TaskState = iota
	Reduce
	Wait
	Exit
)
```
Worker Task 分为 Map task 和 Reduce task, 这两个阶段的结构体可以复用，具体定义如下：
```go
type Task struct {
	State          TaskState
	InFileName     string   // map task 要读取的文件名
	TaskID         int      // map task id 对应文件 id (M个)；reduce task id(R个)
	NReduce        int      // reduce task 的数量
	LocalFileNames []string // 对于 map task，是一个 map task 产生的 R个文件名集合; 对
	/* 对于 reduce task，是多个Map task 产生的中间文件对应该 Reduce task ID 的分片集合，
            该reduce task 将 M 个文件片合并，排序，输出一个文件*/
	OutFileName string // 每个reduce task 的输出文件名
}
```


2 . Coordinator (就是 Master)结构体定义
```go
type Coordinator struct {
	// Your definitions here.
	TaskQue       chan *Task               //等待执行的task
	TaskMeta      map[int]*CoordinatorTask // Coordinator为每个task 维护一个状态信息， int 代表 task ID
	State         TaskState                // Coordinator 阶段 对应的task任务状态
	NReduce       int
	InFiles       []string   // M个文件对应 M 个 Map task
	Intermediates [][]string // [i][j]: i 表示 reduce id([0, R)), j 代表 M个 map task 的索引([0, M))。Intermediates[i] 表示 reduce task i 对应的 M个输入文件片集合
}
```

### Coordinator

1 . 启动 MakeCoordinator

```go
func MakeCoordinator(files []string, nReduce int) *Coordinator {
  c := Coordinator{
    TaskQue:       make(chan *Task, max(nReduce, len(files))),
    TaskMeta:      make(map[int]*CoordinatorTask),
    State:         Map,
    NReduce:       nReduce,
    InFiles:       files,
    Intermediates: make([][]string, nReduce),
    }
  c.createMapTask() // 为每个文件创建一个  map task
  c.server()

  // 启动一个goroutine 检查超时的任务
  go c.checkTimeOut()
  return &c
}

func (c *Coordinator) createMapTask() {
  // 根据输入文件，每个文件是一个 map task (split)
  for idx, filename := range c.InFiles {
    task := Task{
      State:      Map,
      InFileName: filename,
      TaskID:     idx, // 为每个文件 split 分配一个 task
      NReduce:    c.NReduce,
    }
    c.TaskQue <- &task
    c.TaskMeta[idx] = &CoordinatorTask{
      TaskStatus:    Idle,
      TaskReference: &task,
    }
  }
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
  rpc.Register(c)
  rpc.HandleHTTP()
  //l, e := net.Listen("tcp", ":1234")
  sockname := coordinatorSock()
  os.Remove(sockname)
  l, e := net.Listen("unix", sockname)
  if e != nil {
    log.Fatal("listen error:", e)
  }
  go http.Serve(l, nil)
}
```
  - 为每个文件 split 分配一个 map task(M)，并将这M个文件放入队列
  - 开启一个线程监听来自 worker 的 RPC请求
  - 开启一个协程进行超时检查

### Worker

1 . 启动一个 worker(死循环)
```go
func Worker(mapf func(string, string) []KeyValue,
  reducef func(string, []string) string) {

  // Your worker implementation here.
  for {
    task := callTask() // 调用（请求）任务（RPC调用）
    switch task.State {
    case Map:
      mapTask(mapf, task)
      break
    case Reduce:
      reduceTask(reducef, task)
      break
    case Wait:
      time.Sleep(time.Duration(time.Second * 10))
      break
    case Exit:
      fmt.Printf("All of tasks have been completed, nothing to do\n")
      return
    default:
      fmt.Printf("Invalid state code, try again!\n")
    }
  }
}
```   

2 . worker RPC调用
```go
// worker.go
func callTask() *Task {

  args := ExampleArgs{}
  reply := Task{}
  ok := call("Coordinator.AssignTask", &args, &reply)
  return &reply
  }
  func call(rpcname string, args interface{}, reply interface{}) bool {
  sockname := coordinatorSock()
  c, err := rpc.DialHTTP("unix", sockname)
  if err != nil {
    log.Fatal("dialing:", err)
  }
  defer c.Close()

  err = c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

// coordinator.go

// coordinator等待worker调用
func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
  mu.Lock()
  defer mu.Unlock()
  if len(c.TaskQue) > 0 {
    *reply = *<-c.TaskQue
    // 记录 task 的启动时间
    c.TaskMeta[reply.TaskID].TaskStatus = InProcess
    c.TaskMeta[reply.TaskID].StartTime = time.Now()
  } else if c.State == Exit {
    *reply = Task{State: Exit}
  } else {
    *reply = Task{State: Wait}
  }
  return nil
}
```
- 通过rpc请求调用，从MakeCoordinator队列取出一个 Task，返回 Task 信息；根据Task状态，执行不同的任务

3 . 进入 mapTask 阶段
```go
func mapTask(mapf func(string, string) []KeyValue, task *Task) {
   intermediates := []KeyValue{}

   filename := task.InFileName
   file, err := os.Open(filename)
   if err != nil {
     log.Fatalf("cannot open %v", filename)
   }
   content, err := ioutil.ReadAll(file)
   if err != nil {
     log.Fatalf("cannot read %v", filename)
   }
   file.Close()

   kva := mapf(filename, string(content))
   intermediates = append(intermediates, kva...)

   nReduce := task.NReduce

   var mapBuff map[int]ByKey
   mapBuff = make(map[int]ByKey, nReduce)

   // 切片， R份
   for _, kv := range intermediates {
     idx := ihash(kv.Key) % nReduce
     mapBuff[idx] = append(mapBuff[idx], kv)
   }

   var localFileNames []string // 每个 Map Task 产生的 R 个中间文件名称集合

   for i := 0; i < task.NReduce; i++ {
     localFileName := writeToLocalFile(task.TaskID, i, mapBuff[i])
     localFileNames = append(localFileNames, localFileName)
   }

   task.LocalFileNames = localFileNames
   taskCompleted(task)
}
```
- 所有 map 任务结束后才创建 reduce 任务
  
4 . 进入 reduceTask 阶段
```go
func reduceTask(reducef func(string, []string) string, task *Task) {
  //fmt.Printf("This is reduce task\n")
  intermediate := readFromLocalFile(task.LocalFileNames) // 合并 reduce task 的输入文件片集合
  sort.Sort(ByKey(intermediate))

  currentDir, _ := os.Getwd()
  //currentDir += "/out"
  tmpFile, err := ioutil.TempFile(currentDir, "mr-tmp-r-*")
  if err != nil {
    log.Fatal("Fail to creat temp file", err)
  }

  i := 0
  for i < len(intermediate) {
    j := i + 1
    for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
      j++
    }
    values := []string{}
    for k := i; k < j; k++ {
      values = append(values, intermediate[k].Value)
    }
    output := reducef(intermediate[i].Key, values)

    // this is the correct format for each line of Reduce output.
    fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
    i = j
  }
  tmpFile.Close()

  oname := fmt.Sprintf("mr-out-%d", task.TaskID)
  os.Rename(tmpFile.Name(), oname)
  task.OutFileName = oname
  taskCompleted(task)
}
```

## Lab 2: Raft

基于GO语言实现 Raft 共识算法库。测试指令如下：
```
# 分步测试
go test -run 2A
go test -run 2B
go test -run 2C
go test -run 2D

# 全部测试
go test -race
```
Raft 网站：https://raft.github.io/。论文中图二(如下图)是实现Raft 的关键。
![](https://images--hosting.oss-cn-chengdu.aliyuncs.com/ds/raft_summary.jpg)

### Raft 主体

个人把 Raft 主体分成两部分：Raft 结构体定义和 Raft 服务器的创建。

**Raft 结构体定义**
根据论文内容，定义 Raft 结构如下：
```go
type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []Entry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
	//appendEntriesCh chan *Entry

	// Electin state on all servers
	state         State
	heartbeatTime time.Time
	electionTime  time.Time

	// 2D
	lastIncludedIndex int // snapshot
	lastIncludedTerm  int

	applyCh   chan ApplyMsg
	applyCond *sync.Cond
}

```

**Raft 服务的创建**
每个服务器会启动 Raft 服务，有多少个服务器，即 peers 的个数，就有多少个 Raft 服务。这使得多个 Raft 服务并发运行，构成了 Raft 集群。

```go
// the service or tester wants to create a Raft server.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Entry, 0)
	rf.logAppend(Entry{index_init, term_init, nil})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.resetElectionTime()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
```

Make() 需要快速返回，因此需要启动两个长时间运行的协程 ticker() 和 applier()。

**1. ticker()**

ticker() 长时间运行，检测当前 raft 的状态。如果是 ``Follower`` 或者 ``Candidate``, 判断是否选举超时，超过选举时间就进行领导人选举；如果是``Leader``，判断心跳是否超时，心跳超时就发送心跳检测，维护自己领导人地位。
```go
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		rf.mu.Lock()
		switch rf.state {
		case Follower, Candidate:
			if time.Now().After(rf.electionTime) { // election timeout
				rf.leaderElection()
			}
		case Leader:
			if time.Now().After(rf.heartbeatTime) { // heartbeat timeout
				rf.leaderAppendEntries(true) // heartbeat check
				rf.resetHeartbeatTime()
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(gap_time) * time.Millisecond)
	}
}
```
> leaderAppendEntries() 有三个作用:
> 1. 空的 append entry, 心跳检测--Append Entries RPC
> 2. append entries 实体，这里有追加，也有强制性改写(一致性检查) -- Append Entries RPC
> 3. leader 根据条件开启一个安装快照的协程 -- Install Snapshot RPC

**2. applier()**
applier()长时间运行。提交后的 log entries 需要被应用。ApplyMsg 是关于 Raft 应用的结构体，包含 log entries 部分和 snapshot 部分。 
如果发现commitIndex大于lastApplied，将 [rf.lastApplied, rf.commitIndex] 区间的 log entries 应用到状态机，即将 ApplyMsg 提交到 raft 日志应用管道里(类似一个队列); 否则进入等待队列。
```go
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
}


func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		// All Servers 1
		if rf.commitIndex > rf.lastApplied && rf.lastLogEntry().Index > rf.lastApplied {
			rf.lastApplied++
			applied, _ := rf.transfer(rf.lastApplied)
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[applied].Command,
				CommandIndex: rf.log[applied].Index,
				CommandTerm:  rf.log[applied].Term,
			}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}
```

### Leader Election

超过选举超时时间，进行领导人选举，follower 将自己的状态变为``Candidate``，当前任期号加1，给自己投一票。然后依次遍历所有 follower, 向其他所有 follower 发送``RequestVote RPC``(go 协程，并发操作)。一旦成为 leader 后，**周期地**向其他follower 发送 ``AppendEntries RPC``(心跳检测，空的 append entry)，以维护自身 leader 地位。

注意事项：
1. 每次开始选举的时候需要重置election timeout时长，避免和其他机器相同导致一直发生脑裂
2. 如果收到term更大的RPC请求，将自己的状态置为follower，并且更新自己的term
3. 成为 leader 后，立刻开始广播心跳，让那些相同 term 的 candidate 马上变成 follower
4. 成为 leader 后，重置 matchIndex 和 nextIndex。

```go
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// rules for servers
	// all servers 2
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// request vote rpc receiver 1
	if args.Term < rf.currentTerm { // voter refuse vote
		return
	}
	// request vote rpc receiver 2
	lastEntry := rf.lastLogEntry()
	upToDate := args.LastLogTerm > lastEntry.Term ||
		(args.LastLogTerm == lastEntry.Term && args.LastLogIndex >= lastEntry.Index) // 5.4.1 选举限制
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetElectionTime()
		DPrintf("Voter [%d]: vote for candidate [%d] int term %v\n", rf.me, rf.votedFor, rf.currentTerm)
	}
}

func (rf *Raft) sendRequestVote(serverId int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[serverId].Call("Raft.RequestVote", args, reply)
	return ok
}

// ticker call leaderElection()
func (rf *Raft) leaderElection() {
	rf.currentTerm++ // follower vote for self
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTime()

	voteCnt := 1
	lastEntry := rf.lastLogEntry()
	DPrintf("Candidate[%v]: start leader election, term %d\n", rf.me, rf.currentTerm)
	args := RequestVoteArgs{
		// Your data here (2A, 2B).
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastEntry.Index,
		LastLogTerm:  lastEntry.Term,
	}

	var becomeLeader sync.Once
	for peerId, _ := range rf.peers {
		if peerId == rf.me {
			continue
		}
		go func(peerId int) { // candidate request servers to vote
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peerId, &args, &reply)
			if ok == false {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > args.Term {
				DPrintf("Candidate [%d]: server [%d] int new term %v, update over\n", rf.me, peerId, reply.Term)
				rf.setNewTerm(reply.Term)
				return
			}
			if reply.Term < args.Term {
				DPrintf("Candidate [%d]: voter [%d] has been invalid in term [%d]\n", rf.me, peerId, reply.Term)
				return
			}
			if !reply.VoteGranted { // voter 没有给 candidate 投票
				return
			}
			// voter 给 candidate 投票
			voteCnt++

			if voteCnt > len(rf.peers)/2 &&
				rf.currentTerm == args.Term &&
				rf.state == Candidate {
				// Candidate 获得大多数选票，结束选举
				becomeLeader.Do(func() {
					rf.state = Leader
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.nextIndex {
						rf.nextIndex[i] = rf.lastLogEntry().Index + 1
						rf.matchIndex[i] = 0
					}
					rf.leaderAppendEntries(true) // 成为 leader 后发送心跳，维护自己的地位
				})
			}

		}(peerId)
	}
}
```

### Append Entries

运行流程：
1. 客户端远程调用Start() 函数，向 `Leader` 发送命令，希望该命令被所有状态机执行；
2. `Leader` 先将该命令追加到自己的日志中(Leader 只有追加操作)；
3. `Leader` 并行地向其它节点发送 `AppendEntries RPC`(一致性检查，追加 or 强制改写)，等待响应；收到超过半数节点的响应，则认为新的日志记录是被提交的：
4. `Leader` 将命令传给自己的状态机，然后向客户端返回响应(apply 管道); 此外，一旦 `Leader` 知道一条记录被提交了，将在后续的 `AppendEntries RPC` 中通知已经提交记录的 `Followers`;`Follower` 将已提交的命令传给自己的状态机(apply 管道)
5. 如果 `Follower` 宕机/超时：`Leader` 将反复尝试发送 `RPC`；

```go
func (rf *Raft) appendEntriesSub(peerId int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	idx_ := rf.nextIndex[peerId] - 1
	prev_log_index, err := rf.transfer(idx_)
	if err < 0 {
		rf.mu.Unlock()
		return
	}

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.log[prev_log_index].Index,
		PrevLogTerm:  rf.log[prev_log_index].Term,
		Entries:      make([]Entry, len(rf.log[prev_log_index+1:])),
		LeaderCommit: rf.commitIndex,
	}
	copy(args.Entries, rf.log[prev_log_index+1:])
	rf.mu.Unlock()

	reply := AppendEntriesReply{}

	ok := rf.sendAppendEntries(peerId, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != args.Term || rf.state != Leader || reply.Term < rf.currentTerm {
		return
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}

	// Rules for Servers --> Leaders: 3.1
	if reply.Success {
		rf.matchIndex[peerId] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peerId] = args.PrevLogIndex + len(args.Entries) + 1
		// follower 追加 entry 成功
		rf.toCommit()
		return
	}

	if reply.XTerm == -1 {
		rf.nextIndex[peerId] -= reply.XLen
	} else if reply.XTerm >= 0 {
		termNotExit := true
		for index := rf.nextIndex[peerId] - 1; index >= 1; index-- {
			entry, err := rf.getEntry(index)
			if err < 0 {
				continue
			}
			if entry.Term > reply.XTerm {
				continue
			}
			if entry.Term == reply.XTerm {
				rf.nextIndex[peerId] = index + 1
				termNotExit = false
				break
			}
			if entry.Term < reply.XTerm {
				break
			}
		}
		if termNotExit {
			rf.nextIndex[peerId] = reply.XIndex
		}
	} else {
		rf.nextIndex[peerId] = reply.XIndex
	}

	if rf.nextIndex[peerId] < 1 {
		rf.nextIndex[peerId] = 1
	}
}

// ticker() call leaderAppendEntries() to send a heartbeat
// start() call leaderAppendEntries() ro append entries
func (rf *Raft) leaderAppendEntries(heartbeat bool) { // true: leader heartbeat check; false: leader append entries
	for peerId, _ := range rf.peers {
		if peerId == rf.me {
			continue
		}
		if rf.nextIndex[peerId]-1 < rf.frontLogEntry().Index {
			go rf.leaderInstallSnapshot(peerId)
		} else if rf.lastLogEntry().Index >= rf.nextIndex[peerId] || heartbeat { // Rules for Servers --> Leaders: 3
			go rf.appendEntriesSub(peerId)
		}
	}
}
```

### Persistence 
在Raft论文中需要持久化的字段只有3个 currentTerm，voteFor，log[]。
```go
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() { // persiter raftstate
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)

}
func (rf *Raft) persistSnapshot(snapshot []byte) { // persiter raftstate and snapshot
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(raftstate []byte) {
	if raftstate == nil || len(raftstate) < 1 {
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(raftstate)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logEntries []Entry

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logEntries) != nil {
		log.Fatal("Failed to read persist!\n")
		return
	}
	rf.log = make([]Entry, len(logEntries))
	copy(rf.log, logEntries)
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.lastApplied = rf.frontLogEntry().Index
	rf.commitIndex = rf.frontLogEntry().Index
}
```

### Install Snapshot

当Leader发现自己已经找不到nextIndex[i]的日志的时候，就需要直接发送自己快照给Follower，让Follower直接同步快照。因为快照一定是apply好的日志，所以一定是正确的日志。

Follower收到快照请求后，将快照提交到 ApplyMsg 管道，由 CondInstallSnapshot () 决定是否持久化快照， CondInstallSnapshot () 在 raft 的上层服务调用

```go
/*************** InstallSnapshot handler ***************/
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, vote_nil
		rf.persist()
		rf.state = Follower
	}

	if rf.state != Follower {
		rf.state = Follower
	}

	reply.Term = rf.currentTerm
	rf.resetElectionTime()

	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) leaderInstallSnapshot(peerId int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.frontLogEntry().Index,
		LastIncludedTerm:  rf.frontLogEntry().Term,
	}

	args.Data = make([]byte, rf.persister.SnapshotSize())
	copy(args.Data, rf.persister.ReadSnapshot())
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}

	ok := rf.sendInstallSnapshot(peerId, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != args.Term || rf.state != Leader || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = reply.Term, vote_nil
		rf.persist()
		rf.state = Follower
		return
	}
	rf.nextIndex[peerId] = args.LastIncludedIndex + 1
}
// ticker() call leaderAppendEntries() to send a heartbeat
// start() call leaderAppendEntries() ro append entries
func (rf *Raft) leaderAppendEntries(heartbeat bool) { // true: leader heartbeat check; false: leader append entries
	for peerId, _ := range rf.peers {
		if peerId == rf.me {
			continue
		}
		if rf.nextIndex[peerId]-1 < rf.frontLogEntry().Index {
			go rf.leaderInstallSnapshot(peerId)
		} else if rf.lastLogEntry().Index >= rf.nextIndex[peerId] || heartbeat { // Rules for Servers --> Leaders: 3
			go rf.appendEntriesSub(peerId)
		}
	}
}
```

### 被上层服务调用的API

1 .客户端远程调用 Start(), 让leader 将指令内容追加到 log 中，然后同步到其他 Follower
```go
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	index := rf.lastLogEntry().Index + 1
	term := rf.currentTerm
	entry := Entry{
		Command: command,
		Index:   index,
		Term:    term,
	}

	rf.logAppend(entry)
	rf.persist()
	rf.leaderAppendEntries(false) // append entries

	return index, term, true
}
```

2 .将[:index]范围的 log 删除，只保留尾部 log; 将上层的snapshot持久化。
```go
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	//A snapshot of logs int range [:index] is generated. Raft truncates these logs[index:] and saves only the log tails.

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// refuse to install a snapshot
	if rf.frontLogEntry().Index >= index {
		return
	}

	idx, err := rf.transfer(index)
	if err < 0 {
		idx = len(rf.log) - 1
	}

	rf.log = rf.log[idx:]
	rf.log[0].Command = nil
	rf.persistSnapshot(snapshot) // save snapshot data
}
```

3 . 满足 install snapshot 的条件，持久化快照
```go
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	if lastIncludedIndex > rf.lastLogEntry().Index {
		rf.log = make([]Entry, 1)
	} else {
		// in range, ignore out of range error
		idx, _ := rf.transfer(lastIncludedIndex)
		rf.log = rf.log[idx:]
	}
	// dummy node
	rf.log[0].Term = lastIncludedTerm
	rf.log[0].Index = lastIncludedIndex
	rf.log[0].Command = nil

	rf.persistSnapshot(snapshot)

	// reset commit
	if lastIncludedIndex > rf.lastApplied {
		rf.lastApplied = lastIncludedIndex
	}
	if lastIncludedIndex > rf.commitIndex {
		rf.commitIndex = lastIncludedIndex
	}

	return true
}
```

## Lab 3: Fault-tolerant Key/Value Service

在之前 Raft 库的基础上构建能够容错的KV存储服务，没有实现分片。KV存储服务包含多个服务器，每个服务器包含 KV 数据库和Raft节点。服务器之间只通过raft node进行通信，所有的 raft 节点构成一个 raft 服务。

![](https://images--hosting.oss-cn-chengdu.aliyuncs.com/ds/kvraft.jpg)

### Client
存在多个客户端，与多个服务器中的 Leader 交互
Client 的结构
```go
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu       sync.Mutex
	leaderId int
	clientId int64
	seqNum   int64 // Client request sequence number, is incremental
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
```

客户端操作包括 Get(), Put(), Append(), 将这三个操作合并，即这个方法使用公共的 RPC 调用(``sendCommand()``)，远程调用 ``Command()`` 函数。客户端调用sendCommand()，不断向 servers 集群请求，直到找到leader，等待返回。
```go
unc (ck *Clerk) sendComand(key string, value string, opType OpType) string { // Get(), Put(), Append()
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
```



### Servers
**启动KVServer**
1. 做一些初始化工作
2. 开启底层 Raft node
3. 启动一个 applier() 协程，即开启一个 Applyloop, 监控 raft 的 applyCh。
4. 开启一个快照协程，满足条件，压缩 log entries 以及持久化snapshot, snapshot 主要是KV数据库
   
```go
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
```


**Leader 处理客户端请求**
1. Server（Leader）收到Client的Request
2. 通过raft.Start() 提交Op(Entry)给raft Leader, Leader会追加这个Entry，同步给其他 Follower。然后开启的 Wait Channel 等待 Applyloop 返回给自己这个Req结果。
3. Wait Channel 收到Req结果就进行RPC返回

```go
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
```

**ApplyLoop**
1. 每个Server会在单独的协程 ApplyLoop 中等待Raft不断到来的ApplyCh Msg.
2. 如果是CommandValid，则
	1. 执行这个Op(GET全部执行，重复的PUT, APPEND不执行)
	2. Apply Loop 根据Op中的信息将执行结果返回给Wait Channel , 注意，只有Leader中才有 Wait Channel 在等结果
3. 如果SnapshotValid(这里是Follower, 因为只有Follower 才将snapshot 信息提交到ApplyCh), 则
   1. 判断判定snapshot安装条件，满足条件则：初始化安装快照的条件数据，持久化 Snapshot

```go
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
```
**Snapshot & Raftstate**

先说明概念：RafeSate(currentTerm, voteFor, logEntries)。 Snapshot (Server 维护的KeyValue数据库，其实就是内存中一个map, 和Server维护的关于每个Client Request的状态）。

对于Leader：

1. Raft Apply new LogEntries( CommandValid : true ), 这一举动说明 RaftStateSize 在增加
2. Leader执行操作，并根据 RaftStateSize 和阈值maxraftestatesize判断是否需要命令Raft进行Snapshot
3. 如果需要，则将自身的 KV DB，RequestID等信息制作成snapshot, 并调用Leader Raft peer node 的 Snapshot () 接口
4. Leader 安装Snapshot , 这分为三部分，<u>压缩log Entries[], SnapShot 通过Persister进行持久化存储，在Appendentries中将本次的SnapShot信息发送给落后的Follower</u>
5. 返回执行结果给 Wait Channel

对于Follower :  
1. InstallSnapshot 处理Leader的RPC请求，获取 snapshot 数据，通过ApplyCh上报给Server (SnapshotValid: true)
2. Applyloop 收到请求，调用CondInstallSnapshot() 来询问是否可以安装snapshot
3. CondInstallSnapshot() 判定snapshot安装条件，满足，则初始化安装快照的条件数据，持久化 Snapshot

```go
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
```

**参考：**
- https://cloud.tencent.com/developer/article/2134849
- https://blog.csdn.net/qq_40443651/article/details/117172246
- https://zhuanlan.zhihu.com/p/463146084


## Lab 4: Sharded Key/Value Service

在`Lab2`和`Lab3`，实现了基于单`RAFT`（单一集群）的多节点间数据一致性、支持增删查改、数据同步和快照保存的`KV`数据库。但忽视了集群负载问题，随着数据增长到一定程度时，所有的数据请求都集中在`leader`上，增加集群压力，延长请求响应时。

`Lab4`的内容就是将数据按照某种方式分开存储到不同的`RAFT`集群(`Group`)上，分片(`shard`)的策略有很多，比如：所有以`a`开头的键是一个分片，所有以`b`开头的键是一个分片。保证相应数据请求引流到对应的集群，降低单一集群的压力，提供更为高效、更为健壮的服务。

​ 整体架构如下图：
![](https://images--hosting.oss-cn-chengdu.aliyuncs.com/ds/shardkv.png)

1. 具体的`lab4`要实现一个 “分布式的，支持 `multi-raft`分片，能够根据配置同步迁移数据(出于负载均衡和接入退出成员原因)的，Key-Value数据库存储服务”。
2. `shard`表示互不相交并且组成完整数据库的每一个数据库子集。`group`表示`server`的集合，包含一个或多个`server`。一个`shard`只可属于一个`group`，一个`group`可包含(管理)多个`shard`。
3. `lab4A`实现`ShardCtrler`服务，作用：提供高可用的集群配置管理服务，实现分片的负载均衡，并尽可能少地移动分片。记录了每组（`Group`）`ShardKVServer`的集群信息和每个分片（`shard`）服务于哪组（`Group`）`ShardKVServer`。具体实现通过`Raft`维护 一个`Configs`数组，单个`config`具体内容如下：
    - **`Num`**：`config number`，`Num=0`表示`configuration`无效，**边界条件**。
    - **`Shards`**：`shard -> gid`，分片位置信息，`Shards[3]=2`，说明分片序号为`3`的分片负贵的集群是`Group2`（`gid=2`）
    - **`Groups`**：`gid -> servers[]`,集群成员信息，`Group[3]=['server1','server2']`,说明`gid = 3`的集群`Group3`包含两台名称为`server1 & server2`的机器
4. `lab4B`实现`ShardKVServer`服务，`ShardKVServer`则需要实现所有分片的读写任务，相比于MIT 6.824 Lab3 RaftKV 的提供基础的**读写服务**，还需要功能和难点为**配置更新，分片数据迁移，分片数据清理，空日志检测**。

### ShardCtrler
`lab4A`实现`ShardCtrler`服务，作用：提供高可用的集群配置管理服务，记录了每组（`Group`）`ShardKVServer`的集群信息和每个分片（`shard`）服务于哪组（`Group`）`ShardKVServer`。具体实现通过`Raft`维护 一个`Configs`数组。

代码实现基本与`Lab3` 类似，可以直接照抄复制 MIT 6.824 Lab3 RaftKV]，且不需要实现快照服务，具体根据实现 `Join, Leave, Move, Query` 服务。

1. `Query`： 查询最新的`Config`信息。
2. `Move` 将数据库子集`Shard`分配给`GID`的`Group`。
3. `Join`： 新加入的`Group`信息，要求在每一个`group`平衡分布`shard`，即任意两个`group`之间的`shard`数目相差不能为`1`，具体实现每一次找出含有`shard`数目最多的和最少的，最多的给最少的一个，循环直到满足条件为止。坑为：`GID = 0` 是无效配置，一开始所有分片分配给`GID=0`，需要优先分配；`map`的迭代时无序的，不确定顺序的话，同一个命令在不同节点上计算出来的新配置不一致，按`sort`排序之后遍历即可。且 `map` 是引用对象，需要用深拷贝做复制。
4. `Leave`： 移除`Group`，同样别忘记实现均衡，将移除的`Group`的`shard`每一次分配给数目最小的`Group`就行，如果全部删除，别忘记将`shard`置为无效的0。

> 在Join, Leave之后，需要根据分片分布情况进行负载均衡（reBalance).其实就是让每个集群负责的分片数量大致相同，并且进行尽量少的数据迁移。
 
关键代码实现：
```go
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
```

### ShardKV
上面的实验`ShardCtrler` 集群组实现了配置更新，分片均匀分配等任务，`ShardKVServer`则需要承载所有分片的读写任务，相比于 MIT 6.824 Lab3 RaftKV 的提供基础的读写服务，还需要功能为**配置更新，分片数据迁移，分片数据清理，空日志检测**。

#### 客户端Clerk
主要实现为请求逻辑：

1. 使用`key2shard()`去找到一个 `key` 对应哪个分片`Shard`；
2. 根据`Shard`从当前配置`config`中获取的 `gid`；
3. 根据`gid`从当前配置`config`中获取 `group` 信息；
4. 在`group`循环查找`leaderId`，直到返回请求成功、`ErrWrongGroup`或整个 group 都遍历请求过；
5. `Query` 最新的配置，回到**步骤1**循环重复；

```go
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.seqId = 0
	ck.config = ck.sm.Query(-1)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) sendCmd(key string, value string, OpType OpType) string {
	ck.seqId += 1
	args := CommandArgs{
		SeqNum:   ck.seqId,
		ClientId: ck.clientId,
		Key:      key,
		Value:    value,
		OpType:   OpType,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply CommandReply
				ok := srv.Call("ShardKV.Command", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

func (ck *Clerk) Get(key string) string {
	return ck.sendCmd(key, "", OpGet)
}

func (ck *Clerk) Put(key string, value string) {
	ck.sendCmd(key, value, OpPut)
}

func (ck *Clerk) Append(key string, value string) {
	ck.sendCmd(key, value, OpAppend)
}
```

#### 服务端Server

首先明确整体系统的运行方式：

- 客户端首先和`ShardCtrler`交互，获取最新的配置，根据最新配置找到对应`key`的`shard`，请求该`shard`的`group`。
- 服务端`ShardKVServer`会创建多个 `raft` 组来承载所有分片的读写任务。
- 服务端`ShardKVServer`需要定期和`ShardCtrler`交互，保证更新到最新配置(`monitor`)。
- 服务端`ShardKVServer`需要根据最新配置完成**配置更新，分片数据迁移，分片数据清理，空日志检测**等功能。

##### ShardKV 主体
ShardKVServer 给出结构体，相比于MIT 6.824 Lab3 RaftKV的多了currentConfig和lastConfig数据，这样其他协程便能够通过其计算需要需要向谁拉取分片或者需要让谁去删分片。
```go
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
```

启动了六个协程：apply 协程，配置更新协程，数据迁移协程，数据清理协程，空日志检测协程来实现功能。四个协程都需要 leader 来执行

```go
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
	go kv.applier()
	go kv.snapshoter() // leader snapshot --> 同步到其他fallower

	go kv.monitor(kv.configurationAction, CofigurationMonitorTimeout)
	go kv.monitor(kv.migrationAction, MigrationMonitorTimeout)
	go kv.monitor(kv.gcAction, GCMonitorTimeout)
	go kv.monitor(kv.checkEntryInCurrentTermAction, CheckEntryMonitorTimeout)

	return kv
}
```

需要维护分片`Shard`的状态变量来完成该实验，原因如下：

1. 这样可以防止分片`Shard`中间状态被覆盖，从而导致任务被丢弃。只有所有分片`Shard`的状态都为默认状态才能拉取最新配置。
2. 实验`challenge2`限制， `challenge2` 不仅要求 `apply` 协程不被阻塞，还要求配置的更新和分片的状态变化彼此独立。即需要不同 `raft` 组所属的分片数据独立起来，分别提交多条 `raft` 日志来维护状态，因此需要维护状态变量。
3. **必须使用单独的协程异步根据分片的状态来进行分片的迁移和清理等任务**。如果不采用上述方案，`apply` 协程更新配置的时候由 `leader` 异步启动对应的协程，让其独立的根据 `raft` 组为粒度拉取数据？让设想这样一个场景：leader apply 了新配置后便挂了，然后此时 follower 也 apply 了该配置但并不会启动该任务，在该 `raft` 组的新 `leader` 选出来后，该任务已经无法被执行了。所有apply配置的时候只应该更新`shard` 的状态。

每个分片共有 4 种状态：

- `Serving`：分片的默认状态，如果当前 `raft` 组在当前 `config` 下负责管理此分片，则该分片可以提供读写服务，否则该分片暂不可以提供读写服务，但不会阻塞配置更新协程拉取新配置。
- `Pulling`：表示当前 `raft` 组在当前 `config` 下负责管理此分片，暂不可以提供读写服务，需要当前 `raft` 组从上一个配置该分片所属 `raft` 组拉数据过来之后才可以提供读写服务，系统会有一个分片迁移协程检测所有分片的 `Pulling` 状态，接着以 `raft` 组为单位去对应 `raft` 组拉取数据，接着尝试重放该分片的所有数据到本地并将分片状态置为 `Serving`，以继续提供服务。
- `BePulling`：表示当前 `raft` 组在当前 `config` 下不负责管理此分片，不可以提供读写服务，但当前 `raft` 组在上一个 `config` 时复制管理此分片，因此当前 `config` 下负责管理此分片的 `raft` 组拉取完数据后会向本 `raft` 组发送分片清理的 `rpc`，接着本 `raft` 组将数据清空并重置为 `serving` 状态即可。
- `GCing`：表示当前 `raft` 组在当前 `config` 下负责管理此分片，可以提供读写服务，但需要清理掉上一个配置该分片所属 `raft` 组的数据。系统会有一个分片清理协程检测所有分片的 `GCing` 状态，接着以 `raft` 组为单位去对应 `raft` 组删除数据，一旦远程 `raft` 组删除数据成功，则本地会尝试将相关分片的状态置为 `Serving`。

```go
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
```

##### 日志类型

在 `lab3` 中，客户端的请求会被包装成一个 `Op` 传给 `Raft` 层，则在 `lab4` 中，不难想到，`Servers` 之间的交互，也可以看做是包装成 `Op` 传给 `Raft` 层；定义了五种类型的日志：

- `Operation`：客户端传来的读写操作日志，有 `Put`，`Get`，`Append` 等请求。
    
- `Configuration`：配置更新日志，包含一个配置。
    
- `InsertShards`：分片更新日志，包含至少一个分片的数据和配置版本。
    
- `DeleteShards`：分片删除日志，包含至少一个分片的 id 和配置版本。
    
- `EmptyEntry`：空日志，`Data` 为空，使得状态机达到最新。

##### 日志应用
将提交的日志的应用到状态机

```go
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
```

##### 读写服务
读写操作的基本逻辑相比于MIT 6.824 Lab3 RaftKV基本一致，需要增加分片状态判断。根据上述定义，分片的状态为 `Serving` 或 `GCing`，当前 `raft` 组在当前 `config` 下负责管理此分片，本 `raft` 组才可以为该分片提供读写服务，否则返回 `ErrWrongGroup` 让客户端重新拉取最新的 `config` 并重试即可。

`canServe` 的判断需要在向 `raft` 提交前和 `apply` 时都检测一遍以保证正确性并尽可能提升性能。

```go
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
```

##### 配置更新

配置更新协程负责定时检测所有分片的状态，**一旦存在至少一个分片的状态不为默认状态**，则预示其他协程仍然还没有完成任务，那么此时需要阻塞新配置的拉取和提交。

在 `apply` 配置更新日志时需要保证幂等性：

- 不同版本的配置更新日志：`apply` 时仅可逐步递增的去更新配置，否则返回失败。
- 相同版本的配置更新日志：由于配置更新日志仅由配置更新协程提交，而配置更新协程只有检测到比本地更大地配置时才会提交配置更新日志，所以该情形不会出现。

```go
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

// Configuration Update
func (kv *ShardKV) applyConfiguration(nextConfig *shardctrler.Config) *OpReply {
	if nextConfig.Num == kv.currConfig.Num+1 {
		kv.updateShardStatus(nextConfig)
		kv.lastConfig = shardctrler.Config{Num: kv.currConfig.Num, Shards: kv.currConfig.Shards, Groups: shardctrler.DeepCopy(kv.currConfig.Groups)}
		kv.currConfig = shardctrler.Config{Num: nextConfig.Num, Shards: nextConfig.Shards, Groups: shardctrler.DeepCopy(nextConfig.Groups)}
		return &OpReply{OK, ""}
	}
	return &OpReply{ErrOutDated, ""}
}
```

##### 分片迁移
分片迁移协程负责定时检测分片的 `Pulling` 状态，利用 `lastConfig` 计算出对应 `raft` 组的 `gid` 和要拉取的分片，然后并行地去拉取数据。

注意这里使用了 `waitGroup` 来保证所有独立地任务完成后才会进行下一次任务。此外 `wg.Wait()` 一定要在释放读锁之后，否则无法满足 `challenge2` 的要求。

在拉取分片的 `handler` 中，首先仅可由 `leader` 处理该请求，其次如果发现请求中的配置版本大于本地的版本，那说明请求拉取的是未来的数据，则返回 `ErrNotReady` 让其稍后重试，否则将分片数据和去重表都深度拷贝到 `response` 即可。

在 `apply` 分片更新日志时需要保证幂等性：

- 不同版本的配置更新日志：仅可执行与当前配置版本相同地分片更新日志，否则返回 ErrOutDated。
- 相同版本的配置更新日志：仅在对应分片状态为 `Pulling` 时为第一次应用，此时覆盖状态机即可并修改状态为 `GCing`，以让分片清理协程检测到 `GCing` 状态并尝试删除远端的分片。否则说明已经应用过，直接 `break` 即可。
```go
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
```

##### 分片清理
分片清理协程负责定时检测分片的 `GCing` 状态，利用 `lastConfig` 计算出对应 `raft` 组的 `gid` 和要拉取的分片，然后并行地去删除分片。

注意这里使用了 `waitGroup` 来保证所有独立地任务完成后才会进行下一次任务。此外 `wg.Wait()` 一定要在释放读锁之后，否则无法满足 `challenge2` 的要求。

在删除分片的 `handler` 中，首先仅可由 `leader` 处理该请求，其次如果发现请求中的配置版本小于本地的版本，那说明该请求已经执行过，否则本地的 `config` 也无法增大，此时直接返回 OK 即可，否则在本地提交一个删除分片的日志。

在 `apply` 分片删除日志时需要保证幂等性：

- 不同版本的配置更新日志：仅可执行与当前配置版本相同地分片删除日志，否则已经删除过，直接返回 OK 即可。
- 相同版本的配置更新日志：如果分片状态为 `GCing`，说明是本 `raft` 组已成功删除远端 `raft` 组的数据，现需要更新分片状态为默认状态以支持配置的进一步更新；否则如果分片状态为 `BePulling`，则说明本 `raft` 组第一次删除该分片的数据，此时直接重置分片即可。否则说明该请求已经应用过，直接 `break` 返回 OK 即可。

```go
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
```

##### 空日志检测
分片清理协程负责定时检测 `raft` 层的 `leader` 是否拥有当前 `term` 的日志，如果没有则提交一条空日志，这使得新 `leader` 的状态机能够迅速达到最新状态，从而避免多 `raft` 组间的活锁状态。

```go
func (kv *ShardKV) checkEntryInCurrentTermAction() {
	if !kv.rf.HasLogInCurrentTerm() {
		cmd := Command{EmptyEntry, nil}
		kv.Execute(cmd, &OpReply{})
	}
}

// Empty entry
func (kv *ShardKV) applyEmptyEntry() *OpReply {
	return &OpReply{OK, ""}
}
```

**参考：**
- https://blog.csdn.net/qq_40443651/article/details/118034894
- https://www.cnblogs.com/pxlsdz/p/15685837.html
- https://zhuanlan.zhihu.com/p/464097239

