package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

/*************** State ***************/
type State string

const (
	Follower  State = "follower"
	Candidate       = "candidate"
	Leader          = "leader"
)

/*************** Consant ***************/
const (
	// election
	vote_nil int = -1
	// appendEntries
	index_init int = 0
	term_init  int = -1
	// ticker
	gap_time            int = 3
	election_base_time  int = 300
	election_range_time int = 100
	heartbeat_time      int = 50
)

/*************** Raft structure ***************/
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

/*************** Interface ***************/
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) { // kvraft call it, another package
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	term := rf.currentTerm
	isleader := rf.state == Leader
	return term, isleader
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
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

/*************** Persist ***************/
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

func (rf *Raft) RaftPersistSize() int { // kvraft call it, another package
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

/*************** Ticker ***************/
func (rf *Raft) resetElectionTime() {
	sleep_time := rand.Intn(election_range_time) + election_base_time
	rf.electionTime = time.Now().Add(time.Duration(sleep_time) * time.Millisecond)
}

func (rf *Raft) resetHeartbeatTime() {
	rf.heartbeatTime = time.Now().Add(time.Duration(heartbeat_time) * time.Millisecond)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
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

/*************** Apply log ***************/
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

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
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
