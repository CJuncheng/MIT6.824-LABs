package raft

import (
	"sync"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) setNewTerm(term int) {
	if term > rf.currentTerm || rf.currentTerm == 0 {
		rf.state = Follower
		rf.currentTerm = term
		rf.votedFor = vote_nil
		DPrintf("Voter[%d]: set term %v\n", rf.me, rf.currentTerm)
		rf.persist()
	}
}

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

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in 6.824/labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
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
