package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int // leader’s commitIndex
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	//Conflict bool
	XTerm  int
	XIndex int
	XLen   int
}

// handler need to require lock
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer rf.persist()
	// follower 收到 leader 的 AppendEntries RPC 调用, follower 动作
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm { // AppendEntries RPC 1
		return
	}

	if args.Term > rf.currentTerm { // leader 任期大于 follower 的任期, Rules for Servers --> All Servers: 2
		rf.setNewTerm(args.Term)
		return
	}

	if rf.state == Candidate { // Rules for Servers --> Candidates: 3
		rf.state = Follower
	}

	rf.resetElectionTime() //  prevent election timeouts (§5.2)

	if args.PrevLogIndex < rf.frontLogEntry().Index {
		reply.Success = false
		reply.XTerm = -2
		reply.XIndex = rf.frontLogEntry().Index + 1
		return
	}

	if rf.lastLogEntry().Index < args.PrevLogIndex { // AppendEntries RPC 2
		//reply.Conflict = true
		reply.Success = false
		reply.XTerm = -1
		reply.XLen = args.PrevLogIndex - rf.lastLogEntry().Index
		return
	}

	prev_log_index, err := rf.transfer(args.PrevLogIndex)
	if err < 0 {
		return
	}

	if rf.log[prev_log_index].Term != args.PrevLogTerm {
		//reply.Conflict = true
		reply.Success = false
		reply.XTerm = rf.log[prev_log_index].Term
		reply.XIndex = args.PrevLogIndex

		for index := prev_log_index; index >= 1; index-- {
			if rf.log[index-1].Term != reply.XTerm {
				reply.XIndex = index
				break
			}
		}
		return
	}

	if args.Entries != nil && len(args.Entries) != 0 {
		for i, entry := range args.Entries { // conflict check
			entry_rf, err := rf.getEntry(i + args.PrevLogIndex + 1)
			if err < 0 || entry_rf.Term != entry.Term { // is conflict
				rf.log = rf.log[:prev_log_index+1]
				entries := make([]Entry, len(args.Entries))
				copy(entries, args.Entries)
				rf.logAppend(entries...)
			}
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogEntry().Index)
		rf.apply() // follower 唤醒 wait 队列的其他协程
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) toCommit() {
	// Rules for Servers --> leaders 4 : leader commit rule
	if rf.commitIndex >= rf.lastLogEntry().Index {
		return
	}

	for i := rf.lastLogEntry().Index; i > rf.commitIndex; i-- {
		entry, err := rf.getEntry(i)
		if err < 0 {
			continue
		}
		if entry.Term != rf.currentTerm {
			return
		}

		cnt := 1
		for j, match := range rf.matchIndex {
			if j != rf.me && match >= i {
				cnt++
			}
			if cnt > len(rf.peers)/2 {
				rf.commitIndex = i
				rf.apply()
				return
			}
		}
	}
}

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
