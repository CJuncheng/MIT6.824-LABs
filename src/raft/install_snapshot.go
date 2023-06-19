package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	//Offset            int
	Data []byte
	//Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

/*************** Interface ***************/
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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
