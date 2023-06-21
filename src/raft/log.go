package raft

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

func (rf *Raft) logLen() int {
	return len(rf.log)
}

func (rf *Raft) lastLogEntry() *Entry {
	return &rf.log[rf.logLen()-1]
}

func (rf *Raft) frontLogEntry() *Entry {
	return &rf.log[0]
}

func (rf *Raft) logAppend(entries ...Entry) {
	rf.log = append(rf.log, entries...)
}

func (rf *Raft) transfer(index int) (int, int) {
	begin := rf.frontLogEntry().Index
	end := rf.lastLogEntry().Index
	if index < begin || index > end {
		return 0, -1
	}
	return index - begin, 0
}

func (rf *Raft) getEntry(index int) (Entry, int) {
	begin := rf.frontLogEntry().Index
	end := rf.lastLogEntry().Index
	if index < begin || index > end {
		return Entry{index_init, term_init, nil}, -1
	}
	return rf.log[index-begin], 0
}

func (rf *Raft) HasLogInCurrentTerm() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := len(rf.log) - 1; i > 0; i-- {
		if rf.log[i].Term > rf.currentTerm {
			continue
		}
		if rf.log[i].Term == rf.currentTerm {
			return true
		}
		if rf.log[i].Term < rf.currentTerm {
			break
		}
	}
	return false
}
