package raft

import "fmt"

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	entry := LogEntry{command, rf.currentTerm}
	index := rf.log.lastIndex() + 1
	rf.log.append(entry)
	fmt.Printf("%v 在 %v 号添加了日志 %v\n", rf.me, index, entry)

	rf.sendAppendsL(false)

	return index, rf.currentTerm, true
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {

		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{}
			applyMsg.CommandValid = true
			applyMsg.CommandIndex = rf.lastApplied
			applyMsg.Command = rf.log.log[rf.lastApplied].Command
			fmt.Printf("%v 把日志 %v 写进了管道\n", rf.me, rf.log.log[rf.lastApplied])
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

func (rf *Raft) signalApplierL() {
	rf.applyCond.Broadcast()
}
