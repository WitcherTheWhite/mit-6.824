package raft

// AppendEntriesArgs
// AppendEntries请求参数
type AppendEntriesArgs struct {
	Term         int     // leader任期
	LeaderId     int     // leaderID
	PrevLogIndex int     // entries之前的最后一条log
	PrevLogTerm  int     // PrevLogIndex的term
	Entries      []Entry // leader要复制到其他server的log
	LeaderCommit int     // leader的commitIndex
}

// AppendEntriesReply
// AppendEntries响应参数
type AppendEntriesReply struct {
	Term       int  // 任期
	Success    bool // 是否成功
	Conflicted int  // 冲突日志index
}

// AppendEntries rpc handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.electionTimeReset()
	reply.Conflicted = -1
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	// rule 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	reply.Term = rf.currentTerm
	// rule 2
	if rf.log.lastIndex() < args.PrevLogIndex || rf.log.getTermOfIndex(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		if rf.log.lastIndex() < args.PrevLogIndex {
			reply.Conflicted = rf.log.lastIndex()
		}
		return
	}

	//rule 3 and 4
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if rf.log.lastIndex() >= index {
			if rf.log.getTermOfIndex(index) == entry.Term {
				continue
			}
			rf.log.Entries = rf.log.getSliceTo(index - 1)
			rf.persist()
		}
		rf.log.append(entry)
		rf.persist()
		DPrintf("%v 把日志 %v 加入了\n", rf.me, entry)
	}

	// rule 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastIndex())
		rf.signalApplierL()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) advanceCommitL() {
	start := rf.commitIndex + 1
	for index := start; index <= rf.log.lastIndex(); index++ {
		if rf.log.getTermOfIndex(index) != rf.currentTerm {
			continue
		}

		n := 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.matchIndex[i] >= index {
				n++
			}
		}
		if n > len(rf.peers)/2 && n > 1 {
			rf.commitIndex = index
		}
	}
	rf.signalApplierL()
}

func (rf *Raft) processAppendReplyTermL(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Success {
		newnext := args.PrevLogIndex + len(args.Entries) + 1
		newmatch := args.PrevLogIndex + len(args.Entries)
		if newnext > rf.nextIndex[peer] {
			rf.nextIndex[peer] = newnext
		}
		if newmatch > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newmatch
		}
	} else if reply.Conflicted >= 0 {
		rf.nextIndex[peer] = reply.Conflicted + 1
		DPrintf("%v 触发了快速回退\n", rf.me)
		rf.sendAppendL(peer, false)
	} else if rf.nextIndex[peer] > 1 {
		rf.nextIndex[peer]--
		DPrintf("%v 日志不一致 nextindex回退一步\n", rf.me)
		rf.sendAppendL(peer, false)
	}
	rf.advanceCommitL()
}

// 对AppendEntries响应的处理
func (rf *Raft) processAppendReplyL(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
	} else if args.Term == rf.currentTerm {
		rf.processAppendReplyTermL(peer, args, reply)
	}
}

func (rf *Raft) sendAppendL(peer int, heartbeat bool) {
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex < 0 {
		prevLogIndex = 0
	}
	if prevLogIndex > rf.log.lastIndex() {
		prevLogIndex = rf.log.lastIndex()
	}

	entries := make([]Entry, rf.log.lastIndex()-prevLogIndex)
	copy(entries, rf.log.getSliceFrom(prevLogIndex+1))
	args := AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
		prevLogIndex,
		rf.log.getTermOfIndex(prevLogIndex),
		entries,
		rf.commitIndex,
	}
	go func() {
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, &args, &reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.processAppendReplyL(peer, &args, &reply)
		}
	}()
}

// leader发送心跳或者追加日志
func (rf *Raft) sendAppendsL(heartbeat bool) {
	for i := range rf.peers {
		if i != rf.me {
			if rf.log.lastIndex() >= rf.nextIndex[i] || heartbeat {
				rf.sendAppendL(i, heartbeat)
			}
		}
	}
}
