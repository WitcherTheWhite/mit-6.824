package raft

import (
	"fmt"
	"math/rand"
	"time"
)

const HEART_BEAT_INTERVALS = time.Millisecond * 100 // leader发送心跳频率
const ELECTION_TIME_OUT = HEART_BEAT_INTERVALS * 2  // 选举超时时间

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的任期
	CandidateId  int // 请求选票候选人的id
	LastLogIndex int // 候选人最后log entry的序号
	LastLogTerm  int // 候选人最后log entry的任期
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 候选人现在的任期，用于更新
	VoteGranted bool // 值为true说明收到了选票
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果请求任期大于当前任期，成为follower
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	myIndex := rf.log.lastIndex()
	myTerm := rf.log.getLastLogTerm()
	uptodate := args.LastLogTerm > myTerm || (args.LastLogTerm == myTerm && args.LastLogIndex >= myIndex)

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && uptodate {
		fmt.Printf("%v 投票给了 %v\n", rf.me, args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.electionTimeReset()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// 对于所有server，如果在rpc中看到任期大于当前任期，更新当前任期并转换为follower，重置投票权
//
func (rf *Raft) convertToFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.state = Follower
	rf.votedFor = -1
	rf.persist()
}

//
// candidate变为leader
//
func (rf *Raft) becomeLeaderL() {
	rf.state = Leader

	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.log.lastIndex() + 1
	}

	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.matchIndex[i] = 0
	}
}

//
// candidate请求选票，如果获得一半以上选票并且依然是candidate，则成为leader并发送一次心跳
//
func (rf *Raft) requestVote(peer int, args *RequestVoteArgs, vote *int) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(peer, args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.convertToFollower(reply.Term)
			return
		}
		if reply.VoteGranted {
			*vote++
			if *vote > len(rf.peers)/2 {
				if rf.state == Candidate {
					fmt.Printf("%v 在任期 %v 成为了leader\n", rf.me, rf.currentTerm)
					rf.becomeLeaderL()
					rf.sendAppendsL(true)
				}
			}
		}
	}
}

//
// candidate向其他所有server发起投票请求
//
func (rf *Raft) requestVotesL() {
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.log.lastIndex(),
		rf.log.getLastLogTerm(),
	}
	votes := 1
	for i := range rf.peers {
		if i != rf.me {
			go rf.requestVote(i, &args, &votes)
		}
	}
}

//
// 开始选举，当前任期增加1，转换为candidate，为自己投票
//
func (rf *Raft) startElectionL() {
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.persist()
	fmt.Printf("%v 开始选举，任期 %v\n", rf.me, rf.currentTerm)
	rf.requestVotesL()
}

//
// 重置选举时间，在基础超时时间基础上加上一个随机数保证各个服务器不会同时开始选举
//
func (rf *Raft) electionTimeReset() {
	t := time.Now()
	t = t.Add(ELECTION_TIME_OUT)
	ms := rand.Int63() % 300
	t = t.Add(time.Duration(ms) * time.Millisecond)
	rf.electionTime = t
}

//
// 如果当前是leader定期发送心跳，若当前时间超过选举时间发起选举
//
func (rf *Raft) tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		rf.electionTimeReset()
		rf.sendAppendsL(true)
	}

	if time.Now().After(rf.electionTime) {
		rf.electionTimeReset()
		rf.startElectionL()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.tick()
		time.Sleep(50 * time.Millisecond)
	}
}
