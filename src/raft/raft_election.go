package raft

import "time"

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
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) isLogUpToDate(prevLogTerm, prevLogIndex int) bool {
	index := rf.getLastLogIndex() - rf.lastIncludedIndex

	if prevLogTerm == rf.logs[index].Term {
		return prevLogIndex >= rf.logs[index].Index
	}

	return prevLogTerm > rf.logs[index].Term
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	//Debug(dVote, "{server %v term %v index %v } receive args from server %v term %v lastLogIndex %v lastLogTerm %v\n",
	//	rf.me, rf.currentTerm, rf.getLastLogIndex(), args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
	/*
		handle
		1. args.term < currentTerm  :  返回投票失败
		2. args.term == currentTerm and have voted  ： 返回投票失败
		3. args.term > currentTerm: 转换为对方的Follower 并且重置超时计时器
		4. args.term == currentTerm and not vote  :  比较日志
		5. 最终  比较 lastLogIndex and lastLogTerm :
			1. 对方日志新于自己：投票成功
			2. 投票失败
	*/

	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	} else if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.convertTo(Follower)
	}

	if !rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		rf.votedFor = -1
		return
	}

	reply.Term, reply.VoteGranted = rf.currentTerm, true

	rf.votedFor = args.CandidateId
	rf.lastUpdate = time.Now()

	//Debug(dVote, "{server %v term %v index %v } succeed to voteFor %v",
	//	rf.me, rf.currentTerm, rf.getLastLogIndex(), args.CandidateId)
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.convertTo(Candidate)
	rf.mu.Unlock()
	defer rf.persist()

	//Debug(dTimer, "{server %v term %v index %v } start election\n", rf.me, rf.currentTerm, rf.getLastLogIndex())
	args := rf.getRequestVoteArgs()

	voteCount := 1

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.role == Candidate && reply.VoteGranted {
					voteCount++
					// 超过半数投票
					if voteCount > len(rf.peers)/2 {
						rf.convertTo(Leader)
						return
					}
				} else if reply.Term > rf.currentTerm {
					//转变为对应任期的follower
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.convertTo(Follower)
				}
			}
		}(i)
	}

}

func (rf *Raft) getRequestVoteArgs() RequestVoteArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.getLastLogIndex() - rf.lastIncludedIndex
	return RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.logs[index].Index,
		LastLogTerm:  rf.logs[index].Term,
	}
}
