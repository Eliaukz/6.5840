package raft

import "time"

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.mu.Unlock()

	Debug(dInfo, "{server %v term %v index %v } receive args from "+
		"server %v term %v prevLogIndex %v prevLogTerm %v leaderCommit %v len(log) = %v\n",
		rf.me, rf.currentTerm, rf.getLastLogIndex(),
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))

	/*
		handle
			1. args.term < currentTerm :  return false
			2. args.term > currentTerm :  转换为对应的Follower，更新自己的任期
			3. 如果对方的日志与自己日志不匹配： return false
			4. 将自己的日志截断，并将args.entry添加到自己的日志中
			5. 更新自己的commitIndex，然后应用到状态机中
	*/

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.convertTo(Follower)
	}

	rf.lastUpdate = time.Now()

	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if args.PrevLogIndex > 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	rf.logs = rf.logs[:args.PrevLogIndex+1]

	index := rf.getLastLogIndex()

	for i, log := range args.Entries {
		entry := Entry{
			Index:   index + i + 1,
			Term:    log.Term,
			Command: log.Command,
		}
		rf.logs = append(rf.logs, entry)
	}

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.getLastLogIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.getLastLogIndex()
		}

		go rf.applyLogs()
	}

	reply.Term, reply.Success = args.Term, true

	Debug(dInfo, "{server %v term %v index %v } success to apply log commitIndex %d \n",
		rf.me, rf.currentTerm, rf.getLastLogIndex(), rf.commitIndex)

}

func (rf *Raft) getAppendEntries(server int) AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		LeaderCommit: rf.commitIndex,
	}

	if args.PrevLogIndex < 0 {
		args.PrevLogTerm = 0
	} else {
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
	}

	entries := rf.logs[rf.nextIndex[server]:]
	args.Entries = make([]Entry, len(entries))
	copy(args.Entries, entries)

	return args
}

// 收到reply后进行处理
func (rf *Raft) handleAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dInfo, "{server %v term %v index %v } role %v receive reply from <> at term %v success %v",
		rf.me, rf.currentTerm, rf.getLastLogIndex(), rf.role, reply.Term, reply.Success)

	rf.lastUpdate = time.Now()

	/*
		handle
			1. 如果当前该peer已不是leader，则无需处理
			2. 如果reply.term > currentTerm 转变为对应的Follower，更新term
			3. 添加失败，说明对方的日志过于久远，更新nextIndex，等待重新发送新的日志
			4. 计算是否有过半数机器提交了某日志，如果有提交则应用此日志及之前日志。
	*/

	if rf.role != Leader {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.convertTo(Follower)
		return
	}

	if !reply.Success {
		if rf.nextIndex[server] > 0 {
			rf.nextIndex[server]--
		}
		return
	}

	rf.nextIndex[server] = rf.getLastLogIndex() + 1
	rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)

	for n := rf.getLastLogIndex(); n > rf.commitIndex; n-- {
		count := 1

		if rf.logs[n].Term == rf.currentTerm {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			go rf.applyLogs()
			break
		}
	}
	Debug(dInfo, "{server %v term %v index %v } update logs, lastApplyIndex %v commitIndex %v\n",
		rf.me, rf.currentTerm, rf.getLastLogIndex(), rf.lastApplied, rf.commitIndex)
}

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.lastApplied < rf.commitIndex {
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied+1].Command,
			CommandIndex: rf.logs[rf.lastApplied+1].Index,
		}
		rf.applyCh <- msg
		rf.lastApplied++
	}
}
