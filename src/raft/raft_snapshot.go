package raft

import "time"

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if index <= rf.lastIncludedIndex {
		//Debug(dSnap, "{server %v term %v index %v } role %v  reject the snapshot the %v index has snapshoted and the lastest index is %v\n",
		//	rf.me, rf.currentTerm, rf.getLastLogIndex(), rf.role, index, rf.lastIncludedIndex)

		return
	}

	rf.lastIncludedTerm = rf.logs[index-rf.lastIncludedIndex].Term

	// 删除index之前的所有日志，index位置的日志command置为nil
	tmpLogs := make([]Entry, len(rf.logs[index-rf.lastIncludedIndex:]))
	copy(tmpLogs, rf.logs[index-rf.lastIncludedIndex:])

	rf.logs = tmpLogs
	rf.lastIncludedIndex = index
	rf.logs[0].Index = rf.lastIncludedIndex
	rf.logs[0].Term = rf.lastIncludedTerm
	rf.logs[0].Command = nil

	rf.snapshot = snapshot
	//Debug(dSnap, "{server %v term %v index %v } role %v accept the snapshot the %v index has snapshoted and the lastest index is %v\n",
	//	rf.me, rf.currentTerm, rf.getLastLogIndex(), rf.role, index, rf.lastIncludedIndex)

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//Debug(dSnap, "{server %v term %v index %v } receive snapshot from {server %v term %v} lastIncludeIndex %v lastIndexTerm %v\n",
	//	rf.me, rf.currentTerm, rf.getLastLogIndex(), args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)

	/*
		handle
			1. args.Term < currentTerm : return
			2. args.Term > currentTerm : convertTo Follower
			3. 截断自己的log，更新lastIncludeIndex，lastIncludeTerm
			4. 将args.data应用到自己的snapshot中
			5. 更新commitIndex和lastApplied
	*/
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.convertTo(Follower)
		rf.currentTerm = args.Term
	}

	rf.lastUpdate = time.Now()
	reply.Term = rf.currentTerm
	defer rf.persist()

	if args.LastIncludedIndex <= rf.lastIncludedIndex || args.LastIncludedIndex <= rf.lastApplied {
		return
	}

	if rf.getLastLogIndex() < args.LastIncludedIndex {
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.logs = []Entry{{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm, Command: nil}}
	} else {
		tmpLogs := make([]Entry, len(rf.logs[args.LastIncludedIndex-rf.lastIncludedIndex:]))
		copy(tmpLogs, rf.logs[args.LastIncludedIndex-rf.lastIncludedIndex:])

		rf.logs = tmpLogs
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.logs[0] = Entry{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm, Command: nil}
	}

	rf.snapshot = args.Data
	rf.lastApplied = args.LastIncludedIndex
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	//Debug(dSnap, "{server %v term %v index %v} Success install snapshot and lastIncludedIndex %v lastIncludedTerm %v\n",
	//	rf.me, rf.currentTerm, rf.getLastLogIndex(), rf.lastIncludedIndex, rf.lastIncludedTerm)
}

func (rf *Raft) handleInstallSnapshot(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapshot,
	}
	rf.mu.Unlock()

	/*
		handle
			1. not Leader ：return
			2. reply.Term > currentTerm : convertTo follower
			3. 更新 nextIndex and matchIndex
	*/

	reply := InstallSnapshotReply{}

	//Debug(dSnap, "{server %v term %v index %v } Send InstallSnapshot to {Server %v} lastIncludedIndex %v lastIncludedTerm %v\n",
	//	rf.me, rf.currentTerm, rf.getLastLogIndex(), server, rf.lastIncludedIndex, rf.lastIncludedTerm)

	if !rf.sendInstallSnapshot(server, &args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.role != Leader || rf.currentTerm != args.Term {
		// 已经不是Leader或者是过期的Leader
		return
	}

	if reply.Term > rf.currentTerm {
		//Debug(dSnap, "{server %v term %v index %v } Send Snapshot but current term is old (old term %v)\n",
		//	rf.me, rf.currentTerm, rf.getLastLogIndex(), args.Term)

		rf.currentTerm = reply.Term
		rf.convertTo(Follower)
		rf.lastUpdate = time.Now()
		return
	}

	//更新 nextIndex and matchIndex
	if rf.nextIndex[server] <= args.LastIncludedIndex {
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}
}
