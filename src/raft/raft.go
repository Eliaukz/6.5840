package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int //当前投票。-1表示投给自己
	logs        []Entry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	role Role
	//heartChan 用来停止发送heartbeat，lastUpdate 用来记录上次更新的时间，检测选举超时
	heartChan  chan struct{}
	lastUpdate time.Time

	// SnapShot
	snapshot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A).
	return rf.currentTerm, rf.role == Leader
}

// Start the service using Raft (e.g. a k/v server) wants to start
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	term, isLeader := rf.GetState()
	if !isLeader {
		return -1, term, isLeader
	}

	rf.mu.Lock()

	index := rf.getLastLogIndex()
	log := Entry{
		Index:   index + 1,
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, log)
	rf.mu.Unlock()
	rf.persist()
	//Debug(dLeader, "{server %v term %v index %v } start a new command\n", rf.me, rf.currentTerm, rf.getLastLogIndex())

	return index + 1, term, isLeader
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	//Debug(dInfo, "{server %v at term %v index %v} is killed\n", rf.me, rf.currentTerm, rf.getLastLogIndex())
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 600 + (rand.Int63() % 300)
		rf.mu.Lock()
		lastTime, isLeader := rf.lastUpdate, rf.role == Leader
		rf.mu.Unlock()
		if !isLeader && time.Since(lastTime) > time.Duration(ms)*time.Millisecond {
			rf.startElection()
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = []Entry{{Index: 0, Term: 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.role = Follower
	rf.lastUpdate = time.Now()

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshotPersist(persister.ReadSnapshot())
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) getLastLogIndex() int {
	if len(rf.logs) == 0 {
		rf.logs = []Entry{{Index: rf.lastIncludedIndex, Term: rf.lastIncludedTerm}}
	}
	return rf.logs[len(rf.logs)-1].Index
}

func (rf *Raft) convertTo(role Role) {
	if role == Follower && rf.role == Leader {
		close(rf.heartChan)
	} else if role == Leader {

		for i := range rf.nextIndex {
			rf.nextIndex[i] = rf.getLastLogIndex() + 1
		}
		for i := range rf.matchIndex {
			rf.matchIndex[i] = -1
		}

		rf.heartChan = make(chan struct{})
		go rf.heartBeat()
	}

	//Debug(dInfo, "{server %v term %v index %v } convert from %v to %v\n", rf.me, rf.currentTerm, rf.getLastLogIndex(), rf.role, role)

	rf.role = role

}

func (rf *Raft) heartBeat() {
	//Debug(dLeader, "{server %v term %v index %v } send heartbeat to other server", rf.me, rf.currentTerm, rf.getLastLogIndex())

	sleepTime := time.Second / 8

	for rf.killed() == false {
		select {
		case <-rf.heartChan:
			return
		default:
			if _, isLeader := rf.GetState(); isLeader {
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}

					go func(server int) {
						args, isSnapshot := rf.getAppendEntries(server)
						reply := AppendEntriesReply{}

						if isSnapshot {
							go rf.handleInstallSnapshot(server)
						} else if rf.sendAppendEntries(server, &args, &reply) {
							rf.handleAppendEntries(server, &args, &reply)
						}

					}(i)

				}
			} else {
				return
			}
			time.Sleep(sleepTime)
		}
	}

}
