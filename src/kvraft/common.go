package kvraft

import (
	"fmt"
	"log"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	TimeOut        = "TimeOut"
	ErrWrong       = "ErrWrong"
)

type Err string

// PutAppendArgs Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op       string // "Put" or "Append"
	ClientId int64
	SeqNo    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	SeqNo    int64
}

type GetReply struct {
	Err   Err
	Value string
}

func cmpCommand(op1, op2 Op) bool {
	if op1.ClientId == op2.ClientId && op1.SeqNo == op2.SeqNo {
		return true
	}
	return false
}

// Debugging
const debug = false

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

func Debug(topic logTopic, format string, a ...interface{}) {
	if debug {
		prefix := fmt.Sprintf(string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
