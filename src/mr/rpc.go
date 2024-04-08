package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	Running
	Complete
)

type TaskInfo struct {
	RStatus   TaskStatus //是否被处理
	StartTime time.Time
	Task      *Task
}

type Task struct {
	TaskId   int
	Input    string
	Costatus Status
	NReduce  int
	Temp     []string
	Output   string
}

type ExampleArgs struct {
}

type ExampleReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
