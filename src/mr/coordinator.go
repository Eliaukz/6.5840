package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Status int

const (
	Map Status = iota
	Reduce
	Exit
	Wait
)

type Coordinator struct {
	mu        sync.Mutex
	TaskQueue chan *Task
	TaskInfo  map[int]*TaskInfo
	NReduce   int
	Files     []string
	Temp      [][]string
	Exit      bool
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.Exit
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskQueue: make(chan *Task, maxInt(nReduce, len(files))),
		TaskInfo:  make(map[int]*TaskInfo),
		NReduce:   nReduce,
		Files:     files,
		Temp:      make([][]string, nReduce),
		Exit:      false,
	}

	// Your code here.
	for id, file := range files {
		task := Task{
			TaskId:   id,
			Input:    file,
			Costatus: Map,
			NReduce:  nReduce,
		}
		c.TaskInfo[id] = &TaskInfo{
			RStatus: Idle,
			Task:    &task,
		}
		c.TaskQueue <- &task
	}

	c.server()
	go c.heartbeat()
	return &c
}

func (c *Coordinator) AskForTask(args *ExampleArgs, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.TaskQueue) > 0 {
		*reply = *<-c.TaskQueue
		c.TaskInfo[reply.TaskId].RStatus = Running
		c.TaskInfo[reply.TaskId].StartTime = time.Now()
	} else if c.Exit {
		*reply = Task{Costatus: Exit}
	} else {
		*reply = Task{Costatus: Wait}
	}
	return nil
}

func (c *Coordinator) CommitTask(args *Task, reply *ExampleReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.TaskInfo[args.TaskId].RStatus == Complete {
		return nil
	}

	c.TaskInfo[args.TaskId].RStatus = Complete
	go c.GenerateResult(args)
	return nil
}

func (c *Coordinator) heartbeat() {
	for {
		c.mu.Lock()
		if c.Exit {
			c.mu.Unlock()
			return
		}
		for _, task := range c.TaskInfo {
			if task.RStatus == Running && time.Now().Sub(task.StartTime) > time.Second*10 {
				// 该任务超时后重新放回task队列
				task.RStatus = Idle
				c.TaskQueue <- task.Task
			}
		}
		c.mu.Unlock()
		time.Sleep(5 * time.Second)
	}

}

func (c *Coordinator) isAllDone() bool {
	for _, task := range c.TaskInfo {
		if task.RStatus != Complete {
			return false
		}
	}
	return true
}

func (c *Coordinator) GenerateResult(args *Task) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.Costatus {
	case Map:
		for id, file := range args.Temp {
			c.Temp[id] = append(c.Temp[id], file)
		}
		if c.isAllDone() {
			log.Println("coordinator convert to reduce")
			c.TaskInfo = make(map[int]*TaskInfo)
			for id, file := range c.Temp {
				task := Task{
					TaskId:   id,
					Costatus: Reduce,
					NReduce:  c.NReduce,
					Temp:     file,
				}
				c.TaskQueue <- &task
				c.TaskInfo[id] = &TaskInfo{
					RStatus: Idle,
					Task:    &task,
				}

			}
		}
	case Reduce:
		if c.isAllDone() {
			c.Exit = true
		}
	}
}

func maxInt(a, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}
