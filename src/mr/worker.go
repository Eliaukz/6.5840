package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// ByKey 从mrsequential.go中参考
// for sorting by key.
type ByKey []KeyValue

// Len for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := ExampleArgs{}
		reply := Task{}
		call("Coordinator.AskForTask", &args, &reply)
		log.Println("worker get task ", reply.TaskId, " task info ", reply.Costatus)
		switch reply.Costatus {
		case Map:
			mapper(&reply, mapf)
		case Reduce:
			reducer(&reply, reducef)
		case Wait:
			time.Sleep(5 * time.Second)
		case Exit:
			return
		}
	}

}

func mapper(reply *Task, mapf func(string, string) []KeyValue) {
	context, err := os.ReadFile(reply.Input)
	if err != nil {
		log.Fatal(err)
	}

	keyvalues := mapf(reply.Input, string(context))
	temp := make([][]KeyValue, reply.NReduce)
	for _, kv := range keyvalues {
		index := ihash(kv.Key) % reply.NReduce
		temp[index] = append(temp[index], kv)
	}

	reply.Temp = make([]string, 0)
	for i := 0; i < reply.NReduce; i++ {
		reply.Temp = append(reply.Temp, writeToLocalFile(reply.TaskId, i, &temp[i]))
	}

	r := ExampleReply{}
	log.Println("worker finish task ", reply.TaskId)
	call("Coordinator.CommitTask", reply, &r)
}

func reducer(task *Task, reducef func(string, []string) string) {
	var kvs []KeyValue
	for _, file := range task.Temp {
		f, err := os.Open(file)
		if err != nil {
			log.Fatal(err)
		}
		con := json.NewDecoder(f)
		for {
			var t KeyValue
			if err := con.Decode(&t); err != nil {
				break
			}
			kvs = append(kvs, t)
		}
		f.Close()
	}

	sort.Sort(ByKey(kvs))
	src, _ := os.Getwd()
	tempFile, err := os.CreateTemp(src, "mr-tmp-*")
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(kvs); {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		fmt.Fprintf(tempFile, "%v %v\n", kvs[i].Key, output)

		i = j
	}

	tempFile.Close()
	newname := fmt.Sprintf("mr-out-%d", task.TaskId)
	os.Rename(tempFile.Name(), newname)
	task.Output = newname

	r := ExampleReply{}
	log.Println("worker commit task ", task.TaskId)
	call("Coordinator.CommitTask", task, &r)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func writeToLocalFile(x int, y int, kvs *[]KeyValue) string {
	dir, _ := os.Getwd()
	tempFile, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal(err)
	}
	enc := json.NewEncoder(tempFile)
	for _, kv := range *kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal(err)
		}
	}
	tempFile.Close()
	outputName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(tempFile.Name(), outputName)
	return filepath.Join(dir, outputName)
}
