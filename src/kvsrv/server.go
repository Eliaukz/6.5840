package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type client struct {
	seq   int
	value string
}

type KVServer struct {
	mu          sync.Mutex
	data        map[string]string
	clientTable map[int64]client
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if val, ok := kv.data[args.Key]; ok {
		reply.Value = val
	} else {
		reply.Value = ""
	}
	DPrintf("Server: key %s, Get value is %s\n", args.Key, reply.Value)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.clientTable[args.ClientId].seq != args.Seq {
		kv.data[args.Key] = args.Value
		kv.clientTable[args.ClientId] = client{
			seq:   args.Seq,
			value: "",
		}
	}

	DPrintf("Server: ClientId %d with seq %d Put  %s , %s\n", args.ClientId, args.Seq, args.Key, args.Value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.clientTable[args.ClientId].seq != args.Seq {
		reply.Value = kv.data[args.Key]
		kv.data[args.Key] = reply.Value + args.Value
		kv.clientTable[args.ClientId] = client{
			seq:   args.Seq,
			value: reply.Value,
		}
	} else {
		reply.Value = kv.clientTable[args.ClientId].value
	}

	DPrintf("Server: ClientId %d with seq %d Put %s , %s and get reply %v\n", args.ClientId, args.Seq, args.Key, args.Value, reply.Value)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.data = make(map[string]string)
	kv.clientTable = make(map[int64]client)

	return kv
}
