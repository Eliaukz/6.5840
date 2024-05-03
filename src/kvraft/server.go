package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   string
	ClientId int64
	SeqNo    int64
	Key      string
	Value    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister
	bytes        int
	// Your definitions here.
	table  map[int64]int64
	data   map[string]string
	waitCh map[int64]chan Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//Debug(dInfo, "{server %v } is called get by{ client %v seq %v key %v}\n", kv.me, args.ClientId, args.SeqNo, args.Key)
	kv.mu.Lock()
	if args.SeqNo <= kv.table[args.ClientId] {
		v := kv.data[args.Key]
		reply.Value = v
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	command := Op{
		OpType:   "GET",
		SeqNo:    args.SeqNo,
		ClientId: args.ClientId,
		Key:      args.Key,
	}

	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan Op, 1)
	kv.waitCh[args.ClientId] = ch
	kv.mu.Unlock()

	select {
	case op := <-ch:
		kv.mu.Lock()
		delete(kv.waitCh, args.ClientId)
		kv.mu.Unlock()
		if cmpCommand(op, command) {
			reply.Value = op.Value
			reply.Err = OK
		} else {
			reply.Err = ErrWrong
		}
		return
	case <-time.After(500 * time.Millisecond):
		reply.Err = TimeOut
		kv.mu.Lock()
		delete(kv.waitCh, args.ClientId)
		kv.mu.Unlock()
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//Debug(dInfo, "{server %v } is called putAppend by{ client %v seq %v key %v value %v}\n",
	//	kv.me, args.ClientId, args.SeqNo, args.Key, args.Value)

	kv.mu.Lock()
	if args.SeqNo <= kv.table[args.ClientId] {
		reply.Err = OK

		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	command := Op{
		OpType:   args.Op,
		SeqNo:    args.SeqNo,
		ClientId: args.ClientId,
		Key:      args.Key,

		Value: args.Value,
	}

	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan Op, 1)
	kv.waitCh[args.ClientId] = ch
	kv.mu.Unlock()

	select {
	case op := <-ch:
		kv.mu.Lock()
		delete(kv.waitCh, args.ClientId)
		kv.mu.Unlock()
		if cmpCommand(op, command) {
			reply.Err = OK
		} else {
			reply.Err = ErrWrong
		}
		return
	case <-time.After(500 * time.Millisecond):
		reply.Err = TimeOut
		kv.mu.Lock()
		delete(kv.waitCh, args.ClientId)
		kv.mu.Unlock()
		return
	}
}

// Kill the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) isRepeated(clientId, seqNo int64) bool {
	if value, ok := kv.table[clientId]; ok && value >= seqNo {
		return true
	} else {
		return false
	}
}

func (kv *KVServer) execute() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {

			op := msg.Command.(Op)
			kv.mu.Lock()
			kv.bytes += int(unsafe.Sizeof(Op{})) + len(op.Key) + len(op.Key) + len(op.Value) + 8
			if op.OpType == "GET" {

				if op.SeqNo > kv.table[op.ClientId] {
					kv.table[op.ClientId] = op.SeqNo
				}

				if _, ok := kv.waitCh[op.ClientId]; ok {
					op.Value = kv.data[op.Key]
					// 阻塞说明已经被处理过，无需放入map中
					select {
					case kv.waitCh[op.ClientId] <- op:
					default:
					}
				}

			} else {
				if kv.isRepeated(op.ClientId, op.SeqNo) {
					if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate && kv.bytes > kv.maxraftstate {
						snapshot := kv.getSnapshot()
						kv.bytes = 0
						kv.mu.Unlock()
						go func(i int) {
							kv.rf.Snapshot(i, snapshot)
						}(msg.CommandIndex)
						continue

					}
					kv.mu.Unlock()
					continue
				}

				if op.OpType == "Put" {
					kv.data[op.Key] = op.Value
				} else {
					kv.data[op.Key] = kv.data[op.Key] + op.Value
				}
				kv.table[op.ClientId] = op.SeqNo

				if _, ok := kv.waitCh[op.ClientId]; ok {
					select {
					case kv.waitCh[op.ClientId] <- op:
					default:
					}
				}
			}
			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate && kv.bytes > kv.maxraftstate {
				snapshot := kv.getSnapshot()
				kv.bytes = 0
				kv.mu.Unlock()
				go func(i int) {
					kv.rf.Snapshot(i, snapshot)
				}(msg.CommandIndex)
				continue

			}
			kv.mu.Unlock()

		} else {

			kv.mu.Lock()
			snapshot := msg.Snapshot
			kv.readSnapshot(snapshot)
			kv.mu.Unlock()

		}

	}
}

func (kv *KVServer) getSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.table)
	e.Encode(kv.data)
	snapshot := w.Bytes()
	return snapshot
}

func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var table map[int64]int64
	var data2 map[string]string
	if d.Decode(&table) != nil ||
		d.Decode(&data2) != nil {
		panic("decode err")
	} else {
		kv.table = table
		kv.data = data2
	}
}

// StartKVServer servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.table = make(map[int64]int64)
	kv.data = make(map[string]string)
	kv.waitCh = make(map[int64]chan Op)

	kv.persister = persister
	kv.readSnapshot(persister.ReadSnapshot())
	kv.bytes = 0

	go kv.execute()

	return kv
}
