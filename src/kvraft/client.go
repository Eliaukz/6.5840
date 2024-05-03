package kvraft

import (
	"6.5840/labrpc"
	"crypto/rand"
	"math/big"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader   int
	clientId int64
	seq      int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = 0
	ck.clientId = nrand()
	ck.seq = 1
	return ck
}

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqNo:    ck.seq,
	}
	reply := GetReply{}

	for {
		//Debug(dClient, "{client %v seq %v } call get with ket %v\n", ck.clientId, ck.seq, key)
		ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			ck.seq++
			return reply.Value
		} else if !ok || reply.Err == ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
		} else if reply.Err == TimeOut {
			time.Sleep(200 * time.Millisecond)
		}

	}

}

// PutAppend shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqNo:    ck.seq,
	}
	reply := PutAppendReply{}

	for {
		//Debug(dClient, "{client %v seq %v } call putAppend with ket %v value %v\n", ck.clientId, ck.seq, key, value)
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			ck.seq++
			return
		} else if !ok || reply.Err == ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
		} else if reply.Err == TimeOut {
			time.Sleep(200 * time.Millisecond)
		}

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
