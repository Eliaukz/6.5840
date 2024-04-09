package kvsrv

import (
	"6.5840/labrpc"
	"crypto/rand"
	"math/big"
)

type Clerk struct {
	server   *labrpc.ClientEnd
	clientId int64
	seq      int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.clientId = nrand()
	ck.seq = 0
	return ck
}

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{key}
	reply := GetReply{}
	for !ck.server.Call("KVServer.Get", &args, &reply) {
	}
	DPrintf("Client: ClientId %d Get %s send\n", ck.clientId, key)
	return reply.Value
}

// PutAppend shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	ck.seq++
	args := PutAppendArgs{key, value, ck.clientId, ck.seq}
	reply := PutAppendReply{}
	for !ck.server.Call("KVServer."+op, &args, &reply) {
	}

	DPrintf("Client: ClientId %d %s args seq %d send, value is %s\n", ck.clientId, op, ck.seq, value)
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
