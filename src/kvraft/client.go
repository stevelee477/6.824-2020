package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
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
	ck.clientId = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	DPrintf("client %v %v\n", "get", key)
	seq := atomic.AddInt64(&ck.seq, 1)
	leaderId := ck.leaderId
	for {
		args := GetArgs{
			Seq:      seq,
			Key:      key,
			ClientId: ck.clientId,
		}
		reply := GetReply{}
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)

		if !ok {
			time.Sleep(time.Millisecond * 20)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
		switch reply.Err {
		case OK:
			ck.leaderId = leaderId
			return reply.Value
		case ErrNoKey:
			return ""
		case ErrWrongLeader:
			leaderId = (leaderId + 1) % len(ck.servers)
		case ErrApplyTimeout:
			continue
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	DPrintf("client %v %v %v\n", op, key, value)
	seq := atomic.AddInt64(&ck.seq, 1)
	leaderId := ck.leaderId
	for {
		args := PutAppendArgs{
			Seq:      seq,
			Key:      key,
			Value:    value,
			Op:       op,
			ClientId: ck.clientId,
		}
		reply := PutAppendReply{}
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)

		if !ok {
			time.Sleep(time.Millisecond * 20)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
		switch reply.Err {
		case OK:
			ck.leaderId = leaderId
			return
		case ErrWrongLeader:
			leaderId = (leaderId + 1) % len(ck.servers)
		}
	}
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
