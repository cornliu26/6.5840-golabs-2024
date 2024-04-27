package kvsrv

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	id     int64
	seqNum int64 // sequence number
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		server: server,
		id:     nrand(),
		seqNum: 0,
	}
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{key}
	reply := GetReply{}

	ck.callWithRetry("KVServer.Get", &args, &reply)

	return reply.Value
}

func (ck *Clerk) PutAppend(key string, value string, op string) string {
	args := PutAppendArgs{key, value, ck.id, ck.seqNum}
	reply := PutAppendReply{}

	ck.callWithRetry("KVServer."+op, &args, &reply)
	ck.seqNum += 1
	ck.callWithRetry("KVServer.DeleteBuffer", &args, &PutAppendReply{})

	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) callWithRetry(method string, args interface{}, reply interface{}) bool {
	for !ck.server.Call(method, args, reply) {
		time.Sleep(100 * time.Millisecond)
	}
	return true
}
