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

type Buffer struct {
	ClerkSeq int64
	Value    string
}

type KVServer struct {
	mu     sync.Mutex
	kvmap  map[string]string // key -> value
	buffer map[int64]Buffer  // clerkId -> clerkSeq -> value
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, ok := kv.kvmap[args.Key]; ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	buffer, exists := kv.buffer[args.ClerkId]
	if !exists || args.ClerkSeq > buffer.ClerkSeq {
		kv.kvmap[args.Key] = args.Value
		kv.buffer[args.ClerkId] = Buffer{
			ClerkSeq: args.ClerkSeq,
			Value:    args.Value,
		}
	}

	DPrintf("Put key: %s, value: %s, clerkId: %d, clerkSeq: %d", args.Key, args.Value, args.ClerkId, args.ClerkSeq)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if buffer, exists := kv.buffer[args.ClerkId]; exists && args.ClerkSeq <= buffer.ClerkSeq {
		reply.Value = buffer.Value
	} else {
		preValue, existValue := kv.kvmap[args.Key]
		if !existValue {
			preValue = ""
		}
		kv.kvmap[args.Key] = preValue + args.Value
		reply.Value = preValue
		kv.buffer[args.ClerkId] = Buffer{
			ClerkSeq: args.ClerkSeq,
			Value:    preValue,
		}
	}

	DPrintf("Append key: %s, value: %s, clerkId: %d, clerkSeq: %d", args.Key, args.Value, args.ClerkId, args.ClerkSeq)
}

func StartKVServer() *KVServer {
	kv := &KVServer{
		mu:     sync.Mutex{},
		kvmap:  make(map[string]string),
		buffer: make(map[int64]Buffer),
	}
	return kv
}
