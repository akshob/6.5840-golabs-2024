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

type KVServer struct {
	mu sync.Mutex
	data map[string]string
	processed map[int64]bool
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if val, ok := kv.data[args.Key]; ok {
		reply.Value = val
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.processed[args.Id]; !ok {
		kv.processed[args.Id] = true
		kv.data[args.Key] = args.Value
		reply.Value = args.Value
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.processed[args.Id]; !ok {
		kv.processed[args.Id] = true
		if val, ok := kv.data[args.Key]; ok {
			reply.Value = val
			kv.data[args.Key] = val + args.Value
		} else {
			reply.Value = ""
			kv.data[args.Key] = val + args.Value
		}
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.processed = make(map[int64]bool)

	return kv
}
