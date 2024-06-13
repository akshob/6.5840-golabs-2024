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

type clientData struct {
	value string
	seq int
}

type KVServer struct {
	mu sync.Mutex
	data map[string]string
	processed map[int64]clientData
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

	if kv.processed[args.ClientId].seq != args.SeqNum {
		kv.data[args.Key] = args.Value
		kv.processed[args.ClientId] = clientData{"", args.SeqNum}
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.processed[args.ClientId].seq != args.SeqNum {
		reply.Value = kv.data[args.Key]
		kv.data[args.Key] += args.Value
		kv.processed[args.ClientId] = clientData{reply.Value, args.SeqNum}
	} else {
		reply.Value = kv.processed[args.ClientId].value
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.processed = make(map[int64]clientData)

	return kv
}
