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
	old_data map[int64]map[string]string
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

	existingValue := ""

	DPrintf("Server Append - 1 args: %v is_processed: %v existing value: %v\n", *args, kv.processed[args.Id], existingValue)
	if _, ok := kv.processed[args.Id]; !ok {
		kv.processed[args.Id] = true
		kv.old_data[args.Id] = make(map[string]string)
		if val, ok := kv.data[args.Key]; ok {
			existingValue = val
			kv.old_data[args.Id][args.Key] = val
			kv.data[args.Key] = val + args.Value
			DPrintf("Server Append - 2 args: %v is_processed: %v existing value: %v\n", *args, kv.processed[args.Id], existingValue)
		} else {
			existingValue = ""
			kv.old_data[args.Id][args.Key] = ""
			kv.data[args.Key] = val + args.Value
			DPrintf("Server Append - 2 args: %v is_processed: %v existing value: %v\n", *args, kv.processed[args.Id], existingValue)
		}
	} else {
		existingValue = kv.old_data[args.Id][args.Key]
		DPrintf("Server Append - 3 args: %v is_processed: %v existing value: %v\n", *args, kv.processed[args.Id], existingValue)
	}
	DPrintf("Server Append - 4 args: %v is_processed: %v existing value: %v\n", *args, kv.processed[args.Id], existingValue)
	reply.Value = existingValue
}

func (kv *KVServer) DeleteKey(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("Server DeleteKey args: %v deleting old data for id: %v\n", *args, args.Id)
	delete(kv.old_data[args.Id], args.Key)
	delete(kv.old_data, args.Id)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.old_data = make(map[int64]map[string]string)
	kv.processed = make(map[int64]bool)

	return kv
}
