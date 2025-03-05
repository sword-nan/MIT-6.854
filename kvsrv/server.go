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
	// Your definitions here.
	kvPair map[string]string
	cache  sync.Map
}

func (kv *KVServer) modifyCache(id int64, key string, value string, modifyFunc func(kv *KVServer, id int64, key, value string) string) string {
	if v, ok := kv.cache.Load(id); ok {
		return v.(string)
	} else {
		return modifyFunc(kv, id, key, value)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	id, key := args.ID, args.Key
	// Your code here.
	reply.Value = kv.modifyCache(
		id, key, "",
		func(kv *KVServer, id int64, key, value string) string {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			v := kv.kvPair[key]
			kv.cache.Store(id, v)
			return v
		},
	)
}

func (kv *KVServer) Done(args *DoneArgs, reply *DoneReply) { // 标识该请求处理完毕，从 cache 中删除
	kv.cache.Delete(args.ID)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	id, key, value := args.ID, args.Key, args.Value
	reply.Value = kv.modifyCache(
		id, key, value, func(kv *KVServer, id int64, key, value string) string {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			kv.kvPair[key] = value
			kv.cache.Store(id, value)
			return value
		},
	)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	id, key, value := args.ID, args.Key, args.Value
	reply.Value = kv.modifyCache(id, key, value, func(kv *KVServer, id int64, key, value string) string {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		old := kv.kvPair[key]
		kv.kvPair[key] = old + value
		kv.cache.Store(id, old)
		return old
	})
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	// You may need initialization code here.
	kv.kvPair = make(map[string]string)
	return kv
}
