package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KeyValue struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex
	// 存储键值对和版本号
	store map[string]*KeyValue
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		store: make(map[string]*KeyValue),
	}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv, ok := kv.store[args.Key]; ok {
		reply.Value = kv.Value
		reply.Version = kv.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 检查键是否存在
	if existing, ok := kv.store[args.Key]; ok {
		// 键存在，检查版本号
		if existing.Version != args.Version {
			reply.Err = rpc.ErrVersion
			return
		}
		// 版本号匹配，更新值并增加版本号
		existing.Value = args.Value
		existing.Version++
		reply.Err = rpc.OK
	} else {
		// 键不存在
		if args.Version == 0 {
			// 创建新键
			kv.store[args.Key] = &KeyValue{
				Value:   args.Value,
				Version: 1,
			}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
