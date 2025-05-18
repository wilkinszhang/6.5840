package kvsrv

import (
	"time"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := &rpc.GetArgs{Key: key}
	reply := &rpc.GetReply{}

	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Get", args, reply)
		if ok {
			return reply.Value, reply.Version, reply.Err
		}
		// 如果RPC调用失败，等待一段时间后重试
		time.Sleep(100 * time.Millisecond)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := &rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	reply := &rpc.PutReply{}

	// 第一次尝试
	ok := ck.clnt.Call(ck.server, "KVServer.Put", args, reply)
	if !ok {
		// 如果第一次调用失败，继续重试直到成功
		for {
			time.Sleep(100 * time.Millisecond)
			ok = ck.clnt.Call(ck.server, "KVServer.Put", args, reply)
			if ok {
				// 如果重试后收到ErrVersion，返回ErrMaybe
				if reply.Err == rpc.ErrVersion {
					return rpc.ErrMaybe
				}
				break
			}
		}
	} else if reply.Err == rpc.ErrVersion {
		// 如果第一次就收到ErrVersion，直接返回
		return rpc.ErrVersion
	}

	return reply.Err
}
