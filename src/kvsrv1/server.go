package kvsrv

import (
	"fmt"
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
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
	Store Store
}

type Store map[string]Value
type Value struct {
	Value   string
	Version rpc.Tversion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		Store: Store{},
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.Store[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = value.Value
	reply.Version = value.Version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	fmt.Printf("kvsrv: PUT (key%v,v:%s) \n", args.Key, args.Value)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	oldValue, exists := kv.Store[args.Key]
	if !exists {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}
		value := Value{
			Value:   args.Value,
			Version: 1,
		}
		kv.Store[args.Key] = value
		reply.Err = rpc.OK
		return
	}

	// key exist
	if args.Version != oldValue.Version {
		reply.Err = rpc.ErrVersion
		return
	}

	value := Value{
		Value:   args.Value,
		Version: args.Version + 1,
	}
	kv.Store[args.Key] = value
	reply.Err = rpc.OK

}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
