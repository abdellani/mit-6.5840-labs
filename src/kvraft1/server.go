package kvraft

import (
	"fmt"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu    sync.Mutex
	store map[string]Data
}
type Data struct {
	Value   string
	Version uint64
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch op := req.(type) {
	case rpc.GetArgs:
		rs := rpc.GetReply{}
		v, ok := kv.store[op.Key]
		if !ok {
			rs.Err = rpc.ErrNoKey
		} else {
			rs.Value = v.Value
			rs.Version = rpc.Tversion(v.Version)
			rs.Err = rpc.OK
		}
		return rs

	case rpc.PutArgs:
		rs := rpc.PutReply{}
		v, ok := kv.store[op.Key]
		if ok {
			if v.Version == uint64(op.Version) {
				v.Value = op.Value
				v.Version++
				kv.store[op.Key] = v
				rs.Err = rpc.OK
			} else {
				rs.Err = rpc.ErrVersion
			}
		} else {
			if op.Version != 0 {
				rs.Err = rpc.ErrNoKey
			} else {
				newEntry := Data{
					Value:   op.Value,
					Version: 1,
				}
				kv.store[op.Key] = newEntry
				rs.Err = rpc.OK
			}
		}
		return rs
	default:
		panic(fmt.Sprintf("unexpected Op %v", req))
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, response := kv.rsm.Submit(*args)
	switch err {
	case rpc.OK:
		result := response.(rpc.GetReply)
		reply.Value = result.Value
		reply.Version = result.Version
		reply.Err = result.Err
	case rpc.ErrWrongLeader:
		reply.Err = err
	default:
		panic("unexpected")
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, response := kv.rsm.Submit(*args)
	switch err {
	case rpc.OK:
		result := response.(rpc.PutReply)
		reply.Err = result.Err
	case rpc.ErrWrongLeader:
		reply.Err = err
	default:
		panic("unexpected")
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{
		me:    me,
		store: make(map[string]Data),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
