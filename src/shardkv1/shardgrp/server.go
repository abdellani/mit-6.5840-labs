package shardgrp

import (
	"sync/atomic"


	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)


type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	mu              sync.Mutex
	store           map[string]Data
	lastClientsReqs map[uint64]uint64
	lastClientsReps map[uint64]any
}

type Data struct {
	Value   string
	Version uint64
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op1 := req.(rpc.ArgsInterface)
	if kv._isCommandRecentlyExecuted(op1) {
		return kv._getCachedResponse(op1)
	}
	if kv._isCommandTooOld(op1) {
		panic(fmt.Sprintf("Try to run a command that's too old (cid=%d , rid=%d)", op1.GetClientId(), op1.GetRequestId()))
	}
	var rs any

	switch op := req.(type) {
	case rpc.GetArgs:
		rs = *kv.GetHandler(&op)
	case rpc.PutArgs:
		rs = *kv.PutHandler(&op)
	case shardrpc.FreezeShardArgs:
		panic("this is a freeze ops")
	default:
		panic(fmt.Sprintf("unexpected Op %v", req))
	}

	kv._saveReqIdAndResponse(op1, rs)
	return rs

}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.store) != nil ||
		e.Encode(kv.lastClientsReqs) != nil ||
		e.Encode(kv.lastClientsReps) != nil {
		panic("failed to encode kv state")
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if len(data) < 1 {
		return
	}
	var store map[string]Data
	var lastClientsReqs map[uint64]uint64
	var lastClientsReps map[uint64]any
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if d.Decode(&store) != nil ||
		d.Decode(&lastClientsReqs) != nil ||
		d.Decode(&lastClientsReps) != nil {
		panic("error failed while trying to restore")
	} else {
		kv.store = store
		kv.lastClientsReqs = lastClientsReqs
		kv.lastClientsReps = lastClientsReps
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	if kv._isCommandRecentlyExecuted(args) {
		cached, ok := kv._getCachedResponse(args).(rpc.GetReply)
		if !ok {
			panic("can't cast cache response to GetReply")
		}
		kv.mu.Unlock()
		reply.Value = cached.Value
		reply.Version = cached.Version
		reply.Err = cached.Err
		return
	}
	kv.mu.Unlock()

	err, response := kv.rsm.Submit(*args)
	kv.Log("srv: (client Id=%d, req Id=%d ): err =%s", args.ClientId, args.RequestId, err)
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
	// Your code here
	kv.mu.Lock()
	if kv._isCommandRecentlyExecuted(args) {
		cached, ok := kv._getCachedResponse(args).(rpc.PutReply)
		if !ok {
			panic("can't cast cache response to PutReply")
		}
		kv.mu.Unlock()
		reply.Err = cached.Err
		return
	}
	kv.mu.Unlock()
	err, response := kv.rsm.Submit(*args)
	kv.Log("(client Id=%d, req Id=%d ): err =%s", args.ClientId, args.RequestId, err)
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

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
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

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetReply{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{
		gid:             gid,
		me:              me,
		store:           make(map[string]Data),
		lastClientsReqs: make(map[uint64]uint64),
		lastClientsReps: make(map[uint64]any),
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	return []tester.IService{kv, kv.rsm.Raft()}
}
