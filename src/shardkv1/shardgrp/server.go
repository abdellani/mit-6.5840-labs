package shardgrp

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	SHARD_FROZEN  = 0
	SHARD_ALLOWED = 1
	SHARD_DELETED = 2
)

type ShardStatus int
type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	mu              sync.Mutex
	ConfigNum       shardcfg.Tnum
	LastClientsReqs map[uint64]uint64
	LastClientsResp map[uint64]any
	Shrdskv         map[shardcfg.Tshid]*KV
	ShardsCache     map[shardcfg.Tshid]*Cache
	Status          map[shardcfg.Tshid]ShardStatus
}

type KV struct {
	Store map[string]Data
}
type Cache struct {
	LastClientsReqs map[uint64]uint64
	LastClientsResp map[uint64]any
}

type Data struct {
	Value   string
	Version uint64
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	commonKVCommandArgs, isKVCommand := req.(rpc.CommonKVCommandsInterface)
	if isKVCommand {
		// KV commands depends on supported shard
		if !kv._canKeyBeProcessOnThisShardGroup(commonKVCommandArgs) {
			kv.Log("shard not available! gid=%d srv=%d shrd=%d, status=%v, cmd=%+v", kv.gid, kv.me, shardcfg.Key2Shard(commonKVCommandArgs.GetKey()), kv.Status, commonKVCommandArgs)
			switch req.(type) {
			case rpc.GetArgs:
				return rpc.GetReply{Err: rpc.ErrWrongGroup}
			case rpc.PutArgs:
				return rpc.PutReply{Err: rpc.ErrWrongGroup}
			default:
				panic("command not recognized")
			}
		}
	} else {
		// shardgroupd cmd
		//check config number
		command, ok := req.(shardrpc.CommandInterface)
		if !ok {
			log.Panicf("can not recognized command %+v\n", req)
		}
		if kv._isConfigCommandOutdates(command) {
			switch req.(type) {
			case shardrpc.FreezeShardArgs:
				return shardrpc.FreezeShardReply{Err: rpc.ErrVersion}
			case shardrpc.InstallShardArgs:
				return shardrpc.InstallShardReply{Err: rpc.ErrVersion}
			case shardrpc.DeleteShardArgs:
				return shardrpc.DeleteShardReply{Err: rpc.ErrVersion}
			default:
				panic("unexpected type")
			}

		} else {
			kv._updateConfigNum(command.GetNum())
		}
	}

	commonClientCommandArg, ok := req.(rpc.CommonClientCommandsInterface)
	if !ok {
		panic("can case args to commonClientCommandArg")
	}

	if kv._isCommandRecentlyExecuted(commonClientCommandArg) {
		return kv._getCachedResponse(commonClientCommandArg)
	}
	if kv._isCommandTooOld(commonClientCommandArg) {
		panic("Try to run a command that's too old ")
	}
	var rs any

	switch args := req.(type) {
	case rpc.GetArgs:
		return *kv._handleGet(&args)
	case rpc.PutArgs:
		rs = *kv._handlePut(&args)
	case shardrpc.FreezeShardArgs:
		rs = *kv._handleFreeze(&args)
	case shardrpc.InstallShardArgs:
		rs = *kv._handleInstall(&args)
	case shardrpc.DeleteShardArgs:
		rs = *kv._handleDelete(&args)
	default:
		panic(fmt.Sprintf("unexpected Op %v", req))
	}

	kv._saveReqIdAndResponse(commonClientCommandArg, rs)
	return rs

}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.ConfigNum); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.LastClientsReqs); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.LastClientsResp); err != nil {
		panic(err)
	}

	if err := e.Encode(kv.Shrdskv); err != nil {
		panic(err)
	}

	if err := e.Encode(kv.ShardsCache); err != nil {
		panic(err)
	}

	if err := e.Encode(kv.Status); err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", kv.ShardsCache[0])
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	fmt.Println("restoring data")
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var configNum shardcfg.Tnum
	var lastClientsReqs map[uint64]uint64
	var lastClientsResp map[uint64]any
	var shrdskv map[shardcfg.Tshid]*KV
	var shrdsCache map[shardcfg.Tshid]*Cache
	var status map[shardcfg.Tshid]ShardStatus

	if d.Decode(&configNum) != nil ||
		d.Decode(&lastClientsReqs) != nil ||
		d.Decode(&lastClientsResp) != nil ||
		d.Decode(&shrdskv) != nil ||
		d.Decode(&shrdsCache) != nil ||
		d.Decode(&status) != nil {
		panic("error failed while trying to restore")
	} else {
		kv.ConfigNum = configNum
		kv.LastClientsReqs = lastClientsReqs
		kv.LastClientsResp = lastClientsResp
		kv.Shrdskv = shrdskv
		kv.ShardsCache = shrdsCache
		kv.Status = status
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, response := kv.rsm.Submit(*args)
	kv.Log("(action=%s,ci=%d,ri=%2d,err=%-10.10s)", ACTION_GET, args.ClientId, args.RequestId, err)
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
	kv.mu.Lock()
	if kv._isCommandRecentlyExecuted(args) {
		cached, ok := kv._getCachedResponse(args).(rpc.PutReply)
		if !ok {
			panic("can't cast cache response to PutReply")
		}
		kv.mu.Unlock()
		reply.Err = cached.Err
		kv.Log("(action=%s,ci=%d,ri=%2d,err=%-10.10s)", ACTION_PUT, args.ClientId, args.RequestId, reply.Err)
		return
	}
	kv.mu.Unlock()
	err, response := kv.rsm.Submit(*args)
	kv.Log("(action=%s,ci=%d,ri=%2d,err=%-10.10s)", ACTION_PUT, args.ClientId, args.RequestId, err)
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
	kv.mu.Lock()
	if kv._isConfigCommandOutdates(args) {
		reply.Err = rpc.ErrVersion
		kv.Log("(action=%s,ci=%d,ri=%2d,err=%-10.10s)", ACTION_FREEZE, args.ClientId, args.RequestId, reply.Err)
		kv.mu.Unlock()
		return
	}
	if kv._isCommandRecentlyExecuted(args) {
		cached, ok := kv._getCachedResponse(args).(shardrpc.FreezeShardReply)
		if !ok {
			panic("can't cast cache response to shardrpc.FreezeShardReply")
		}
		kv.mu.Unlock()
		reply.Num = cached.Num
		reply.State = cached.State
		reply.Err = cached.Err
		kv.Log("(action=%s,ci=%d,ri=%2d,err=%-10.10s)", ACTION_FREEZE, args.ClientId, args.RequestId, reply.Err)
		return
	}
	kv.mu.Unlock()
	err, response := kv.rsm.Submit(*args)
	kv.Log("(action=%s,ci=%d,ri=%2d,err=%-10.10s)", ACTION_FREEZE, args.ClientId, args.RequestId, err)
	switch err {
	case rpc.OK:
		result, ok := response.(shardrpc.FreezeShardReply)
		if !ok {
			panic("failed to parse FreezeShardReply")
		}
		reply.State = result.State
		reply.Num = result.Num
		reply.Err = result.Err
		return
	case rpc.ErrWrongLeader:
		reply.Err = rpc.ErrWrongLeader
		return
	default:
		log.Panicf("unexpected response (err=%v)", err)
	}
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	kv.mu.Lock()
	if kv._isConfigCommandOutdates(args) {
		kv.mu.Unlock()
		reply.Err = rpc.ErrVersion
		kv.Log("(action=%s,ci=%d,ri=%2d,err=%-10.10s)", ACTION_INSTALL, args.ClientId, args.RequestId, reply.Err)
		return
	}
	if kv._isCommandRecentlyExecuted(args) {
		cached, ok := kv._getCachedResponse(args).(shardrpc.InstallShardReply)
		if !ok {
			panic("can't cast cache response to InstallShardReply")
		}
		kv.mu.Unlock()
		reply.Err = cached.Err
		kv.Log("(action=%s,ci=%d,ri=%2d,err=%-10.10s)", ACTION_INSTALL, args.ClientId, args.RequestId, reply.Err)
		return
	}
	kv.mu.Unlock()
	err, response := kv.rsm.Submit(*args)
	kv.Log("(action=%s,ci=%d,ri=%2d,err=%-10.10s)", ACTION_INSTALL, args.ClientId, args.RequestId, err)
	switch err {
	case rpc.OK:
		result, ok := response.(shardrpc.InstallShardReply)
		if !ok {
			panic("failed to parse InstallShardReply")
		}
		reply.Err = result.Err
		return
	case rpc.ErrWrongLeader:
		reply.Err = rpc.ErrWrongLeader
		return
	default:
		log.Panicf("unexpected response: err=%v", err)
	}

}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	kv.mu.Lock()
	if kv._isConfigCommandOutdates(args) {
		kv.mu.Unlock()
		reply.Err = rpc.ErrVersion
		kv.Log("(action=%s,ci=%d,ri=%2d,err=%-10.10s)", ACTION_DELETE, args.ClientId, args.RequestId, reply.Err)
		return
	}
	if kv._isCommandRecentlyExecuted(args) {
		cached, ok := kv._getCachedResponse(args).(shardrpc.DeleteShardReply)
		if !ok {
			panic("can't cast cache response to DeleteShardReply")
		}
		kv.mu.Unlock()
		reply.Err = cached.Err
		kv.Log("(action=%s,ci=%d,ri=%2d,err=%-10.10s)", ACTION_DELETE, args.ClientId, args.RequestId, reply.Err)
		return
	}
	kv.mu.Unlock()
	err, response := kv.rsm.Submit(*args)
	kv.Log("(action=%s,ci=%d,ri=%2d,err=%-10.10s)", ACTION_DELETE, args.ClientId, args.RequestId, err)
	switch err {
	case rpc.OK:
		result, ok := response.(shardrpc.DeleteShardReply)
		if !ok {
			log.Panicf("failed to parse DeleteShardReply (r=%v)\n", response)
		}
		reply.Err = result.Err
		return
	case rpc.ErrWrongLeader:
		reply.Err = err
		return
	default:
		log.Panicf("unexpected response (errr = %v)", err)
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
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.Log(fmt.Sprintf("killed (gid:%d,state:%v)", kv.gid, kv.Status))
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
	labgob.Register(shardrpc.FreezeShardReply{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.InstallShardReply{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(shardrpc.DeleteShardReply{})
	labgob.Register(rsm.Op{})
	labgob.Register(KV{})

	kv := &KVServer{
		gid:             gid,
		me:              me,
		Status:          make(map[shardcfg.Tshid]ShardStatus),
		Shrdskv:         make(map[shardcfg.Tshid]*KV),
		ShardsCache:     make(map[shardcfg.Tshid]*Cache),
		LastClientsReqs: make(map[uint64]uint64),
		LastClientsResp: make(map[uint64]any),
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here
	fmt.Printf("starting server id:%d,gid:%d\n", kv.me, kv.gid)
	if gid == shardcfg.Gid1 {
		for i := range shardcfg.NShards {
			kv.Status[shardcfg.Tshid(i)] = SHARD_ALLOWED
			kv.Shrdskv[shardcfg.Tshid(i)] = &KV{
				Store: map[string]Data{},
			}
			kv.ShardsCache[shardcfg.Tshid(i)] = &Cache{
				LastClientsReqs: make(map[uint64]uint64),
				LastClientsResp: make(map[uint64]any),
			}
		}
	}

	return []tester.IService{kv, kv.rsm.Raft()}
}
