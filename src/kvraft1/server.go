package kvraft

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	tester "6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu              sync.Mutex
	store           map[string]Data
	lastClientsReqs map[uint64]uint64
	lastClientsReps map[uint64]any
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
	op1 := req.(rpc.CommonKVCommandsInterface)
	if kv._isCommandRecentlyExecuted(op1) {
		return kv._getCachedResponse(op1)
	}
	if kv._isCommandTooOld(op1) {
		panic(fmt.Sprintf("Try to run a command that's too old (cid=%d , rid=%d)", op1.GetClientId(), op1.GetRequestId()))
	}

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
		kv._saveReqIdAndResponse(op, rs)
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
		kv._saveReqIdAndResponse(op, rs)
		return rs

	default:
		panic(fmt.Sprintf("unexpected Op %v", req))
	}
}

func (kv *KVServer) _isCommandRecentlyExecuted(arg rpc.CommonKVCommandsInterface) bool {
	v, ok := kv.lastClientsReqs[arg.GetClientId()]
	if !ok {
		return false
	}
	return v == arg.GetRequestId()
}
func (kv *KVServer) _getCachedResponse(arg rpc.CommonKVCommandsInterface) any {
	v, ok := kv.lastClientsReps[arg.GetClientId()]
	if !ok {
		panic("tried to retrieve unaccessible key")
	}
	return v
}

func (kv *KVServer) _isCommandTooOld(arg rpc.CommonKVCommandsInterface) bool {
	v, ok := kv.lastClientsReqs[arg.GetClientId()]
	if !ok {
		return false
	}
	return arg.GetRequestId() < v
}

func (kv *KVServer) _saveReqIdAndResponse(args rpc.CommonKVCommandsInterface, response any) {
	kv._updateRecentReqId(args)
	kv._updateResponse(args, response)
}
func (kv *KVServer) _updateRecentReqId(arg rpc.CommonKVCommandsInterface) {
	kv.lastClientsReqs[arg.GetClientId()] = arg.GetRequestId()
}
func (kv *KVServer) _updateResponse(args rpc.CommonKVCommandsInterface, response any) {
	kv.lastClientsReps[args.GetClientId()] = response
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
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if len(data) < 1 {
		return
	}
	// Your code here
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
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
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
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
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
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetReply{})

	kv := &KVServer{
		me:              me,
		store:           make(map[string]Data),
		lastClientsReqs: make(map[uint64]uint64),
		lastClientsReps: make(map[uint64]any),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}

func (kv *KVServer) Log(format string, args ...any) {
	if os.Getenv("DEBUG") != "true" {
		return
	}
	now := time.Now()
	formatted := raft.FormatTime(now)
	message := fmt.Sprintf(format, args...)
	fmt.Println(formatted, " - srv: ", kv.me, " : ", message)
}
