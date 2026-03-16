package rsm

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id  uint64
	Me  int
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	pendingReqs map[uint64]chan any
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pendingReqs:  make(map[uint64]chan any),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.Reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}

	reqId := binary.LittleEndian.Uint64(b[:])
	op := Op{
		Me:  rsm.me,
		Id:  reqId,
		Req: req,
	}
	resp := make(chan any, 1)
	rsm.mu.Lock()
	rsm.pendingReqs[reqId] = resp
	rsm.mu.Unlock()
	defer func() {
		rsm.mu.Lock()
		rsm.Log("deleting key %d", reqId)
		delete(rsm.pendingReqs, reqId)
		rsm.mu.Unlock()
	}()

	_, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}

	rsm.Log("submit cmd id:=%d", reqId)
	ticker := time.NewTicker(1 * time.Second)
	timer := time.NewTimer(5 * time.Second)
	defer func() {
		ticker.Stop()
		timer.Stop()
	}()

	for {
		select {
		case result, ok := <-resp:
			if !ok {
				rsm.Log("cmd failed id=%d", reqId)
				return rpc.ErrWrongLeader, nil
			}
			rsm.Log("received resp for cmd id:=%d", reqId)
			return rpc.OK, result
		case <-ticker.C:
			cterm, isLeader := rsm.rf.GetState()
			if isLeader && cterm == term {
				continue
			}
			rsm.Log("ticker cmd id:=%d (isleader=%v, term=%v)", reqId, isLeader, cterm)
			return rpc.ErrWrongLeader, nil
		case <-timer.C:
			rsm.Log("time out cmd=%d", reqId)
			return rpc.ErrWrongLeader, nil
		}
	}
}

func (rsm *RSM) Reader() {
	for cmd := range rsm.applyCh {
		if cmd.SnapshotValid {
			rsm.Log("snapshot lsi %d", cmd.SnapshotIndex)
			rsm.sm.Restore(cmd.Snapshot)
			continue
		}
		if !cmd.CommandValid {
			continue
		}
		op, ok := cmd.Command.(Op)
		if !ok {
			// no op
			continue
		}
		resp := rsm.sm.DoOp(op.Req)
		if rsm.maxraftstate > 0 {
			if (float32(rsm.rf.PersistBytes()) / float32(rsm.maxraftstate)) > 0.8 {
				rsm.rf.Snapshot(cmd.CommandIndex, rsm.sm.Snapshot())
			}
		}
		rsm.Log("receive op: me=%d id=%d cmdIdx=%d", op.Me, op.Id, cmd.CommandIndex)

		if op.Me == rsm.me {
			rsm.SendResult(op.Id, resp)
		}
	}
	rsm.mu.Lock()
	rsm.Log("closing reader")
	for _, v := range rsm.pendingReqs {
		close(v)
	}
	rsm.mu.Unlock()

}

func (rsm *RSM) SendResult(reqId uint64, resp any) {
	rsm.mu.Lock()
	req, ok := rsm.pendingReqs[reqId]
	delete(rsm.pendingReqs, reqId)
	rsm.mu.Unlock()
	if !ok {
		return
	}
	go func() {
		//no need to close the channel, GC will do it
		req <- resp
	}()
}

func (rsm *RSM) Log(format string, args ...any) {
	if os.Getenv("DEBUG") != "true" {
		return
	}
	now := time.Now()
	formatted := raft.FormatTime(now)
	message := fmt.Sprintf(format, args...)
	fmt.Println(formatted, " - rsm ", rsm.me, " : ", message)
}
