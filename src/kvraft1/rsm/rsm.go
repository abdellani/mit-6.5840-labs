package rsm

import (
	"log"
	"math/rand/v2"
	"sync"

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
	Id  int
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
	pendingReqs []Req
}

type Req struct {
	term  int
	index int
	id    int
	resp  chan any
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
		pendingReqs:  make([]Req, 0),
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
	reqId := int(rand.Float64() * 1000_000_000)
	op := Op{
		Me:  rsm.me,
		Id:  reqId,
		Req: req,
	}
	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}
	resp := make(chan any, 1)

	rsm.mu.Lock()
	rsm.pendingReqs = append(rsm.pendingReqs, Req{
		id:    reqId,
		term:  term,
		index: index,
		resp:  resp,
	})
	rsm.mu.Unlock()
	rsm.Log("submit cmd id:=%d", reqId)
	result, ok := <-resp

	if !ok {
		rsm.Log("cmd failed id=%d", reqId)
		return rpc.ErrWrongLeader, nil
	}
	rsm.Log("received resp for cmd id:=%d", reqId)
	return rpc.OK, result
}

func (rsm *RSM) Reader() {
	for cmd := range rsm.applyCh {
		op, ok := cmd.Command.(Op)
		if !ok {
			log.Panic("failed casting command")
		}
		resp := rsm.sm.DoOp(op.Req)
		rsm.Log("receive op: me=%d id=%d cmdIdx=%d", op.Me, op.Id, cmd.CommandIndex)

		if op.Me == rsm.me {
			rsm.SendResult(op.Id, resp)
		}
		term, _ := rsm.Raft().GetState()
		rsm.RemovePendingReqsLessThenIndexOrTerm(cmd.CommandIndex, term)

	}
	rsm.mu.Lock()
	for _, req := range rsm.pendingReqs {
		close(req.resp)
	}
	rsm.mu.Unlock()

}

func (rsm *RSM) SendResult(reqId int, resp any) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	for _, req := range rsm.pendingReqs {
		if req.id != reqId {
			continue
		}
		req.resp <- resp
		// req will be removed on RemovePendingReqsLessThenIndexOrTerm
	}
}

func (rsm *RSM) RemovePendingReqsLessThenIndexOrTerm(index, term int) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	updatedPendingReqsList := rsm.pendingReqs[:0]
	for _, req := range rsm.pendingReqs {
		if req.index <= index ||
			req.term < term {
			close(req.resp)
			continue
		}
		updatedPendingReqsList = append(updatedPendingReqsList, req)
	}

	rsm.pendingReqs = updatedPendingReqsList
}

func (rsm *RSM) Log(format string, args ...any) {
	// if os.Getenv("DEBUG") != "true" {
	// 	return
	// }
	// now := time.Now()
	// formatted := raft.FormatTime(now)
	// message := fmt.Sprintf(format, args...)
	// fmt.Println(formatted, " - ", rsm.me, " : ", message)
}
