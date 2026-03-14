package kvraft

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	raft "6.5840/raft1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	clientId uint64
	reqId    uint64
	leader   int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	var b [8]byte
	_, err := rand.Read(b[:])
	if err != nil {
		panic(err)
	}

	clientId := binary.LittleEndian.Uint64(b[:])
	ck.clientId = clientId
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	args := rpc.GetArgs{
		CommonKVCommandsAttributes: rpc.CommonKVCommandsAttributes{
			CommonClientAttributes: rpc.CommonClientAttributes{
				ClientId:  ck.clientId,
				RequestId: ck.generateRequestId(),
			},
			Key: key,
		},
	}
	defer func(reqId int) { ck.Log("cid %d done with reqid %d ", ck.clientId, reqId) }(int(args.RequestId))
	for {
		leader := ck.getLeaderId()
		reply := rpc.GetReply{}
		ck.Log(" sending  rid: %d -> op: get", ck.reqId)
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Get", &args, &reply)
		if !ok {
			ck.setNextNodeAsLeader()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ck.Log(" received rid: %d -> op: get,  response %v", ck.reqId, reply)
		switch reply.Err {
		case rpc.ErrWrongLeader:
			ck.setNextNodeAsLeader()
		case rpc.OK, rpc.ErrNoKey:
			return reply.Value, reply.Version, reply.Err
		default:
			panic(fmt.Sprintf("unexpected error '%v' (clId=%d, redId=%d)", reply.Err, args.ClientId, args.RequestId))
		}
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
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.

	args := rpc.PutArgs{
		CommonKVCommandsAttributes: rpc.CommonKVCommandsAttributes{
			CommonClientAttributes: rpc.CommonClientAttributes{
				ClientId:  ck.clientId,
				RequestId: ck.generateRequestId(),
			},
			Key: key,
		},
		Value:   value,
		Version: version,
	}
	retry := false
	defer func(reqId int) { ck.Log("cid %d done with reqid %d ", ck.clientId, reqId) }(int(args.RequestId))

	for {
		leaderId := ck.getLeaderId()
		reply := rpc.PutReply{}
		ck.Log("sending  rid: %d -> op: put", ck.reqId)
		ok := ck.clnt.Call(ck.servers[leaderId], "KVServer.Put", &args, &reply)
		if !ok {
			retry = true
			ck.setNextNodeAsLeader()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ck.Log("received rid: %d -> op: put,  response %v", ck.reqId, reply.Err)

		switch reply.Err {
		case rpc.ErrWrongLeader:
			ck.setNextNodeAsLeader()
		case rpc.ErrVersion:
			if retry {
				return rpc.ErrMaybe
			} else {
				return reply.Err
			}
		case rpc.OK:
			return rpc.OK
		default:
			panic(fmt.Sprintf("unexpected error :'%v' ", reply.Err))
		}
	}

}

func (ck *Clerk) generateRequestId() uint64 {
	return atomic.AddUint64(&ck.reqId, 1)
}

func (ck *Clerk) setNextNodeAsLeader() {
	ck.leader = (ck.leader + 1) % len(ck.servers)
}
func (ck *Clerk) getLeaderId() int {
	return ck.leader
}

func (ck *Clerk) Log(format string, args ...any) {
	if os.Getenv("DEBUG") != "true" {
		return
	}
	now := time.Now()
	formatted := raft.FormatTime(now)
	message := fmt.Sprintf(format, args...)
	fmt.Println(formatted, " - ci :", ck.clientId, " : ", message)
}
