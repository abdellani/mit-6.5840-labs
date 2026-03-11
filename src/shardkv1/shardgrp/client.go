package shardgrp

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
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

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}

	var b [8]byte
	_, err := rand.Read(b[:])
	if err != nil {
		panic(err)
	}

	clientId := binary.LittleEndian.Uint64(b[:])
	ck.clientId = clientId
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	args := rpc.GetArgs{
		Args: rpc.Args{
			ClientId:  ck.clientId,
			RequestId: ck.generateRequestId(),
		},
		Key: key,
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

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	args := rpc.PutArgs{
		Args: rpc.Args{
			ClientId:  ck.clientId,
			RequestId: ck.generateRequestId(),
		},
		Key:     key,
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

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	return nil, ""
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
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
