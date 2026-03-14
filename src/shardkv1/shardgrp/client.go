package shardgrp

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	raft "6.5840/raft1"
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
		CommonKVCommandsAttributes: rpc.CommonKVCommandsAttributes{
			CommonClientAttributes: rpc.CommonClientAttributes{
				ClientId:  ck.clientId,
				RequestId: ck.generateRequestId(),
			},
			Key: key,
		},
	}
	nonOkCount := 0

	defer func(reqId int) { ck.Log("cid %d done with reqid %d ", ck.clientId, reqId) }(int(args.RequestId))
	for {
		// fmt.Println("Get")
		leader := ck.getLeaderId()
		reply := rpc.GetReply{}
		ck.Log(" sending  rid: %d -> op: get", ck.reqId)
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Get", &args, &reply)
		// fmt.Printf("GET : err  %v  ok? %v nonOkCount %d\n", reply.Err, ok, nonOkCount)
		if nonOkCount > 10 {
			// fmt.Println("time to try another group")
			return "", 0, rpc.ErrWrongGroup
		}
		if !ok {
			nonOkCount++
			ck.setNextNodeAsLeader()
			continue
		}
		ck.Log(" received rid: %d -> op: get,  response %v", ck.reqId, reply)
		switch reply.Err {
		case rpc.ErrWrongLeader:
			ck.setNextNodeAsLeader()
		case rpc.OK, rpc.ErrNoKey, rpc.ErrWrongGroup:
			return reply.Value, reply.Version, reply.Err
		default:
			panic(fmt.Sprintf("unexpected error '%v' (clId=%d, redId=%d)", reply.Err, args.ClientId, args.RequestId))
		}
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
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
	nonOkCount := 0
	for {
		// fmt.Println("PUT")
		leaderId := ck.getLeaderId()
		reply := rpc.PutReply{}
		ck.Log("sending  rid: %d -> op: put", ck.reqId)
		ok := ck.clnt.Call(ck.servers[leaderId], "KVServer.Put", &args, &reply)
		// fmt.Printf("PUT : err  %v  ok? %v nonOkCount %d \n", reply.Err, ok, nonOkCount)
		if nonOkCount > 10 {
			// fmt.Println("time to try another group")
			return rpc.ErrWrongGroup
		}
		if !ok {
			nonOkCount++
			retry = true
			ck.setNextNodeAsLeader()
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
		case rpc.OK, rpc.ErrMaybe, rpc.ErrWrongGroup:
			return reply.Err
		default:
			panic(fmt.Sprintf("unexpected error :'%v' ", reply.Err))
		}
	}

}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	args := shardrpc.FreezeShardArgs{
		CommonClientAttributes: rpc.CommonClientAttributes{
			ClientId:  ck.clientId,
			RequestId: ck.generateRequestId(),
		},
		Shard: s,
		Num:   num,
	}

	for {
		leaderId := ck.getLeaderId()
		reply := shardrpc.FreezeShardReply{}
		ck.Log("sending  rid: %d -> op: put", ck.reqId)
		ok := ck.clnt.Call(ck.servers[leaderId], "KVServer.FreezeShard", &args, &reply)
		fmt.Printf("FreezeShards request (ci=%d,ri=%d,ok?=%v,reply=%v,lid=%d)\n", ck.clientId, ck.reqId, ok, reply.Err, ck.leader)
		if !ok {
			ck.setNextNodeAsLeader()
			continue
		}
		ck.Log("received rid: %d -> op: put,  response %v", ck.reqId, reply.Err)

		switch reply.Err {
		case rpc.ErrWrongLeader:
			time.Sleep(20 * time.Millisecond)
			ck.setNextNodeAsLeader()
		case rpc.ErrVersion:
			return nil, reply.Err
		case rpc.OK:
			return reply.State, reply.Err
		default:
			panic(fmt.Sprintf("unexpected error :'%v' ", reply.Err))
		}
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.InstallShardArgs{
		CommonClientAttributes: rpc.CommonClientAttributes{
			ClientId:  ck.clientId,
			RequestId: ck.generateRequestId(),
		},
		Shard: s,
		State: state,
		Num:   num,
	}

	for {
		// fmt.Println("InstallShard")
		leaderId := ck.getLeaderId()
		reply := shardrpc.InstallShardReply{}
		ck.Log("sending  rid: %d -> op: IS", ck.reqId)
		ok := ck.clnt.Call(ck.servers[leaderId], "KVServer.InstallShard", &args, &reply)
		fmt.Printf("InstallShards request (ci=%d,ri=%d,ok?=%v,reply=%v,lid=%d)\n", ck.clientId, ck.reqId, ok, reply.Err, ck.leader)
		if !ok {
			ck.setNextNodeAsLeader()
			continue
		}
		ck.Log("received rid: %d -> op: IS,  response %v", ck.reqId, reply.Err)

		switch reply.Err {
		case rpc.ErrWrongLeader:
			time.Sleep(20 * time.Millisecond)
			ck.setNextNodeAsLeader()
		case rpc.OK, rpc.ErrVersion:
			return reply.Err
		default:
			panic(fmt.Sprintf("unexpected error :'%v' ", reply.Err))
		}
	}

}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{
		CommonClientAttributes: rpc.CommonClientAttributes{
			ClientId:  ck.clientId,
			RequestId: ck.generateRequestId(),
		},
		Shard: s,
		Num:   num,
	}

	for {
		// fmt.Println("DeleteShard")
		leaderId := ck.getLeaderId()
		reply := shardrpc.InstallShardReply{}
		ck.Log("sending  rid: %d -> op: DEL", ck.reqId)
		ok := ck.clnt.Call(ck.servers[leaderId], "KVServer.DeleteShard", &args, &reply)
		fmt.Printf("DeleteShards request (ci=%d,ri=%d,ok?=%v,reply=%v,lid=%d)\n", ck.clientId, ck.reqId, ok, reply.Err, ck.leader)
		if !ok {
			ck.setNextNodeAsLeader()
			continue
		}
		ck.Log("received rid: %d -> op: IS,  response %v", ck.reqId, reply.Err)

		switch reply.Err {
		case rpc.ErrWrongLeader:
			time.Sleep(20 * time.Millisecond)
			ck.setNextNodeAsLeader()
		case rpc.OK, rpc.ErrVersion:
			return reply.Err
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
