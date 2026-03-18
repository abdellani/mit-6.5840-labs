package shardgrp

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	raft "6.5840/raft1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ACTION_GET     = "GET"
	ACTION_PUT     = "PUT"
	ACTION_FREEZE  = "FRZ"
	ACTION_INSTALL = "INS"
	ACTION_DELETE  = "DEL"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	clientId uint64
	reqId    uint64
	leader   int
	gid      tester.Tgid
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

	defer func(reqId int) { ck.Log("done with reqid %d ", reqId) }(int(args.RequestId))
	for {
		leader := ck.GetLeaderId()
		reply := rpc.GetReply{}
		ck.Log("SEND (ri=%4d,op=%s,shard=%d,key=%-10.10s)", ck.reqId, ACTION_GET, shardcfg.Key2Shard(args.Key), args.Key)
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Get", &args, &reply)
		ck.Log("RESP (ri=%4d,op=%s,ok?=%v,reply=%v,lid=%4d,nonOkCount=%d)", ck.reqId, ACTION_GET, ok, reply.Err, ck.leader, nonOkCount)
		if ok {
			nonOkCount = 0
		} else {
			if nonOkCount > 10 {
				ck.Log("time to try another group")
				return "", 0, rpc.ErrWrongGroup
			}
			nonOkCount++
			ck.setNextNodeAsLeader()
			continue
		}
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
	defer func(reqId int) { ck.Log("done with reqid %d ", reqId) }(int(args.RequestId))
	nonOkCount := 0
	for {
		leaderId := ck.GetLeaderId()
		reply := rpc.PutReply{}
		ck.Log("SEND (ri=%4d,op=%s,shard=%d,key=%-10.10s,vrsn=%4d)", ck.reqId, ACTION_PUT, shardcfg.Key2Shard(args.Key), args.Key, args.Version)
		ok := ck.clnt.Call(ck.servers[leaderId], "KVServer.Put", &args, &reply)
		ck.Log("RESP (ri=%4d,op=%s,ok?=%v,reply=%v,lid=%4d,nonOkCount=%d)", ck.reqId, ACTION_PUT, ok, reply.Err, ck.leader, nonOkCount)
		if ok {
			nonOkCount = 0
		} else {
			if nonOkCount > 10 {
				ck.Log("time to try another group")
				return rpc.ErrWrongGroup
			}
			nonOkCount++
			retry = true
			ck.setNextNodeAsLeader()
			time.Sleep(20 * time.Millisecond)
			continue
		}
		switch reply.Err {
		case rpc.ErrWrongLeader:
			ck.setNextNodeAsLeader()
			time.Sleep(20 * time.Millisecond)
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

func (ck *Clerk) FreezeShard(ctx context.Context, s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{
		CommonClientAttributes: rpc.CommonClientAttributes{
			ClientId:  ck.clientId,
			RequestId: ck.generateRequestId(),
		},
		Shard: s,
		Num:   num,
	}

	for {
		select {
		case <-ctx.Done():
			return nil, rpc.ErrMaybe
		default:
		}
		leaderId := ck.GetLeaderId()
		reply := shardrpc.FreezeShardReply{}
		ck.Log("SEND (ri=%4d,op=%s,shard=%d,cnum=%d,lid=%4d)", ck.reqId, ACTION_FREEZE, args.Shard, num, ck.leader)
		ok := ck.clnt.Call(ck.servers[leaderId], "KVServer.FreezeShard", &args, &reply)
		ck.Log("RESP (ri=%4d,op=%s,ok?=%v,reply=%v)", ck.reqId, ACTION_FREEZE, ok, reply.Err)
		if !ok {
			ck.setNextNodeAsLeader()
			continue
		}
		switch reply.Err {
		case rpc.ErrWrongLeader:
			ck.setNextNodeAsLeader()
		case rpc.ErrVersion:
			return nil, reply.Err
		case rpc.OK:
			return reply.State, reply.Err
		case rpc.ErrNoKey:
			return nil, reply.Err
		default:
			panic(fmt.Sprintf("unexpected error :'%v' ", reply.Err))
		}
	}
}

func (ck *Clerk) InstallShard(ctx context.Context, s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
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
		select {
		case <-ctx.Done():
			return rpc.ErrMaybe
		default:
		}
		leaderId := ck.GetLeaderId()
		reply := shardrpc.InstallShardReply{}
		ck.Log("SEND (ri=%4d,op=%s,shard=%d,cnum=%d,lid=%4d)", ck.reqId, ACTION_INSTALL, args.Shard, num, ck.leader)
		ok := ck.clnt.Call(ck.servers[leaderId], "KVServer.InstallShard", &args, &reply)
		ck.Log("RESP (ri=%4d,op=%s,ok?=%v,reply=%v)", ck.reqId, ACTION_INSTALL, ok, reply.Err)
		if !ok {
			ck.setNextNodeAsLeader()
			continue
		}
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

func (ck *Clerk) DeleteShard(ctx context.Context, s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{
		CommonClientAttributes: rpc.CommonClientAttributes{
			ClientId:  ck.clientId,
			RequestId: ck.generateRequestId(),
		},
		Shard: s,
		Num:   num,
	}

	for {
		select {
		case <-ctx.Done():
			return rpc.ErrMaybe
		default:
		}
		leaderId := ck.GetLeaderId()
		reply := shardrpc.InstallShardReply{}
		ck.Log("SEND (ri=%4d,op=%s,shard=%d,cnum=%d,lid=%4d)", ck.reqId, ACTION_DELETE, args.Shard, num, ck.leader)
		ok := ck.clnt.Call(ck.servers[leaderId], "KVServer.DeleteShard", &args, &reply)
		ck.Log("RESP (ri=%4d,op=%s,ok?=%v,reply=%v)", ck.reqId, ACTION_DELETE, ok, reply.Err)
		if !ok {
			ck.setNextNodeAsLeader()
			continue
		}
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

func (ck *Clerk) DecRequestId() {
	ck.reqId--
}

func (ck *Clerk) setNextNodeAsLeader() {
	ck.leader = (ck.leader + 1) % len(ck.servers)
}
func (ck *Clerk) GetLeaderId() int {
	return ck.leader
}

func (ck *Clerk) Log(format string, args ...any) {
	// if os.Getenv("DEBUG") != "true" {
	// 	return
	// }
	now := time.Now()
	formatted := raft.FormatTime(now)
	message := fmt.Sprintf(format, args...)
	fmt.Printf("%s - ci %15d : %s \n", formatted, ck.clientId, message)
}

func (ck *Clerk) UpdateServers(gid tester.Tgid, leader int, srvs []string) {
	ck.gid = gid
	ck.leader = leader
	ck.servers = srvs
}

func (ck *Clerk) GetGid() tester.Tgid {
	return ck.gid
}
