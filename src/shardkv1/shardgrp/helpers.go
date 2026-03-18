package shardgrp

import (
	"fmt"
	"os"
	"time"

	"6.5840/kvsrv1/rpc"
	raft "6.5840/raft1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
)

func (kv *KVServer) _canKeyBeProcessOnThisShardGroup(command rpc.CommonKVCommandsInterface) bool {
	shardId := shardcfg.Key2Shard(command.GetKey())
	switch kv._shardStatus(shardId) {
	case SHARD_ALLOWED:
		return true
	case SHARD_FROZEN:
		_, ok := command.(rpc.GetArgs)
		if ok {
			return true
		}
		_, ok = command.(rpc.PutArgs)
		if ok {
			return false
		}
		panic("unexpected command")
	case SHARD_DELETED:
		return false
	default:
		panic("unexpected shard status")
	}
}

func (kv *KVServer) _shardStatus(shid shardcfg.Tshid) ShardStatus {
	status, ok := kv.Status[shid]
	if !ok {
		return SHARD_DELETED
	}
	return status
}

func (kv *KVServer) _isCommandRecentlyExecuted(args rpc.CommonClientCommandsInterface) bool {
	switch op := args.(type) {
	case rpc.CommonKVCommandsInterface:
		shrdCache, ok := kv._getShardCache(op.GetKey())
		if !ok {
			return false
		}
		return shrdCache._isCommandRecentlyExecuted(op)
	case rpc.CommonClientCommandsInterface:
		return args.GetRequestId() == kv.LastClientsReqs[args.GetClientId()]
	}
	panic("shoud not reach this !")
}

// TODO move the repetetive logic related to choosing function depending on type of args
func (kv *KVServer) _getCachedResponse(args rpc.CommonClientCommandsInterface) any {
	switch op := args.(type) {
	case rpc.CommonKVCommandsInterface:
		shrdCache, ok := kv._getShardCache(op.GetKey())
		if !ok {
			panic("trying to access shrdCache that doesn't exist")
		}
		return shrdCache._getCachedResponse(op)
	case rpc.CommonClientCommandsInterface:
		response, ok := kv.LastClientsResp[op.GetClientId()]
		if !ok {
			panic("trying to read empty key on cache")
		}
		return response
	}
	panic("unkown command type")
}

func (kv *KVServer) _isCommandTooOld(args rpc.CommonClientCommandsInterface) bool {
	switch op := args.(type) {
	case rpc.CommonKVCommandsInterface:
		shrdCache, ok := kv._getShardCache(op.GetKey())
		if !ok {
			panic("trying to access shrdcache that doesn't exist")
		}
		return shrdCache._isCommandTooOld(op)
	case rpc.CommonClientCommandsInterface:
		return op.GetRequestId() < kv.LastClientsReqs[op.GetClientId()]
	default:
		panic("unrecognized command")
	}

}

func (kv *KVServer) _saveReqIdAndResponse(args rpc.CommonClientCommandsInterface, response any) {
	switch op := args.(type) {
	case rpc.CommonKVCommandsInterface:
		shrdkv, ok := kv._getShardCache(op.GetKey())
		if !ok {
			panic("trying to access shrdCache that doesn't exist")
		}
		shrdkv._saveReqIdAndResponse(op, response)
		return
	case rpc.CommonClientCommandsInterface:
		kv.LastClientsReqs[op.GetClientId()] = op.GetRequestId()
		kv.LastClientsResp[op.GetClientId()] = response
		return
	}
	panic("cmd not recognized")
}
func (kv *KVServer) _getShardKV(key string) (*KV, bool) {
	shardId := shardcfg.Key2Shard(key)
	shrdkv, ok := kv.Shrdskv[shardId]
	return shrdkv, ok
}

func (kv *KVServer) _getShardCache(key string) (*Cache, bool) {
	shardId := shardcfg.Key2Shard(key)
	shrdCache, ok := kv.ShardsCache[shardId]
	return shrdCache, ok
}

func (kv *KVServer) _isConfigCommandOutdates(command shardrpc.CommandInterface) bool {
	return kv.ConfigNum > command.GetNum()
}

func (kv *KVServer) Log(format string, args ...any) {
	if os.Getenv("DEBUG") != "true" {
		return
	}
	now := time.Now()
	formatted := raft.FormatTime(now)
	message := fmt.Sprintf(format, args...)
	fmt.Printf("%s - gid %2d srv %2d : %s\n", formatted, kv.gid, kv.me, message)

}

func (kv *KVServer) _updateConfigNum(num shardcfg.Tnum) {
	if num < kv.ConfigNum {
		panic("should not decrease config num")
	}
	if kv.ConfigNum < num {
		fmt.Println("updating config to ", num)
	}
	kv.ConfigNum = num
}
