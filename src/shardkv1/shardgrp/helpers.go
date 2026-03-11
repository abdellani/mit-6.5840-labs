package shardgrp

import (
	"fmt"
	"os"
	"time"

	"6.5840/kvsrv1/rpc"
	raft "6.5840/raft1"
)

func (kv *KVServer) _isCommandRecentlyExecuted(arg rpc.ArgsInterface) bool {
	v, ok := kv.lastClientsReqs[arg.GetClientId()]
	if !ok {
		return false
	}
	return v == arg.GetRequestId()
}
func (kv *KVServer) _getCachedResponse(arg rpc.ArgsInterface) any {
	v, ok := kv.lastClientsReps[arg.GetClientId()]
	if !ok {
		panic("tried to retrieve unaccessible key")
	}
	return v
}

func (kv *KVServer) _isCommandTooOld(arg rpc.ArgsInterface) bool {
	v, ok := kv.lastClientsReqs[arg.GetClientId()]
	if !ok {
		return false
	}
	return arg.GetRequestId() < v
}

func (kv *KVServer) _saveReqIdAndResponse(args rpc.ArgsInterface, response any) {
	kv._updateRecentReqId(args)
	kv._updateResponse(args, response)
}

func (kv *KVServer) _updateRecentReqId(arg rpc.ArgsInterface) {
	kv.lastClientsReqs[arg.GetClientId()] = arg.GetRequestId()
}

func (kv *KVServer) _updateResponse(args rpc.ArgsInterface, response any) {
	kv.lastClientsReps[args.GetClientId()] = response
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
