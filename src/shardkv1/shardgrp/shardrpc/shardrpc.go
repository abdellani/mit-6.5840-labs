package shardrpc

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
)

type FreezeShardArgs struct {
	rpc.CommonClientAttributes
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

func (a FreezeShardArgs) GetNum() shardcfg.Tnum {
	return a.Num
}

type FreezeShardReply struct {
	State []byte
	Num   shardcfg.Tnum
	Err   rpc.Err
}

type InstallShardArgs struct {
	rpc.CommonClientAttributes
	Shard shardcfg.Tshid
	State []byte
	Num   shardcfg.Tnum
}

func (a InstallShardArgs) GetNum() shardcfg.Tnum {
	return a.Num
}

type InstallShardReply struct {
	Err rpc.Err
}

type DeleteShardArgs struct {
	rpc.CommonClientAttributes
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

func (a DeleteShardArgs) GetNum() shardcfg.Tnum {
	return a.Num
}

type DeleteShardReply struct {
	Err rpc.Err
}

type CommandInterface interface {
	GetNum() shardcfg.Tnum
}
