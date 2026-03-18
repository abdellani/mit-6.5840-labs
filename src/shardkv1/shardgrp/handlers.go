package shardgrp

import (
	"bytes"

	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
)

func (kv *KVServer) _handleGet(op *rpc.GetArgs) *rpc.GetReply {
	shardkv, ok := kv._getShardKV(op.Key)
	if !ok {
		panic("trying to access a shard that doesn't exist")
	}
	return shardkv._getHandler(op)
}

func (kv *KVServer) _handlePut(op *rpc.PutArgs) *rpc.PutReply {
	shardkv, ok := kv._getShardKV(op.Key)
	if !ok {
		panic("trying to access a shard that doesn't exist")
	}
	return shardkv._putHandler(op)
}

func (kv *KVServer) _handleFreeze(op *shardrpc.FreezeShardArgs) *shardrpc.FreezeShardReply {
	status, ok := kv.Status[op.Shard]
	kv.Log("freezing shard (shrd=%d,num=%d) on (gid=%d,sid=%d,cn=%d,ok?=%v)", op.Shard, op.Num, kv.gid, kv.me, kv.ConfigNum, ok)

	if !ok {
		// panic("trying to freeze a shard that doesn't exist")
		return &shardrpc.FreezeShardReply{
			State: []byte{},
			Num:   kv.ConfigNum,
			Err:   rpc.ErrNoKey,
		}
	}

	if status == SHARD_FROZEN {
		return &shardrpc.FreezeShardReply{
			State: kv._encodeDataRelatedToShard(op.Shard),
			Num:   kv.ConfigNum,
			Err:   rpc.OK,
		}
	}

	kv.Status[op.Shard] = SHARD_FROZEN
	return &shardrpc.FreezeShardReply{
		State: kv._encodeDataRelatedToShard(op.Shard),
		Num:   kv.ConfigNum,
		Err:   rpc.OK,
	}
}

func (kv *KVServer) _handleInstall(op *shardrpc.InstallShardArgs) *shardrpc.InstallShardReply {
	_, ok := kv.Shrdskv[op.Shard]
	if ok {
		// log.Panicf("trying to install a shard that's already installed shrd=%d on (gid=%d,sid=%d) \n", op.Shard, kv.gid, kv.me)
		return &shardrpc.InstallShardReply{
			Err: rpc.OK,
		}
	}
	kv.Log("installing shard  (shrd=%d,num=%d) on (gid=%d,sid=%d,cn=%d)", op.Shard, op.Num, kv.gid, kv.me, kv.ConfigNum)

	shardKV, shardCache := kv._decodeShardKV(op.State)
	kv.Shrdskv[op.Shard] = shardKV
	kv.ShardsCache[op.Shard] = shardCache
	kv.Status[op.Shard] = SHARD_ALLOWED
	return &shardrpc.InstallShardReply{
		Err: rpc.OK,
	}
}

func (kv *KVServer) _handleDelete(op *shardrpc.DeleteShardArgs) *shardrpc.DeleteShardReply {
	status, ok := kv.Status[op.Shard]
	if !ok {
		return &shardrpc.DeleteShardReply{
			Err: rpc.OK,
		}
	}

	if status != SHARD_FROZEN {
		panic("trying to delete a shard that's not frozen")
	}
	kv.Log("Deleting a shard  (shrd=%d,num=%d) on (gid=%d,sid=%d,cn=%d)", op.Shard, op.Num, kv.gid, kv.me, kv.ConfigNum)

	delete(kv.Status, op.Shard)
	delete(kv.Shrdskv, op.Shard)
	delete(kv.ShardsCache, op.Shard)
	return &shardrpc.DeleteShardReply{
		Err: rpc.OK,
	}
}

func (kv *KVServer) _decodeShardKV(state []byte) (*KV, *Cache) {
	if len(state) < 1 {
		panic("empty state")
	}
	shardKV := KV{}
	shardCache := Cache{}
	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)

	if d.Decode(&shardKV) != nil ||
		d.Decode(&shardCache) != nil {
		panic("error decoding shardkv")
	}
	return &shardKV, &shardCache
}

func (kv *KVServer) _encodeDataRelatedToShard(shardId shardcfg.Tshid) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	data, ok := kv.Shrdskv[shardId]
	if !ok {
		panic("trying access data store that doesn't exist")
	}
	cache, ok := kv.ShardsCache[shardId]
	if !ok {
		panic("trying access data cache that doesn't exist")
	}
	if err := e.Encode(data); err != nil {
		panic("err")
	}
	if err := e.Encode(cache); err != nil {
		panic("failed to encode kv state")
	}
	return w.Bytes()
}

func (kv *KV) _getHandler(op *rpc.GetArgs) *rpc.GetReply {
	rs := rpc.GetReply{}
	v, ok := kv.Store[op.Key]
	if !ok {
		rs.Err = rpc.ErrNoKey
	} else {
		rs.Value = v.Value
		rs.Version = rpc.Tversion(v.Version)
		rs.Err = rpc.OK
	}
	return &rs
}

func (kv *KV) _putHandler(op *rpc.PutArgs) *rpc.PutReply {
	rs := rpc.PutReply{}
	v, ok := kv.Store[op.Key]
	if ok {
		if v.Version == uint64(op.Version) {
			v.Value = op.Value
			v.Version++
			kv.Store[op.Key] = v
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
			kv.Store[op.Key] = newEntry
			rs.Err = rpc.OK
		}
	}
	return &rs
}
