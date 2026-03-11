package shardgrp

import "6.5840/kvsrv1/rpc"

func (kv *KVServer) GetHandler(op *rpc.GetArgs) *rpc.GetReply {
	rs := rpc.GetReply{}
	v, ok := kv.store[op.Key]
	if !ok {
		rs.Err = rpc.ErrNoKey
	} else {
		rs.Value = v.Value
		rs.Version = rpc.Tversion(v.Version)
		rs.Err = rpc.OK
	}
	return &rs
}

func (kv *KVServer) PutHandler(op *rpc.PutArgs) *rpc.PutReply {
	rs := rpc.PutReply{}
	v, ok := kv.store[op.Key]
	if ok {
		if v.Version == uint64(op.Version) {
			v.Value = op.Value
			v.Version++
			kv.store[op.Key] = v
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
			kv.store[op.Key] = newEntry
			rs.Err = rpc.OK
		}
	}
	return &rs
}
