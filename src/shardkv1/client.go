package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"fmt"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	// You will have to modify this struct.
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	// You'll have to add code here.
	return ck
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	for {
		clnt := ck.getShrdGrpClient(key)
		value, version, err := clnt.Get(key)
		if err == rpc.ErrWrongGroup {
			fmt.Println("wrong group, trying again")
			continue
		}
		return value, version, err
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	for {
		clnt := ck.getShrdGrpClient(key)
		err := clnt.Put(key, value, version)
		switch err {
		case rpc.ErrWrongGroup:
			fmt.Println("wrong group, trying again")
			continue
		default:
			return err
		}

	}
}

func (ck *Clerk) getShrdGrpClient(key string) *shardgrp.Clerk {
	shrd := shardcfg.Key2Shard(key)
	shrdConfig := ck.sck.Query()
	_, srvs, ok := shrdConfig.GidServers(shrd)
	if !ok {
		panic("shard not found on configuration")
	}
	return shardgrp.MakeClerk(ck.clnt, srvs)
}
