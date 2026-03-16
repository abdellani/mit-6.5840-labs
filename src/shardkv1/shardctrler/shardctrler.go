package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"fmt"
	"log"
	"sync"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	mu    sync.Mutex
	clnts map[tester.Tgid]*shardgrp.Clerk
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	sck.clnts = make(map[tester.Tgid]*shardgrp.Clerk)
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	for {
		currentConf := sck.Query()
		newConfigString, _, err := sck.Get("new")

		if err == rpc.ErrNoKey {
			return
		}
		newConfig := shardcfg.FromString(newConfigString)

		if currentConf.Num == newConfig.Num {
			return
		}
		fmt.Printf("currentConf.Num %d -- newConfig.Num %d\n", currentConf.Num, newConfig.Num)

		fmt.Printf("rerun config %d\n", newConfig.Num)
		sck.UpdateShardGrp(currentConf, newConfig)

	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	fmt.Println("Init config")
	serializedConfig := cfg.String()
	for {
		err := sck.Put("current", serializedConfig, 0)
		switch err {
		case rpc.OK:
			return
		case rpc.ErrMaybe:
			_, _, err := sck.Get("current")
			if err == rpc.OK {
				return
			}
			fmt.Println("retrying ...")
			continue
		default:
			log.Panicf("error %v", err)
		}
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	old := sck.Query()
	_, v, err := sck.Get("new")
	serializedConfig := new.String()
	fmt.Printf("saving configNum %d to 'new'\n", new.Num)
	switch err {
	case rpc.ErrNoKey:
		err = sck.Put("new", serializedConfig, 0)
		fmt.Printf("err %v\n", err)
	case rpc.OK:
		err = sck.Put("new", serializedConfig, v)
		fmt.Printf("err %v\n", err)
	default:
		panic(err)
	}

	sck.UpdateShardGrp(old, new)

}
func (sck *ShardCtrler) UpdateShardGrp(old, new *shardcfg.ShardConfig) {
	shardsToMove := *sck.CalculateShardsToMove(old, new)
	fmt.Printf("Applying: old config=%d --> new config=%d\n", old.Num, new.Num)
	defer func() { fmt.Printf("done with config num=%d\n", new.Num) }()
	var wg sync.WaitGroup
	for _, shardToMove := range shardsToMove {
		wg.Add(1)
		go func(shardToMove shardcfg.Tshid) {
			defer wg.Done()
			_, srvsSRC, ok := old.GidServers(shardToMove)
			if !ok {
				panic("error retrieving shard's details from old configuration_")
			}

			_, srvsDST, ok := new.GidServers(shardToMove)
			if !ok {
				panic("error retrieving shard's details from new configuration")
			}

			clientSRC := shardgrp.MakeClerk(sck.clnt, srvsSRC)
			clientDST := shardgrp.MakeClerk(sck.clnt, srvsDST)

			state, err := clientSRC.FreezeShard(shardToMove, new.Num)
			if err != rpc.OK {
				panic("failed to freeze")
			}
			err = clientDST.InstallShard(shardToMove, state, new.Num)
			if err != rpc.OK {
				panic("failed to install")
			}
			err = clientSRC.DeleteShard(shardToMove, new.Num)
			if err != rpc.OK {
				log.Panicf("failed to delete (err = %v)", err)
			}
		}(shardToMove)
	}
	wg.Wait()
	serializedConfig := new.String()
	for {
		err := sck.Put("current", serializedConfig, rpc.Tversion(old.Num))
		switch err {
		case rpc.OK:
			return
		case rpc.ErrMaybe:
			rec, v, _ := sck.Get("current")
			if v == rpc.Tversion(old.Num)+1 {
				fmt.Println("ErrMaybe, retrying ...")
				received := shardcfg.FromString(rec)
				fmt.Printf("old %+v\n", old)
				fmt.Printf("received %+v\n", received)
				fmt.Printf("v = %d", v)
				fmt.Printf("new %+v\n", new)
				return
			}
			fmt.Println("retrying")
		}
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	serialized, _, err := sck.Get("current")
	switch err {
	case rpc.OK:
		return shardcfg.FromString(serialized)
	default:
		fmt.Println("error")
		return nil
	}
}

func (sck *ShardCtrler) CalculateShardsToMove(old, new *shardcfg.ShardConfig) *[]shardcfg.Tshid {
	result := []shardcfg.Tshid{}
	for i := 0; i < len(old.Shards); i++ {
		if old.Shards[i] == new.Shards[i] {
			continue
		}
		result = append(result, shardcfg.Tshid(i))
	}
	return &result
}
