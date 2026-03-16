package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	raft "6.5840/raft1"
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
	me    int64
	mu    sync.Mutex
	clnts map[tester.Tgid]*shardgrp.Clerk
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{
		clnt: clnt,
		me:   (rand.Int63() % 10000),
	}
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
	sck.Log("InitController")
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
		sck.Log("ccn:%d -- ncn: %d", currentConf.Num, newConfig.Num)
		sck.Log("rerun ncn %d\n", newConfig.Num)
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
	sck.Log("start: Init config")
	defer func() { sck.Log("done: Init config") }()

	serializedConfig := cfg.String()
	for {
		err := sck.Put("current", serializedConfig, 0)
		switch err {
		case rpc.OK:
			sck.Log("saved configuration %d", cfg.Num)
			return
		case rpc.ErrMaybe:
			_, _, err := sck.Get("current")
			if err == rpc.OK {
				return
			}
			fmt.Println("retrying to save current configration")
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
	sck.Log("start:ChangeConfigTo(%d)", new.Num)
	defer func() { sck.Log("done: ChangeConfigTo(%d)", new.Num) }()
	sck.Log("saving cn %d to 'new'", new.Num)
	old := sck.Query()
	_, v, err := sck.Get("new")
	serializedConfig := new.String()

	switch err {
	case rpc.ErrNoKey:
		err = sck.Put("new", serializedConfig, 0)
	case rpc.OK:
		err = sck.Put("new", serializedConfig, v)
	default:
		panic(err)
	}
	sck.Log("save? %v", err)
	sck.UpdateShardGrp(old, new)

}
func (sck *ShardCtrler) UpdateShardGrp(old, new *shardcfg.ShardConfig) {
	shardsToMove := *sck.CalculateShardsToMove(old, new)
	sck.Log("Applying: oc=%d -> nc=%d", old.Num, new.Num)
	defer func() { sck.Log("done: oc=%d -> nc=%d", old.Num, new.Num) }()
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
	sck.Log("Saving Result: oc=%d -> nc=%d", old.Num, new.Num)
	serializedConfig := new.String()
	for {
		err := sck.Put("current", serializedConfig, rpc.Tversion(old.Num))
		switch err {
		case rpc.OK:
			return
		case rpc.ErrMaybe:
			rec, v, _ := sck.Get("current")
			if v == rpc.Tversion(old.Num)+1 {
				shardcfg.FromString(rec)
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
		fmt.Println("Query failed")
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

func (sck *ShardCtrler) Log(format string, args ...any) {
	if os.Getenv("DEBUG") != "true" {
		return
	}
	now := time.Now()
	formatted := raft.FormatTime(now)
	message := fmt.Sprintf(format, args...)
	fmt.Printf("%s - sh %5d : %s \n", formatted, sck.me, message)

}
