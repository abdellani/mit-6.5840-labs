package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"context"
	"fmt"
	"log"
	"math/rand"
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

	currentConf := sck.Query()
	newConfig, _ := sck.QueryNewConf()
	fmt.Printf("cc %v\n", currentConf.Shards)
	fmt.Printf("nc %v \n", newConfig.Shards)
	if currentConf.Num == newConfig.Num {
		sck.Log("cconf:%d -- nconf: %d (no work needed)", currentConf.Num, newConfig.Num)

		sck.Log("checking n+1 %d", currentConf.Num)
		target := fmt.Sprintf("%d", currentConf.Num+1)
		conf, _, err := sck.Get(target)
		sck.Log("err %v", err)
		if err == rpc.OK {
			sck.Log("calling ChangeConfigTo")
			configuration := shardcfg.FromString(conf)
			sck.ChangeConfigTo(configuration)
		}
		return
	}
	sck.Log("ccn:%d -- ncn: %d", currentConf.Num, newConfig.Num)
	sck.Log("rerun ncn %d", newConfig.Num)

	sck.UpdateShardGrp(currentConf, newConfig)
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
	sck.Log("cfg %+v", cfg.Shards)
	serializedConfig := cfg.String()
	stop := false
	for !stop {
		err := sck.Put("current", serializedConfig, 0)
		switch err {
		case rpc.OK:
			sck.Log("saved configuration %d to current", cfg.Num)
			stop = true
		case rpc.ErrMaybe:
			cfg, v, err := sck.Get("current")
			if err == rpc.OK {
				if v == 0 {
					//not saved yet
					continue
				}
				sck.Log("saved configuration to current (after errMaybe)")
				stop = true
			} else if err == rpc.ErrNoKey {
				continue
			} else {
				log.Panicf("error:%v,v:%d,cfg:%v\n", err, v, cfg)
			}
		default:
			log.Panicf("error %v", err)
		}
	}
	for {
		err := sck.Put("new", serializedConfig, 0)
		switch err {
		case rpc.OK:
			sck.Log("saved configuration %d  to new", cfg.Num)
			return
		case rpc.ErrMaybe:
			cfg, v, err := sck.Get("new")
			if err == rpc.OK {
				if v == 0 {
					continue
				}
				return
			} else if err == rpc.ErrNoKey {
				continue
			} else {
				log.Panicf("error:%v,v:%d,cfg:%v\n", err, v, cfg)
			}
		default:
			log.Panicf("error %v", err)
		}
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(newConfig *shardcfg.ShardConfig) {
	// Your code here.
	sck.Log("start:ChangeConfigTo(%d)", newConfig.Num)
	defer func() { sck.Log("done: ChangeConfigTo(%d)", newConfig.Num) }()

	serializedNewConfig1 := newConfig.String()
	err1 := sck.Put(fmt.Sprintf("%d", newConfig.Num), serializedNewConfig1, 0)
	if err1 == rpc.OK {
		fmt.Printf("save config %d (tmp)\n", newConfig.Num)
	}

	currentConfig := sck.Query()
	if newConfig.Num <= currentConfig.Num {
		sck.Log("current configuration is already at Num %d", currentConfig.Num)
		return
	}
	sck.Log("ccn:%d,config:%v", currentConfig.Num, currentConfig.Shards)
	newStoredConfig, v := sck.QueryNewConf()
	if newConfig.Num <= newStoredConfig.Num {
		sck.Log("already another process working ")
		return
	}

	sck.Log("ncn:%d,newconfig:%v", newConfig.Num, newConfig.Shards)
	sck.Log("saving cn %d (v=%d) to 'new' (old=%d)", newConfig.Num, v, newStoredConfig.Num)

	serializedNewConfig := newConfig.String()
	for {
		err := sck.Put("new", serializedNewConfig, v)

		sck.Log("save? %v", err)
		switch err {
		case rpc.OK:
			sck.UpdateShardGrp(currentConfig, newConfig)
			return
		case rpc.ErrVersion:
			sck.Log("another process is already working on cn=%d", newConfig.Num)
			return
		case rpc.ErrMaybe:
			n1, v1 := sck.QueryNewConf()
			if v1 == v {
				// not saved
				continue
			}
			if v1 < v {
				panic("should not happen!")
			}
			// maybe another server save at the same time !
			// check configuration
			if n1.Num != newConfig.Num {
				sck.Log("another server already saved new cofig  ")
				return
			}
			//same num
			// maybe same num, but a different shard configuration
			for i := range len(n1.Shards) {
				if n1.Shards[i] != newConfig.Shards[i] {
					sck.Log("another server already save ")
					return
				}
			}
			//same config, can relaunch now !!!
			sck.UpdateShardGrp(currentConfig, newConfig)
			return
		default:
			panic(err)
		}

	}

}
func (sck *ShardCtrler) UpdateShardGrp(old, new *shardcfg.ShardConfig) {
	shardsToMove := *sck.CalculateShardsToMove(old, new)
	sck.Log("Applying: oc=%d -> nc=%d", old.Num, new.Num)
	if old.Num >= new.Num {
		panic("should not decrease config num")
	}
	defer func() { sck.Log("done: oc=%d -> nc=%d", old.Num, new.Num) }()
	ctx, cancel := context.WithCancel(context.Background())
	var wl sync.WaitGroup

	wl.Add(1)
	defer wl.Wait()
	go func(cancel context.CancelFunc) {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		defer wl.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				query := sck.Query()
				if query.Num >= new.Num {
					cancel()
					sck.Log("this loop (%d->%d) is already outdated. quitting ... ", old.Num, new.Num)
					return
				}
				sck.Log("not synchronized. ")
			}
		}
	}(cancel)

	var wg sync.WaitGroup
	for _, shardToMove := range shardsToMove {
		wg.Add(1)
		go func(ctx context.Context, shardToMove shardcfg.Tshid) {
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

			state, err := clientSRC.FreezeShard(ctx, shardToMove, new.Num)
			if err == rpc.ErrNoKey {
				//considering that this key is already moved by another process
				return
			}
			if err != rpc.OK {
				sck.Log("aborting on freeze")
				return
			}
			err = clientDST.InstallShard(ctx, shardToMove, state, new.Num)
			if err != rpc.OK {
				sck.Log("aborting on install")
				return
			}
			err = clientSRC.DeleteShard(ctx, shardToMove, new.Num)
			if err != rpc.OK {
				sck.Log("aborting on delete")
				return
			}
		}(ctx, shardToMove)
	}
	wg.Wait()

	sck.Log("Saving Result: oc=%d -> nc=%d", old.Num, new.Num)
	serializedConfig := new.String()
	for {
		select {
		case <-ctx.Done():
			sck.Log("left before saving")
			return
		default:
		}

		err := sck.Put("current", serializedConfig, rpc.Tversion(old.Num))
		sck.Log("saved results %s ", err)
		switch err {
		case rpc.OK:
			sck.Log("updated cc:%d", new.Num)
			cancel()
			return
		case rpc.ErrMaybe:
			c, _, err1 := sck.Get("current")
			switch err1 {
			case rpc.OK:
				cnf := shardcfg.FromString(c)
				if cnf.Num == new.Num {
					sck.Log("current configuration saved")
					cancel()
					return
				} else if cnf.Num > new.Num {
					sck.Log("current config already have more recent configuration n saved")
					cancel()
					return
				} else {
					continue
				}
			default:
				log.Panicf("err(put):%v,err:%v", err, err1)
			}
		case rpc.ErrVersion:
			sck.Log("error:ErrVersion 'current' is already updated ")
		default:
			panic(err)
		}
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	serialized, v, err := sck.Get("current")
	switch err {
	case rpc.OK:
		return shardcfg.FromString(serialized)
	default:
		log.Panicf("err:%v,v:%d failed\n", err, v)
	}
	return nil
}

func (sck *ShardCtrler) QueryNewConf() (*shardcfg.ShardConfig, rpc.Tversion) {
	// Your code here.
	serialized, v, err := sck.Get("new")
	switch err {
	case rpc.OK:
		return shardcfg.FromString(serialized), v
	default:
		log.Panicf("err:%v,v:%d failed\n", err, v)
	}
	return nil, 0
}

func (sck *ShardCtrler) IsCurrentConfigSync() bool {
	currentConf := sck.Query()
	newConf, _ := sck.QueryNewConf()
	sck.Log("cc:%d,nc:%d", currentConf.Num, newConf.Num)
	return currentConf.Num == newConf.Num
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
	// if os.Getenv("DEBUG") != "true" {
	// 	return
	// }
	now := time.Now()
	formatted := raft.FormatTime(now)
	message := fmt.Sprintf(format, args...)
	fmt.Printf("%s - sh %5d : %s \n", formatted, sck.me, message)

}
