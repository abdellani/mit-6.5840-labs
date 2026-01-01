package lock

import (
	"log"
	"time"

	rpc "6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	ID string
	L  string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck: ck,
		ID: kvtest.RandValue(8),
		L:  l,
	}
	// You may add code here
	return lk

}

func (lk *Lock) Acquire() {
	// Your code here

	var version rpc.Tversion = 0
	for {
		err := lk.ck.Put(lk.L, lk.ID, version)
		switch err {
		case rpc.OK:
			// lock acquired
			return
		case rpc.ErrVersion:
			for {
				value, v, err1 := lk.ck.Get(lk.L)
				if err1 == rpc.ErrNoKey {
					log.Panic("can put on non existing key")
				}
				if value != "" {
					//lock is already acquired by another lock
					time.Sleep(1 * time.Second)
					continue
				}
				version = v
				break
			}
		default:
			log.Panic("error not considered")
		}

	}
}

func (lk *Lock) Release() {
	// Your code here
	value, version, err := lk.ck.Get(lk.L)
	if value != lk.ID {
		//lock not acquired by this lock
		return
	}
	if err == rpc.ErrNoKey {
		//lock not acquired by this lock
		return
	}
	lk.ck.Put(lk.L, "", version)

}
