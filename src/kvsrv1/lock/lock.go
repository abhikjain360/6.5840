package lock

import (
	"fmt"
	"math/rand/v2"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck   kvtest.IKVClerk
	l    string
	lock string
}

type state string

const (
	free state = "free"
)

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, l: l}
	return lk
}

func (lk *Lock) Acquire() {
	locked := fmt.Sprintf("%d", rand.Uint64())

	for {
		val, version, err := lk.ck.Get(lk.l)

		if err == rpc.ErrNoKey || val == string(free) {
			err := lk.ck.Put(lk.l, string(locked), version)
			if err == rpc.OK {
				break
			}
			continue
		}

		if val == locked {
			break
		}
	}

	lk.lock = locked
}

func (lk *Lock) Release() {
	val, version, _ := lk.ck.Get(lk.l)
	if val != lk.lock {
		return
	}

	if err := lk.ck.Put(lk.l, string(free), version); err != rpc.OK && err != rpc.ErrMaybe {
		panic(fmt.Sprintf("put erred when trying to release: %v\n", err))
	}
}
