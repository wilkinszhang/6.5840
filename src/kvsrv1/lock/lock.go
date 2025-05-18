package lock

import (
	"6.5840/kvtest1"
	"time"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// 锁的唯一标识符
	lockId string
	// 锁的键名
	lockKey string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:      ck,
		lockId:  kvtest.RandValue(8),
		lockKey: l,
	}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		// 尝试获取锁
		value, version, err := lk.ck.Get(lk.lockKey)
		if err == rpc.ErrNoKey {
			// 锁不存在，尝试创建
			err = lk.ck.Put(lk.lockKey, lk.lockId, 0)
			if err == rpc.OK {
				return
			}
		} else if err == rpc.OK {
			// 锁存在，检查是否被自己持有
			if value == lk.lockId {
				return
			}
		}
		// 等待一段时间后重试
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	for {
		// 获取当前锁的状态
		value, version, err := lk.ck.Get(lk.lockKey)
		if err == rpc.OK && value == lk.lockId {
			// 只有锁被自己持有时才释放
			err = lk.ck.Put(lk.lockKey, "", version)
			if err == rpc.OK {
				return
			}
		}
		// 等待一段时间后重试
		time.Sleep(100 * time.Millisecond)
	}
}
