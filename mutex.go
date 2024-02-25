package redisson

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"runtime"
	"strconv"
	"sync"
	"time"
)

var mutexScript = struct {
	lockScript    string
	renewalScript string
	unlockScript  string
}{}

type Mutex struct {
	ctx         context.Context
	redisClient *redis.Client
	lockName    string
	instanceID  string

	pubSubCount int
	pubSubMux   sync.Mutex
	pubSub      *redis.PubSub

	expiration  time.Duration
	waitTimeout time.Duration

	freeCh chan struct{}

	closeOnce sync.Once
	closed    bool
}

func NewMutex(ctx context.Context, redisClient *redis.Client, instanceID string, lockName string) (*Mutex, error) {
	mutex := &Mutex{
		ctx:         ctx,
		redisClient: redisClient,
		lockName:    lockName,
		instanceID:  instanceID,

		pubSub: redisClient.Subscribe(ctx, channelName(lockName)),

		expiration:  10 * time.Second,
		waitTimeout: 30 * time.Second,
	}
	//注册析构函数
	runtime.SetFinalizer(mutex, func(mux *Mutex) {
		mux.Close()
	})
	//将订阅释放锁通知的操作放至调用lock方法
	if err := mutex.pubSub.Unsubscribe(ctx, channelName(lockName)); err != nil {
		return nil, err
	}
	return mutex, nil
}

func (m *Mutex) Lock() error {
	m.assertClose()

	lifetimeMillisecond := int64(m.expiration / time.Millisecond)

	ctx, cancel := context.WithTimeout(m.ctx, m.waitTimeout)
	defer cancel()

	//订阅锁释放事件
	if err := m.subscribe(); err != nil {
		return err
	}
	defer m.unsubscribe()

	threadID := m.instanceID + ":" + strconv.FormatInt(goroutineID(), 10)
	if err := m.tryLock(ctx, threadID, lifetimeMillisecond); err != nil {
		return err
	}

	//加锁成功后才会执行以下的
	freeCh := make(chan struct{})
	m.freeCh = freeCh

	//加锁成功，开个协程，定时续锁
	go func() {
		ticker := time.NewTicker(m.expiration / 3)
		defer ticker.Stop()
		for {
			select {
			case <-freeCh:
				return
			case <-ticker.C:
				if res, err := m.redisClient.Eval(
					m.ctx, mutexScript.renewalScript,
					[]string{lockName(m.lockName)}, lifetimeMillisecond, threadID,
				).Int64(); err != nil || res == 0 {
					return
				}
			}
		}
	}()
	return nil
}

func (m *Mutex) tryLock(ctx context.Context, ID string, lifetimeMillisecond int64) error {
	// 尝试加锁
	pTTL, err := m.lockInner(ID, lifetimeMillisecond)
	if err != nil {
		return err
	}
	if pTTL == 0 {
		return nil
	}

	ttlTimer := time.NewTimer(time.Duration(pTTL) * time.Millisecond)
	defer ttlTimer.Stop()

	select {
	case <-ctx.Done():
		//申请锁的耗时如果大于等于最大等待时间，则申请锁失败.
		return ErrWaitTimeout
	case <-ttlTimer.C:
		//针对“redis 中存在未维护的锁”，即当锁自然过期后，并不会发布通知的锁
		return m.tryLock(ctx, ID, lifetimeMillisecond)
	case <-m.pubSub.Channel():
		//收到解锁通知，则尝试抢锁
		return m.tryLock(ctx, ID, lifetimeMillisecond)
	}
}

func (m *Mutex) lockInner(ID string, expiration int64) (int64, error) {
	pTTL, err := m.redisClient.Eval(
		m.ctx, mutexScript.lockScript,
		[]string{lockName(m.lockName)}, ID,
		expiration,
	).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return pTTL.(int64), nil
}

func (m *Mutex) Unlock() error {
	m.assertClose()

	goID := goroutineID()
	if err := m.unlockInner(goID); err != nil {
		return fmt.Errorf("unlock err: %v", err)
	}

	if m.freeCh != nil {
		close(m.freeCh)
		m.freeCh = nil
	}
	return nil
}

func (m *Mutex) unlockInner(goID int64) error {
	res, err := m.redisClient.Eval(
		m.ctx, mutexScript.unlockScript,
		[]string{lockName(m.lockName), channelName(m.lockName)},
		m.instanceID+":"+strconv.FormatInt(goID, 10), 1,
	).Int64()
	if err != nil {
		return err
	}
	if res == 0 {
		return ErrMismatch
	}
	return nil
}

func (m *Mutex) subscribe() error {
	m.pubSubMux.Lock()
	defer m.pubSubMux.Unlock()

	m.pubSubCount++
	if m.pubSubCount == 1 {
		if err := m.pubSub.Subscribe(m.ctx, channelName(m.lockName)); err != nil {
			return err
		}
	}
	return nil
}

func (m *Mutex) unsubscribe() error {
	m.pubSubMux.Lock()
	defer m.pubSubMux.Unlock()

	if m.pubSubCount <= 0 {
		panic("unsubscribe must be called after subscribe")
	}

	m.pubSubCount--
	if m.pubSubCount == 0 {
		if err := m.pubSub.Unsubscribe(m.ctx, channelName(m.lockName)); err != nil {
			return err
		}
	}
	return nil
}

func (m *Mutex) Close() (err error) {
	m.closeOnce.Do(func() {
		m.closed = true
		err = m.pubSub.Close()
	})
	return
}

func (m *Mutex) assertClose() {
	if m.closed {
		panic("do not call a closed lock")
	}
}

func init() {
	mutexScript.lockScript = `
	-- KEYS[1] 锁名
	-- ARGV[1] 协程唯一标识：客户端标识+协程ID
	-- ARGV[2] 过期时间
	if redis.call('setnx',KEYS[1],ARGV[1]) == 1 then
		redis.call('pexpire',KEYS[1],ARGV[2])
		return nil
	end
	return redis.call('pttl',KEYS[1])
`

	mutexScript.renewalScript = `
	-- KEYS[1] 锁名
	-- ARGV[1] 过期时间
	-- ARGV[2] 客户端协程唯一标识
	if redis.call('get',KEYS[1])==ARGV[2] then
		return redis.call('pexpire',KEYS[1],ARGV[1])
	end
	return 0
`

	mutexScript.unlockScript = `
	-- KEYS[1] 锁名
	-- KEYS[2] 发布订阅的channel
	-- ARGV[1] 协程唯一标识：客户端标识+协程ID
	-- ARGV[2] 解锁时发布的消息
	if redis.call('exists',KEYS[1]) == 1 then
		if (redis.call('get',KEYS[1]) == ARGV[1]) then
			redis.call('del',KEYS[1])
		else
			return 0
		end
	end
	redis.call('publish',KEYS[2],ARGV[2])
	return 1
`
}
