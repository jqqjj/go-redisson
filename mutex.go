package redisson

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
)

var mutexScript = struct {
	lockScript    string
	renewalScript string
	unlockScript  string
}{}

type Mutex struct {
	ctx         context.Context
	lockName    string
	redisClient *redis.Client
	InstanceID  string

	expiration  time.Duration
	waitTimeout time.Duration

	freeCh chan struct{}
}

func NewMutex(ctx context.Context, redisClient *redis.Client, InstanceID string, lockName string) *Mutex {
	return &Mutex{
		ctx:         ctx,
		lockName:    lockName,
		redisClient: redisClient,
		InstanceID:  InstanceID,

		expiration:  10 * time.Second,
		waitTimeout: 30 * time.Second,
	}
}

func (m *Mutex) Lock() error {
	//单位：ms
	expiration := int64(m.expiration / time.Millisecond)

	ctx, cancel := context.WithTimeout(m.ctx, m.waitTimeout)
	defer cancel()

	pubSub := m.redisClient.Subscribe(ctx, ChannelName(m.lockName))
	defer pubSub.Close()
	defer pubSub.Unsubscribe(ctx, ChannelName(m.lockName))

	threadID := m.InstanceID + ":" + strconv.FormatInt(GoroutineID(), 10)
	err := m.tryLock(ctx, pubSub, threadID, expiration)
	if err != nil {
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
					[]string{LockName(m.lockName)}, expiration, threadID,
				).Int64(); err != nil || res == 0 {
					return
				}
			}
		}
	}()
	return nil
}

func (m *Mutex) tryLock(ctx context.Context, pubSub *redis.PubSub, ID string, expiration int64) error {
	// 尝试加锁
	pTTL, err := m.lockInner(ID, expiration)
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
		return m.tryLock(ctx, pubSub, ID, expiration)
	case <-pubSub.Channel():
		//收到解锁通知，则尝试抢锁
		return m.tryLock(ctx, pubSub, ID, expiration)
	}
}

func (m *Mutex) lockInner(ID string, expiration int64) (int64, error) {
	pTTL, err := m.redisClient.Eval(
		m.ctx, mutexScript.lockScript,
		[]string{LockName(m.lockName)}, ID,
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
	goID := GoroutineID()

	if err := m.unlockInner(goID); err != nil {
		return fmt.Errorf("unlock err: %w", err)
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
		[]string{LockName(m.lockName), ChannelName(m.lockName)},
		m.InstanceID+":"+strconv.FormatInt(goID, 10), 1,
	).Int64()
	if err != nil {
		return err
	}
	if res == 0 {
		return ErrMismatch
	}
	return nil
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
