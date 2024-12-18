package redisson

import (
	"context"
	"errors"
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
	redisClient *redis.Client
	instanceID  string
	lockName    string

	redisPubSubMux sync.Mutex
	redisPubSub    *redis.PubSub

	startOnce sync.Once
	closeOnce sync.Once
	closed    chan struct{}
	pubSub    *PubSub[string, string]
}

func NewMutex(redisClient *redis.Client, instanceID string, lockName string) (*Mutex, error) {
	mutex := &Mutex{
		redisClient: redisClient,
		lockName:    lockName,
		instanceID:  instanceID,

		closed: make(chan struct{}),
		pubSub: NewPubSub[string, string](),
	}
	//注册析构函数
	runtime.SetFinalizer(mutex, func(mux *Mutex) {
		mux.Close()
	})
	return mutex, nil
}

func (m *Mutex) Lock() error {
	m.start()
	if m.isClosed() {
		return ErrClosed
	}

	//设置抢占锁超时时间，60秒抢不到就报错
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	noticeFree := make(chan string)
	m.pubSub.Subscribe(ctx, channelName(m.lockName), noticeFree)

	var lockLifetimeSecond int64 = 15 //锁只保留15秒
	fromId := m.instanceID + ":" + strconv.FormatInt(goroutineID(), 10)
	if err := m.tryLock(ctx, fromId, noticeFree, lockLifetimeSecond*1000); err != nil {
		return err
	}

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ticker := time.NewTicker(time.Duration(lockLifetimeSecond) * time.Second / 3) //仅三分之一时间时，自动续锁
		defer ticker.Stop()
		ch := make(chan string)
		m.pubSub.Subscribe(ctx, channelName(m.lockName), ch)
		for {
			select {
			case payload := <-ch:
				if fromId == payload {
					return
				}
			case <-ticker.C:
				if res, err := m.redisClient.Eval(
					context.Background(), mutexScript.renewalScript,
					[]string{lockName(m.lockName)}, fromId, lockLifetimeSecond*1000,
				).Int64(); err != nil || res == 0 {
					return
				}
			}
		}
	}()
	return nil
}

func (m *Mutex) tryLock(ctx context.Context, fromId string, noticeFree chan string, lifetimeMillisecond int64) error {
	// 尝试加锁
	pTTL, err := m.lockInner(fromId, lifetimeMillisecond)
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
		return m.tryLock(ctx, fromId, noticeFree, lifetimeMillisecond)
	case <-noticeFree:
		//收到解锁通知，则尝试抢锁
		return m.tryLock(ctx, fromId, noticeFree, lifetimeMillisecond)
	}
}

func (m *Mutex) lockInner(fromId string, expiration int64) (int64, error) {
	pTTL, err := m.redisClient.Eval(
		context.Background(), mutexScript.lockScript,
		[]string{lockName(m.lockName)}, fromId,
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
	fromId := m.instanceID + ":" + strconv.FormatInt(goroutineID(), 10)
	return m.unlockInner(fromId)
}

func (m *Mutex) unlockInner(fromId string) error {
	res, err := m.redisClient.Eval(
		context.Background(), mutexScript.unlockScript,
		[]string{lockName(m.lockName), channelName(m.lockName)}, fromId,
	).Int64()
	if err != nil {
		return err
	}
	if res == 0 {
		return ErrMismatch
	}
	return nil
}

func (m *Mutex) start() {
	m.startOnce.Do(func() {
		m.redisPubSubMux.Lock()
		defer m.redisPubSubMux.Unlock()

		if m.isClosed() || m.redisPubSub != nil {
			return
		}
		m.redisPubSub = m.redisClient.Subscribe(context.Background(), channelName(m.lockName))
		go func(pubSub *PubSub[string, string], channel <-chan *redis.Message, closed chan struct{}) {
			for {
				select {
				case event := <-channel:
					pubSub.Publish(event.Channel, event.Payload)
				case <-closed:
					return
				}
			}
		}(m.pubSub, m.redisPubSub.Channel(), m.closed)
	})
}

func (m *Mutex) Close() (err error) {
	m.closeOnce.Do(func() {
		m.redisPubSubMux.Lock()
		defer m.redisPubSubMux.Unlock()

		close(m.closed)
		if m.redisPubSub != nil {
			err = m.redisPubSub.Close()
		}
	})
	return
}

func (m *Mutex) isClosed() bool {
	select {
	case <-m.closed:
		return true
	default:
		return false
	}
}

func init() {
	mutexScript.lockScript = `
	-- KEYS[1] 锁名
	-- ARGV[1] 锁来源：客户端标识+协程ID
	-- ARGV[2] 过期时间
	if redis.call('setnx',KEYS[1],ARGV[1]) == 1 then
		redis.call('pexpire',KEYS[1],ARGV[2])
		return nil
	end
	return redis.call('pttl',KEYS[1])
`

	mutexScript.renewalScript = `
	-- KEYS[1] 锁名
	-- ARGV[1] 锁来源：客户端标识+协程ID
	-- ARGV[2] 过期时间(毫秒)
	if redis.call('get',KEYS[1])==ARGV[1] then
		return redis.call('pexpire',KEYS[1],ARGV[2])
	end
	return 0
`

	mutexScript.unlockScript = `
	-- KEYS[1] 锁名
	-- KEYS[2] 发布订阅的channel
	-- ARGV[1] 锁来源：客户端标识+协程ID
	if redis.call('exists',KEYS[1]) == 1 then
		if (redis.call('get',KEYS[1]) == ARGV[1]) then
			redis.call('del',KEYS[1])
		else
			return 0
		end
	end
	redis.call('publish',KEYS[2],ARGV[1])
	return 1
`
}
