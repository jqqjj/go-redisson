# Redisson
基于 redis 使用 golang 实现：“互斥锁”。

## 使用
```go
import (
  "github.com/jqqjj/go-redisson"
  "github.com/redis/go-redis/v9"
)

client := redis.NewClient(&redis.Options{Addr: ":6379"})
mu := redisson.NewMutex(context.Background(), client, "UuidOfProcess", "lockName")
var err error
if err = mu.Lock(); err != nil {
  log.Fatal(err)
}
if err = mu.Unlock(); err != nil {
  log.Fatal(err)
}

```
## 特性
- 实现看门狗自动续期锁
- 使用 redis 订阅监听锁的释放快速竞争
- 不可重入
