package main

import (
	"context"
	"fmt"
	"github.com/jqqjj/go-redisson"
	"github.com/redis/go-redis/v9"
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	b("AAAA")
}

func c(ID string) {
	var (
		err error
		db  *redis.Client
	)
	//redis init
	if db, err = InitRedis("localhost", "", 6379, 0); err != nil {
		log.Fatalln(err)
	}

	//ctx, _ := context.WithTimeout(context.Background(), time.Second*30)
	ctx, _ := context.WithCancel(context.Background())
	lock, err := redisson.NewMutex(ctx, db, ID, "1")
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			time.Sleep(time.Second * 5)
			log.Println(runtime.NumGoroutine())
			runtime.GC()
		}
	}()

	go func() {
		if err := lock.Lock(); err != nil {
			log.Fatalln(err)
		}
		time.Sleep(time.Minute + time.Second*20)
		if err := lock.Unlock(); err != nil {
			log.Fatalln(err)
		}
		log.Println("已释放")
	}()

	time.Sleep(time.Second)
	if err = lock.Lock(); err != nil {
		log.Println("加锁失败", err)
	}

	select {}
}

func b(ID string) {
	var (
		err error
		db  *redis.Client
	)
	//redis init
	if db, err = InitRedis("localhost", "", 6379, 0); err != nil {
		log.Fatalln(err)
	}

	//ctx, _ := context.WithTimeout(context.Background(), time.Second*30)
	ctx, _ := context.WithCancel(context.Background())
	lock, err := redisson.NewMutex(ctx, db, ID, "1")
	if err != nil {
		log.Fatal(err)
	}

	_ = lock
	go func() {
		for {
			time.Sleep(time.Second * 5)
			log.Println(runtime.NumGoroutine())
		}
	}()

	time.Sleep(time.Second * 8)

	go func() {
		a("CC")
	}()
	go func() {
		a("DD")
	}()

	time.Sleep(time.Second * 8)
	runtime.GC()

	select {}
}

func a(ID string) {
	var (
		err error
		db  *redis.Client
	)
	//redis init
	if db, err = InitRedis("localhost", "", 6379, 0); err != nil {
		log.Fatalln(err)
	}

	//ctx, _ := context.WithTimeout(context.Background(), time.Second*30)
	ctx, _ := context.WithCancel(context.Background())
	lock, err := redisson.NewMutex(ctx, db, ID, "1")
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()
		var err error
		for i := 0; i < 50; i++ {
			if err = lock.Lock(); err != nil {
				log.Fatalf("(%s:%d)加锁失败: %v", ID, goroutineID(), err)
			}
			time.Sleep(time.Millisecond * 5)
			log.Printf("(%s:%d)+++++", ID, goroutineID())
			time.Sleep(time.Millisecond * 100)
			if err = lock.Unlock(); err != nil {
				log.Fatalf("(%s:%d)解锁失败: %v", ID, goroutineID(), err)
			}
			log.Printf("(%s:%d)-----", ID, goroutineID())
			time.Sleep(time.Millisecond)
		}
	}()
	go func() {
		defer wg.Done()
		var err error
		for i := 0; i < 50; i++ {
			if err = lock.Lock(); err != nil {
				log.Fatalf("(%s:%d)加锁失败: %v", ID, goroutineID(), err)
			}
			time.Sleep(time.Millisecond * 5)
			log.Printf("(%s:%d)+++++", ID, goroutineID())
			time.Sleep(time.Millisecond * 100)
			if err = lock.Unlock(); err != nil {
				log.Fatalf("(%s:%d)解锁失败: %v", ID, goroutineID(), err)
			}
			log.Printf("(%s:%d)-----", ID, goroutineID())
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()
	lock.Close()
}

func InitRedis(host, password string, port int, databaseIndex int) (conn *redis.Client, err error) {
	conn = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", host, port),
		Password: password,      // 密码
		DB:       databaseIndex, // 默认DB
	})
	if err = conn.Ping(context.Background()).Err(); err != nil {
		return nil, err
	} else {
		return conn, nil
	}
}

func goroutineID() int64 {
	buf := make([]byte, 35)
	runtime.Stack(buf, false)
	s := string(buf)
	parseInt, _ := strconv.ParseInt(strings.TrimSpace(s[10:strings.IndexByte(s, '[')]), 10, 64)
	return parseInt
}
