package redisson

import (
	"context"
	"sync"
)

type PubSub[TOPIC comparable, DATA any] struct {
	mux         sync.RWMutex
	subscribers map[TOPIC][]struct {
		ctx context.Context
		ch  chan<- DATA
	}
}

func NewPubSub[TOPIC comparable, DATA any]() *PubSub[TOPIC, DATA] {
	return &PubSub[TOPIC, DATA]{
		subscribers: make(map[TOPIC][]struct {
			ctx context.Context
			ch  chan<- DATA
		}),
	}
}

func (e *PubSub[TOPIC, DATA]) Subscribe(ctx context.Context, topic TOPIC, ch chan<- DATA) {
	e.mux.Lock()
	defer e.mux.Unlock()

	e.subscribers[topic] = append(e.subscribers[topic], struct {
		ctx context.Context
		ch  chan<- DATA
	}{ctx: ctx, ch: ch})

	go func() {
		<-ctx.Done()

		e.mux.Lock()
		defer e.mux.Unlock()

		index := -1
		for i, v := range e.subscribers[topic] {
			if v.ch == ch {
				index = i
				break
			}
		}
		if index > -1 {
			copy(e.subscribers[topic][index:], e.subscribers[topic][index+1:])
			e.subscribers[topic] = e.subscribers[topic][:len(e.subscribers[topic])-1]
		}
		if len(e.subscribers[topic]) == 0 {
			delete(e.subscribers, topic)
		}
	}()
}

func (e *PubSub[TOPIC, DATA]) Publish(topic TOPIC, data DATA) {
	var (
		ok         bool
		collection []struct {
			ctx context.Context
			ch  chan<- DATA
		}
	)

	e.mux.RLock()
	defer e.mux.RUnlock()

	if collection, ok = e.subscribers[topic]; !ok {
		return
	}

	for _, v := range collection {
		select {
		case <-v.ctx.Done():
			continue
		case v.ch <- data:
		default:
			go func(ctx context.Context, ch chan<- DATA) {
				select {
				case <-ctx.Done():
				case ch <- data:
				}
			}(v.ctx, v.ch)
		}
	}
}

func (e *PubSub[TOPIC, DATA]) TopicCount() int {
	return len(e.subscribers)
}

func (e *PubSub[TOPIC, DATA]) SubscriberCountOfTopic(topic TOPIC) int {
	e.mux.RLock()
	defer e.mux.RUnlock()

	if _, ok := e.subscribers[topic]; ok {
		return len(e.subscribers[topic])
	}
	return 0
}
