package consul

import (
	"context"
	"sync"
	"sync/atomic"
)

type nullValue[T any] struct{ value T }

type signalValue[T any] struct {
	v    atomic.Value
	ch   chan struct{}
	once sync.Once
}

func (sv *signalValue[T]) init() {
	sv.once.Do(func() {
		sv.ch = make(chan struct{}, 1)
	})
}

func (sv *signalValue[T]) Load() T {
	sv.init()
	v, _ := sv.v.Load().(nullValue[T])
	return v.value
}

func (sv *signalValue[T]) Store(ctx context.Context, val T) bool {
	sv.init()
	sv.v.Store(nullValue[T]{val})
	select {
	case <-ctx.Done():
		return false
	case sv.ch <- struct{}{}:
	default:
	}
	return true
}

func (sv *signalValue[T]) Wait(ctx context.Context) bool {
	sv.init()
	select {
	case <-ctx.Done():
		return false
	case <-sv.ch:
		return true
	}
}

func (sv *signalValue[T]) Signal() <-chan struct{} {
	sv.init()
	return sv.ch
}

type signalMap[K, V any] struct {
	m    sync.Map
	ch   chan struct{}
	once sync.Once
}

func (sm *signalMap[K, V]) init() {
	sm.once.Do(func() {
		sm.ch = make(chan struct{}, 1)
	})
}

func (sm *signalMap[K, V]) All() iterSeq2[K, V] {
	return func(yield func(K, V) bool) {
		sm.init()
		sm.m.Range(func(key, value any) bool {
			return yield(key.(K), value.(V))
		})
	}
}

func (sm *signalMap[K, V]) Delete(ctx context.Context, key K) bool {
	sm.init()
	_, loaded := sm.m.LoadAndDelete(key)
	if !loaded {
		// no changes, no need to signal
		return true
	}
	select {
	case <-ctx.Done():
		return false
	case sm.ch <- struct{}{}:
	default:
	}
	return true
}

func (sm *signalMap[K, V]) DeleteFunc(ctx context.Context, del func(K, V) bool) bool {
	sm.init()
	var count int
	sm.m.Range(func(key, value any) bool {
		if del(key.(K), value.(V)) {
			sm.m.Delete(key)
			count++
		}
		return true
	})
	if count == 0 {
		// no changes, no need to signal
		return true
	}
	select {
	case <-ctx.Done():
		return false
	case sm.ch <- struct{}{}:
	default:
	}
	return true
}

func (sm *signalMap[K, V]) Store(ctx context.Context, key K, value V) bool {
	sm.init()
	sm.m.Store(key, value)
	select {
	case <-ctx.Done():
		return false
	case sm.ch <- struct{}{}:
	default:
	}
	return true
}

func (sm *signalMap[K, V]) Wait(ctx context.Context) bool {
	sm.init()
	select {
	case <-ctx.Done():
		return false
	case <-sm.ch:
		return true
	}
}

func (sm *signalMap[K, V]) Signal() <-chan struct{} {
	sm.init()
	return sm.ch
}
