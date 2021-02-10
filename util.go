package cluster

import (
	"fmt"
	"sort"
	"sync"
)

type none struct{}

type topicPartition struct {
	Topic     string
	Partition int32
}

func (tp *topicPartition) String() string {
	return fmt.Sprintf("%s-%d", tp.Topic, tp.Partition)
}

type offsetInfo struct {
	Offset   int64
	Metadata string
}

func (i offsetInfo) NextOffset(fallback int64) int64 {
	if i.Offset > -1 {
		return i.Offset
	}
	return fallback
}

type int32Slice []int32

func (p int32Slice) Len() int           { return len(p) }
func (p int32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (p int32Slice) Diff(o int32Slice) (res []int32) {
	on := len(o)
	for _, x := range p {
		n := sort.Search(on, func(i int) bool { return o[i] >= x })
		if n < on && o[n] == x {
			continue
		}
		res = append(res, x)
	}
	return
}

// --------------------------------------------------------------------




type loopTomb struct {
	c    chan none      // 关闭管道
	once sync.Once      // 控制最多关闭一次
	wg   sync.WaitGroup // 当关闭时，等待所有协程全部退出
}

func newLoopTomb() *loopTomb {
	return &loopTomb{
		c: make(chan none),
	}
}

// 调用 t.stop() 会使所有后台协程均退出。
func (t *loopTomb) stop()  {
	t.once.Do(
		func() {
			close(t.c)
		},
	)
}

func (t *loopTomb) Close() {
	t.stop()
	t.wg.Wait()
}

// 返回监听管道，如果有协程退出，或者有主动的 Close 操作，该管道会被触发。
func (t *loopTomb) Dying() <-chan none {
	return t.c
}

// 每 t.Go 一次，会创建一个后台协程，任何一个后台协程退出，都会触发 t.stop() ，会使所有后台协程均退出。
func (t *loopTomb) Go(f func(<-chan none)) {
	t.wg.Add(1)
	go func() {
		defer t.stop()
		defer t.wg.Done()
		// 执行 f ，并把 t.c 传递给它，用于触发其退出
		f(t.c)
	}()
}
