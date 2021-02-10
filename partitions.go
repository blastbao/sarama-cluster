package cluster

import (
	"sort"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)




// PartitionConsumer allows code to consume individual partitions from the cluster.
//
// See docs for Consumer.Partitions() for more on how to implement this.
//
// PartitionConsumer 允许消费独立的分区。
type PartitionConsumer interface {

	// 匿名包含
	sarama.PartitionConsumer

	// Topic returns the consumed topic name
	Topic() string

	// Partition returns the consumed partition
	Partition() int32

	// InitialOffset returns the offset used for creating the PartitionConsumer instance.
	// The returned offset can be a literal offset, or OffsetNewest, or OffsetOldest
	//
	// InitialOffset 返回用于创建 PartitionConsumer 实例的偏移量。
	// 返回值可以是一个数字偏移量，或者是 offsetlatest，或者是 OffsetOldest 。
	InitialOffset() int64
  
	// MarkOffset marks the offset of a message as preocessed.
	MarkOffset(offset int64, metadata string)

	// ResetOffset resets the offset to a previously processed message.
	ResetOffset(offset int64, metadata string)
}


type partitionConsumer struct {

	// 匿名包含，其实现核心消费逻辑
	sarama.PartitionConsumer

	// 分区状态信息
	state partitionState
	mu    sync.Mutex

	topic         string	// Topic 名称
	partition     int32		// 分区 ID
	initialOffset int64		// 初始偏移量

	closeOnce sync.Once		// 控制只关闭一次
	closeErr  error			// 错误信息

	dying, dead chan none	//
}

func newPartitionConsumer(manager sarama.Consumer, topic string, partition int32, info offsetInfo, defaultOffset int64) (*partitionConsumer, error) {

	offset := info.NextOffset(defaultOffset)

	// Creates a PartitionConsumer on the given topic/partition with the given offset.
	// Offset can be a literal offset, or OffsetNewest or OffsetOldest.
	pcm, err := manager.ConsumePartition(topic, partition, offset)

	// Resume from default offset, if requested offset is out-of-range
	if err == sarama.ErrOffsetOutOfRange {
		info.Offset = -1
		offset = defaultOffset
		pcm, err = manager.ConsumePartition(topic, partition, offset)
	}
	if err != nil {
		return nil, err
	}

	return &partitionConsumer{
		PartitionConsumer: pcm,
		state:             partitionState{Info: info},

		topic:         topic,
		partition:     partition,
		initialOffset: offset,

		dying: make(chan none),
		dead:  make(chan none),
	}, nil
}

// Topic implements PartitionConsumer
func (c *partitionConsumer) Topic() string {
	return c.topic
}

// Partition implements PartitionConsumer
func (c *partitionConsumer) Partition() int32 {
	return c.partition
}

// InitialOffset implements PartitionConsumer
func (c *partitionConsumer) InitialOffset() int64 {
	return c.initialOffset
}

// AsyncClose implements PartitionConsumer
func (c *partitionConsumer) AsyncClose() {
	// 只关闭一次
	c.closeOnce.Do(func() {
		// 执行关闭，保存错误信息
		c.closeErr = c.PartitionConsumer.Close()
		// 管道通知
		close(c.dying)
	})
}

// Close implements PartitionConsumer
func (c *partitionConsumer) Close() error {
	c.AsyncClose()
	<-c.dead
	return c.closeErr
}

func (c *partitionConsumer) waitFor(stopper <-chan none) {

	// 等待 stopper 和 c.dying 上的关闭信号
	select {
	case <-stopper:
	case <-c.dying:
	}
	// 关闭 c.dead，触发 c.Close() 的返回
	close(c.dead)
}


// 多路复用: 把 c.Messages() 转发到 messages，把 c.Errors() 转发到 errors 。
func (c *partitionConsumer) multiplex(stopper <-chan none, messages chan<- *sarama.ConsumerMessage, errors chan<- error) {

	// 关闭 c.dead，触发 c.Close() 的返回
	defer close(c.dead)

	for {
		select {

		// 监听消息
		case msg, ok := <-c.Messages():

			// 消息管道被关闭，直接退出
			if !ok {
				return
			}

			// 把 partitionConsumer.message 的消息转发到 consumer.messages
			select {
			case messages <- msg:
			case <-stopper:
				return
			case <-c.dying:
				return
			}

		// 监听错误
		case err, ok := <-c.Errors():

			// 错误管道被关闭，直接退出
			if !ok {
				return
			}

			// 把 partitionConsumer.error 的消息转发到 consumer.error
			select {
			case errors <- err:
			case <-stopper:
				return
			case <-c.dying:
				return
			}

		// 监听退出信号
		case <-stopper:
			return

		// 监听退出信号
		case <-c.dying:
			return
		}
	}
}


// 获取 Consumer 状态
func (c *partitionConsumer) getState() partitionState {
	c.mu.Lock()
	state := c.state
	c.mu.Unlock()
	return state
}

// 提交 offset
func (c *partitionConsumer) markCommitted(offset int64) {
	c.mu.Lock()
	if offset == c.state.Info.Offset {
		c.state.Dirty = false
	}
	c.mu.Unlock()
}

// MarkOffset implements PartitionConsumer
//
// 提交 offset
func (c *partitionConsumer) MarkOffset(offset int64, metadata string) {
	c.mu.Lock()
	if next := offset + 1; next > c.state.Info.Offset {
		c.state.Info.Offset = next
		c.state.Info.Metadata = metadata
		c.state.Dirty = true
	}
	c.mu.Unlock()
}


// ResetOffset implements PartitionConsumer
//
// 重置 offset
func (c *partitionConsumer) ResetOffset(offset int64, metadata string) {
	c.mu.Lock()

	if next := offset + 1; next <= c.state.Info.Offset {
		c.state.Info.Offset = next
		c.state.Info.Metadata = metadata
		c.state.Dirty = true
	}

	c.mu.Unlock()
}

// --------------------------------------------------------------------

type partitionState struct {
	Info       offsetInfo	// 消费偏移量
	Dirty      bool			//
	LastCommit time.Time	// 上次提交时间
}

// --------------------------------------------------------------------


// 保存 topic.partition 对应的 consumer
type partitionMap struct {
	data map[topicPartition]*partitionConsumer
	mu   sync.RWMutex
}

func newPartitionMap() *partitionMap {
	return &partitionMap{
		data: make(map[topicPartition]*partitionConsumer),
	}
}


// 检查 Topic 是否已经有 consumer 在订阅。
func (m *partitionMap) IsSubscribedTo(topic string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// 如果 topic 已经有 consumer 在订阅，就返回 true 。
	for tp := range m.data {
		if tp.Topic == topic {
			return true
		}
	}
	return false
}

// 根据 topic.partition 获取正在订阅的 consumer
func (m *partitionMap) Fetch(topic string, partition int32) *partitionConsumer {
	m.mu.RLock()
	pc, _ := m.data[topicPartition{topic, partition}]
	m.mu.RUnlock()
	return pc
}

// 保存订阅 topic.partition 的 partition consumer
func (m *partitionMap) Store(topic string, partition int32, pc *partitionConsumer) {
	m.mu.Lock()
	m.data[topicPartition{topic, partition}] = pc
	m.mu.Unlock()
}

// 获取当前所有 consumer 的消费状态集合
func (m *partitionMap) Snapshot() map[topicPartition]partitionState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snap := make(map[topicPartition]partitionState, len(m.data))
	for tp, pc := range m.data {
		snap[tp] = pc.getState()
	}
	return snap
}

// 停止所有正在消费的 consumers
func (m *partitionMap) Stop() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var wg sync.WaitGroup
	for tp := range m.data {
		wg.Add(1)
		go func(p *partitionConsumer) {
			_ = p.Close()
			wg.Done()
		}(m.data[tp])
	}
	wg.Wait()
}

// 清空
func (m *partitionMap) Clear() {
	m.mu.Lock()
	for tp := range m.data {
		delete(m.data, tp)
	}
	m.mu.Unlock()
}

// 获取正在被订阅的 topic => partitions 的列表集合
func (m *partitionMap) Info() map[string][]int32 {
	info := make(map[string][]int32)
	m.mu.RLock()
	for tp := range m.data {
		info[tp.Topic] = append(info[tp.Topic], tp.Partition)
	}
	m.mu.RUnlock()

	for topic := range info {
		sort.Sort(int32Slice(info[topic]))
	}
	return info
}
