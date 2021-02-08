package cluster

import (
	"sync"

	"github.com/Shopify/sarama"
)


// OffsetStash allows to accumulate offsets and mark them as processed in a bulk
//
// OffsetStash 负责累计 offsets 并将其批量提交。
type OffsetStash struct {
	offsets map[topicPartition]offsetInfo
	mu      sync.Mutex
}

// NewOffsetStash inits a blank stash
func NewOffsetStash() *OffsetStash {
	return &OffsetStash{
		offsets: make(map[topicPartition]offsetInfo),
	}
}

// MarkOffset stashes the provided message offset
func (s *OffsetStash) MarkOffset(msg *sarama.ConsumerMessage, metadata string) {
	s.MarkPartitionOffset(msg.Topic, msg.Partition, msg.Offset, metadata)
}

// MarkPartitionOffset stashes the offset for the provided topic/partition combination
//
// MarkPartitionOffset 存储新 offset 值，新 offset 需要比已缓存值大。
func (s *OffsetStash) MarkPartitionOffset(topic string, partition int32, offset int64, metadata string) {

	s.mu.Lock()
	defer s.mu.Unlock()

	// offset 是 topic/partition 维度的
	key := topicPartition{
		Topic: topic,
		Partition: partition,
	}

	// 如果当前 offset 比缓存值大，就更新 offset
	if info := s.offsets[key]; offset >= info.Offset {
		info.Offset = offset		// 设置 offset
		info.Metadata = metadata	// 设置 metadata
		s.offsets[key] = info		// 更新
	}
}


// ResetPartitionOffset stashes the offset for the provided topic/partition combination.
// Difference between ResetPartitionOffset and MarkPartitionOffset is that, ResetPartitionOffset supports earlier offsets
//
// ResetPartitionOffset 存储新 offset 值，新 offset 需要比已缓存值小。
// ResetPartitionOffset 和 MarkPartitionOffset 的区别在于，ResetPartitionOffset 只能保存更小的偏移。
func (s *OffsetStash) ResetPartitionOffset(topic string, partition int32, offset int64, metadata string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// offset 是 topic/partition 维度的
	key := topicPartition{
		Topic: topic,
		Partition: partition,
	}

	// 如果当前 offset 比缓存值小，就更新 offset
	if info := s.offsets[key]; offset <= info.Offset {
		info.Offset = offset		// 设置 offset
		info.Metadata = metadata	// 设置 metadata
		s.offsets[key] = info		// 更新
	}
}

// ResetOffset stashes the provided message offset
// See ResetPartitionOffset for explanation
func (s *OffsetStash) ResetOffset(msg *sarama.ConsumerMessage, metadata string) {
	s.ResetPartitionOffset(msg.Topic, msg.Partition, msg.Offset, metadata)
}

// Offsets returns the latest stashed offsets by topic-partition
//
// 返回 s.offsets 中存储的最新的 offset 集合
func (s *OffsetStash) Offsets() map[string]int64 {

	s.mu.Lock()
	defer s.mu.Unlock()

	res := make(map[string]int64, len(s.offsets))
	for tp, info := range s.offsets {
		res[tp.String()] = info.Offset
	}
	return res
}
