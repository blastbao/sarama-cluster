package cluster

import (
	"math"
	"sort"

	"github.com/Shopify/sarama"
)

// NotificationType defines the type of notification
type NotificationType uint8

// String describes the notification type
func (t NotificationType) String() string {
	switch t {
	case RebalanceStart:
		return "rebalance start"
	case RebalanceOK:
		return "rebalance OK"
	case RebalanceError:
		return "rebalance error"
	}
	return "unknown"
}

const (
	UnknownNotification NotificationType = iota
	RebalanceStart
	RebalanceOK
	RebalanceError
)

// Notification are state events emitted by the consumers on rebalance
type Notification struct {

	// Type exposes the notification type
	// 通知类型
	Type NotificationType

	// Claimed contains topic/partitions that were claimed by this rebalance cycle
	// Claimed 包含这个 rebalance 周期中涉及的 topic/partitions
	Claimed map[string][]int32

	// Released contains topic/partitions that were released as part of this rebalance cycle
	// Released 包含 topic/partitions
	Released map[string][]int32

	// Current are topic/partitions that are currently claimed to the consumer
	Current map[string][]int32
}



func newNotification(current map[string][]int32) *Notification {
	return &Notification{
		Type:    RebalanceStart,
		Current: current,
	}
}

func (n *Notification) success(current map[string][]int32) *Notification {

	o := &Notification{
		Type:     RebalanceOK,
		Claimed:  make(map[string][]int32),
		Released: make(map[string][]int32),
		Current:  current,
	}

	for topic, partitions := range current {
		o.Claimed[topic] = int32Slice(partitions).Diff(int32Slice(n.Current[topic]))
	}

	for topic, partitions := range n.Current {
		o.Released[topic] = int32Slice(partitions).Diff(int32Slice(current[topic]))
	}

	return o
}

func (n *Notification) error() *Notification {

	o := &Notification{
		Type:     RebalanceError,
		Claimed:  make(map[string][]int32),
		Released: make(map[string][]int32),
		Current:  make(map[string][]int32),
	}

	for topic, partitions := range n.Claimed {
		o.Claimed[topic] = append(make([]int32, 0, len(partitions)), partitions...)
	}

	for topic, partitions := range n.Released {
		o.Released[topic] = append(make([]int32, 0, len(partitions)), partitions...)
	}

	for topic, partitions := range n.Current {
		o.Current[topic] = append(make([]int32, 0, len(partitions)), partitions...)
	}

	return o
}

// --------------------------------------------------------------------



type topicInfo struct {
	Partitions []int32	// 存储了 topic 的所有 partitionIds
	MemberIDs  []string // 存储了 topic 的所有 members
}

func (info topicInfo) Perform(s Strategy) map[string][]int32 {
	if s == StrategyRoundRobin {
		return info.RoundRobin()
	}
	return info.Ranges()
}


func (info topicInfo) Ranges() map[string][]int32 {

	// 把 memberIds 从小到大排序
	sort.Strings(info.MemberIDs)

	// 获取 members 总数
	mlen := len(info.MemberIDs)

	// 获取 partitions 总数
	plen := len(info.Partitions)

	// 把 partitions 平均分为 mlen 份，每个 member 订阅一份，可能有 member 无 partition 可订阅。
	res := make(map[string][]int32, mlen)
	for pos, memberID := range info.MemberIDs {
		n, i := float64(plen)/float64(mlen), float64(pos)
		min := int(math.Floor(i*n + 0.5))
		max := int(math.Floor((i+1)*n + 0.5))
		sub := info.Partitions[min:max]
		if len(sub) > 0 {
			res[memberID] = sub
		}
	}
	return res
}

func (info topicInfo) RoundRobin() map[string][]int32 {

	// 把 memberIds 从小到大排序
	sort.Strings(info.MemberIDs)

	// 获取 members 总数
	mlen := len(info.MemberIDs)

	// 遍历 partitionIds 循环的分配给每个 member
	res := make(map[string][]int32, mlen)
	for i, pnum := range info.Partitions {
		memberID := info.MemberIDs[i%mlen]
		res[memberID] = append(res[memberID], pnum)
	}
	return res
}


// --------------------------------------------------------------------

type balancer struct {
	client   sarama.Client
	topics   map[string]topicInfo
	strategy Strategy
}

// members 包含当前组的所有成员订阅的 topics 列表，map[memberId][topics...]
// client 用于根据 topic 查询其下所有 partition ids 列表
func newBalancerFromMeta(client sarama.Client, strategy Strategy, members map[string]sarama.ConsumerGroupMemberMetadata) (*balancer, error) {

	// 根据 strategy 创建 balancer
	balancer := newBalancer(client, strategy)

	// 把 memberId 添加到 Topic 的订阅成员列表中
	for memberID, meta := range members {
		for _, topic := range meta.Topics {
			if err := balancer.Topic(topic, memberID); err != nil {
				return nil, err
			}
		}
	}

	return balancer, nil
}


func newBalancer(client sarama.Client, strategy Strategy) *balancer {
	return &balancer{
		client: client,
		topics: make(map[string]topicInfo),
		strategy: strategy,
	}
}

// 把 memberId 添加到 Topic 的订阅成员列表中
func (r *balancer) Topic(name string, memberID string) error {
	topic, ok := r.topics[name]
	if !ok {
		// 根据 topic 查询 partition id 列表
		nums, err := r.client.Partitions(name)
		if err != nil {
			return err
		}
		topic = topicInfo{
			Partitions: nums,
			MemberIDs:  make([]string, 0, 1),
		}
	}
	// 把 memberId 保存到 topic 成员中
	topic.MemberIDs = append(topic.MemberIDs, memberID)
	// 更新 topic 下配置
	r.topics[name] = topic
	return nil
}


// 返回 map[memberID][topic]partitions ，即每个 memberId 在 topic 下订阅的 partitions 列表。
func (r *balancer) Perform() map[string]map[string][]int32 {

	res := make(map[string]map[string][]int32, 1)
	for topic, info := range r.topics {
		// info 中保存着订阅了此 topic 的所有 partitions 和 memberIds
		// info.Perform 就是把这些 partitions 按照 strategy 分配给每个 member 。
		for memberID, partitions := range info.Perform(r.strategy) {
			if _, ok := res[memberID]; !ok {
				res[memberID] = make(map[string][]int32, 1)
			}
			res[memberID][topic] = partitions
		}
	}
	return res
}
