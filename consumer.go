package cluster

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
)

// Consumer is a cluster group consumer
type Consumer struct {
	client    *Client
	ownClient bool

	consumer sarama.Consumer
	subs     *partitionMap

	consumerID string			// 没用到
	groupID    string			// 消费组 ID

	memberID     string			// 组成员 ID ，joinGroup 时分配
	generationID int32			// 代 ID ，joinGroup 时分配
	membershipMu sync.RWMutex

	coreTopics  []string	//?
	extraTopics []string	//?

	dying, dead chan none
	closeOnce   sync.Once

	consuming     int32		// 0: 未在消费中, 1: 正在消费中
	messages      chan *sarama.ConsumerMessage
	errors        chan error
	partitions    chan PartitionConsumer
	notifications chan *Notification

	commitMu sync.Mutex
}

// NewConsumer initializes a new consumer
func NewConsumer(addrs []string, groupID string, topics []string, config *Config) (*Consumer, error) {


	// 创建消费组 client
	client, err := NewClient(addrs, config)
	if err != nil {
		return nil, err
	}

	//
	consumer, err := NewConsumerFromClient(client, groupID, topics)
	if err != nil {
		return nil, err
	}

	consumer.ownClient = true
	return consumer, nil
}


// NewConsumerFromClient initializes a new consumer from an existing client.
//
// Please note that clients cannot be shared between consumers (due to Kafka internals),
// they can only be re-used which requires the user to call Close() on the first consumer
// before using this method again to initialize another one. Attempts to use a client with
// more than one consumer at a time will return errors.
//
//
// 首先创建全局 consumer ，按 topic 及 partition 创建并发送至 partitionConsumer，
// 各 partitionConsumer 按 leader-broker 创建 brokerConsumer ，brokerConsumer 周期性发起 fetch 请求，消费数据。
//
func NewConsumerFromClient(client *Client, groupID string, topics []string) (*Consumer, error) {


	// 检查 client 是否 `正在使用中`
	if !client.claim() {
		return nil, errClientInUse
	}

	// 创建 consumer
	consumer, err := sarama.NewConsumerFromClient(client.Client)
	if err != nil {
		client.release()
		return nil, err
	}

	// 排序 Topics
	sort.Strings(topics)

	c := &Consumer{
		client:   client,				// group client
		consumer: consumer,				// sarama.consumer
		subs:     newPartitionMap(),    // 保存 topic.partition 对应的 consumer
		groupID:  groupID,				// 消费组 ID

		coreTopics: topics,				// 需要订阅的 topics

		dying: make(chan none),			// "正在关闭"
		dead:  make(chan none),			// "已经关闭"

		messages:      make(chan *sarama.ConsumerMessage),					// 消息管道
		errors:        make(chan error, client.config.ChannelBufferSize),	// 错误管道
		partitions:    make(chan PartitionConsumer, 1),						// 分区管道
		notifications: make(chan *Notification),							// 通知管道
	}

	// 获取 groupID 消费组的 coordinator broker 信息并缓存到本地，以 map[groupId]broker 来保存。
	if err := c.client.RefreshCoordinator(groupID); err != nil {
		client.release()
		return nil, err
	}

	go c.mainLoop()
	return c, nil
}


// Messages returns the read channel for the messages that are returned by
// the broker.
//
// This channel will only return if Config.Group.Mode option is set to
// ConsumerModeMultiplex (default).
//
func (c *Consumer) Messages() <-chan *sarama.ConsumerMessage {
	return c.messages
}


// Partitions returns the read channels for individual partitions of this broker.
//
// This channel will only return if Config.Group.Mode option is set to
// ConsumerModePartitions.
//
// The Partitions() channel must be listened to for the life of this consumer;
// when a rebalance happens old partitions will be closed (naturally come to
// completion) and new ones will be emitted. The returned channel will only close
// when the consumer is completely shut down.
//
//
// 在 consumer 的运行过程中，必须监听 Partitions() 通道，当发生 rebalance 时，旧分区将被关闭，新分区将被发出。
// 当 consumer 完全关闭后，Partitions() 通道才会被关闭。
func (c *Consumer) Partitions() <-chan PartitionConsumer {
	return c.partitions
}


// Errors returns a read channel of errors that occur during offset management, if
// enabled. By default, errors are logged and not returned over this channel. If
// you want to implement any custom error handling, set your config's
// Consumer.Return.Errors setting to true, and read from this channel.
func (c *Consumer) Errors() <-chan error {
	return c.errors
}

// Notifications returns a channel of Notifications that occur during consumer
// rebalancing. Notifications will only be emitted over this channel, if your config's
// Group.Return.Notifications setting to true.
//
// Notifications 返回 consumer 在 rebalance 期间的通知管道。
func (c *Consumer) Notifications() <-chan *Notification {
	return c.notifications
}


// HighWaterMarks returns the current high water marks for each topic and partition
// Consistency between partitions is not guaranteed since high water marks are updated separately.
//
// HighWaterMarks 返回每个 Topic 当前的高水位标记。
func (c *Consumer) HighWaterMarks() map[string]map[int32]int64 {
	return c.consumer.HighWaterMarks()
}


// MarkOffset marks the provided message as processed, alongside a metadata string
// that represents the state of the partition consumer at that point in time. The
// metadata string can be used by another consumer to restore that state, so it
// can resume consumption.
//
// Note: calling MarkOffset does not necessarily commit the offset to the backend
// store immediately for efficiency reasons, and it may never be committed if
// your application crashes. This means that you may end up processing the same
// message twice, and your processing should ideally be idempotent.
//
//
//
//
//
func (c *Consumer) MarkOffset(msg *sarama.ConsumerMessage, metadata string) {
	if sub := c.subs.Fetch(msg.Topic, msg.Partition); sub != nil {
		sub.MarkOffset(msg.Offset, metadata)
	}
}



// MarkPartitionOffset marks an offset of the provided topic/partition as processed.
// See MarkOffset for additional explanation.
func (c *Consumer) MarkPartitionOffset(topic string, partition int32, offset int64, metadata string) {
	if sub := c.subs.Fetch(topic, partition); sub != nil {
		sub.MarkOffset(offset, metadata)
	}
}

// MarkOffsets marks stashed offsets as processed.
// See MarkOffset for additional explanation.
func (c *Consumer) MarkOffsets(s *OffsetStash) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for tp, info := range s.offsets {
		if sub := c.subs.Fetch(tp.Topic, tp.Partition); sub != nil {
			sub.MarkOffset(info.Offset, info.Metadata)
		}
		delete(s.offsets, tp)
	}
}

// ResetOffset marks the provided message as processed, alongside a metadata string
// that represents the state of the partition consumer at that point in time. The
// metadata string can be used by another consumer to restore that state, so it
// can resume consumption.
//
// Difference between ResetOffset and MarkOffset is that it allows to rewind to an earlier offset
func (c *Consumer) ResetOffset(msg *sarama.ConsumerMessage, metadata string) {
	if sub := c.subs.Fetch(msg.Topic, msg.Partition); sub != nil {
		sub.ResetOffset(msg.Offset, metadata)
	}
}

// ResetPartitionOffset marks an offset of the provided topic/partition as processed.
// See ResetOffset for additional explanation.
func (c *Consumer) ResetPartitionOffset(topic string, partition int32, offset int64, metadata string) {
	sub := c.subs.Fetch(topic, partition)
	if sub != nil {
		sub.ResetOffset(offset, metadata)
	}
}

// ResetOffsets marks stashed offsets as processed.
// See ResetOffset for additional explanation.
func (c *Consumer) ResetOffsets(s *OffsetStash) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for tp, info := range s.offsets {
		if sub := c.subs.Fetch(tp.Topic, tp.Partition); sub != nil {
			sub.ResetOffset(info.Offset, info.Metadata)
		}
		delete(s.offsets, tp)
	}
}

// Subscriptions returns the consumed topics and partitions
func (c *Consumer) Subscriptions() map[string][]int32 {
	return c.subs.Info()
}

// CommitOffsets allows to manually commit previously marked offsets. By default there is no
// need to call this function as the consumer will commit offsets automatically
// using the Config.Consumer.Offsets.CommitInterval setting.
//
// Please be aware that calling this function during an internal rebalance cycle may return
// broker errors (e.g. sarama.ErrUnknownMemberId or sarama.ErrIllegalGeneration).
//
// 可以通过 CommitOffsets 手动提交偏移量，默认情况下，不需要调用这个函数，因为 consumer 会自动提交偏移量。
// 注意，在 rebalance 过程中调用此函数可能返回 broker 错误，例如 sarama.ErrUnknownMemberId 和 sarama.ErrIllegalGeneration 。
func (c *Consumer) CommitOffsets() error {

	// 确保串行提交
	c.commitMu.Lock()
	defer c.commitMu.Unlock()

	// 获取组成员 ID 和
	memberID, generationID := c.membership()

	// 构造 `提交偏移量` 请求
	req := &sarama.OffsetCommitRequest{
		Version:                 2,				//
		ConsumerGroup:           c.groupID,		// 组 ID
		ConsumerGroupGeneration: generationID,	//
		ConsumerID:              memberID,		// 成员 ID
		RetentionTime:           -1,			//
	}

	// ???
	if ns := c.client.config.Consumer.Offsets.Retention; ns != 0 {
		req.RetentionTime = int64(ns / time.Millisecond)
	}

	// 获取当前所有 consumer 的消费状态集合，包含 消费偏移量 等信息
	snap := c.subs.Snapshot()

	// 对处于 dirty 状态的 offsets，写入到提交请求中
	dirty := false
	for tp, state := range snap {
		// 如果 dirty 为 true ，才需要提交
		if state.Dirty {
			dirty = true
			req.AddBlock(tp.Topic, tp.Partition, state.Info.Offset, 0, state.Info.Metadata)
		}
	}

	// 不存在 dirty 的 offsets ，则无需提交
	if !dirty {
		return nil
	}

	// 根据 groupId 获取 coordinator 所在的 broker
	broker, err := c.client.Coordinator(c.groupID)
	if err != nil {
		c.closeCoordinator(broker, err)
		return err
	}

	// 发送提交请求
	resp, err := broker.CommitOffset(req)
	if err != nil {
		c.closeCoordinator(broker, err)
		return err
	}

	// 检查返回错误
	for topic, errs := range resp.Errors {
		for partition, kerr := range errs {
			// 如果 topic-partition 关联的错误非 `sarama.ErrNoError` ，则提交出错，保存错误。
			if kerr != sarama.ErrNoError {
				err = kerr
			// 如果 topic-partition 关联的错误为 `sarama.ErrNoError` ，则提交成功，更新本地缓存的 offsets 。
			} else if state, ok := snap[topicPartition{topic, partition}]; ok {
				if sub := c.subs.Fetch(topic, partition); sub != nil {
					sub.markCommitted(state.Info.Offset)
				}
			}
		}
	}
	return err
}

// Close safely closes the consumer and releases all resources
func (c *Consumer) Close() (err error) {
	c.closeOnce.Do(func() {


		// 触发关闭，收到此信号后，所有后台协程均会退出
		close(c.dying)
		// 当 mainLoop 主协程最后退出时，会执行 close(c.dead)，触发本处返回
		<-c.dead

		// 停止所有 consumers 并提交偏移量
		if e := c.release(); e != nil {
			err = e
		}

		// 关闭 sarama consumer
		if e := c.consumer.Close(); e != nil {
			err = e
		}

		// 关闭消息管道
		close(c.messages)
		// 关闭错误管道
		close(c.errors)

		// 发送 `退出消费组` 请求给 Coordinator 所在 Broker
		if e := c.leaveGroup(); e != nil {
			err = e
		}
		// 关闭 rebalance 分区管道
		close(c.partitions)
		// 关闭 rebalance 通知管道
		close(c.notifications)

		// drain
		for range c.messages {
		}
		for range c.errors {
		}
		for p := range c.partitions {
			_ = p.Close() 		// 逐个关闭
		}
		for range c.notifications {
		}

		// 释放 group client
		c.client.release()

		// 关闭 sarama client
		if c.ownClient {
			if e := c.client.Close(); e != nil {
				err = e
			}
		}
	})
	return
}

// mainLoop 是个无限循环，仅能通过 Consumer.Close() 中 close(c.dying) 来主动结束。
// 在 mainLoop 退出前，会 close(c.dead) 从而触发 Consumer.Close() 结束阻塞式等待并返回。
func (c *Consumer) mainLoop() {

	defer close(c.dead)
	defer atomic.StoreInt32(&c.consuming, 0) // mainLoop() 退出后重置为 0 ，表明当前未消费

	for {

		// 设置当前状态为 `未在消费中`
		atomic.StoreInt32(&c.consuming, 0)

		// Check if close was requested
		select {
		case <-c.dying:
			return
		default:
		}

		// Start next consume cycle
		//
		// nextTick() 是阻塞式的，如果过程中出错导致退出，for 会再次启动它，重新进行初始化和消息消费
		c.nextTick()
	}
}

func (c *Consumer) nextTick() {

	// Remember previous subscriptions
	//
	// 如果开启了通知机制，就创建一个 Notification 对象，其保存此前的订阅关系
	var notification *Notification
	if c.client.config.Group.Return.Notifications {
		notification = newNotification(c.subs.Info())
	}

	// Refresh coordinator
	//
	// 刷新 coordinator
	if err := c.refreshCoordinator(); err != nil {
		c.rebalanceError(err, nil)
		return
	}

	// Release subscriptions
	//
	// 停止所有 consumers 并提交偏移量
	if err := c.release(); err != nil {
		c.rebalanceError(err, nil)
		return
	}

	// Issue rebalance start notification
	//
	// 开始 rebalance 前，发送 Notification ，把 rebalance 前的订阅列表发送出去
	if c.client.config.Group.Return.Notifications {
		c.handleNotification(newNotification(c.subs.Info()))
	}


	// Rebalance, fetch new subscriptions
	//
	// 执行 rebalance 得到新的订阅列表 sub = map[topic][partitions...]
	subs, err := c.rebalance()
	if err != nil {
		c.rebalanceError(err, notification)
		return
	}

	// Coordinate loops, make sure everything is stopped on exit
	tomb := newLoopTomb()
	defer tomb.Close() // tomb.Close() 会等待所有后台协程退出，当其返回代表 hbLoop、twLoop、cmLoop 协程均已退出。

	// Start the heartbeat
	//
	// 启动心跳协程
	tomb.Go(c.hbLoop)


	// Subscribe to topic/partitions
	//
	// 用最新分配的订阅类别，创建对应的一组 PartitionConsumers ，并启动消费
	if err := c.subscribe(tomb, subs); err != nil {
		c.rebalanceError(err, notification)
		return
	}


	// Update/issue notification with new claims
	//
	// 完成 rebalance 后，发送 Notification ，把 rebalance 后的最新订阅列表发送出去
	if c.client.config.Group.Return.Notifications {
		notification = notification.success(subs)
		c.handleNotification(notification)
	}


	// Start topic watcher loop
	//
	// 启动定时刷新 topic 协程
	tomb.Go(c.twLoop)

	// Start consuming and committing offsets
	//
	// 启动定时提交 Offset 协程
	tomb.Go(c.cmLoop)

	// 设置 c.consuming 为 1 ，表明正在消费中
	atomic.StoreInt32(&c.consuming, 1)

	// Wait for signals
	//
	// 等待退出信号
	select {
	case <-tomb.Dying(): 	// tomb.Dying() 返回监听管道，如果有任一后台协程退出，或者有主动的 tomb.Close() 操作，该管道会被触发。
	case <-c.dying:			// 等待 Consumer.Close() 关闭通知
	}
}

// heartbeat loop, triggered by the mainLoop
func (c *Consumer) hbLoop(stopped <-chan none) {

	// 心跳定时器
	ticker := time.NewTicker(c.client.config.Group.Heartbeat.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 发送心跳请求给 group coordinator 所在的 broker
			switch err := c.heartbeat(); err {
			case nil, sarama.ErrNoError:
			case sarama.ErrNotCoordinatorForConsumer, sarama.ErrRebalanceInProgress:
				return
			default:
				c.handleError(&Error{Ctx: "heartbeat", error: err})
				return
			}
		case <-stopped:
			return
		case <-c.dying:
			return
		}
	}
}

// topic watcher loop, triggered by the mainLoop
func (c *Consumer) twLoop(stopped <-chan none) {

	//
	ticker := time.NewTicker(c.client.config.Metadata.RefreshFrequency / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:

			// 获取所有可用的 topics
			topics, err := c.client.Topics()
			if err != nil {
				c.handleError(&Error{Ctx: "topics", error: err})
				return
			}

			// ???vv
			for _, topic := range topics {
				if !c.isKnownCoreTopic(topic) && !c.isKnownExtraTopic(topic) && c.isPotentialExtraTopic(topic) {
					return
				}
			}

		case <-stopped:
			return
		case <-c.dying:
			return
		}
	}
}

// commit loop, triggered by the mainLoop
func (c *Consumer) cmLoop(stopped <-chan none) {

	// 定时提交 Offset
	ticker := time.NewTicker(c.client.config.Consumer.Offsets.CommitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 提交 Offset
			if err := c.commitOffsetsWithRetry(c.client.config.Group.Offsets.Retry.Max); err != nil {
				c.handleError(&Error{Ctx: "commit", error: err})
				return
			}
		case <-stopped:
			return
		case <-c.dying:
			return
		}
	}
}


//
func (c *Consumer) rebalanceError(err error, n *Notification) {

	if n != nil {
		// Get a copy of the notification that represents the notification's error state
		n = n.error()
		c.handleNotification(n)
	}

	switch err {
	case sarama.ErrRebalanceInProgress:
	default:
		c.handleError(&Error{Ctx: "rebalance", error: err})
	}

	select {
	case <-c.dying:
	case <-time.After(c.client.config.Metadata.Retry.Backoff):
	}
}

// 如果开启了通知，就把 n 写入 c.notifications 管道中。
func (c *Consumer) handleNotification(n *Notification) {
	if c.client.config.Group.Return.Notifications {
		select {
		case c.notifications <- n:
		case <-c.dying:
			return
		}
	}
}

// 如果开启了错误，就把 e 写入 c.errors 管道中。
func (c *Consumer) handleError(e *Error) {
	if c.client.config.Consumer.Return.Errors {
		select {
		case c.errors <- e:
		case <-c.dying:
			return
		}
	} else {
		sarama.Logger.Printf("%s error: %s\n", e.Ctx, e.Error())
	}
}

// Releases the consumer and commits offsets, called from rebalance() and Close()
// 停止所有 consumers 并提交偏移量，由 rebalance() 和 Close() 调用。
func (c *Consumer) release() (err error) {

	// Stop all consumers
	// 停止所有正在消费的 partitionConsumers
	c.subs.Stop()

	// Clear subscriptions on exit
	// 在函数退出前，清空保存的 partitionConsumers 订阅信息
	defer c.subs.Clear()

	// Wait for messages to be processed
	// 创建定时器
	timeout := time.NewTimer(c.client.config.Group.Offsets.Synchronization.DwellTime)
	defer timeout.Stop()

	// ???
	select {
	case <-c.dying:
	case <-timeout.C:	// 等待消息处理完
	}

	// Commit offsets, continue on errors
	// 提交偏移量
	if e := c.commitOffsetsWithRetry(c.client.config.Group.Offsets.Retry.Max); e != nil {
		err = e
	}

	return
}

// --------------------------------------------------------------------

// Performs a heartbeat, part of the mainLoop()
//
// 发送心跳请求给 group coordinator 所在的 broker
func (c *Consumer) heartbeat() error {

	// 获取 groupId 的 Coordinator 所在 broker
	broker, err := c.client.Coordinator(c.groupID)
	if err != nil {
		c.closeCoordinator(broker, err)
		return err
	}

	// 发送心跳给 broker
	memberID, generationID := c.membership()
	resp, err := broker.Heartbeat(&sarama.HeartbeatRequest{
		GroupId:      c.groupID,
		MemberId:     memberID,
		GenerationId: generationID,
	})

	if err != nil {
		c.closeCoordinator(broker, err)
		return err
	}

	return resp.Err
}

// Performs a rebalance, part of the mainLoop()
//
// 执行 rebalance
//
//
//
//
// rebalance 本质上是一组协议，目前 kafka 提供 5 个协议来处理与 consumer group coordination 相关问题：
//
//	Heartbeat 请求：成员定期上报心跳到 coordinator 来表明自己还活着
//	LeaveGroup 请求：成员主动通知 coordinator 我要离开 group
//	SyncGroup 请求：group leader 把分配方案告诉组内所有成员
//	JoinGroup 请求：成员请求加入组
//	DescribeGroup 请求：显示组的所有信息，包括成员信息、协议名称、分配方案、订阅信息等，通常该请求是给管理员使用
//
//
// Reference:
// 	https://www.cnblogs.com/huxi2b/p/6223228.html
//
func (c *Consumer) rebalance() (map[string][]int32, error) {

	memberID, _ := c.membership()
	sarama.Logger.Printf("cluster/consumer %s rebalance\n", memberID)

	// 获取所有可用 topics
	allTopics, err := c.client.Topics()
	if err != nil {
		return nil, err
	}

	// 获取不在 coreTopics 但是在白名单里的 topics 列表，并排序
	c.extraTopics = c.selectExtraTopics(allTopics)
	sort.Strings(c.extraTopics)


	// Re-join consumer group
	// 发送 joinGroup 请求到 coordinate broker
	strategy, err := c.joinGroup()

	// 出错返回
	switch {
	case err == sarama.ErrUnknownMemberId: // 如果 memberId 不能识别，就置空
		c.membershipMu.Lock()
		c.memberID = ""
		c.membershipMu.Unlock()
		return nil, err
	case err != nil:
		return nil, err
	}

	// Sync consumer group state, fetch subscriptions
	//
	// 如果当前成员为 group leader，上面 joinGroup 会返回非 nil 的 strategy ，需要执行分配方案，并在 SyncGroup 请求中带回给 coordinator ；
	// 如果当前成员非 group leader，也要发送 syncGroup 请求，只不过分配方案为空。
	subs, err := c.syncGroup(strategy)
	switch {
	case err == sarama.ErrRebalanceInProgress:	// 当前正在 rebalance 过程中，报错给客户端
		return nil, err
	case err != nil:
		_ = c.leaveGroup() // 其它错误，则离开 group
		return nil, err
	}

	// syncGroup 之后，broker 会返回新的分配方案，即 subs 。
	return subs, nil
}

// Performs the subscription, part of the mainLoop()
//
// 以 map[topic][partitions...] 并发创建 partitionConsumer
func (c *Consumer) subscribe(tomb *loopTomb, subs map[string][]int32) error {

	// fetch offsets
	//
	// 去 Coordinator 查询当前 member 在 c.groupId 下订阅的 topic/partition 的最近提交 offset 信息
	offsets, err := c.fetchOffsets(subs)
	if err != nil {
		_ = c.leaveGroup()
		return err
	}

	// create consumers in parallel
	//
	// 以 sub[topic][partitions...] 并发创建 partitionConsumer
	var mu sync.Mutex
	var wg sync.WaitGroup
	for topic, partitions := range subs {
		for _, partition := range partitions {
			wg.Add(1)
			// 获取 topic/partition 的偏移量信息
			info := offsets[topic][partition]
			go func(topic string, partition int32) {
				// 创建的 partitionConsumer 负责从 offset 处消费 topic-partition 下数据
				if e := c.createConsumer(tomb, topic, partition, info); e != nil {
					mu.Lock()
					err = e
					mu.Unlock()
				}
				wg.Done()
			}(topic, partition)

		}
	}

	// 等待并发创建完成
	wg.Wait()

	// 如果任何一个 partitionConsumer 创建出错，则停止所有 partitionConsumer 并退出 Group 。
	if err != nil {
		_ = c.release()
		_ = c.leaveGroup()
	}

	// 返回错误
	return err
}

// --------------------------------------------------------------------

// Send a request to the broker to join group on rebalance()
func (c *Consumer) joinGroup() (*balancer, error) {

	memberID, _ := c.membership()

	req := &sarama.JoinGroupRequest{
		GroupId:        c.groupID,
		MemberId:       memberID,
		SessionTimeout: int32(c.client.config.Group.Session.Timeout / time.Millisecond),
		ProtocolType:   "consumer",
	}

	meta := &sarama.ConsumerGroupMemberMetadata{
		Version:  1,										//
		Topics:   append(c.coreTopics, c.extraTopics...),	//
		UserData: c.client.config.Group.Member.UserData,   	// 加入消费组时可以传递自定义元数据。
	}

	err := req.AddGroupProtocolMetadata(string(StrategyRange), meta)
	if err != nil {
		return nil, err
	}

	err = req.AddGroupProtocolMetadata(string(StrategyRoundRobin), meta)
	if err != nil {
		return nil, err
	}

	// 发送 joinGroup 请求给 coordinator broker
	broker, err := c.client.Coordinator(c.groupID)
	if err != nil {
		c.closeCoordinator(broker, err)
		return nil, err
	}
	resp, err := broker.JoinGroup(req)
	if err != nil {
		c.closeCoordinator(broker, err)
		return nil, err
	} else if resp.Err != sarama.ErrNoError {
		c.closeCoordinator(broker, resp.Err)
		return nil, resp.Err
	}

	var strategy *balancer

	// 如果当前节点为 group leader 节点，coordinator 会把组成员及每个成员订阅的 topics 列表返回给它，
	// leader 需要根据这些数据执行重新分配，并在后续 SyncGroup 请求中将分配方案返回给 coordinator ，
	// coordinator 负责把分配方案通知组内所有成员。
	if resp.LeaderId == resp.MemberId {
		// 获取 groupId 下所有 members，其中包含当前组的每个成员及其订阅的 topics 列表
		members, err := resp.GetMembers()
		if err != nil {
			return nil, err
		}
		// 根据 strategy 创建 balancer，执行 strategy.Perform 会完成订阅关系的重新分配
		strategy, err = newBalancerFromMeta(c.client, Strategy(resp.GroupProtocol), members)
		if err != nil {
			return nil, err
		}
	}

	c.membershipMu.Lock()
	c.memberID = resp.MemberId
	c.generationID = resp.GenerationId
	c.membershipMu.Unlock()

	return strategy, nil
}

// Send a request to the broker to sync the group on rebalance().
// Returns a list of topics and partitions to consume.
//
// 向 broker 发送 SyncGroup 请求，
//
//
func (c *Consumer) syncGroup(strategy *balancer) (map[string][]int32, error) {

	memberID, generationID := c.membership()

	// 构造 syncGroup 请求
	req := &sarama.SyncGroupRequest{
		GroupId:      c.groupID,
		MemberId:     memberID,
		GenerationId: generationID,
	}

	// 如果 strategy 非空，则执行重新分配，把分配结果保存到 req 中发送给 coordinator ， coordinator 后续会广播给所有 members 。
	if strategy != nil {
		for memberID, topics := range strategy.Perform() {
			if err := req.AddGroupAssignmentMember(memberID, &sarama.ConsumerGroupMemberAssignment{
				Topics: topics,
			}); err != nil {
				return nil, err
			}
		}
	}

	// 获取 coordinator broker
	broker, err := c.client.Coordinator(c.groupID)
	if err != nil {
		c.closeCoordinator(broker, err)
		return nil, err
	}

	// 发送 joinGroup req 给 coordinator ，coordinator 收到请求后会返回属于当前 member 的分配方案
	resp, err := broker.SyncGroup(req)
	if err != nil {
		c.closeCoordinator(broker, err)
		return nil, err
	} else if resp.Err != sarama.ErrNoError {
		c.closeCoordinator(broker, resp.Err)
		return nil, resp.Err
	}


	// Return if there is nothing to subscribe to
	//
	// 如果没有要订阅的内容，直接返回
	if len(resp.MemberAssignment) == 0 {
		return nil, nil
	}

	// Get assigned subscriptions
	members, err := resp.GetMemberAssignment()
	if err != nil {
		return nil, err
	}

	// Sort partitions, for each topic
	for topic := range members.Topics {
		sort.Sort(int32Slice(members.Topics[topic]))
	}

	// 返回需要订阅的 map[topic]patitionIds
	return members.Topics, nil
}

// Fetches latest committed offsets for all subscriptions
//
//
func (c *Consumer) fetchOffsets(subs map[string][]int32) (map[string]map[int32]offsetInfo, error) {


	// 保存结果: map[topic][partition][offset]
	offsets := make(map[string]map[int32]offsetInfo, len(subs))

	// 构造请求
	req := &sarama.OffsetFetchRequest{
		Version:       1,
		ConsumerGroup: c.groupID,
	}
	for topic, partitions := range subs {
		offsets[topic] = make(map[int32]offsetInfo, len(partitions))
		for _, partition := range partitions {
			offsets[topic][partition] = offsetInfo{Offset: -1}
			req.AddPartition(topic, partition)
		}
	}

	// 发送 FetchOffset 请求给 Coordinator
	broker, err := c.client.Coordinator(c.groupID)
	if err != nil {
		c.closeCoordinator(broker, err)
		return nil, err
	}
	resp, err := broker.FetchOffset(req)
	if err != nil {
		c.closeCoordinator(broker, err)
		return nil, err
	}

	// 解析响应
	for topic, partitions := range subs {
		for _, partition := range partitions {
			block := resp.GetBlock(topic, partition)
			if block == nil {
				return nil, sarama.ErrIncompleteResponse
			}

			if block.Err == sarama.ErrNoError {
				offsets[topic][partition] = offsetInfo{Offset: block.Offset, Metadata: block.Metadata}
			} else {
				return nil, block.Err
			}
		}
	}

	// 返回结果
	return offsets, nil
}

// Send a request to the broker to leave the group on failes rebalance() and on Close()
func (c *Consumer) leaveGroup() error {

	// 获取 group 的 Coordinator 所在 broker
	broker, err := c.client.Coordinator(c.groupID)
	if err != nil {
		c.closeCoordinator(broker, err)
		return err
	}

	// 获取 memberId
	memberID, _ := c.membership()

	// 发送 leaveGroup 请求
	if _, err = broker.LeaveGroup(&sarama.LeaveGroupRequest{
		GroupId:  c.groupID,
		MemberId: memberID,
	}); err != nil {
		// 请求出错....
		c.closeCoordinator(broker, err)
	}

	return err
}

// --------------------------------------------------------------------

func (c *Consumer) createConsumer(tomb *loopTomb, topic string, partition int32, info offsetInfo) error {

	memberID, _ := c.membership()
	sarama.Logger.Printf("cluster/consumer %s consume %s/%d from %d\n", memberID, topic, partition, info.NextOffset(c.client.config.Consumer.Offsets.Initial))


	// Create partitionConsumer
	// 创建 partitionConsumer 来消费 topic-partition 上数据
	pc, err := newPartitionConsumer(c.consumer, topic, partition, info, c.client.config.Consumer.Offsets.Initial)
	if err != nil {
		return err
	}


	// Store partitionConsumer in subscriptions
	// 把 partitionConsumer 订阅信息登记在 c.subs 中
	c.subs.Store(topic, partition, pc)


	// Start partition consumer goroutine
	// 启动后台协程
	tomb.Go(func(stopper <-chan none) {
		// 如果是独立消费模式，消费之直接通过 partitionConsumer.Messages() 和 partitionConsumer.Errors() 来消费，这里不需要做什么。
		if c.client.config.Group.Mode == ConsumerModePartitions {
			pc.waitFor(stopper)
		// 如果是多路复用模式，则每个 pc *partitionConsumer 的 message、error 都汇总到 consumer.messages 和 consumer.errors 管道中。
		} else {
			pc.multiplex(stopper, c.messages, c.errors)
		}
	})


	// 如果是独立消费模式，把 pc 推送到 c.partitions 管道中
	if c.client.config.Group.Mode == ConsumerModePartitions {
		select {
		case c.partitions <- pc:
		case <-c.dying:
			pc.Close()
		}
	}

	return nil
}

func (c *Consumer) commitOffsetsWithRetry(retries int) error {
	// 调用 CommitOffsets() 手动提交偏移量
	err := c.CommitOffsets()
	// 如果提交出错，则重试
	if err != nil && retries > 0 {
		return c.commitOffsetsWithRetry(retries - 1) // 递归调用
	}
	return err
}

func (c *Consumer) closeCoordinator(broker *sarama.Broker, err error) {

	// 关闭同 broker 的网络连接
	if broker != nil {
		_ = broker.Close()
	}

	// 在出错情况下，刷新 groupId 的 coordinator
	switch err {
	case sarama.ErrConsumerCoordinatorNotAvailable, sarama.ErrNotCoordinatorForConsumer:
		_ = c.client.RefreshCoordinator(c.groupID)
	}
}


// 获取哪些不在 coreTopics 但是在白名单里的 topics 列表
func (c *Consumer) selectExtraTopics(allTopics []string) []string {
	extra := allTopics[:0]
	for _, topic := range allTopics {
		if !c.isKnownCoreTopic(topic) && c.isPotentialExtraTopic(topic) {
			extra = append(extra, topic)
		}
	}
	return extra
}


// 检查 topic 是否存在于 c.coreTopics 中
func (c *Consumer) isKnownCoreTopic(topic string) bool {
	pos := sort.SearchStrings(c.coreTopics, topic)
	return pos < len(c.coreTopics) && c.coreTopics[pos] == topic
}

// 检查 topic 是否存在于 c.extraTopics 中
func (c *Consumer) isKnownExtraTopic(topic string) bool {
	pos := sort.SearchStrings(c.extraTopics, topic)
	return pos < len(c.extraTopics) && c.extraTopics[pos] == topic
}


func (c *Consumer) isPotentialExtraTopic(topic string) bool {

	rx := c.client.config.Group.Topics

	// 如果 topic 在黑名单中，返回 false
	if rx.Blacklist != nil && rx.Blacklist.MatchString(topic) {
		return false
	}

	// 如果 topic 在白名单中，返回 true
	if rx.Whitelist != nil && rx.Whitelist.MatchString(topic) {
		return true
	}

	return false
}



func (c *Consumer) refreshCoordinator() error {

	if err := c.refreshMetadata(); err != nil {
		return err
	}

	// 根据 groupId 获取 Coordinator 并缓存到本地。
	return c.client.RefreshCoordinator(c.groupID)
}


func (c *Consumer) refreshMetadata() (err error) {

	// 如果需要保存所有 topic 的 meta 数据，就直接调用 RefreshMetadata()
	if c.client.config.Metadata.Full {
		err = c.client.RefreshMetadata()
	} else {
		// 否则，先通过 Topics() 获取可用 topic ，然后刷新他们
		var topics []string
		if topics, err = c.client.Topics(); err == nil && len(topics) != 0 {
			err = c.client.RefreshMetadata(topics...)
		}
	}

	// maybe we didn't have authorization to describe all topics
	// 也许有些主题没有订阅权限，那么就只刷新需要订阅的主题
	switch err {
	case sarama.ErrTopicAuthorizationFailed:
		err = c.client.RefreshMetadata(c.coreTopics...)
	}

	return
}

func (c *Consumer) membership() (memberID string, generationID int32) {
	c.membershipMu.RLock()
	memberID, generationID = c.memberID, c.generationID
	c.membershipMu.RUnlock()
	return
}
