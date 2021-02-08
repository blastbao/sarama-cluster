package cluster

import (
	"regexp"
	"time"

	"github.com/Shopify/sarama"
)

var minVersion = sarama.V0_9_0_0



type ConsumerMode uint8


const (
	ConsumerModeMultiplex ConsumerMode = iota 	// 多路复用: 不同 topic/partiton 下的 messages 和 errors 共用管道
	ConsumerModePartitions						// 分区独立: 不同 topic/partiton 下的 messages 和 errors 独立管道
)

// Config extends sarama.Config with Group specific namespace
type Config struct {


	// 匿名包含 `sarama.Config`
	sarama.Config


	// Group is the namespace for group management properties
	//
	// Group 消费组配置
	Group struct {

		// The strategy to use for the allocation of partitions to consumers (defaults to StrategyRange)
		// 分区分配策略 ( 默认为 StrategyRange )
		PartitionStrategy Strategy


		// By default, messages and errors from the subscribed topics and partitions are all multiplexed and
		// made available through the consumer's Messages() and Errors() channels.
		//
		// Users who require low-level access can enable ConsumerModePartitions where individual partitions
		// are exposed on the Partitions() channel. Messages and errors must then be consumed on the partitions
		// themselves.
		//
		//
		// 多路复用 or 独立通道:
		//
		// 默认情况下，来自所订阅 topics 和 partitions 的 messages 和 errors 是多路复用的，
		// 订阅者通过 messages() 和 errors() 管道来获取。
		//
		// 使用低级 API 的用户可以开启 ConsumerModePartitions ，这样便能够在 Partition 维度上独立订阅 messages 和 errors 。
		Mode ConsumerMode


		// 偏移量提交方式:
		Offsets struct {
			Retry struct {
				// The number retries when committing offsets (defaults to 3).
				// 提交偏移量失败时的重试次数(默认为3)。
				Max int
			}
			Synchronization struct {
				// The duration allowed for other clients to commit their offsets before resumption in this client, e.g. during a rebalance
				// NewConfig sets this to the Consumer.MaxProcessingTime duration of the Sarama configuration
				//
				// 允许其他客户端在此客户端恢复前提交他们的 offsets ，例如在 rebalance 期间。
				DwellTime time.Duration
			}
		}


		Session struct {
			// The allowed session timeout for registered consumers (defaults to 30s).
			// Must be within the allowed server range.
			Timeout time.Duration
		}


		Heartbeat struct {
			// Interval between each heartbeat (defaults to 3s). It should be no more
			// than 1/3rd of the Group.Session.Timeout setting
			Interval time.Duration
		}


		// Return specifies which group channels will be populated. If they are set to true,
		// you must read from the respective channels to prevent deadlock.
		Return struct {
			// If enabled, rebalance notification will be returned on the
			// Notifications channel (default disabled).
			Notifications bool
		}

		// ???
		Topics struct {
			// An additional whitelist of topics to subscribe to.
			// 要订阅的 topic 白名单
			Whitelist *regexp.Regexp

			// An additional blacklist of topics to avoid. If set, this will precede over the Whitelist setting.
			// 不能订阅的 topic 黑名单
			Blacklist *regexp.Regexp
		}

		Member struct {
			// Custom metadata to include when joining the group. The user data for all joined members
			// can be retrieved by sending a DescribeGroupRequest to the broker that is the
			// coordinator for the group.
			//
			// 在加入消费组时可以传递自定义元数据。
			UserData []byte
		}
	}
}

// NewConfig returns a new configuration instance with sane defaults.
//
// NewConfig 创建一个默认的消费组配置。
func NewConfig() *Config {
	c := &Config{
		Config: *sarama.NewConfig(),
	}
	c.Group.PartitionStrategy = StrategyRange
	c.Group.Offsets.Retry.Max = 3
	c.Group.Offsets.Synchronization.DwellTime = c.Consumer.MaxProcessingTime
	c.Group.Session.Timeout = 30 * time.Second
	c.Group.Heartbeat.Interval = 3 * time.Second
	c.Config.Version = minVersion
	return c
}

// Validate checks a Config instance.
// It will return a sarama.ConfigurationError if the specified values don't make sense.
func (c *Config) Validate() error {

	// 只支持毫秒精度，纳秒会被截断
	if c.Group.Heartbeat.Interval%time.Millisecond != 0 {
		sarama.Logger.Println("Group.Heartbeat.Interval only supports millisecond precision; nanoseconds will be truncated.")
	}

	// 只支持毫秒精度，纳秒会被截断
	if c.Group.Session.Timeout%time.Millisecond != 0 {
		sarama.Logger.Println("Group.Session.Timeout only supports millisecond precision; nanoseconds will be truncated.")
	}

	// 分区分配策略只支持 Range 和 RoundRobin
	if c.Group.PartitionStrategy != StrategyRange && c.Group.PartitionStrategy != StrategyRoundRobin {
		sarama.Logger.Println("Group.PartitionStrategy is not supported; range will be assumed.")
	}

	// 消费者版本号高于 minVersion
	if !c.Version.IsAtLeast(minVersion) {
		sarama.Logger.Println("Version is not supported; 0.9. will be assumed.")
		c.Version = minVersion
	}

	// 调用 sarama.Config 的 validate() 函数
	if err := c.Config.Validate(); err != nil {
		return err
	}

	// validate the Group values
	switch {
	case c.Group.Offsets.Retry.Max < 0:
		return sarama.ConfigurationError("Group.Offsets.Retry.Max must be >= 0")
	case c.Group.Offsets.Synchronization.DwellTime <= 0:
		return sarama.ConfigurationError("Group.Offsets.Synchronization.DwellTime must be > 0")
	case c.Group.Offsets.Synchronization.DwellTime > 10*time.Minute:
		return sarama.ConfigurationError("Group.Offsets.Synchronization.DwellTime must be <= 10m")
	case c.Group.Heartbeat.Interval <= 0:
		return sarama.ConfigurationError("Group.Heartbeat.Interval must be > 0")
	case c.Group.Session.Timeout <= 0:
		return sarama.ConfigurationError("Group.Session.Timeout must be > 0")
	case !c.Metadata.Full && c.Group.Topics.Whitelist != nil:
		return sarama.ConfigurationError("Metadata.Full must be enabled when Group.Topics.Whitelist is used")
	case !c.Metadata.Full && c.Group.Topics.Blacklist != nil:
		return sarama.ConfigurationError("Metadata.Full must be enabled when Group.Topics.Blacklist is used")
	}

	// ensure offset is correct
	switch c.Consumer.Offsets.Initial {
	case sarama.OffsetOldest, sarama.OffsetNewest:
	default:
		return sarama.ConfigurationError("Consumer.Offsets.Initial must be either OffsetOldest or OffsetNewest")
	}

	return nil
}
