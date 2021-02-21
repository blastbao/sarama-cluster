package cluster

import (
	"errors"
	"sync/atomic"

	"github.com/Shopify/sarama"
)

var errClientInUse = errors.New("cluster: client is already used by another consumer")



// Client is a group client
//
// Client 是消费组客户端
type Client struct {
	// 匿名包含 `sarama.Client`，用于拉取元数据
	sarama.Client

	// 消费组配置
	config Config

	// 状态: 空闲=0, 使用中=1
	inUse uint32
}

// NewClient creates a new client instance
func NewClient(addrs []string, config *Config) (*Client, error) {

	// 如果没有指定消费组配置，使用默认配置
	if config == nil {
		config = NewConfig()
	}

	// 校验配置合法性
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// 创建 sarama 客户端，传入 brokers 地址和一些配置信息
	client, err := sarama.NewClient(addrs, &config.Config)
	if err != nil {
		return nil, err
	}

	// 创建 消费组 客户端
	return &Client{Client: client, config: *config}, nil
}

// ClusterConfig returns the cluster configuration.
func (c *Client) ClusterConfig() *Config {
	cfg := c.config
	return &cfg
}

func (c *Client) claim() bool {
	return atomic.CompareAndSwapUint32(&c.inUse, 0, 1)
}

func (c *Client) release() {
	atomic.CompareAndSwapUint32(&c.inUse, 1, 0)
}
