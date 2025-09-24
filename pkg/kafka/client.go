package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go/sasl/plain"
	"sync"
	"time"

	"github.com/coorrer/erebus/internal/config"
	"github.com/segmentio/kafka-go"
	"github.com/zeromicro/go-zero/core/logx"
)

// Client Kafka客户端
type Client struct {
	config    config.DataSource
	writers   map[string]*kafka.Writer // 按主题缓存的writer
	reader    *kafka.Reader
	mu        sync.RWMutex
	connected bool
}

// Message Kafka消息结构
type Message struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int
	Offset    int64
	Timestamp time.Time
	Headers   []Header
}

// Header Kafka消息头
type Header struct {
	Key   string
	Value []byte
}

// ProducerMessage 生产者消息
type ProducerMessage struct {
	Topic    string
	Key      []byte
	Value    []byte
	Headers  []Header
	Metadata interface{}
}

// ConsumerMessage 消费者消息
type ConsumerMessage struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
	Headers   []Header
}

// NewClient 创建新的Kafka客户端
func NewClient(cfg config.DataSource) (*Client, error) {
	client := &Client{
		config:  cfg,
		writers: make(map[string]*kafka.Writer),
	}

	// 测试连接
	if err := client.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to Kafka: %v", err)
	}

	client.connected = true
	logx.Infof("Kafka client initialized for %s:%d", cfg.Host, cfg.Port)
	return client, nil
}

// Ping 测试连接
func (c *Client) Ping() error {
	// 尝试创建临时连接来测试
	conn, err := c.createConn("ping-topic")
	if err != nil {
		return err
	}
	defer conn.Close()

	// 获取broker列表
	brokers, err := conn.Brokers()
	if err != nil {
		return err
	}

	if len(brokers) == 0 {
		return fmt.Errorf("no brokers available")
	}

	logx.Debugf("Kafka connection successful, %d brokers available", len(brokers))
	return nil
}

// createConn 创建Kafka连接
func (c *Client) createConn(topic string) (*kafka.Conn, error) {
	brokers := []string{fmt.Sprintf("%s:%d", c.config.Host, c.config.Port)}
	conn, err := kafka.DialLeader(context.Background(), "tcp", brokers[0], topic, 0)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// getWriter 获取或创建主题的writer
func (c *Client) getWriter(topic string) *kafka.Writer {
	c.mu.RLock()
	writer, exists := c.writers[topic]
	c.mu.RUnlock()

	if exists {
		return writer
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 再次检查，防止重复创建
	if writer, exists := c.writers[topic]; exists {
		return writer
	}

	brokers := []string{fmt.Sprintf("%s:%d", c.config.Host, c.config.Port)}

	// 配置writer
	writerConfig := kafka.WriterConfig{
		Brokers:      brokers,
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 100 * time.Millisecond,
		RequiredAcks: 1,
	}

	// 添加认证
	if c.config.User != "" && c.config.Password != "" {
		writerConfig.Dialer = &kafka.Dialer{
			Timeout: 10 * time.Second,
			SASLMechanism: plain.Mechanism{
				Username: c.config.User,
				Password: c.config.Password,
			},
		}
	}

	writer = kafka.NewWriter(writerConfig)
	c.writers[topic] = writer

	return writer
}

// SendMessage 发送单条消息
func (c *Client) SendMessage(topic string, key, value []byte, headers ...Header) error {
	writer := c.getWriter(topic)

	kafkaHeaders := make([]kafka.Header, len(headers))
	for i, h := range headers {
		kafkaHeaders[i] = kafka.Header{
			Key:   h.Key,
			Value: h.Value,
		}
	}

	msg := kafka.Message{
		Key:     key,
		Value:   value,
		Headers: kafkaHeaders,
		Time:    time.Now(),
	}

	return writer.WriteMessages(context.Background(), msg)
}

// SendMessages 批量发送消息
func (c *Client) SendMessages(topic string, messages []ProducerMessage) error {
	if len(messages) == 0 {
		return nil
	}

	writer := c.getWriter(topic)

	kafkaMessages := make([]kafka.Message, len(messages))
	for i, msg := range messages {
		kafkaHeaders := make([]kafka.Header, len(msg.Headers))
		for j, h := range msg.Headers {
			kafkaHeaders[j] = kafka.Header{
				Key:   h.Key,
				Value: h.Value,
			}
		}

		kafkaMessages[i] = kafka.Message{
			Topic:   msg.Topic,
			Key:     msg.Key,
			Value:   msg.Value,
			Headers: kafkaHeaders,
			Time:    time.Now(),
		}
	}

	return writer.WriteMessages(context.Background(), kafkaMessages...)
}

// SendJSONMessage 发送JSON格式消息
func (c *Client) SendJSONMessage(topic string, key []byte, value interface{}, headers ...Header) error {
	jsonData, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %v", err)
	}

	return c.SendMessage(topic, key, jsonData, headers...)
}

// SendJSONMessages 批量发送JSON格式消息
func (c *Client) SendJSONMessages(topic string, messages []interface{}) error {
	if len(messages) == 0 {
		return nil
	}

	producerMessages := make([]ProducerMessage, len(messages))
	for i, msg := range messages {
		jsonData, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("failed to marshal message %d: %v", i, err)
		}

		producerMessages[i] = ProducerMessage{
			Topic: topic,
			Value: jsonData,
		}
	}

	return c.SendMessages(topic, producerMessages)
}

// CreateConsumer 创建消费者
func (c *Client) CreateConsumer(topic string, groupID string) (*kafka.Reader, error) {
	brokers := []string{fmt.Sprintf("%s:%d", c.config.Host, c.config.Port)}

	config := kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		MaxWait:        1 * time.Second,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: time.Second,
	}

	// 添加认证
	if c.config.User != "" && c.config.Password != "" {
		config.Dialer = &kafka.Dialer{
			Timeout: 10 * time.Second,
			SASLMechanism: plain.Mechanism{
				Username: c.config.User,
				Password: c.config.Password,
			},
		}
	}

	reader := kafka.NewReader(config)
	c.reader = reader

	return reader, nil
}

// ConsumeMessages 消费消息
func (c *Client) ConsumeMessages(ctx context.Context, handler func(message ConsumerMessage) error) error {
	if c.reader == nil {
		return fmt.Errorf("consumer not initialized")
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			msg, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if err == context.Canceled {
					return nil
				}
				logx.Errorf("Error reading message: %v", err)
				continue
			}

			headers := make([]Header, len(msg.Headers))
			for i, h := range msg.Headers {
				headers[i] = Header{
					Key:   h.Key,
					Value: h.Value,
				}
			}

			consumerMsg := ConsumerMessage{
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Key:       msg.Key,
				Value:     msg.Value,
				Timestamp: msg.Time,
				Headers:   headers,
			}

			if err := handler(consumerMsg); err != nil {
				logx.Errorf("Error handling message: %v", err)
			}
		}
	}
}

// CreateTopic 创建主题
func (c *Client) CreateTopic(topic string, partitions int, replicationFactor int) error {
	conn, err := c.createConn(topic)
	if err != nil {
		return err
	}
	defer conn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: replicationFactor,
		},
	}

	return conn.CreateTopics(topicConfigs...)
}

// DeleteTopic 删除主题
func (c *Client) DeleteTopic(topic string) error {
	conn, err := c.createConn(topic)
	if err != nil {
		return err
	}
	defer conn.Close()

	return conn.DeleteTopics(topic)
}

// Close 关闭客户端
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 关闭所有writers
	for topic, writer := range c.writers {
		if err := writer.Close(); err != nil {
			logx.Errorf("Error closing writer for topic %s: %v", topic, err)
		}
	}

	// 关闭reader
	if c.reader != nil {
		if err := c.reader.Close(); err != nil {
			logx.Errorf("Error closing reader: %v", err)
		}
	}

	c.connected = false
	logx.Info("Kafka client closed")
	return nil
}

// IsConnected 检查连接状态
func (c *Client) IsConnected() bool {
	return c.connected
}

// GetConfig 获取配置
func (c *Client) GetConfig() config.DataSource {
	return c.config
}

// HealthCheck 健康检查
func (c *Client) HealthCheck() error {
	if !c.connected {
		return fmt.Errorf("kafka client is not connected")
	}

	return c.Ping()
}

// GetTopics 获取所有主题
func (c *Client) GetTopics() ([]string, error) {
	conn, err := c.createConn("__consumer_offsets") // 使用内部主题测试连接
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, err
	}

	topics := make(map[string]bool)
	for _, p := range partitions {
		topics[p.Topic] = true
	}

	result := make([]string, 0, len(topics))
	for topic := range topics {
		result = append(result, topic)
	}

	return result, nil
}
