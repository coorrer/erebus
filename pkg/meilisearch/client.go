// 文件: pkg/meilisearch/client.go
package meilisearch

import (
	"fmt"
	"github.com/coorrer/erebus/internal/config"
	"github.com/meilisearch/meilisearch-go"
	"github.com/zeromicro/go-zero/core/logx"
	"net/http"
	"time"
)

// Config Meilisearch 客户端配置
type Config struct {
	Host    string `json:"Host"`
	Port    int    `json:"Port"`
	APIKey  string `json:"APIKey,optional"`
	Index   string `json:"Index,optional"`
	Timeout int    `json:"Timeout,optional"` // 超时时间（秒）
}

// Client 封装 Meilisearch 客户端
type Client struct {
	client meilisearch.ServiceManager
	config config.DataSource
}

// Document Meilisearch 文档
type Document map[string]interface{}

// SearchResponse 搜索响应
type SearchResponse struct {
	//Hits   []interface{} `json:"hits"`
	Offset int64 `json:"offset"`
	Limit  int64 `json:"limit"`
	Total  int64 `json:"total"`
}

// NewClient 创建新的 Meilisearch 客户端
func NewClient(cfg config.DataSource) (*Client, error) {
	// 设置默认值
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10
	}
	// 创建客户端
	client := meilisearch.New(cfg.Host, meilisearch.WithAPIKey(cfg.APIKey), meilisearch.WithCustomClient(&http.Client{Timeout: cfg.Timeout}))

	health, err := client.Health()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Meilisearch: %v", err)
	}

	if health.Status != "available" {
		return nil, fmt.Errorf("Meilisearch is not available: %s", health.Status)
	}

	logx.Infof("Connected to Meilisearch at %s:%d", cfg.Host, cfg.Port)
	return &Client{client: client, config: cfg}, nil
}

// HealthCheck 健康检查
func (c *Client) HealthCheck() error {
	_, err := c.client.Health()
	return err
}

// Close 关闭连接
func (c *Client) Close() error {
	// Meilisearch 客户端不需要显式关闭连接
	return nil
}

// IndexExists 检查索引是否存在
func (c *Client) IndexExists(index string) (bool, error) {
	if index == "" {
		index = c.config.Index
	}

	_, err := c.client.Index(index).FetchInfo()
	if err != nil {
		if meiliError, ok := err.(*meilisearch.Error); ok && meiliError.StatusCode == 404 {
			return false, nil
		}
		return false, fmt.Errorf("failed to check index existence: %v", err)
	}

	return true, nil
}

// CreateIndex 创建索引
func (c *Client) CreateIndex(index string, primaryKey string) error {
	if index == "" {
		index = c.config.Index
	}

	_, err := c.client.CreateIndex(&meilisearch.IndexConfig{
		Uid:        index,
		PrimaryKey: primaryKey,
	})

	if err != nil {
		return fmt.Errorf("failed to create index: %v", err)
	}

	logx.Infof("Created index: %s", index)
	return nil
}

// AddDocuments 添加文档到索引
func (c *Client) AddDocuments(index string, documents []Document) (*meilisearch.TaskInfo, error) {
	if index == "" {
		index = c.config.Index
	}

	task, err := c.client.Index(index).AddDocuments(documents, &index)
	if err != nil {
		return nil, fmt.Errorf("failed to add documents: %v", err)
	}

	logx.Infof("Added %d documents to index %s (task: %d)", len(documents), index, task.TaskUID)
	return task, nil
}

// UpdateDocuments 更新文档
func (c *Client) UpdateDocuments(index string, documents []Document) (*meilisearch.TaskInfo, error) {
	if index == "" {
		index = c.config.Index
	}

	task, err := c.client.Index(index).UpdateDocuments(documents, &index)
	if err != nil {
		return nil, fmt.Errorf("failed to update documents: %v", err)
	}

	logx.Infof("Updated %d documents in index %s (task: %d)", len(documents), index, task.TaskUID)
	return task, nil
}

// DeleteDocuments 删除文档
func (c *Client) DeleteDocuments(index string, documentIDs []string) (*meilisearch.TaskInfo, error) {
	if index == "" {
		index = c.config.Index
	}

	task, err := c.client.Index(index).DeleteDocuments(documentIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to delete documents: %v", err)
	}

	logx.Infof("Deleted %d documents from index %s (task: %d)", len(documentIDs), index, task.TaskUID)
	return task, nil
}

// DeleteAllDocuments 删除所有文档
func (c *Client) DeleteAllDocuments(index string) (*meilisearch.TaskInfo, error) {
	if index == "" {
		index = c.config.Index
	}

	task, err := c.client.Index(index).DeleteAllDocuments()
	if err != nil {
		return nil, fmt.Errorf("failed to delete all documents: %v", err)
	}

	logx.Infof("Deleted all documents from index %s (task: %d)", index, task.TaskUID)
	return task, nil
}

// Search 执行搜索
func (c *Client) Search(index, query string, options *meilisearch.SearchRequest) (*SearchResponse, error) {
	if index == "" {
		index = c.config.Index
	}

	result, err := c.client.Index(index).Search(query, options)
	if err != nil {
		return nil, fmt.Errorf("failed to search: %v", err)
	}

	return &SearchResponse{
		//Hits:   result.Hits,
		Offset: result.Offset,
		Limit:  result.Limit,
		Total:  result.TotalHits,
	}, nil
}

// GetTask 获取任务状态
func (c *Client) GetTask(taskID int64) (*meilisearch.Task, error) {
	task, err := c.client.GetTask(taskID)
	if err != nil {
		return nil, fmt.Errorf("failed to get task: %v", err)
	}

	return task, nil
}

// WaitForTask 等待任务完成
func (c *Client) WaitForTask(taskID int64, timeout time.Duration) (*meilisearch.Task, error) {
	task, err := c.client.WaitForTask(taskID, timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to wait for task: %v", err)
	}

	return task, nil
}

// GetIndexStats 获取索引统计信息
func (c *Client) GetIndexStats(index string) (*meilisearch.StatsIndex, error) {
	if index == "" {
		index = c.config.Index
	}

	stats, err := c.client.Index(index).GetStats()
	if err != nil {
		return nil, fmt.Errorf("failed to get index stats: %v", err)
	}

	return stats, nil
}

// CreateIndexIfNotExists 如果索引不存在则创建
func (c *Client) CreateIndexIfNotExists(index, primaryKey string) error {
	exists, err := c.IndexExists(index)
	if err != nil {
		return err
	}

	if !exists {
		return c.CreateIndex(index, primaryKey)
	}

	return nil
}
