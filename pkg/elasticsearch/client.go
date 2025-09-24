package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/coorrer/erebus/internal/config"
	"github.com/zeromicro/go-zero/core/logx"
)

// Client Elasticsearch客户端
type Client struct {
	config     config.DataSource
	httpClient *http.Client
	baseURL    string
}

// IndexDocument 索引文档结构
type IndexDocument struct {
	ID      string                 `json:"id,omitempty"`
	Index   string                 `json:"index,omitempty"`
	Body    map[string]interface{} `json:"body"`
	Routing string                 `json:"routing,omitempty"`
}

// BulkResponse 批量操作响应
type BulkResponse struct {
	Took   int64                         `json:"took"`
	Errors bool                          `json:"errors"`
	Items  []map[string]BulkItemResponse `json:"items"`
}

// BulkItemResponse 批量操作项响应
type BulkItemResponse struct {
	Index   string     `json:"_index"`
	Type    string     `json:"_type"`
	ID      string     `json:"_id"`
	Version int64      `json:"_version"`
	Result  string     `json:"result"`
	Status  int        `json:"status"`
	Error   *BulkError `json:"error,omitempty"`
}

// BulkError 批量操作错误
type BulkError struct {
	Type   string     `json:"type"`
	Reason string     `json:"reason"`
	Cause  *BulkError `json:"caused_by,omitempty"`
}

// SearchResponse 搜索响应
type SearchResponse struct {
	Took     int64      `json:"took"`
	TimedOut bool       `json:"timed_out"`
	Hits     SearchHits `json:"hits"`
}

// SearchHits 搜索命中结果
type SearchHits struct {
	Total SearchTotal `json:"total"`
	Hits  []SearchHit `json:"hits"`
}

// SearchTotal 命中总数
type SearchTotal struct {
	Value    int64  `json:"value"`
	Relation string `json:"relation"`
}

// SearchHit 搜索命中
type SearchHit struct {
	Index  string                 `json:"_index"`
	Type   string                 `json:"_type"`
	ID     string                 `json:"_id"`
	Score  float64                `json:"_score"`
	Source map[string]interface{} `json:"_source"`
}

// IndexSettings 索引设置
type IndexSettings struct {
	NumberOfShards   int                    `json:"number_of_shards,omitempty"`
	NumberOfReplicas int                    `json:"number_of_replicas,omitempty"`
	Analysis         map[string]interface{} `json:"analysis,omitempty"`
}

// IndexMapping 索引映射
type IndexMapping struct {
	Properties map[string]interface{} `json:"properties,omitempty"`
}

// IndexCreateRequest 创建索引请求
type IndexCreateRequest struct {
	Settings *IndexSettings `json:"settings,omitempty"`
	Mappings *IndexMapping  `json:"mappings,omitempty"`
}

// NewClient 创建新的Elasticsearch客户端
func NewClient(cfg config.DataSource) (*Client, error) {
	// 构建基础URL
	baseURL := fmt.Sprintf("http://%s:%d", cfg.Host, cfg.Port)
	if cfg.Database != "" {
		baseURL = fmt.Sprintf("%s/%s", baseURL, cfg.Database)
	}

	client := &Client{
		config: cfg,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		baseURL: baseURL,
	}

	// 测试连接
	if err := client.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to Elasticsearch: %v", err)
	}

	logx.Infof("Elasticsearch client initialized for %s", baseURL)
	return client, nil
}

// Ping 测试连接
func (c *Client) Ping() error {
	url := fmt.Sprintf("%s/_cluster/health", c.baseURL)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("elasticsearch ping failed with status: %d", resp.StatusCode)
	}

	return nil
}

// BulkIndex 批量索引文档
func (c *Client) BulkIndex(index string, docs []IndexDocument) (*BulkResponse, error) {
	if len(docs) == 0 {
		return &BulkResponse{}, nil
	}

	// 构建批量请求体
	var buffer bytes.Buffer
	for _, doc := range docs {
		// 动作和元数据
		action := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": index,
				"_id":    doc.ID,
			},
		}

		actionBytes, _ := json.Marshal(action)
		buffer.Write(actionBytes)
		buffer.WriteByte('\n')

		// 文档数据
		docBytes, _ := json.Marshal(doc.Body)
		buffer.Write(docBytes)
		buffer.WriteByte('\n')
	}

	url := fmt.Sprintf("%s/_bulk", c.baseURL)
	req, err := http.NewRequest("POST", url, &buffer)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bulk index failed with status %d: %s", resp.StatusCode, string(body))
	}

	var bulkResponse BulkResponse
	if err := json.Unmarshal(body, &bulkResponse); err != nil {
		return nil, fmt.Errorf("failed to parse bulk response: %v", err)
	}

	// 检查是否有错误
	if bulkResponse.Errors {
		for _, item := range bulkResponse.Items {
			for operation, result := range item {
				if result.Error != nil {
					logx.Errorf("Bulk operation %s failed for doc %s: %s", operation, result.ID, result.Error.Reason)
				}
			}
		}
	}

	return &bulkResponse, nil
}

// BulkDelete 批量删除文档
func (c *Client) BulkDelete(index string, documentIDs []string) (*BulkResponse, error) {
	if len(documentIDs) == 0 {
		return &BulkResponse{}, nil
	}

	// 构建批量删除请求体
	var buffer bytes.Buffer
	for _, docID := range documentIDs {
		action := map[string]interface{}{
			"delete": map[string]interface{}{
				"_index": index,
				"_id":    docID,
			},
		}

		actionBytes, _ := json.Marshal(action)
		buffer.Write(actionBytes)
		buffer.WriteByte('\n')
	}

	url := fmt.Sprintf("%s/_bulk", c.baseURL)
	req, err := http.NewRequest("POST", url, &buffer)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bulk delete failed with status %d: %s", resp.StatusCode, string(body))
	}

	var bulkResponse BulkResponse
	if err := json.Unmarshal(body, &bulkResponse); err != nil {
		return nil, fmt.Errorf("failed to parse bulk delete response: %v", err)
	}

	return &bulkResponse, nil
}

// CreateIndex 创建索引
func (c *Client) CreateIndex(index string, settings *IndexSettings, mappings *IndexMapping) error {
	url := fmt.Sprintf("%s/%s", c.baseURL, index)

	createRequest := IndexCreateRequest{
		Settings: settings,
		Mappings: mappings,
	}

	body, err := json.Marshal(createRequest)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(body))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		responseBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("create index failed with status %d: %s", resp.StatusCode, string(responseBody))
	}

	logx.Infof("Index %s created successfully", index)
	return nil
}

// DeleteIndex 删除索引
func (c *Client) DeleteIndex(index string) error {
	url := fmt.Sprintf("%s/%s", c.baseURL, index)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}

	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete index failed with status: %d", resp.StatusCode)
	}

	logx.Infof("Index %s deleted successfully", index)
	return nil
}

// IndexExists 检查索引是否存在
func (c *Client) IndexExists(index string) (bool, error) {
	url := fmt.Sprintf("%s/%s", c.baseURL, index)
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return false, err
	}

	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK, nil
}

// Search 执行搜索查询
func (c *Client) Search(index string, query map[string]interface{}) (*SearchResponse, error) {
	url := fmt.Sprintf("%s/%s/_search", c.baseURL, index)

	body, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("search failed with status %d: %s", resp.StatusCode, string(responseBody))
	}

	var searchResponse SearchResponse
	if err := json.Unmarshal(responseBody, &searchResponse); err != nil {
		return nil, fmt.Errorf("failed to parse search response: %v", err)
	}

	return &searchResponse, nil
}

// GetDocument 获取单个文档
func (c *Client) GetDocument(index, docID string) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/%s/_doc/%s", c.baseURL, index, docID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get document failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Source map[string]interface{} `json:"_source"`
		Found  bool                   `json:"found"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	if !result.Found {
		return nil, fmt.Errorf("document %s not found in index %s", docID, index)
	}

	return result.Source, nil
}

// UpdateDocument 更新文档
func (c *Client) UpdateDocument(index, docID string, doc map[string]interface{}) error {
	url := fmt.Sprintf("%s/%s/_doc/%s", c.baseURL, index, docID)

	body, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(body))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		responseBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("update document failed with status %d: %s", resp.StatusCode, string(responseBody))
	}

	return nil
}

// DeleteDocument 删除单个文档
func (c *Client) DeleteDocument(index, docID string) error {
	url := fmt.Sprintf("%s/%s/_doc/%s", c.baseURL, index, docID)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}

	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete document failed with status: %d", resp.StatusCode)
	}

	return nil
}

// RefreshIndex 刷新索引
func (c *Client) RefreshIndex(index string) error {
	url := fmt.Sprintf("%s/%s/_refresh", c.baseURL, index)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return err
	}

	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("refresh index failed with status: %d", resp.StatusCode)
	}

	return nil
}

// GetIndexStats 获取索引统计信息
func (c *Client) GetIndexStats(index string) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/%s/_stats", c.baseURL, index)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get index stats failed with status %d: %s", resp.StatusCode, string(body))
	}

	var stats map[string]interface{}
	if err := json.Unmarshal(body, &stats); err != nil {
		return nil, err
	}

	return stats, nil
}

// setAuth 设置认证信息
func (c *Client) setAuth(req *http.Request) {
	if c.config.User != "" && c.config.Password != "" {
		req.SetBasicAuth(c.config.User, c.config.Password)
	}
}

// Close 关闭客户端
func (c *Client) Close() error {
	// HTTP客户端不需要显式关闭，但可以清理资源
	c.httpClient = nil
	logx.Info("Elasticsearch client closed")
	return nil
}

// ExecuteQuery 执行原始查询（用于复杂的ES查询）
func (c *Client) ExecuteQuery(method, endpoint string, body interface{}) (map[string]interface{}, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyBytes, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		bodyReader = bytes.NewBuffer(bodyBytes)
	}

	url := fmt.Sprintf("%s/%s", c.baseURL, strings.TrimPrefix(endpoint, "/"))
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return nil, err
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	c.setAuth(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("query failed with status %d: %s", resp.StatusCode, string(responseBody))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(responseBody, &result); err != nil {
		return nil, err
	}

	return result, nil
}
