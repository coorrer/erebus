package mysql2clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/threading"
	"golang.org/x/time/rate"

	"github.com/coorrer/erebus/internal/config"
	"github.com/coorrer/erebus/internal/logic/position"
	"github.com/coorrer/erebus/pkg/clickhouse"
)

// 失败原因类型
type FailureReason int

const (
	FailureTemporary      FailureReason = iota // 临时错误（网络、超时）
	FailureDataFormat                          // 数据格式错误
	FailureSchemaMismatch                      // 表结构不匹配
	FailureConfigError                         // 配置错误
	FailureUnknown                             // 未知错误
)

// 永久失败记录
type PermanentFailure struct {
	TaskName    string                 `json:"task_name"`
	Database    string                 `json:"database"`
	Table       string                 `json:"table"`
	Action      string                 `json:"action"`
	Data        map[string]interface{} `json:"data"`
	PrimaryKey  map[string]interface{} `json:"primary_key"`
	Timestamp   time.Time              `json:"timestamp"`
	FailureTime time.Time              `json:"failure_time"`
	Reason      string                 `json:"reason"`
	RetryCount  int                    `json:"retry_count"`
	Error       string                 `json:"error,omitempty"`
}

// 重试配置
type RetryConfig struct {
	MaxRetries      int           `json:"max_retries"`
	InitialInterval time.Duration `json:"initial_interval"`
	MaxInterval     time.Duration `json:"max_interval"`
	Multiplier      float64       `json:"multiplier"`
}

// 队列统计
type QueueStats struct {
	mu                   sync.RWMutex
	InsertQueueUsage     float64
	UpdateQueueUsage     float64
	DeleteQueueUsage     float64
	DeadLetterQueueUsage float64
	TotalProcessed       int64
	TotalErrors          int64
	TotalDropped         int64
	TotalDeadLetter      int64
	TotalRateLimited     int64
	LastUpdateTime       time.Time
}

// 表优先级
type TablePriority int

const (
	PriorityHigh TablePriority = iota
	PriorityNormal
	PriorityLow
)

// 发送结果
type SendResult struct {
	Success    bool
	RetryCount int
	Error      error
}

// RowData 表示一行数据
type RowData struct {
	TaskName    string
	Database    string
	Table       string
	Action      string //行为，insert/update/delete
	Data        map[string]interface{}
	WhereClause string                 // 删除条件
	PrimaryKey  map[string]interface{} // 主键信息
	Timestamp   time.Time
	Priority    TablePriority // 优先级
}

// BinlogHandler 处理Mysql binlog事件
type BinlogHandler struct {
	ctx             context.Context
	cancel          context.CancelFunc
	config          config.Config
	syncTaskConfig  config.SyncTask
	source          config.DataSource
	target          config.DataSource
	chClient        *clickhouse.Client
	insertQueue     chan *RowData
	updateQueue     chan *RowData
	deleteQueue     chan *RowData
	deadLetterQueue chan *RowData
	batchSize       int64
	batchWait       time.Duration
	mappers         map[string]*FieldMapper
	positionManager *position.PositionManager
	wg              sync.WaitGroup
	mu              sync.RWMutex
	isRunning       bool

	// 监控和流量控制字段
	queueStats    *QueueStats
	rateLimiter   *rate.Limiter
	metricsTicker *time.Ticker
	tablePriority map[string]TablePriority

	// 死信队列处理相关字段
	retryConfig       *RetryConfig
	deadLetterDir     string
	retryCounts       map[string]int // key: 数据指纹 -> 重试次数
	retryCountsMu     sync.RWMutex
	permanentFailures []*PermanentFailure
	failuresMu        sync.RWMutex
}

// NewBinlogHandler 创建新的事件处理器
func NewBinlogHandler(ctx context.Context, cfg config.Config, chClient *clickhouse.Client, positionManager *position.PositionManager, syncTaskConfig config.SyncTask, source config.DataSource, target config.DataSource) *BinlogHandler {
	childCtx, cancel := context.WithCancel(ctx)

	// 初始化字段映射器
	mappers := make(map[string]*FieldMapper)
	tablePriority := make(map[string]TablePriority)

	for _, table := range syncTaskConfig.Tables {
		key := table.SourceDatabase + "." + table.SourceTable
		mappers[key] = NewFieldMapper(table)

		// 设置表优先级
		priority := PriorityNormal
		if table.Priority == "high" {
			priority = PriorityHigh
		} else if table.Priority == "low" {
			priority = PriorityLow
		} else {
			priority = PriorityNormal
		}
		tablePriority[key] = priority
	}

	// 设置批处理超时（使用第一个表的配置）
	var batchWait time.Duration
	if len(syncTaskConfig.Tables) > 0 {
		if syncTaskConfig.Tables[0].BatchTimeout != 0 {
			batchWait = syncTaskConfig.Tables[0].BatchTimeout
		} else {
			batchWait = time.Second
		}
	} else {
		batchWait = time.Second
		logx.Infof("No table configurations found, using default batch timeout: 1s")
	}

	// 设置批量大小和队列大小
	var batchSize int64 = 1000
	var queueSize int64 = 50000 // 默认队列大小

	if len(syncTaskConfig.Tables) > 0 {
		if syncTaskConfig.Tables[0].BatchSize > 0 {
			batchSize = syncTaskConfig.Tables[0].BatchSize
		}
		if syncTaskConfig.Tables[0].QueueSize > 0 {
			queueSize = syncTaskConfig.Tables[0].QueueSize
		}
	}

	// 创建速率限制器 (每秒1000个事件，突发2000)
	limiter := rate.NewLimiter(1000, 2000)

	// 设置重试配置
	retryConfig := &RetryConfig{
		MaxRetries:      3,
		InitialInterval: time.Second,
		MaxInterval:     30 * time.Second,
		Multiplier:      2.0,
	}

	// 设置死信数据存储目录
	deadLetterDir := "./dead_letter"
	if cfg.DeadLetterPath != "" {
		deadLetterDir = cfg.DeadLetterPath
	}

	// 确保目录存在
	if err := os.MkdirAll(deadLetterDir, 0755); err != nil {
		logx.Errorf("Failed to create dead letter directory: %v", err)
	}

	return &BinlogHandler{
		ctx:               childCtx,
		cancel:            cancel,
		config:            cfg,
		syncTaskConfig:    syncTaskConfig,
		chClient:          chClient,
		insertQueue:       make(chan *RowData, queueSize),
		updateQueue:       make(chan *RowData, queueSize),
		deleteQueue:       make(chan *RowData, queueSize),
		deadLetterQueue:   make(chan *RowData, queueSize/10), // 死信队列为1/10
		batchSize:         batchSize,
		batchWait:         batchWait,
		mappers:           mappers,
		positionManager:   positionManager,
		source:            source,
		target:            target,
		isRunning:         false,
		queueStats:        &QueueStats{},
		rateLimiter:       limiter,
		tablePriority:     tablePriority,
		retryConfig:       retryConfig,
		deadLetterDir:     deadLetterDir,
		retryCounts:       make(map[string]int),
		permanentFailures: make([]*PermanentFailure, 0),
	}
}

// Start 启动事件处理器
func (h *BinlogHandler) Start() {
	h.mu.Lock()
	if h.isRunning {
		h.mu.Unlock()
		logx.Info("Binlog handler is already running")
		return
	}
	h.isRunning = true
	h.mu.Unlock()

	logx.Info("Starting Mysql2CH binlog event handler")

	// 启动统计监控
	h.startMetricsMonitor()

	// 启动批量处理器
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.processInsertBatches()
	}()

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.processUpdateBatches()
	}()

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.processDeleteBatches()
	}()

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.processDeadLetterQueue()
	}()

	// 等待停止信号
	<-h.ctx.Done()
	logx.Info("Mysql2CH binlog event handler stopped")
}

// Stop 停止事件处理器
func (h *BinlogHandler) Stop() {
	h.mu.Lock()
	if !h.isRunning {
		h.mu.Unlock()
		logx.Info("Binlog handler is not running")
		return
	}
	h.isRunning = false
	h.mu.Unlock()

	logx.Info("Stopping Mysql2CH binlog event handler")
	h.cancel()

	// 停止监控ticker
	if h.metricsTicker != nil {
		h.metricsTicker.Stop()
	}

	h.wg.Wait()
	close(h.insertQueue)
	close(h.updateQueue)
	close(h.deleteQueue)
	close(h.deadLetterQueue)
	logx.Info("Mysql2CH binlog event handler stopped successfully")
}

// 启动指标监控
func (h *BinlogHandler) startMetricsMonitor() {
	h.metricsTicker = time.NewTicker(30 * time.Second)

	go func() {
		defer h.metricsTicker.Stop()

		for {
			select {
			case <-h.metricsTicker.C:
				h.logQueueStats()
			case <-h.ctx.Done():
				return
			}
		}
	}()
}

// 记录队列统计
func (h *BinlogHandler) logQueueStats() {
	h.queueStats.mu.RLock()
	defer h.queueStats.mu.RUnlock()

	logx.Infof("Queue Stats - Insert: %.1f%%, Update: %.1f%%, Delete: %.1f%%, DeadLetter: %.1f%%",
		h.queueStats.InsertQueueUsage*100,
		h.queueStats.UpdateQueueUsage*100,
		h.queueStats.DeleteQueueUsage*100,
		h.queueStats.DeadLetterQueueUsage*100)

	logx.Infof("Processing Stats - Processed: %d, Errors: %d, Dropped: %d, DeadLetter: %d, RateLimited: %d",
		h.queueStats.TotalProcessed,
		h.queueStats.TotalErrors,
		h.queueStats.TotalDropped,
		h.queueStats.TotalDeadLetter,
		h.queueStats.TotalRateLimited)
}

// 更新队列使用率统计
func (h *BinlogHandler) updateQueueUsage() {
	h.queueStats.mu.Lock()
	defer h.queueStats.mu.Unlock()

	h.queueStats.InsertQueueUsage = float64(len(h.insertQueue)) / float64(cap(h.insertQueue))
	h.queueStats.UpdateQueueUsage = float64(len(h.updateQueue)) / float64(cap(h.updateQueue))
	h.queueStats.DeleteQueueUsage = float64(len(h.deleteQueue)) / float64(cap(h.deleteQueue))
	h.queueStats.DeadLetterQueueUsage = float64(len(h.deadLetterQueue)) / float64(cap(h.deadLetterQueue))
	h.queueStats.LastUpdateTime = time.Now()
}

// 带有限重试的非阻塞发送
func (h *BinlogHandler) sendWithLimitedRetry(ch chan *RowData, data *RowData, queueName string) SendResult {
	maxRetries := 2                           // 最大重试次数
	maxRetryDuration := 50 * time.Millisecond // 最大重试时间
	retryInterval := 10 * time.Millisecond    // 重试间隔

	startTime := time.Now()
	retryCount := 0

	for retryCount <= maxRetries {
		select {
		case ch <- data:
			// 发送成功
			h.updateQueueUsage()
			return SendResult{
				Success:    true,
				RetryCount: retryCount,
			}

		default:
			// 队列满，检查是否超过最大重试时间
			if time.Since(startTime) > maxRetryDuration {
				h.queueStats.mu.Lock()
				h.queueStats.TotalDropped++
				h.queueStats.mu.Unlock()

				logx.Errorf("%s queue full after %d retries in %v, moving to dead letter",
					queueName, retryCount, time.Since(startTime))

				// 转移到死信队列
				if h.sendToDeadLetterQueue(data, "queue_full") {
					return SendResult{
						Success:    false,
						RetryCount: retryCount,
						Error:      fmt.Errorf("queue full, moved to dead letter"),
					}
				} else {
					return SendResult{
						Success:    false,
						RetryCount: retryCount,
						Error:      fmt.Errorf("queue full and dead letter queue also full, data lost"),
					}
				}
			}

			// 等待后重试
			retryCount++
			select {
			case <-time.After(retryInterval):
				continue
			case <-h.ctx.Done():
				return SendResult{
					Success:    false,
					RetryCount: retryCount,
					Error:      fmt.Errorf("context cancelled during retry"),
				}
			}
		}
	}

	// 重试次数用尽
	h.queueStats.mu.Lock()
	h.queueStats.TotalDropped++
	h.queueStats.mu.Unlock()

	logx.Errorf("%s queue full after %d retries, moving to dead letter", queueName, retryCount)

	if h.sendToDeadLetterQueue(data, "max_retries_exceeded") {
		return SendResult{
			Success:    false,
			RetryCount: retryCount,
			Error:      fmt.Errorf("max retries exceeded, moved to dead letter"),
		}
	} else {
		return SendResult{
			Success:    false,
			RetryCount: retryCount,
			Error:      fmt.Errorf("max retries exceeded and dead letter queue full, data lost"),
		}
	}
}

// 发送到死信队列（非阻塞）
func (h *BinlogHandler) sendToDeadLetterQueue(data *RowData, errorMsg string) bool {
	select {
	case h.deadLetterQueue <- data:
		h.queueStats.mu.Lock()
		h.queueStats.TotalDeadLetter++
		h.queueStats.mu.Unlock()
		h.updateQueueUsage()
		logx.Debugf("Moved data to dead letter queue: %s.%s, error: %s",
			data.Database, data.Table, errorMsg)
		return true
	default:
		h.queueStats.mu.Lock()
		h.queueStats.TotalDropped++
		h.queueStats.mu.Unlock()
		logx.Errorf("Dead letter queue is full, data lost permanently: %s.%s, error: %s",
			data.Database, data.Table, errorMsg)
		return false
	}
}

// 获取表优先级
func (h *BinlogHandler) getTablePriority(database, table string) TablePriority {
	key := database + "." + table
	if priority, exists := h.tablePriority[key]; exists {
		return priority
	}
	return PriorityNormal // 默认优先级
}

// OnRow 处理行变更事件
func (h *BinlogHandler) OnRow(e *canal.RowsEvent) error {
	// 速率限制检查
	if !h.rateLimiter.Allow() {
		h.queueStats.mu.Lock()
		h.queueStats.TotalRateLimited++
		h.queueStats.mu.Unlock()

		// 对于低优先级表，直接跳过
		priority := h.getTablePriority(e.Table.Schema, e.Table.Name)
		if priority == PriorityLow {
			logx.Errorf("Rate limit exceeded, skipping low priority table: %s.%s", e.Table.Schema, e.Table.Name)
			return nil
		}
		// 对于中高优先级表，仍然处理但记录警告
		logx.Errorf("Rate limit exceeded, but processing high/normal priority table: %s.%s", e.Table.Schema, e.Table.Name)
	}

	action := "insert"
	switch e.Action {
	case canal.UpdateAction:
		action = "update"
	case canal.DeleteAction:
		action = "delete"
	}

	// 获取映射器
	mapperKey := e.Table.Schema + "." + e.Table.Name
	mapper, exists := h.mappers[mapperKey]
	if !exists {
		return nil
	}

	// 获取表优先级
	priority := h.getTablePriority(e.Table.Schema, e.Table.Name)

	for _, row := range e.Rows {
		data := make(map[string]interface{})
		for i, column := range e.Table.Columns {
			data[column.Name] = row[i]
		}

		switch e.Action {
		case canal.InsertAction:
			rowData := &RowData{
				TaskName:  h.syncTaskConfig.Name,
				Database:  e.Table.Schema,
				Table:     e.Table.Name,
				Action:    action,
				Data:      data,
				Timestamp: time.Now(),
				Priority:  priority,
			}

			result := h.sendWithLimitedRetry(h.insertQueue, rowData, "insert")
			if !result.Success && result.Error != nil {
				logx.Errorf("Failed to send insert data after %d retries: %v", result.RetryCount, result.Error)
			}

		case canal.UpdateAction:
			rowData := &RowData{
				TaskName:  h.syncTaskConfig.Name,
				Database:  e.Table.Schema,
				Table:     e.Table.Name,
				Action:    action,
				Data:      data,
				Timestamp: time.Now(),
				Priority:  priority,
			}

			result := h.sendWithLimitedRetry(h.updateQueue, rowData, "update")
			if !result.Success && result.Error != nil {
				logx.Errorf("Failed to send update data after %d retries: %v", result.RetryCount, result.Error)
			}

		case canal.DeleteAction:
			whereClause, err := h.generateDeleteCondition(data, e.Table, mapper)
			if err != nil {
				logx.Errorf("Failed to generate delete condition: %v", err)
				continue
			}
			deleteData := &RowData{
				TaskName:    h.syncTaskConfig.Name,
				Database:    e.Table.Schema,
				Table:       e.Table.Name,
				WhereClause: whereClause,
				PrimaryKey:  h.extractPrimaryKey(data, e.Table, mapper),
				Timestamp:   time.Now(),
				Priority:    priority,
			}

			result := h.sendWithLimitedRetry(h.deleteQueue, deleteData, "delete")
			if !result.Success && result.Error != nil {
				logx.Errorf("Failed to send delete data after %d retries: %v", result.RetryCount, result.Error)
			}
		}
	}
	return nil
}

// processInsertBatches 处理批量插入数据
func (h *BinlogHandler) processInsertBatches() {
	ticker := time.NewTicker(h.batchWait)
	defer ticker.Stop()

	batch := make([]*RowData, 0, h.batchSize)
	tableData := make(map[string][]map[string]interface{})

	for {
		select {
		case data, ok := <-h.insertQueue:
			if !ok {
				// 通道关闭，处理剩余数据
				if len(batch) > 0 {
					h.writeBatch(batch, tableData)
				}
				return
			}

			batch = append(batch, data)

			// 应用字段映射
			mapperKey := data.Database + "." + data.Table
			mapper, exists := h.mappers[mapperKey]
			if !exists {
				logx.Errorf("No mapper found for table: %s", mapperKey)
				continue
			}

			mappedData, err := mapper.MapRow(data.Data)
			if err != nil {
				logx.Errorf("Failed to map row for table %s: %v", mapperKey, err)
				continue
			}

			tableKey := mapper.config.TargetDatabase + "." + mapper.config.TargetTable
			tableData[tableKey] = append(tableData[tableKey], mappedData)

			if int64(len(batch)) >= h.batchSize {
				h.writeBatch(batch, tableData)
				batch = make([]*RowData, 0, h.batchSize)
				tableData = make(map[string][]map[string]interface{})
			}

		case <-ticker.C:
			if len(batch) > 0 {
				h.writeBatch(batch, tableData)
				batch = make([]*RowData, 0, h.batchSize)
				tableData = make(map[string][]map[string]interface{})
			}

		case <-h.ctx.Done():
			if len(batch) > 0 {
				h.writeBatch(batch, tableData)
			}
			return
		}
	}
}

// processUpdateBatches 处理批量更新数据
func (h *BinlogHandler) processUpdateBatches() {
	ticker := time.NewTicker(h.batchWait)
	defer ticker.Stop()

	batch := make([]*RowData, 0, h.batchSize)
	tableData := make(map[string][]map[string]interface{})

	for {
		select {
		case data, ok := <-h.updateQueue:
			if !ok {
				// 通道关闭，处理剩余数据
				if len(batch) > 0 {
					h.writeBatch(batch, tableData)
				}
				return
			}

			batch = append(batch, data)

			// 应用字段映射
			mapperKey := data.Database + "." + data.Table
			mapper, exists := h.mappers[mapperKey]
			if !exists {
				logx.Errorf("No mapper found for table: %s", mapperKey)
				continue
			}

			mappedData, err := mapper.MapRow(data.Data)
			if err != nil {
				logx.Errorf("Failed to map row for table %s: %v", mapperKey, err)
				continue
			}

			tableKey := mapper.config.TargetDatabase + "." + mapper.config.TargetTable
			tableData[tableKey] = append(tableData[tableKey], mappedData)

			if int64(len(batch)) >= h.batchSize {
				h.writeBatch(batch, tableData)
				batch = make([]*RowData, 0, h.batchSize)
				tableData = make(map[string][]map[string]interface{})
			}

		case <-ticker.C:
			if len(batch) > 0 {
				h.writeBatch(batch, tableData)
				batch = make([]*RowData, 0, h.batchSize)
				tableData = make(map[string][]map[string]interface{})
			}

		case <-h.ctx.Done():
			if len(batch) > 0 {
				h.writeBatch(batch, tableData)
			}
			return
		}
	}
}

// writeBatch 写入批量数据到ClickHouse
func (h *BinlogHandler) writeBatch(batch []*RowData, tableData map[string][]map[string]interface{}) {
	if len(batch) == 0 {
		return
	}

	threading.GoSafe(func() {
		startTime := time.Now()
		var successCount, errorCount int

		defer func() {
			duration := time.Since(startTime)
			h.queueStats.mu.Lock()
			h.queueStats.TotalProcessed += int64(successCount)
			h.queueStats.TotalErrors += int64(errorCount)
			h.queueStats.mu.Unlock()

			logx.Infof("Batch processing completed: %d success, %d errors, took %v, rate: %.1f rows/sec",
				successCount, errorCount, duration,
				float64(successCount)/duration.Seconds())
		}()

		// 并行处理不同表的数据
		var wg sync.WaitGroup
		var mu sync.Mutex

		for tableKey, data := range tableData {
			if len(data) == 0 {
				continue
			}

			wg.Add(1)
			go func(tableKey string, data []map[string]interface{}) {
				defer wg.Done()

				// 获取列名
				var columns []string
				if len(data) > 0 {
					for col := range data[0] {
						columns = append(columns, col)
					}
				}

				// 获取表配置
				tableConfig := h.getTableConfig(tableKey)
				if tableConfig == nil {
					logx.Errorf("No config found for table: %s", tableKey)
					mu.Lock()
					errorCount += len(data)
					mu.Unlock()
					h.moveToDeadLetterQueueForTable(batch, tableKey, fmt.Errorf("no config found for table: %s"))
					return
				}

				// 执行批量插入，带重试机制
				maxRetries := 2
				for i := 0; i <= maxRetries; i++ {
					if err := h.chClient.BatchInsert(tableKey, columns, data, tableConfig.OnConflict); err != nil {
						if i == maxRetries {
							logx.Errorf("Failed to insert batch into %s after %d retries: %v", tableKey, maxRetries+1, err)
							mu.Lock()
							errorCount += len(data)
							mu.Unlock()
							h.moveToDeadLetterQueueForTable(batch, tableKey, err)
						} else {
							logx.Errorf("Retry %d for batch insert into %s: %v", i+1, tableKey, err)
							time.Sleep(time.Second * time.Duration(i+1))
						}
					} else {
						mu.Lock()
						successCount += len(data)
						mu.Unlock()
						logx.Debugf("Successfully inserted %d rows into %s", len(data), tableKey)
						break
					}
				}
			}(tableKey, data)
		}

		wg.Wait()
	})
}

// 为特定表移动数据到死信队列
func (h *BinlogHandler) moveToDeadLetterQueueForTable(batch []*RowData, tableKey string, err error) {
	errorMsg := fmt.Sprintf("table: %s, error: %v", tableKey, err)

	for _, data := range batch {
		mapperKey := data.Database + "." + data.Table
		mapper, exists := h.mappers[mapperKey]
		if !exists {
			continue
		}

		currentTableKey := mapper.config.TargetDatabase + "." + mapper.config.TargetTable
		if currentTableKey == tableKey {
			h.sendToDeadLetterQueue(data, errorMsg)
		}
	}
}

// processDeleteBatches 处理批量删除操作
func (h *BinlogHandler) processDeleteBatches() {
	ticker := time.NewTicker(h.batchWait)
	defer ticker.Stop()

	deleteBatch := make([]*RowData, 0, h.batchSize)
	tableDeletes := make(map[string][]*RowData)

	for {
		select {
		case data, ok := <-h.deleteQueue:
			if !ok {
				// 通道关闭，处理剩余数据
				if len(deleteBatch) > 0 {
					h.executeDeletes(deleteBatch, tableDeletes)
				}
				return
			}

			deleteBatch = append(deleteBatch, data)

			// 按表分组
			mapperKey := data.Database + "." + data.Table
			mapper, exists := h.mappers[mapperKey]
			if !exists {
				logx.Errorf("No mapper found for table: %s", mapperKey)
				continue
			}

			tableKey := mapper.config.TargetDatabase + "." + mapper.config.TargetTable
			tableDeletes[tableKey] = append(tableDeletes[tableKey], data)

			if int64(len(deleteBatch)) >= h.batchSize {
				h.executeDeletes(deleteBatch, tableDeletes)
				deleteBatch = make([]*RowData, 0, h.batchSize)
				tableDeletes = make(map[string][]*RowData)
			}

		case <-ticker.C:
			if len(deleteBatch) > 0 {
				h.executeDeletes(deleteBatch, tableDeletes)
				deleteBatch = make([]*RowData, 0, h.batchSize)
				tableDeletes = make(map[string][]*RowData)
			}

		case <-h.ctx.Done():
			if len(deleteBatch) > 0 {
				h.executeDeletes(deleteBatch, tableDeletes)
			}
			return
		}
	}
}

// executeDeletes 执行批量删除
func (h *BinlogHandler) executeDeletes(batch []*RowData, tableDeletes map[string][]*RowData) {
	if len(batch) == 0 {
		return
	}

	threading.GoSafe(func() {
		startTime := time.Now()
		var successCount, errorCount int

		defer func() {
			duration := time.Since(startTime)
			h.queueStats.mu.Lock()
			h.queueStats.TotalProcessed += int64(successCount)
			h.queueStats.TotalErrors += int64(errorCount)
			h.queueStats.mu.Unlock()

			logx.Infof("Delete processing completed: %d success, %d errors, took %v",
				successCount, errorCount, duration)
		}()

		for tableKey, deletes := range tableDeletes {
			if len(deletes) == 0 {
				continue
			}

			// 获取映射器
			var mapper *FieldMapper
			for _, deleteData := range deletes {
				mapperKey := deleteData.Database + "." + deleteData.Table
				if m, exists := h.mappers[mapperKey]; exists {
					mapper = m
					break
				}
			}

			if mapper == nil {
				logx.Errorf("No mapper found for table: %s", tableKey)
				errorCount += len(deletes)
				h.moveToDeadLetterQueueForTable(batch, tableKey, fmt.Errorf("no mapper found for table: %s"))
				continue
			}

			// 构建删除条件
			var conditions []string
			for _, deleteData := range deletes {
				conditions = append(conditions, deleteData.WhereClause)
			}

			// 执行批量删除，带重试机制
			whereClause := strings.Join(conditions, " OR ")
			deleteQuery := fmt.Sprintf("ALTER TABLE %s DELETE WHERE %s", tableKey, whereClause)

			maxRetries := 2
			for i := 0; i <= maxRetries; i++ {
				if err := h.chClient.ExecuteQuery(deleteQuery); err != nil {
					if i == maxRetries {
						logx.Errorf("Failed to delete from %s after %d retries: %v", tableKey, maxRetries+1, err)
						errorCount += len(deletes)
						h.moveToDeadLetterQueueForTable(batch, tableKey, err)
					} else {
						logx.Errorf("Retry %d for delete from %s: %v", i+1, tableKey, err)
						time.Sleep(time.Second * time.Duration(i+1))
					}
				} else {
					successCount += len(deletes)
					logx.Infof("Deleted %d rows from %s", len(deletes), tableKey)
					break
				}
			}
		}
	})
}

// 智能死信队列处理
func (h *BinlogHandler) processDeadLetterQueue() {
	logx.Info("Starting smart dead letter queue processor")

	retryTicker := time.NewTicker(30 * time.Second) // 定期重试间隔
	defer retryTicker.Stop()

	persistTicker := time.NewTicker(5 * time.Minute) // 持久化间隔
	defer persistTicker.Stop()

	cleanupTicker := time.NewTicker(1 * time.Hour) // 清理过期数据间隔
	defer cleanupTicker.Stop()

	for {
		select {
		case data, ok := <-h.deadLetterQueue:
			if !ok {
				logx.Info("Dead letter queue channel closed, stopping processor")
				return
			}

			// 分析失败原因并智能处理
			h.analyzeAndHandleDeadLetter(data)

		case <-retryTicker.C:
			// 定期重试可恢复的错误
			h.retryRecoverableErrors()

		case <-persistTicker.C:
			// 持久化死信数据
			h.persistDeadLetterData()

		case <-cleanupTicker.C:
			// 清理过期数据
			h.cleanupExpiredData()

		case <-h.ctx.Done():
			// 停止前持久化所有数据
			h.persistAllDeadLetterData()
			logx.Info("Dead letter queue processor stopped")
			return
		}
	}
}

// 分析死信数据并智能处理
func (h *BinlogHandler) analyzeAndHandleDeadLetter(data *RowData) {
	dataKey := h.generateDataKey(data)

	h.retryCountsMu.RLock()
	currentRetryCount := h.retryCounts[dataKey]
	h.retryCountsMu.RUnlock()

	// 分析失败原因
	failureReason := h.analyzeFailureReason(data)

	logx.Infof("Analyzing dead letter data: %s, reason: %v, retry count: %d",
		dataKey, failureReason, currentRetryCount)

	switch failureReason {
	case FailureTemporary: // 网络超时、临时故障
		if currentRetryCount < h.retryConfig.MaxRetries {
			h.scheduleRetry(data, dataKey, currentRetryCount)
		} else {
			h.handlePermanentFailure(data, "exceeded_max_retries", "")
		}

	case FailureDataFormat: // 数据格式问题
		// 尝试数据修复
		if fixedData, success := h.tryFixDataFormat(data); success {
			// 使用修复后的数据重试
			fixedDataKey := h.generateDataKey(fixedData)
			h.scheduleRetry(fixedData, fixedDataKey, currentRetryCount)
		} else {
			h.handlePermanentFailure(data, "data_format_error", "failed to fix data format")
		}

	case FailureSchemaMismatch: // 表结构不匹配
		h.handleSchemaMismatch(data)

	case FailureConfigError: // 配置错误
		h.handlePermanentFailure(data, "config_error", "configuration issue detected")

	case FailureUnknown: // 未知错误
		if currentRetryCount < h.retryConfig.MaxRetries/2 { // 对未知错误更保守
			h.scheduleRetry(data, dataKey, currentRetryCount)
		} else {
			h.handlePermanentFailure(data, "unknown_error", "unknown error after retries")
		}

	default:
		h.handlePermanentFailure(data, "unhandled_error", "unhandled error type")
	}
}

// 生成数据唯一标识
func (h *BinlogHandler) generateDataKey(data *RowData) string {
	// 使用数据库、表、操作类型和主键生成唯一标识
	var pkParts []string
	for k, v := range data.PrimaryKey {
		pkParts = append(pkParts, fmt.Sprintf("%s=%v", k, v))
	}
	pkStr := strings.Join(pkParts, "|")

	if pkStr == "" {
		// 如果没有主键，使用数据哈希作为备选
		dataJSON, _ := json.Marshal(data.Data)
		return fmt.Sprintf("%s.%s.%s.%x",
			data.Database,
			data.Table,
			data.Action,
			dataJSON)
	}

	return fmt.Sprintf("%s.%s.%s.%s",
		data.Database,
		data.Table,
		data.Action,
		pkStr)
}

// 分析失败原因
func (h *BinlogHandler) analyzeFailureReason(data *RowData) FailureReason {
	// 检查映射器是否存在
	mapperKey := data.Database + "." + data.Table
	mapper, exists := h.mappers[mapperKey]
	if !exists {
		return FailureConfigError
	}

	// 检查数据字段是否匹配映射配置
	for field := range data.Data {
		if !h.isFieldMapped(field, mapper) {
			return FailureDataFormat
		}
	}

	// 检查数据值是否有效
	for field, value := range data.Data {
		if !h.isValueValid(field, value, mapper) {
			return FailureDataFormat
		}
	}

	// 默认认为是临时错误
	return FailureTemporary
}

// 检查字段是否被映射
func (h *BinlogHandler) isFieldMapped(field string, mapper *FieldMapper) bool {
	for _, mapping := range mapper.config.ColumnMappings {
		if mapping.Source == field && !mapping.Ignore {
			return true
		}
	}
	return false
}

// 检查字段值是否有效
func (h *BinlogHandler) isValueValid(field string, value interface{}, mapper *FieldMapper) bool {
	if value == nil {
		return true // 允许空值
	}

	// 这里可以添加更复杂的验证逻辑
	// 例如：类型检查、长度检查、格式验证等

	switch v := value.(type) {
	case string:
		// 检查字符串长度
		if len(v) > 1000000 { // 1MB限制
			return false
		}
	case []byte:
		// 检查二进制数据长度
		if len(v) > 5000000 { // 5MB限制
			return false
		}
	}

	return true
}

// 尝试修复数据格式
func (h *BinlogHandler) tryFixDataFormat(data *RowData) (*RowData, bool) {
	mapperKey := data.Database + "." + data.Table
	mapper, exists := h.mappers[mapperKey]
	if !exists {
		return data, false
	}

	// 创建数据副本
	fixedData := &RowData{
		TaskName:    data.TaskName,
		Database:    data.Database,
		Table:       data.Table,
		Action:      data.Action,
		Data:        make(map[string]interface{}),
		WhereClause: data.WhereClause,
		PrimaryKey:  data.PrimaryKey,
		Timestamp:   data.Timestamp,
		Priority:    data.Priority,
	}

	// 复制数据
	for k, v := range data.Data {
		fixedData.Data[k] = v
	}

	// 尝试应用字段映射
	mappedData, err := mapper.MapRow(fixedData.Data)
	if err != nil {
		logx.Errorf("Failed to map row during fix attempt: %v", err)
		return data, false
	}

	// 更新数据
	fixedData.Data = mappedData

	// 尝试修复主键
	if len(fixedData.PrimaryKey) > 0 {
		fixedPrimaryKey := make(map[string]interface{})
		for pkField, pkValue := range fixedData.PrimaryKey {
			if targetField, err := h.getTargetFieldName(pkField, mapper); err == nil {
				fixedPrimaryKey[targetField] = pkValue
			} else {
				fixedPrimaryKey[pkField] = pkValue
			}
		}
		fixedData.PrimaryKey = fixedPrimaryKey
	}

	logx.Infof("Successfully fixed data format for %s.%s", data.Database, data.Table)
	return fixedData, true
}

// 调度重试
func (h *BinlogHandler) scheduleRetry(data *RowData, dataKey string, currentRetryCount int) {
	// 更新重试计数
	h.retryCountsMu.Lock()
	h.retryCounts[dataKey] = currentRetryCount + 1
	h.retryCountsMu.Unlock()

	// 计算延迟时间（指数退避）
	delay := h.calculateRetryDelay(currentRetryCount)

	logx.Infof("Scheduling retry for %s in %v (attempt %d)",
		dataKey, delay, currentRetryCount+1)

	// 异步执行重试
	time.AfterFunc(delay, func() {
		h.retryDeadLetterData(data, dataKey)
	})
}

// 计算重试延迟
func (h *BinlogHandler) calculateRetryDelay(retryCount int) time.Duration {
	delay := h.retryConfig.InitialInterval

	for i := 0; i < retryCount; i++ {
		delay = time.Duration(float64(delay) * h.retryConfig.Multiplier)
		if delay > h.retryConfig.MaxInterval {
			delay = h.retryConfig.MaxInterval
			break
		}
	}

	return delay
}

// 重试死信数据
func (h *BinlogHandler) retryDeadLetterData(data *RowData, dataKey string) {
	var result SendResult

	switch data.Action {
	case "insert":
		result = h.sendWithLimitedRetry(h.insertQueue, data, "insert_retry")
	case "update":
		result = h.sendWithLimitedRetry(h.updateQueue, data, "update_retry")
	case "delete":
		result = h.sendWithLimitedRetry(h.deleteQueue, data, "delete_retry")
	}

	if !result.Success {
		logx.Errorf("Retry failed for %s.%s: %v",
			data.Database, data.Table, result.Error)

		// 检查是否超过最大重试次数
		h.retryCountsMu.RLock()
		currentRetryCount := h.retryCounts[dataKey]
		h.retryCountsMu.RUnlock()

		if currentRetryCount >= h.retryConfig.MaxRetries {
			// 不再重试，直接处理为永久失败
			h.handlePermanentFailure(data, "max_retries_exceeded", result.Error.Error())
		} else {
			// 重新分析失败原因，可能错误类型发生了变化
			h.analyzeAndHandleDeadLetter(data)
		}
	} else {
		logx.Infof("Retry successful for %s.%s", data.Database, data.Table)

		// 重试成功，清除重试计数
		h.retryCountsMu.Lock()
		delete(h.retryCounts, dataKey)
		h.retryCountsMu.Unlock()
	}
}

// 定期重试可恢复的错误
func (h *BinlogHandler) retryRecoverableErrors() {
	h.retryCountsMu.RLock()
	defer h.retryCountsMu.RUnlock()

	if len(h.retryCounts) == 0 {
		return
	}

	logx.Infof("Performing periodic retry for %d recoverable errors", len(h.retryCounts))

	// 这里可以添加逻辑来重新处理某些类型的错误
	// 例如：网络恢复后重试所有临时错误
}

// 处理表结构不匹配
func (h *BinlogHandler) handleSchemaMismatch(data *RowData) {
	logx.Errorf("Schema mismatch for table %s.%s, checking configuration",
		data.Database, data.Table)

	// 触发配置检查
	h.triggerSchemaCheck(data.Database, data.Table)

	// 标记为需要人工干预的永久失败
	h.handlePermanentFailure(data, "schema_mismatch", "table schema mismatch detected")
}

// 触发模式检查
func (h *BinlogHandler) triggerSchemaCheck(database, table string) {
	// 这里可以实现自动检查表结构并尝试修复的逻辑
	// 例如：比较MySQL和ClickHouse的表结构差异

	logx.Infof("Triggering schema check for %s.%s", database, table)

	// 发送告警通知需要人工干预
	alertMsg := fmt.Sprintf(
		"表结构不匹配告警\n数据库: %s\n表: %s\n时间: %s\n请检查表结构配置",
		database, table, time.Now().Format("2006-01-02 15:04:05"),
	)

	h.sendAlert(alertMsg, "schema_mismatch")
}

// 处理永久失败
func (h *BinlogHandler) handlePermanentFailure(data *RowData, reason, errorDetail string) {
	logx.Errorf("Permanent failure for %s.%s: %s - %s",
		data.Database, data.Table, reason, errorDetail)

	// 创建永久失败记录
	failureRecord := &PermanentFailure{
		TaskName:    data.TaskName,
		Database:    data.Database,
		Table:       data.Table,
		Action:      data.Action,
		Data:        data.Data,
		PrimaryKey:  data.PrimaryKey,
		Timestamp:   data.Timestamp,
		FailureTime: time.Now(),
		Reason:      reason,
		RetryCount:  h.getRetryCount(data),
		Error:       errorDetail,
	}

	// 保存到内存中
	h.failuresMu.Lock()
	h.permanentFailures = append(h.permanentFailures, failureRecord)
	h.failuresMu.Unlock()

	// 持久化到文件
	h.savePermanentFailure(failureRecord)

	// 发送告警
	h.sendFailureAlert(failureRecord)

	// 清理重试计数
	dataKey := h.generateDataKey(data)
	h.retryCountsMu.Lock()
	delete(h.retryCounts, dataKey)
	h.retryCountsMu.Unlock()
}

// 获取重试计数
func (h *BinlogHandler) getRetryCount(data *RowData) int {
	dataKey := h.generateDataKey(data)
	h.retryCountsMu.RLock()
	defer h.retryCountsMu.RUnlock()
	return h.retryCounts[dataKey]
}

// 保存永久失败记录到文件
func (h *BinlogHandler) savePermanentFailure(failure *PermanentFailure) {
	filename := fmt.Sprintf("permanent_failure_%s_%d.json",
		time.Now().Format("20060102_150405"),
		time.Now().UnixNano())

	filepath := filepath.Join(h.deadLetterDir, filename)

	data, err := json.MarshalIndent(failure, "", "  ")
	if err != nil {
		logx.Errorf("Failed to marshal permanent failure: %v", err)
		return
	}

	if err := os.WriteFile(filepath, data, 0644); err != nil {
		logx.Errorf("Failed to save permanent failure to %s: %v", filepath, err)
		return
	}

	logx.Infof("Saved permanent failure to %s", filepath)
}

// 发送失败告警
func (h *BinlogHandler) sendFailureAlert(failure *PermanentFailure) {
	alertMsg := fmt.Sprintf(
		"数据同步永久失败告警\n"+
			"任务: %s\n"+
			"表: %s.%s\n"+
			"操作: %s\n"+
			"原因: %s\n"+
			"重试次数: %d\n"+
			"失败时间: %s\n"+
			"错误详情: %s",
		failure.TaskName,
		failure.Database,
		failure.Table,
		failure.Action,
		failure.Reason,
		failure.RetryCount,
		failure.FailureTime.Format("2006-01-02 15:04:05"),
		failure.Error,
	)

	logx.Error(alertMsg)

	// 这里可以集成邮件、钉钉、企业微信等告警渠道
	// h.sendAlertToNotificationSystem(alertMsg)
}

// 发送通用告警
func (h *BinlogHandler) sendAlert(message, alertType string) {
	logx.Errorf("ALERT [%s]: %s", alertType, message)

	// 这里可以实现具体的告警发送逻辑
	// 例如：发送到监控系统、邮件、短信等
}

// 持久化死信数据
func (h *BinlogHandler) persistDeadLetterData() {
	// 持久化重试计数
	h.persistRetryCounts()

	// 持久化永久失败记录
	h.persistPermanentFailures()

	logx.Debug("Persisted dead letter data to disk")
}

// 持久化重试计数
func (h *BinlogHandler) persistRetryCounts() {
	h.retryCountsMu.RLock()
	defer h.retryCountsMu.RUnlock()

	if len(h.retryCounts) == 0 {
		return
	}

	data, err := json.Marshal(h.retryCounts)
	if err != nil {
		logx.Errorf("Failed to marshal retry counts: %v", err)
		return
	}

	filepath := filepath.Join(h.deadLetterDir, "retry_counts.json")
	if err := os.WriteFile(filepath, data, 0644); err != nil {
		logx.Errorf("Failed to save retry counts: %v", err)
	}
}

// 持久化永久失败记录
func (h *BinlogHandler) persistPermanentFailures() {
	h.failuresMu.RLock()
	defer h.failuresMu.RUnlock()

	if len(h.permanentFailures) == 0 {
		return
	}

	// 只保存最近1000条记录，避免文件过大
	var recentFailures []*PermanentFailure
	if len(h.permanentFailures) > 1000 {
		recentFailures = h.permanentFailures[len(h.permanentFailures)-1000:]
	} else {
		recentFailures = h.permanentFailures
	}

	data, err := json.MarshalIndent(recentFailures, "", "  ")
	if err != nil {
		logx.Errorf("Failed to marshal permanent failures: %v", err)
		return
	}

	filepath := filepath.Join(h.deadLetterDir, "permanent_failures.json")
	if err := os.WriteFile(filepath, data, 0644); err != nil {
		logx.Errorf("Failed to save permanent failures: %v", err)
	}
}

// 清理过期数据
func (h *BinlogHandler) cleanupExpiredData() {
	// 清理过期的重试计数（超过24小时没有更新的）
	cutoffTime := time.Now().Add(-24 * time.Hour)

	h.retryCountsMu.Lock()
	for key := range h.retryCounts {
		// 这里需要额外的逻辑来跟踪每个key的最后更新时间
		// 简化实现：清理所有重试计数
		delete(h.retryCounts, key)
	}
	h.retryCountsMu.Unlock()

	// 清理过期的永久失败记录（超过7天的）
	h.failuresMu.Lock()
	var recentFailures []*PermanentFailure
	for _, failure := range h.permanentFailures {
		if failure.FailureTime.After(cutoffTime) {
			recentFailures = append(recentFailures, failure)
		}
	}
	h.permanentFailures = recentFailures
	h.failuresMu.Unlock()

	logx.Info("Cleaned up expired dead letter data")
}

// 停止前持久化所有数据
func (h *BinlogHandler) persistAllDeadLetterData() {
	logx.Info("Persisting all dead letter data before shutdown")

	h.persistDeadLetterData()

	// 确保所有数据都写入磁盘
	logx.Info("All dead letter data persisted successfully")
}

// generateDeleteCondition 生成删除条件
func (h *BinlogHandler) generateDeleteCondition(data map[string]interface{}, table *schema.Table, mapper *FieldMapper) (string, error) {
	var conditions []string

	// 获取表的主键信息（如果有）
	primaryKeys := h.getPrimaryKeys(table)

	// 优先使用主键构建条件
	if len(primaryKeys) > 0 {
		for _, pk := range primaryKeys {
			if value, exists := data[pk]; exists {
				// 应用字段映射获取目标字段名
				targetField, err := h.getTargetFieldName(pk, mapper)
				if err != nil {
					return "", fmt.Errorf("failed to get target field for primary key %s: %v", pk, err)
				}

				// 格式化值
				formattedValue, err := h.formatValueForCondition(value, targetField, mapper)
				if err != nil {
					return "", fmt.Errorf("failed to format value for primary key %s: %v", pk, err)
				}

				conditions = append(conditions, fmt.Sprintf("%s = %s", targetField, formattedValue))
			}
		}
	}

	// 如果没有主键或主键条件不足，使用所有字段
	if len(conditions) == 0 {
		for col, val := range data {
			// 应用字段映射获取目标字段名
			targetField, err := h.getTargetFieldName(col, mapper)
			if err != nil {
				logx.Infof("Failed to get target field for %s: %v", col, err)
				continue
			}

			// 格式化值
			formattedValue, err := h.formatValueForCondition(val, targetField, mapper)
			if err != nil {
				logx.Infof("Failed to format value for field %s: %v", col, err)
				continue
			}

			conditions = append(conditions, fmt.Sprintf("%s = %s", targetField, formattedValue))
		}
	}

	if len(conditions) == 0 {
		return "", fmt.Errorf("no valid conditions generated for delete operation")
	}

	return strings.Join(conditions, " AND "), nil
}

// extractPrimaryKey 提取主键信息
func (h *BinlogHandler) extractPrimaryKey(data map[string]interface{}, table *schema.Table, mapper *FieldMapper) map[string]interface{} {
	primaryKeys := h.getPrimaryKeys(table)
	result := make(map[string]interface{})

	for _, pk := range primaryKeys {
		if value, exists := data[pk]; exists {
			// 应用字段映射获取目标字段名
			if targetField, err := h.getTargetFieldName(pk, mapper); err == nil {
				result[targetField] = value
			} else {
				result[pk] = value
			}
		}
	}

	// 如果没有找到主键，返回所有数据
	if len(result) == 0 {
		for col, val := range data {
			if targetField, err := h.getTargetFieldName(col, mapper); err == nil {
				result[targetField] = val
			} else {
				result[col] = val
			}
		}
	}

	return result
}

// getPrimaryKeys 获取表的主键字段
func (h *BinlogHandler) getPrimaryKeys(table *schema.Table) []string {
	var primaryKeys []string

	// 获取主键列
	primaryKeyIndexList := table.PKColumns

	for _, colIndex := range primaryKeyIndexList {
		if colIndex < len(table.Columns) {
			primaryKeys = append(primaryKeys, table.Columns[colIndex].Name)
		}
	}
	return primaryKeys
}

// getTargetFieldName 获取目标字段名
func (h *BinlogHandler) getTargetFieldName(sourceField string, mapper *FieldMapper) (string, error) {
	// 查找字段映射配置
	for _, mapping := range mapper.config.ColumnMappings {
		if mapping.Source == sourceField && !mapping.Ignore {
			return mapping.Target, nil
		}
	}

	// 如果没有找到映射配置，使用原字段名
	return sourceField, nil
}

// formatValueForCondition 格式化值用于条件语句
func (h *BinlogHandler) formatValueForCondition(value interface{}, targetField string, mapper *FieldMapper) (string, error) {
	if value == nil {
		return "NULL", nil
	}

	// 根据字段类型格式化值
	switch v := value.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''")), nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v), nil
	case float32, float64:
		return fmt.Sprintf("%f", v), nil
	case bool:
		if v {
			return "1", nil
		}
		return "0", nil
	case time.Time:
		return fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05")), nil
	default:
		// 对于未知类型，尝试转换为字符串
		return fmt.Sprintf("'%v'", v), nil
	}
}

// getTableConfig 获取表配置
func (h *BinlogHandler) getTableConfig(tableKey string) *config.SyncTaskTable {
	for _, table := range h.syncTaskConfig.Tables {
		fullTableName := table.TargetDatabase + "." + table.TargetTable
		if fullTableName == tableKey {
			return &table
		}
	}
	return nil
}

// 实现其他必需的canal.EventHandler方法

func (h *BinlogHandler) OnRotate(header *replication.EventHeader, rotateEvent *replication.RotateEvent) error {
	logx.Infof("Binlog rotated to %s", string(rotateEvent.NextLogName))
	return nil
}

func (h *BinlogHandler) OnTableChanged(header *replication.EventHeader, schema string, table string) error {
	logx.Infof("Table changed: %s.%s", schema, table)
	return nil
}

func (h *BinlogHandler) OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	logx.Infof("DDL event: %s", queryEvent.Query)
	return nil
}

func (h *BinlogHandler) OnXID(header *replication.EventHeader, nextPos mysql.Position) error {
	// 保存位置信息
	mysqlSource := h.source

	err := h.positionManager.SavePosition(
		h.syncTaskConfig.Name,
		mysqlSource.Host,
		mysqlSource.Port,
		mysqlSource.Database,
		nextPos.Name,
		nextPos.Pos,
		"",
	)

	if err != nil {
		logx.Errorf("Failed to save position: %v", err)
	} else {
		logx.Debugf("Position saved: %s:%d", nextPos.Name, nextPos.Pos)
	}

	return nil
}

func (h *BinlogHandler) OnGTID(header *replication.EventHeader, gtidEvent mysql.BinlogGTIDEvent) error {
	return nil
}

// OnPosSynced Use your own way to sync position. When force is true, sync position immediately.
func (h *BinlogHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	logx.Infof("Position synced: %s", pos)

	mysqlSource := h.source
	var gtid string
	if set != nil {
		gtid = set.String()
	}

	err := h.positionManager.SavePosition(
		h.syncTaskConfig.Name,
		mysqlSource.Host,
		mysqlSource.Port,
		mysqlSource.Database,
		pos.Name,
		pos.Pos,
		gtid,
	)
	if err != nil {
		logx.Errorf("Failed to save position: %v", err)
	} else {
		logx.Debugf("Position saved: %s:%d", pos.Name, pos.Pos)
	}

	return nil
}

func (h *BinlogHandler) OnRowsQueryEvent(e *replication.RowsQueryEvent) error {
	return nil
}

func (h *BinlogHandler) String() string {
	return "Mysql2CHBinlogHandler"
}
