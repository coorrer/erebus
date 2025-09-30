package mysql2clickhouse

import (
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/schema"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/threading"
	"golang.org/x/time/rate"

	"github.com/coorrer/erebus/internal/config"
	"github.com/coorrer/erebus/internal/logic/position"
	"github.com/coorrer/erebus/pkg/clickhouse"
)

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

	// 新增：监控和流量控制字段
	queueStats    *QueueStats
	rateLimiter   *rate.Limiter
	metricsTicker *time.Ticker
	tablePriority map[string]TablePriority // 表优先级配置
}

// 新增：队列统计
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

// 新增：表优先级
type TablePriority int

const (
	PriorityHigh TablePriority = iota
	PriorityNormal
	PriorityLow
)

// 新增：发送结果
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
	Priority    TablePriority // 新增：优先级
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
		} else {
			priority = PriorityLow
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
	var queueSize int = 50000 // 默认队列大小

	if len(syncTaskConfig.Tables) > 0 {
		if syncTaskConfig.Tables[0].BatchSize > 0 {
			batchSize = syncTaskConfig.Tables[0].BatchSize
		}
		if syncTaskConfig.Tables[0].QueueSize > 0 {
			queueSize = int(syncTaskConfig.Tables[0].QueueSize)
		}
	}

	// 创建速率限制器 (每秒1000个事件，突发2000)
	limiter := rate.NewLimiter(1000, 2000)

	return &BinlogHandler{
		ctx:             childCtx,
		cancel:          cancel,
		config:          cfg,
		syncTaskConfig:  syncTaskConfig,
		chClient:        chClient,
		insertQueue:     make(chan *RowData, queueSize),
		updateQueue:     make(chan *RowData, queueSize),
		deleteQueue:     make(chan *RowData, queueSize),
		deadLetterQueue: make(chan *RowData, queueSize/10), // 死信队列为1/10
		batchSize:       batchSize,
		batchWait:       batchWait,
		mappers:         mappers,
		positionManager: positionManager,
		source:          source,
		target:          target,
		isRunning:       false,
		queueStats:      &QueueStats{},
		rateLimiter:     limiter,
		tablePriority:   tablePriority,
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

// 新增：启动指标监控
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

// 新增：记录队列统计
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

// 新增：更新队列使用率统计
func (h *BinlogHandler) updateQueueUsage() {
	h.queueStats.mu.Lock()
	defer h.queueStats.mu.Unlock()

	h.queueStats.InsertQueueUsage = float64(len(h.insertQueue)) / float64(cap(h.insertQueue))
	h.queueStats.UpdateQueueUsage = float64(len(h.updateQueue)) / float64(cap(h.updateQueue))
	h.queueStats.DeleteQueueUsage = float64(len(h.deleteQueue)) / float64(cap(h.deleteQueue))
	h.queueStats.DeadLetterQueueUsage = float64(len(h.deadLetterQueue)) / float64(cap(h.deadLetterQueue))
	h.queueStats.LastUpdateTime = time.Now()
}

// 新增：带有限重试的非阻塞发送
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
				if h.sendToDeadLetterQueue(data) {
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

	if h.sendToDeadLetterQueue(data) {
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

// 新增：发送到死信队列（非阻塞）
func (h *BinlogHandler) sendToDeadLetterQueue(data *RowData) bool {
	select {
	case h.deadLetterQueue <- data:
		h.queueStats.mu.Lock()
		h.queueStats.TotalDeadLetter++
		h.queueStats.mu.Unlock()
		h.updateQueueUsage()
		logx.Debugf("Moved data to dead letter queue: %s.%s", data.Database, data.Table)
		return true
	default:
		h.queueStats.mu.Lock()
		h.queueStats.TotalDropped++
		h.queueStats.mu.Unlock()
		logx.Error("Dead letter queue is full, data lost permanently")
		return false
	}
}

// 新增：获取表优先级
func (h *BinlogHandler) getTablePriority(database, table string) TablePriority {
	key := database + "." + table
	if priority, exists := h.tablePriority[key]; exists {
		return priority
	}
	return PriorityNormal // 默认优先级
}

// OnRow 处理行变更事件（使用新的发送逻辑）
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

// writeBatch 写入批量数据到ClickHouse（优化版本）
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
	for _, data := range batch {
		mapperKey := data.Database + "." + data.Table
		mapper, exists := h.mappers[mapperKey]
		if !exists {
			continue
		}

		currentTableKey := mapper.config.TargetDatabase + "." + mapper.config.TargetTable
		if currentTableKey == tableKey {
			h.sendToDeadLetterQueue(data)
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

// processDeadLetterQueue 处理死信队列
func (h *BinlogHandler) processDeadLetterQueue() {
	logx.Info("Starting dead letter queue processor")

	// 可以定期将死信数据持久化到文件或数据库
	persistTicker := time.NewTicker(5 * time.Minute)
	defer persistTicker.Stop()

	for {
		select {
		case data, ok := <-h.deadLetterQueue:
			if !ok {
				logx.Info("Dead letter queue channel closed, stopping processor")
				return
			}

			// 记录死信数据
			logx.Errorf("Dead letter data received - Task: %s, Table: %s.%s, Action: %s, Time: %s",
				data.TaskName, data.Database, data.Table, data.Action, data.Timestamp.Format(time.RFC3339))

			// 这里可以添加将死信数据持久化到文件或数据库的逻辑
			// 例如: h.saveDeadLetterToFile(data)

		case <-persistTicker.C:
			// 定期处理死信数据的持久化
			h.persistDeadLetterData()

		case <-h.ctx.Done():
			logx.Info("Dead letter queue processor stopped")
			return
		}
	}
}

// 新增：持久化死信数据（示例实现）
func (h *BinlogHandler) persistDeadLetterData() {
	// 这里可以实现将死信队列中的数据持久化到文件或数据库
	// 防止进程重启时数据丢失
	logx.Debug("Persisting dead letter data...")
	// 实现细节根据具体需求来定
}

// 以下方法保持不变（generateDeleteCondition, extractPrimaryKey, getPrimaryKeys, getTargetFieldName, formatValueForCondition等）
// ... 保持原有的这些方法不变 ...

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
