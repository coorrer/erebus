// 文件名: internal/logic/mysql2kafka/binlog_handler.go
package mysql2kafka

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

	"github.com/coorrer/erebus/internal/config"
	"github.com/coorrer/erebus/internal/logic/position"
	"github.com/coorrer/erebus/pkg/kafka"
)

// BinlogHandler 处理Mysql binlog事件
type BinlogHandler struct {
	ctx             context.Context
	cancel          context.CancelFunc
	config          config.Config
	syncTaskConfig  config.SyncTask
	source          config.DataSource
	target          config.DataSource
	kafkaClient     *kafka.Client
	insertQueue     chan *RowData
	updateQueue     chan *RowData
	deleteQueue     chan *RowData
	deadLetterQueue chan *RowData // 死信队列
	batchSize       int64
	batchWait       time.Duration
	mappers         map[string]*FieldMapper
	positionManager *position.PositionManager
	taskName        string
	tableConfigs    []config.SyncTaskTable
	wg              sync.WaitGroup
	mu              sync.RWMutex
	isRunning       bool
	stopTimeout     time.Duration
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
}

// KafkaMessage Kafka消息结构
type KafkaMessage struct {
	TaskName  string                 `json:"task_name"`
	Database  string                 `json:"database"`
	Table     string                 `json:"table"`
	Action    string                 `json:"action"`
	Data      map[string]interface{} `json:"data"`
	Timestamp time.Time              `json:"timestamp"`
	SyncTime  time.Time              `json:"sync_time"`
}

// NewBinlogHandler 创建新的事件处理器
func NewBinlogHandler(ctx context.Context, cfg config.Config, kafkaClient *kafka.Client, positionManager *position.PositionManager, syncTaskConfig config.SyncTask, source config.DataSource, target config.DataSource) *BinlogHandler {
	childCtx, cancel := context.WithCancel(ctx)

	// 初始化字段映射器
	mappers := make(map[string]*FieldMapper)
	for _, table := range syncTaskConfig.Tables {
		key := table.SourceDatabase + "." + table.SourceTable
		mappers[key] = NewFieldMapper(table)
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

	// 设置批量大小（使用第一个表的配置）
	var batchSize int64 = 1000 // 默认值
	if len(syncTaskConfig.Tables) > 0 && syncTaskConfig.Tables[0].BatchSize > 0 {
		batchSize = syncTaskConfig.Tables[0].BatchSize
	}

	return &BinlogHandler{
		ctx:             childCtx,
		cancel:          cancel,
		config:          cfg,
		syncTaskConfig:  syncTaskConfig,
		kafkaClient:     kafkaClient,
		insertQueue:     make(chan *RowData, 10000),
		updateQueue:     make(chan *RowData, 10000),
		deleteQueue:     make(chan *RowData, 10000),
		deadLetterQueue: make(chan *RowData, 10000), // 死信队列
		batchSize:       batchSize,
		batchWait:       batchWait,
		mappers:         mappers,
		positionManager: positionManager,
		source:          source,
		target:          target,
		taskName:        syncTaskConfig.Name,
		tableConfigs:    syncTaskConfig.Tables,
		isRunning:       false,
		stopTimeout:     30 * time.Second,
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

	logx.Info("Starting Mysql2Kafka binlog event handler")

	// 启动插入处理器
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.processInsertBatches()
	}()

	// 启动更新处理器
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.processUpdateBatches()
	}()

	// 启动删除处理器
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.processDeleteBatches()
	}()

	// 启动死信队列处理器
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.processDeadLetterQueue()
	}()

	// 等待停止信号
	<-h.ctx.Done()
	logx.Info("Mysql2Kafka binlog event handler stopped")
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

	logx.Info("Stopping Mysql2Kafka binlog event handler")
	h.cancel()

	// 使用带超时的等待组
	done := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(done)
	}()

	// 等待停止完成或超时
	select {
	case <-done:
		logx.Info("Mysql2Kafka binlog event handler stopped successfully")
	case <-time.After(h.stopTimeout):
		logx.Error("Binlog handler stop timeout, forcing shutdown")
	}

	// 关闭所有队列
	close(h.insertQueue)
	close(h.updateQueue)
	close(h.deleteQueue)
	close(h.deadLetterQueue)
}

// OnRow 处理行变更事件
func (h *BinlogHandler) OnRow(e *canal.RowsEvent) error {
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
		logx.Errorf("No mapper found for table: %s", mapperKey)
		return fmt.Errorf("no mapper found for table: %s", mapperKey)
	}

	for _, row := range e.Rows {
		data := make(map[string]interface{})
		for i, column := range e.Table.Columns {
			data[column.Name] = row[i]
		}

		// 创建基础RowData
		rowData := &RowData{
			TaskName:  h.taskName,
			Database:  e.Table.Schema,
			Table:     e.Table.Name,
			Action:    action,
			Data:      data,
			Timestamp: time.Now(),
		}

		// 根据操作类型设置特定字段
		switch e.Action {
		case canal.InsertAction, canal.UpdateAction:
			// 对于插入和更新操作，只需要基础数据
			rowData.PrimaryKey = h.extractPrimaryKey(data, e.Table, mapper)

		case canal.DeleteAction:
			// 对于删除操作，需要生成删除条件和主键信息
			whereClause, err := h.generateDeleteCondition(data, e.Table, mapper)
			if err != nil {
				logx.Errorf("Failed to generate delete condition: %v", err)
				continue
			}
			rowData.WhereClause = whereClause
			rowData.PrimaryKey = h.extractPrimaryKey(data, e.Table, mapper)
		}

		// 根据操作类型分发到不同的队列
		switch e.Action {
		case canal.InsertAction:
			select {
			case h.insertQueue <- rowData:
			case <-h.ctx.Done():
				return nil
			default:
				logx.Error("Insert queue is full, dropping data")
			}
		case canal.UpdateAction:
			select {
			case h.updateQueue <- rowData:
			case <-h.ctx.Done():
				return nil
			default:
				logx.Error("Update queue is full, dropping data")
			}
		case canal.DeleteAction:
			select {
			case h.deleteQueue <- rowData:
			case <-h.ctx.Done():
				return nil
			default:
				logx.Error("Delete queue is full, dropping data")
			}
		}
	}
	return nil
}

// processInsertBatches 处理批量插入数据
func (h *BinlogHandler) processInsertBatches() {
	h.processDataBatches("insert", h.insertQueue)
}

// processUpdateBatches 处理批量更新数据
func (h *BinlogHandler) processUpdateBatches() {
	h.processDataBatches("update", h.updateQueue)
}

// processDataBatches 处理数据批次（插入和更新）
func (h *BinlogHandler) processDataBatches(action string, queue chan *RowData) {
	ticker := time.NewTicker(h.batchWait)
	defer ticker.Stop()

	batch := make([]*RowData, 0, h.batchSize)
	tableData := make(map[string][]*RowData) // 按表分组的数据

	for {
		select {
		case data, ok := <-queue:
			if !ok {
				// 通道关闭，处理剩余数据
				if len(batch) > 0 {
					h.writeBatch(action, batch, tableData)
				}
				return
			}

			batch = append(batch, data)

			// 按表分组
			tableKey := data.Database + "." + data.Table
			tableData[tableKey] = append(tableData[tableKey], data)

			if int64(len(batch)) >= h.batchSize {
				h.writeBatch(action, batch, tableData)
				batch = make([]*RowData, 0, h.batchSize)
				tableData = make(map[string][]*RowData)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				h.writeBatch(action, batch, tableData)
				batch = make([]*RowData, 0, h.batchSize)
				tableData = make(map[string][]*RowData)
			}

		case <-h.ctx.Done():
			if len(batch) > 0 {
				h.writeBatch(action, batch, tableData)
			}
			return
		}
	}
}

// processDeleteBatches 处理批量删除数据
func (h *BinlogHandler) processDeleteBatches() {
	ticker := time.NewTicker(h.batchWait)
	defer ticker.Stop()

	batch := make([]*RowData, 0, h.batchSize)
	tableData := make(map[string][]*RowData) // 按表分组的数据

	for {
		select {
		case data, ok := <-h.deleteQueue:
			if !ok {
				// 通道关闭，处理剩余数据
				if len(batch) > 0 {
					h.writeBatch("delete", batch, tableData)
				}
				return
			}

			batch = append(batch, data)

			// 按表分组
			tableKey := data.Database + "." + data.Table
			tableData[tableKey] = append(tableData[tableKey], data)

			if int64(len(batch)) >= h.batchSize {
				h.writeBatch("delete", batch, tableData)
				batch = make([]*RowData, 0, h.batchSize)
				tableData = make(map[string][]*RowData)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				h.writeBatch("delete", batch, tableData)
				batch = make([]*RowData, 0, h.batchSize)
				tableData = make(map[string][]*RowData)
			}

		case <-h.ctx.Done():
			if len(batch) > 0 {
				h.writeBatch("delete", batch, tableData)
			}
			return
		}
	}
}

// writeBatch 写入批量数据到Kafka
func (h *BinlogHandler) writeBatch(action string, batch []*RowData, tableData map[string][]*RowData) {
	if len(batch) == 0 {
		return
	}

	threading.GoSafe(func() {
		startTime := time.Now()
		var successCount, errorCount int

		defer func() {
			duration := time.Since(startTime)
			logx.Infof("%s batch processing completed: %d success, %d errors, took %v",
				action, successCount, errorCount, duration)
		}()

		// 为每个表创建消息并发送到Kafka
		for tableKey, rows := range tableData {
			if len(rows) == 0 {
				continue
			}

			// 获取主题名（使用目标表名或配置的主题名）
			topic := h.getTopicForTable(tableKey)

			// 创建Kafka消息
			messages := make([]interface{}, len(rows))
			for i, row := range rows {
				// 应用字段映射
				mapperKey := row.Database + "." + row.Table
				mapper, exists := h.mappers[mapperKey]
				if !exists {
					logx.Errorf("No mapper found for table: %s", mapperKey)
					continue
				}

				mappedData, err := mapper.MapRow(row.Data)
				if err != nil {
					logx.Errorf("Failed to map row for table %s: %v", mapperKey, err)
					continue
				}

				// 创建Kafka消息
				kafkaMsg := KafkaMessage{
					TaskName:  row.TaskName,
					Database:  row.Database,
					Table:     row.Table,
					Action:    row.Action,
					Data:      mappedData,
					Timestamp: row.Timestamp,
					SyncTime:  time.Now(),
				}

				messages[i] = kafkaMsg
			}

			// 发送到Kafka
			if err := h.kafkaClient.SendJSONMessages(topic, messages); err != nil {
				logx.Errorf("Failed to send %s batch to topic %s: %v", action, topic, err)
				errorCount += len(rows)
				// 将失败的数据移动到死信队列
				h.moveToDeadLetterQueue(rows, topic, err)
			} else {
				successCount += len(rows)
				logx.Debugf("Successfully sent %d %s messages to topic %s", len(rows), action, topic)
			}
		}

		logx.Infof("Processed %s batch of %d rows (%d success, %d errors)",
			action, len(batch), successCount, errorCount)
	})
}

// getTopicForTable 获取表对应的Kafka主题
func (h *BinlogHandler) getTopicForTable(tableKey string) string {
	// 查找表配置
	for _, tableConfig := range h.tableConfigs {
		fullTableName := tableConfig.SourceDatabase + "." + tableConfig.SourceTable
		if fullTableName == tableKey {
			// 如果配置了目标表名，使用目标表名作为主题
			if tableConfig.TargetTable != "" {
				return tableConfig.TargetTable
			}
			break
		}
	}

	// 默认使用表名作为主题（将点替换为下划线）
	return strings.ReplaceAll(tableKey, ".", "_")
}

// processDeadLetterQueue 处理死信队列
func (h *BinlogHandler) processDeadLetterQueue() {
	logx.Info("Starting dead letter queue processor")

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

			// 将死信数据保存到文件
			if err := h.saveDeadLetterToFile(data, "processing failed"); err != nil {
				logx.Errorf("Failed to save dead letter data to file: %v", err)
			}

		case <-h.ctx.Done():
			logx.Info("Dead letter queue processor stopped")
			return
		}
	}
}

// moveToDeadLetterQueue 将失败的数据移动到死信队列
func (h *BinlogHandler) moveToDeadLetterQueue(batch []*RowData, topic string, err error) {
	logx.Errorf("Moving %d rows to dead letter queue for topic %s due to error: %v", len(batch), topic, err)

	for _, data := range batch {
		select {
		case h.deadLetterQueue <- data:
			logx.Debugf("Moved data to dead letter queue: %s.%s", data.Database, data.Table)
		default:
			logx.Error("Dead letter queue is full, data lost")
		}
	}
}

// saveDeadLetterToFile 将死信数据保存到文件
func (h *BinlogHandler) saveDeadLetterToFile(data *RowData, errorMsg string) error {
	// 创建死信数据目录
	deadLetterDir := filepath.Join(h.config.PositionStoragePath, "dead_letter", h.syncTaskConfig.Name)
	if err := os.MkdirAll(deadLetterDir, 0755); err != nil {
		return fmt.Errorf("failed to create dead letter directory: %v", err)
	}

	// 生成文件名（按日期和表名组织）
	dateStr := time.Now().Format("2006-01-02")
	fileName := fmt.Sprintf("%s_%s_%s.json", dateStr, data.Database, data.Table)
	filePath := filepath.Join(deadLetterDir, fileName)

	// 创建死信记录
	deadLetterRecord := map[string]interface{}{
		"task":         data.TaskName,
		"database":     data.Database,
		"table":        data.Table,
		"action":       data.Action,
		"data":         data.Data,
		"where_clause": data.WhereClause,
		"primary_key":  data.PrimaryKey,
		"timestamp":    data.Timestamp.Format(time.RFC3339),
		"error":        errorMsg,
		"saved_at":     time.Now().Format(time.RFC3339),
	}

	// 转换为JSON
	jsonData, err := json.MarshalIndent(deadLetterRecord, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal dead letter data: %v", err)
	}

	// 追加到文件
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open dead letter file: %v", err)
	}
	defer file.Close()

	// 写入数据并添加换行符
	if _, err := file.Write(append(jsonData, '\n')); err != nil {
		return fmt.Errorf("failed to write dead letter data: %v", err)
	}

	return nil
}

// getPrimaryKeys 获取表的主键字段
func (h *BinlogHandler) getPrimaryKeys(table *schema.Table) []string {
	var primaryKeys []string

	if table == nil || table.Columns == nil {
		return primaryKeys
	}

	// 获取主键列
	primaryKeyIndexList := table.PKColumns

	for _, colIndex := range primaryKeyIndexList {
		if colIndex < len(table.Columns) {
			primaryKeys = append(primaryKeys, table.Columns[colIndex].Name)
		}
	}
	return primaryKeys
}

// extractPrimaryKey 提取主键信息
func (h *BinlogHandler) extractPrimaryKey(data map[string]interface{}, table *schema.Table, mapper *FieldMapper) map[string]interface{} {
	primaryKeys := h.getPrimaryKeys(table)
	result := make(map[string]interface{})

	for _, pk := range primaryKeys {
		if value, exists := data[pk]; exists {
			result[pk] = value
		}
	}

	// 如果没有找到主键，返回空map
	return result
}

// generateDeleteCondition 生成删除条件
func (h *BinlogHandler) generateDeleteCondition(data map[string]interface{}, table *schema.Table, mapper *FieldMapper) (string, error) {
	primaryKeys := h.getPrimaryKeys(table)

	if len(primaryKeys) == 0 {
		return "", fmt.Errorf("no primary keys found for table %s.%s", table.Schema, table.Name)
	}

	var conditions []string
	for _, pk := range primaryKeys {
		if value, exists := data[pk]; exists {
			// 格式化值
			formattedValue, err := h.formatValueForCondition(value)
			if err != nil {
				return "", fmt.Errorf("failed to format value for primary key %s: %v", pk, err)
			}
			conditions = append(conditions, fmt.Sprintf("%s = %s", pk, formattedValue))
		}
	}

	if len(conditions) == 0 {
		return "", fmt.Errorf("no valid conditions generated for delete operation")
	}

	return strings.Join(conditions, " AND "), nil
}

// formatValueForCondition 格式化值用于条件语句
func (h *BinlogHandler) formatValueForCondition(value interface{}) (string, error) {
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
	return "Mysql2KafkaBinlogHandler"
}

// GetQueueSizes 获取各队列大小
func (h *BinlogHandler) GetQueueSizes() (int, int, int, int) {
	return len(h.insertQueue), len(h.updateQueue), len(h.deleteQueue), len(h.deadLetterQueue)
}
