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
	deadLetterQueue chan *RowData // 新增: 死信队列
	batchSize       int64
	batchWait       time.Duration
	mappers         map[string]*FieldMapper
	positionManager *position.PositionManager
	wg              sync.WaitGroup
	mu              sync.RWMutex
	isRunning       bool
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

// NewBinlogHandler 创建新的事件处理器
func NewBinlogHandler(ctx context.Context, cfg config.Config, chClient *clickhouse.Client, positionManager *position.PositionManager, syncTaskConfig config.SyncTask, source config.DataSource, target config.DataSource) *BinlogHandler {
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
		chClient:        chClient,
		insertQueue:     make(chan *RowData, 10000),
		updateQueue:     make(chan *RowData, 10000),
		deleteQueue:     make(chan *RowData, 10000),
		deadLetterQueue: make(chan *RowData, 10000), // 新增: 初始化死信队列
		batchSize:       batchSize,
		batchWait:       batchWait,
		mappers:         mappers,
		positionManager: positionManager,
		source:          source,
		target:          target,
		isRunning:       false,
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

	// 启动批量处理器
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.processInsertBatches()
	}()

	// 启动批量处理器
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
	h.wg.Wait()
	close(h.insertQueue)
	close(h.updateQueue)
	close(h.deleteQueue)
	close(h.deadLetterQueue) // 新增: 关闭死信队列
	logx.Info("Mysql2CH binlog event handler stopped successfully")
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
		return nil
	}

	for _, row := range e.Rows {
		data := make(map[string]interface{})
		for i, column := range e.Table.Columns {
			data[column.Name] = row[i]
		}

		switch e.Action {
		case canal.InsertAction:
			// 处理插入
			rowData := &RowData{
				TaskName:  h.syncTaskConfig.Name,
				Database:  e.Table.Schema,
				Table:     e.Table.Name,
				Action:    action,
				Data:      data,
				Timestamp: time.Now(),
			}

			select {
			case h.insertQueue <- rowData:
			case <-h.ctx.Done():
				return nil
			default:
				logx.Error("insert queue is full, dropping data")
			}
		case canal.UpdateAction:
			// 处理更新
			rowData := &RowData{
				TaskName:  h.syncTaskConfig.Name,
				Database:  e.Table.Schema,
				Table:     e.Table.Name,
				Action:    action,
				Data:      data,
				Timestamp: time.Now(),
			}

			select {
			case h.updateQueue <- rowData:
			case <-h.ctx.Done():
				return nil
			default:
				logx.Error("insert queue is full, dropping data")
			}
		case canal.DeleteAction:
			// 处理删除
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
			}

			select {
			case h.deleteQueue <- deleteData:
			case <-h.ctx.Done():
				return nil
			default:
				logx.Error("Delete queue is full, dropping delete data")
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
			// 记录处理时间
			logx.Infof("Batch processing completed: %d success, %d errors, took %v",
				successCount, errorCount, duration)
		}()

		for tableKey, data := range tableData {
			if len(data) == 0 {
				continue
			}

			// 获取列名
			var columns []string
			for col := range data[0] {
				columns = append(columns, col)
			}

			// 获取表配置
			tableConfig := h.getTableConfig(tableKey)
			if tableConfig == nil {
				logx.Errorf("No config found for table: %s", tableKey)
				errorCount += len(data)
				// 新增: 将失败的数据放入死信队列
				h.moveToDeadLetterQueue(batch, tableKey, fmt.Errorf("no config found for table: %s", tableKey))
				continue
			}

			// 执行批量插入
			if err := h.chClient.BatchInsert(tableKey, columns, data, tableConfig.OnConflict); err != nil {
				logx.Errorf("Failed to insert batch into %s: %v", tableKey, err)
				errorCount += len(data)
				// 新增: 将失败的数据放入死信队列
				h.moveToDeadLetterQueue(batch, tableKey, err)
			} else {
				successCount += len(data)
				logx.Debugf("Successfully inserted %d rows into %s", len(data), tableKey)
			}
		}

		logx.Infof("Processed batch of %d rows (%d success, %d errors)",
			len(batch), successCount, errorCount)
	})
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
				// 新增: 将失败的数据放入死信队列
				h.moveToDeadLetterQueue(batch, tableKey, fmt.Errorf("no mapper found for table: %s", tableKey))
				continue
			}

			// 构建删除条件
			var conditions []string
			for _, deleteData := range deletes {
				conditions = append(conditions, deleteData.WhereClause)
			}

			// 执行批量删除
			whereClause := strings.Join(conditions, " OR ")
			deleteQuery := fmt.Sprintf("ALTER TABLE %s DELETE WHERE %s", tableKey, whereClause)

			if err := h.chClient.ExecuteQuery(deleteQuery); err != nil {
				logx.Errorf("Failed to delete from %s: %v", tableKey, err)
				errorCount += len(deletes)
				// 新增: 将失败的数据放入死信队列
				h.moveToDeadLetterQueue(batch, tableKey, err)
			} else {
				successCount += len(deletes)
				logx.Infof("Deleted %d rows from %s", len(deletes), tableKey)
			}
		}

		logx.Infof("Processed %d delete operations (%d success, %d errors)",
			len(batch), successCount, errorCount)
	})
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

			// 这里可以添加将死信数据持久化到文件或数据库的逻辑
			// 例如: h.saveDeadLetterToFile(data)

		case <-h.ctx.Done():
			logx.Info("Dead letter queue processor stopped")
			return
		}
	}
}

// moveToDeadLetterQueue 将失败的数据移动到死信队列
func (h *BinlogHandler) moveToDeadLetterQueue(batch []*RowData, tableKey string, err error) {
	logx.Errorf("Moving %d rows to dead letter queue for table %s due to error: %v", len(batch), tableKey, err)

	for _, data := range batch {
		// 检查数据是否属于当前失败的表
		mapperKey := data.Database + "." + data.Table
		mapper, exists := h.mappers[mapperKey]
		if !exists {
			continue
		}

		currentTableKey := mapper.config.TargetDatabase + "." + mapper.config.TargetTable
		if currentTableKey != tableKey {
			continue
		}

		select {
		case h.deadLetterQueue <- data:
			logx.Debugf("Moved data to dead letter queue: %s.%s", data.Database, data.Table)
		default:
			logx.Error("Dead letter queue is full, data lost")
		}
	}
}

// generateDeleteCondition 生成删除条件（修复版，应用字段映射）
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

// extractPrimaryKey 提取主键信息（应用字段映射）
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

// getCurrentPosition 获取当前处理的位置
func (h *BinlogHandler) getCurrentPosition() *mysql.Position {
	// 这里需要从Canal获取当前位置信息
	// 由于Canal接口限制，可能需要通过其他方式获取
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
