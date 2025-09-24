package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/coorrer/erebus/internal/config"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/zeromicro/go-zero/core/logx"
)

// Config MySQL 客户端配置
type Config struct {
	Host     string `json:"Host"`
	Port     int    `json:"Port"`
	User     string `json:"User"`
	Password string `json:"Password"`
	Database string `json:"Database"`
	Charset  string `json:"Charset,optional"` // 默认 utf8mb4
	Timeout  int    `json:"Timeout,optional"` // 连接超时（秒）
	MaxIdle  int    `json:"MaxIdle,optional"` // 最大空闲连接数
	MaxOpen  int    `json:"MaxOpen,optional"` // 最大打开连接数
}

// Client 封装 MySQL 客户端
type Client struct {
	db     *sql.DB
	config config.DataSource
}

// QueryResult 查询结果
type QueryResult struct {
	Columns []string
	Rows    []map[string]interface{}
}

// BatchInsertOptions 批量插入选项
type BatchInsertOptions struct {
	OnDuplicateKeyUpdate string   // ON DUPLICATE KEY UPDATE 子句
	IgnoreDuplicates     bool     // 是否使用 INSERT IGNORE
	Columns              []string // 指定列（如果为空则使用所有列）
	BatchSize            int      // 每批插入的行数
}

// NewClient 创建新的 MySQL 客户端
func NewClient(cfg config.DataSource) (*Client, error) {
	var charset = "utf8mb4"
	var timeout = 10
	var maxIdle = 5
	var maxOpen = 20

	// 构建 DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&timeout=%ds&parseTime=true",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database, charset, timeout)

	// 创建数据库连接
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open MySQL connection: %v", err)
	}

	// 配置连接池
	db.SetMaxIdleConns(maxIdle)
	db.SetMaxOpenConns(maxOpen)
	db.SetConnMaxLifetime(5 * time.Minute)

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping MySQL: %v", err)
	}

	logx.Infof("Connected to MySQL at %s:%d/%s", cfg.Host, cfg.Port, cfg.Database)
	return &Client{db: db, config: cfg}, nil
}

// HealthCheck 健康检查
func (c *Client) HealthCheck() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return c.db.PingContext(ctx)
}

// Close 关闭连接
func (c *Client) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// Query 执行查询
func (c *Client) Query(query string, args ...interface{}) (*QueryResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	// 获取列名
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
	}

	// 准备结果切片
	result := &QueryResult{
		Columns: columns,
		Rows:    make([]map[string]interface{}, 0),
	}

	// 创建值的切片和指针的切片
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// 遍历行
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		// 创建行数据映射
		rowData := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				rowData[col] = string(b) // 将 []byte 转换为 string
			} else {
				rowData[col] = val
			}
		}

		result.Rows = append(result.Rows, rowData)
	}

	// 检查遍历过程中是否有错误
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %v", err)
	}

	return result, nil
}

// QueryRow 执行查询并返回单行
func (c *Client) QueryRow(query string, args ...interface{}) (map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	row := c.db.QueryRowContext(ctx, query, args...)

	// 获取列名
	rows, err := c.db.QueryContext(ctx, "SELECT 1 LIMIT 0") // 仅获取元数据
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
	}

	// 创建值的切片和指针的切片
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// 扫描行
	if err := row.Scan(valuePtrs...); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // 没有找到行
		}
		return nil, fmt.Errorf("failed to scan row: %v", err)
	}

	// 创建行数据映射
	rowData := make(map[string]interface{})
	for i, col := range columns {
		val := values[i]
		if b, ok := val.([]byte); ok {
			rowData[col] = string(b) // 将 []byte 转换为 string
		} else {
			rowData[col] = val
		}
	}

	return rowData, nil
}

// Execute 执行非查询语句
func (c *Client) Execute(query string, args ...interface{}) (sql.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute statement: %v", err)
	}

	return result, nil
}

// BatchInsert 批量插入数据
func (c *Client) BatchInsert(table string, data []map[string]interface{}, options BatchInsertOptions) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// 确定要插入的列
	var columns []string
	if len(options.Columns) > 0 {
		columns = options.Columns
	} else {
		// 从第一行数据获取所有列
		for col := range data[0] {
			columns = append(columns, col)
		}
	}

	// 分批处理
	if options.BatchSize <= 0 {
		options.BatchSize = 1000 // 默认批次大小
	}

	var totalRows int64
	for i := 0; i < len(data); i += options.BatchSize {
		end := i + options.BatchSize
		if end > len(data) {
			end = len(data)
		}

		batch := data[i:end]
		rowsAffected, err := c.batchInsert(table, batch, columns, options)
		if err != nil {
			return totalRows, err
		}

		totalRows += rowsAffected
	}

	return totalRows, nil
}

// batchInsert 执行单批插入
func (c *Client) batchInsert(table string, data []map[string]interface{}, columns []string, options BatchInsertOptions) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// 构建 SQL 语句
	var sqlBuilder strings.Builder

	// INSERT [IGNORE] INTO
	if options.IgnoreDuplicates {
		sqlBuilder.WriteString("INSERT IGNORE INTO ")
	} else {
		sqlBuilder.WriteString("INSERT INTO ")
	}

	sqlBuilder.WriteString(table)
	sqlBuilder.WriteString(" (")

	// 列名
	for i, col := range columns {
		if i > 0 {
			sqlBuilder.WriteString(", ")
		}
		sqlBuilder.WriteString("`")
		sqlBuilder.WriteString(col)
		sqlBuilder.WriteString("`")
	}
	sqlBuilder.WriteString(") VALUES ")

	// 值占位符
	var valueArgs []interface{}
	for rowIdx, row := range data {
		if rowIdx > 0 {
			sqlBuilder.WriteString(", ")
		}
		sqlBuilder.WriteString("(")
		for colIdx, col := range columns {
			if colIdx > 0 {
				sqlBuilder.WriteString(", ")
			}
			sqlBuilder.WriteString("?")
			valueArgs = append(valueArgs, row[col])
		}
		sqlBuilder.WriteString(")")
	}

	// ON DUPLICATE KEY UPDATE
	if options.OnDuplicateKeyUpdate != "" {
		sqlBuilder.WriteString(" ON DUPLICATE KEY UPDATE ")
		sqlBuilder.WriteString(options.OnDuplicateKeyUpdate)
	}

	query := sqlBuilder.String()

	// 执行插入
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := c.db.ExecContext(ctx, query, valueArgs...)
	if err != nil {
		return 0, fmt.Errorf("failed to execute batch insert: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %v", err)
	}

	logx.Infof("Inserted %d rows into %s", rowsAffected, table)
	return rowsAffected, nil
}

// BeginTransaction 开始事务
func (c *Client) BeginTransaction() (*sql.Tx, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
	}

	return tx, nil
}

// WithTransaction 在事务中执行函数
func (c *Client) WithTransaction(fn func(tx *sql.Tx) error) error {
	tx, err := c.BeginTransaction()
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // 重新抛出 panic
		}
	}()

	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("transaction error: %v, rollback error: %v", err, rbErr)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

// GetTableSchema 获取表结构
func (c *Client) GetTableSchema(table string) (map[string]string, error) {
	query := `
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY, EXTRA
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    `

	result, err := c.Query(query, c.config.Database, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %v", err)
	}

	schema := make(map[string]string)
	for _, row := range result.Rows {
		columnName := row["COLUMN_NAME"].(string)
		dataType := row["DATA_TYPE"].(string)
		isNullable := row["IS_NULLABLE"].(string)
		columnKey := row["COLUMN_KEY"].(string)

		// 构建类型描述
		typeDesc := dataType
		if isNullable == "YES" {
			typeDesc += " NULL"
		} else {
			typeDesc += " NOT NULL"
		}

		if columnKey == "PRI" {
			typeDesc += " PRIMARY KEY"
		}

		schema[columnName] = typeDesc
	}

	return schema, nil
}

// TableExists 检查表是否存在
func (c *Client) TableExists(table string) (bool, error) {
	query := `
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    `

	result, err := c.Query(query, c.config.Database, table)
	if err != nil {
		return false, fmt.Errorf("failed to check table existence: %v", err)
	}

	if len(result.Rows) == 0 {
		return false, nil
	}

	count, ok := result.Rows[0]["COUNT(*)"].(int64)
	if !ok {
		return false, nil
	}

	return count > 0, nil
}

// CreateTable 创建表
func (c *Client) CreateTable(table string, schema string) error {
	_, err := c.Execute(schema)
	if err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}

	logx.Infof("Created table: %s", table)
	return nil
}

// CreateTableIfNotExists 如果表不存在则创建
func (c *Client) CreateTableIfNotExists(table string, schema string) error {
	exists, err := c.TableExists(table)
	if err != nil {
		return err
	}

	if !exists {
		return c.CreateTable(table, schema)
	}

	return nil
}

// GetServerVersion 获取 MySQL 服务器版本
func (c *Client) GetServerVersion() (string, error) {
	row, err := c.QueryRow("SELECT VERSION() as version")
	if err != nil {
		return "", fmt.Errorf("failed to get server version: %v", err)
	}

	if row == nil {
		return "", fmt.Errorf("no version information returned")
	}

	version, ok := row["version"].(string)
	if !ok {
		return "", fmt.Errorf("invalid version format")
	}

	return version, nil
}

// GetTableSize 获取表大小（字节）
func (c *Client) GetTableSize(table string) (int64, error) {
	query := `
        SELECT 
            SUM(data_length + index_length) as size
        FROM information_schema.TABLES 
        WHERE table_schema = ? AND table_name = ?
        GROUP BY table_schema, table_name
    `

	row, err := c.QueryRow(query, c.config.Database, table)
	if err != nil {
		return 0, fmt.Errorf("failed to get table size: %v", err)
	}

	if row == nil {
		return 0, nil
	}

	size, ok := row["size"].(int64)
	if !ok {
		// 处理可能是 float64 的情况
		if sizeFloat, ok := row["size"].(float64); ok {
			return int64(sizeFloat), nil
		}
		return 0, fmt.Errorf("invalid size format")
	}

	return size, nil
}

// GetRowCount 获取表的行数
func (c *Client) GetRowCount(table string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) as count FROM `%s`", table)

	row, err := c.QueryRow(query)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %v", err)
	}

	if row == nil {
		return 0, nil
	}

	count, ok := row["count"].(int64)
	if !ok {
		return 0, fmt.Errorf("invalid count format")
	}

	return count, nil
}

// TruncateTable 清空表
func (c *Client) TruncateTable(table string) error {
	query := fmt.Sprintf("TRUNCATE TABLE `%s`", table)

	_, err := c.Execute(query)
	if err != nil {
		return fmt.Errorf("failed to truncate table: %v", err)
	}

	logx.Infof("Truncated table: %s", table)
	return nil
}

// DropTable 删除表
func (c *Client) DropTable(table string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table)

	_, err := c.Execute(query)
	if err != nil {
		return fmt.Errorf("failed to drop table: %v", err)
	}

	logx.Infof("Dropped table: %s", table)
	return nil
}

// GetDatabaseStats 获取数据库统计信息
func (c *Client) GetDatabaseStats() (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	// 获取表数量
	tableCountQuery := `
        SELECT COUNT(*) as table_count 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = ?
    `
	tableCountRow, err := c.QueryRow(tableCountQuery, c.config.Database)
	if err != nil {
		return nil, fmt.Errorf("failed to get table count: %v", err)
	}
	if tableCountRow != nil {
		stats["table_count"] = tableCountRow["table_count"]
	}

	// 获取总数据大小
	totalSizeQuery := `
        SELECT SUM(data_length + index_length) as total_size
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = ?
    `
	totalSizeRow, err := c.QueryRow(totalSizeQuery, c.config.Database)
	if err != nil {
		return nil, fmt.Errorf("failed to get total size: %v", err)
	}
	if totalSizeRow != nil {
		stats["total_size"] = totalSizeRow["total_size"]
	}

	// 获取连接信息
	connectionQuery := `
        SELECT 
            VARIABLE_VALUE as connections,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Threads_connected') as threads_connected,
            (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Threads_running') as threads_running
        FROM performance_schema.global_status 
        WHERE VARIABLE_NAME = 'Threads_connected'
    `
	connectionRow, err := c.QueryRow(connectionQuery)
	if err == nil && connectionRow != nil {
		stats["connections"] = connectionRow["connections"]
		stats["threads_connected"] = connectionRow["threads_connected"]
		stats["threads_running"] = connectionRow["threads_running"]
	}

	return stats, nil
}
