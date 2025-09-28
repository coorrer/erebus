package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/zeromicro/go-zero/core/logx"

	"github.com/coorrer/erebus/internal/config"
)

// Client 封装ClickHouse客户端
type Client struct {
	conn   driver.Conn
	config config.DataSource
}

// NewClient 创建新的ClickHouse客户端
func NewClient(cfg config.DataSource) (*Client, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.User,
			Password: cfg.Password,
		},
		DialTimeout: 10 * time.Second,
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})

	if err != nil {
		return nil, fmt.Errorf("failed to connect to clickHouse: %v", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping clickHouse: %v", err)
	}

	logx.Debugf("connected to clickHouse at %s:%d", cfg.Host, cfg.Port)
	return &Client{conn: conn, config: cfg}, nil
}

// BatchInsert 执行批量插入
func (c *Client) BatchInsert(table string, columns []string, data []map[string]interface{}, onConflict string) error {
	if len(data) == 0 {
		return nil
	}

	startTime := time.Now()

	// 构建插入语句
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES", table, strings.Join(columns, ", "))

	// 添加冲突处理
	if onConflict != "" {
		query += " ON CONFLICT " + onConflict
	}

	// 打印模拟的 Sql 语句（用于调试）
	if len(data) > 0 {
		c.logSimulatedSql(table, columns, data)
	}

	batch, err := c.conn.PrepareBatch(context.Background(), query)
	if err != nil {
		logx.Errorf("failed to prepare batch for table %s: %v", table, err)
		return fmt.Errorf("failed to prepare batch: %v", err)
	}

	// 添加所有行
	for _, row := range data {
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			values[i] = row[col]
		}
		if err := batch.Append(values...); err != nil {
			logx.Errorf("failed to append row to batch for table %s: %v", table, err)
			return fmt.Errorf("failed to append row: %v", err)
		}
	}

	// 执行批量插入
	if err := batch.Send(); err != nil {
		logx.Errorf("failed to send batch for table %s: %v", table, err)
		return fmt.Errorf("failed to send batch: %v", err)
	}

	duration := time.Since(startTime)
	logx.Debugf("inserted %d rows into %s in %v", len(data), table, duration)
	return nil
}

// logSimulatedSql 打印模拟的 Sql 语句（用于调试）
func (c *Client) logSimulatedSql(table string, columns []string, data []map[string]interface{}) {
	// 生成模拟的 INSERT 语句（前3行）
	sampleRows := 3
	if len(data) < sampleRows {
		sampleRows = len(data)
	}

	var valueStrings []string
	for i := 0; i < sampleRows; i++ {
		var values []string
		for _, col := range columns {
			value := data[i][col]
			values = append(values, formatSqlValue(value))
		}
		valueStrings = append(valueStrings, "("+strings.Join(values, ", ")+")")
	}

	sampleSql := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		table,
		strings.Join(columns, ", "),
		strings.Join(valueStrings, ", "))

	if len(data) > sampleRows {
		sampleSql += fmt.Sprintf(" ... and %d more rows", len(data)-sampleRows)
	}

	logx.Debugf("simulated Sql (sample): %s", sampleSql)
}

// formatSqlValue 格式化 Sql 值
func formatSqlValue(value interface{}) string {
	if value == nil {
		return "NULL"
	}

	switch v := value.(type) {
	case string:
		// 转义单引号
		escaped := strings.ReplaceAll(v, "'", "''")
		return "'" + escaped + "'"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%f", v)
	case bool:
		if v {
			return "1"
		}
		return "0"
	case time.Time:
		return "'" + v.Format("2006-01-02 15:04:05") + "'"
	default:
		// 对于未知类型，转换为字符串并转义
		str := fmt.Sprintf("%v", v)
		escaped := strings.ReplaceAll(str, "'", "''")
		return "'" + escaped + "'"
	}
}

// HealthCheck 健康检查
func (c *Client) HealthCheck() error {
	return c.conn.Ping(context.Background())
}

// Close 关闭连接
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// ExecuteQuery 执行自定义查询
func (c *Client) ExecuteQuery(query string, args ...interface{}) error {
	// 打印真实执行的 Sql
	if len(args) > 0 {
		// 如果有参数，构建带参数的 Sql
		formattedQuery := formatQueryWithArgs(query, args)
		logx.Debugf("Executing Sql: %s", formattedQuery)
	} else {
		logx.Debugf("Executing Sql: %s", query)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startTime := time.Now()
	err := c.conn.Exec(ctx, query, args...)
	duration := time.Since(startTime)

	if err != nil {
		logx.Errorf("Failed to execute query: %s, error: %v", query, err)
		return fmt.Errorf("failed to execute query: %v", err)
	}

	logx.Debugf("Query executed successfully in %v: %s", duration, query)
	return nil
}

// formatQueryWithArgs 将查询和参数格式化为完整的 Sql 语句
func formatQueryWithArgs(query string, args []interface{}) string {
	// 简单的参数替换（注意：这不适用于所有情况，仅用于调试）
	result := query
	for _, arg := range args {
		// 找到第一个 ? 并替换
		idx := strings.Index(result, "?")
		if idx == -1 {
			break
		}

		formattedValue := formatSqlValue(arg)
		result = result[:idx] + formattedValue + result[idx+1:]
	}

	return result
}

// GetTableSchema 获取表结构
func (c *Client) GetTableSchema(table string) (map[string]string, error) {
	query := fmt.Sprintf("DESCRIBE TABLE %s", table)
	rows, err := c.conn.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	schema := make(map[string]string)
	for rows.Next() {
		var (
			name        string
			typeStr     string
			defaultType string
			defaultExpr string
			comment     string
			codecExpr   string
			ttlExpr     string
		)

		if err := rows.Scan(&name, &typeStr, &defaultType, &defaultExpr, &comment, &codecExpr, &ttlExpr); err != nil {
			return nil, err
		}
		schema[name] = typeStr
	}

	return schema, nil
}

// DeleteRows 删除行数据
func (c *Client) DeleteRows(table string, whereClause string) error {
	query := fmt.Sprintf("ALTER TABLE %s DELETE WHERE %s", table, whereClause)
	logx.Debugf("Executing DELETE Sql: %s", query)
	return c.ExecuteQuery(query)
}
