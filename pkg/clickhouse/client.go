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
		return nil, fmt.Errorf("failed to connect to ClickHouse: %v", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %v", err)
	}

	logx.Infof("Connected to ClickHouse at %s:%d", cfg.Host, cfg.Port)
	return &Client{conn: conn, config: cfg}, nil
}

// BatchInsert 执行批量插入
func (c *Client) BatchInsert(table string, columns []string, data []map[string]interface{}, onConflict string) error {
	if len(data) == 0 {
		return nil
	}

	// 构建插入语句
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES", table, strings.Join(columns, ", "))

	// 添加冲突处理
	if onConflict != "" {
		query += " ON CONFLICT " + onConflict
	}

	batch, err := c.conn.PrepareBatch(context.Background(), query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %v", err)
	}

	// 添加所有行
	for _, row := range data {
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			values[i] = row[col]
		}
		if err := batch.Append(values...); err != nil {
			return fmt.Errorf("failed to append row: %v", err)
		}
	}

	// 执行批量插入
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %v", err)
	}

	logx.Infof("Inserted %d rows into %s", len(data), table)
	return nil
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := c.conn.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}

	logx.Infof("Executed query: %s", query)
	return nil
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
	return c.ExecuteQuery(query)
}
