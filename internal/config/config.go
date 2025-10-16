package config

import (
	"time"

	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

// DataSourceType 数据源类型
type DataSourceType string

const (
	DataSourceMysql         DataSourceType = "mysql"
	DataSourceClickHouse    DataSourceType = "clickhouse"
	DataSourceElasticsearch DataSourceType = "elasticsearch"
	DataSourceMeiliSearch   DataSourceType = "meiliSearch"
	DataSourceKafka         DataSourceType = "kafka"
)

// DataSource 统一数据源配置
type DataSource struct {
	Name     string         `json:"Name"`
	Type     DataSourceType `json:"Type"`
	Host     string         `json:"Host"`
	Port     int            `json:"Port"`
	Database string         `json:"Database,optional"`
	User     string         `json:"User,optional"`
	Password string         `json:"Password,optional"`
	ServerID uint32         `json:"ServerId,optional"`
	Index    string         `json:"Index,optional"`
	APIKey   string         `json:"APIKey,optional"`
	Timeout  time.Duration  `json:"Timeout,optional"`
}

// SyncTaskType 同步任务类型
type SyncTaskType string

const (
	SyncTaskMysql2ClickHouse    SyncTaskType = "mysql2clickhouse"
	SyncTaskMysql2Elasticsearch SyncTaskType = "mysql2elasticsearch"
	SyncTaskMysql2Mysql         SyncTaskType = "mysql2mysql"
	SyncTaskMysql2Kafka         SyncTaskType = "mysql2kafka"
	SyncTaskMysql2MeiliSearch   SyncTaskType = "mysql2meiliSearch"
)

// ColumnMapping 定义复杂列映射
type ColumnMapping struct {
	Source       string        `json:"Source"`
	Target       string        `json:"Target"`
	Type         string        `json:"Type,optional"`
	DefaultValue interface{}   `json:"DefaultValue,optional"`
	Required     bool          `json:"Required,default=false"`
	Transform    string        `json:"Transform,optional"`
	Ignore       bool          `json:"Ignore,default=false"`
	Condition    string        `json:"Condition,optional"`
	EnumMapping  []EnumMapping `json:"EnumMapping,omitempty"` // 通用枚举映射
}

// EnumMapping 枚举映射定义
type EnumMapping struct {
	Source string      `json:"Source"` // MySQL中的枚举字符串值
	Target interface{} `json:"Target"` // 目标库中的枚举值（可以是int8, int16, string等）
}

// SyncTaskTable 表同步配置
type SyncTaskTable struct {
	SourceDatabase string          `json:"SourceDatabase"`
	SourceTable    string          `json:"SourceTable"`
	TargetDatabase string          `json:"TargetDatabase,optional"`
	TargetTable    string          `json:"TargetTable,optional"`
	ColumnMappings []ColumnMapping `json:"ColumnMappings"`
	WhereClause    string          `json:"WhereClause,optional"`
	BatchSize      int64           `json:"BatchSize,default=1000"`
	BatchTimeout   time.Duration   `json:"BatchTimeout"`
	OnConflict     string          `json:"OnConflict,optional"`
	Priority       string          `json:"Priority,optional"`
	QueueSize      int64           `json:"QueueSize,optional"`
}

// SyncTask 同步任务配置
type SyncTask struct {
	Name        string          `json:"Name"`
	Type        SyncTaskType    `json:"Type"`
	Enabled     bool            `json:"Enabled,default=true"`
	Source      string          `json:"Source"`
	Target      string          `json:"Target"`
	Description string          `json:"Description,optional"`
	Tables      []SyncTaskTable `json:"Tables"`
}

// Config 定义应用总配置
type Config struct {
	service.ServiceConf
	DataSources         []DataSource    `json:"DataSources"`
	SyncTasks           []SyncTask      `json:"SyncTasks,optional"`
	Redis               redis.RedisConf `json:"Redis,optional"`
	PositionStoragePath string          `json:"PositionStoragePath"`
	DeadLetterPath      string          `json:"DeadLetterPath"`
}
