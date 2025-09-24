# Erebus - 多目标实时数据同步平台

Erebus 是一个高性能、可扩展的实时数据同步平台，支持将 MySQL 数据实时同步到多种目标数据存储系统。

## 🚀 功能特性

### 核心功能
- **基于 MySQL binlog 的实时数据同步**
- **多目标支持**：支持同步到 ClickHouse、MeiliSearch、Elasticsearch、Kafka
- **灵活的数据映射**：支持字段级映射、类型转换和复杂的数据转换
- **断点续传**：基于位点管理，支持故障恢复和数据一致性保证
- **批量处理优化**：智能批处理机制，平衡吞吐量和延迟
- **死信队列**：失败数据处理和重试机制
- **优雅启停**：平滑启动和停止，确保数据完整性

### 高级特性
- **字段级条件过滤**：支持基于条件的字段同步
- **数据类型转换**：自动类型识别和转换
- **自定义转换函数**：支持多种内置转换函数
- **连接池管理**：优化的连接池和资源管理
- **监控指标**：丰富的运行指标和日志输出
- **配置热加载**：支持运行时配置更新

## 📋 支持的同步类型

| 同步类型 | 目标系统 | 状态 | 特性 |
|---------|---------|------|------|
| `mysql2clickhouse` | ClickHouse | ✅ 稳定 | 批量插入、更新、删除支持，字段映射 |
| `mysql2meilisearch` | MeiliSearch | ✅ 稳定 | 文档索引、实时搜索支持 |
| `mysql2elasticsearch` | Elasticsearch | ✅ 稳定 | 全文搜索、复杂查询支持 |
| `mysql2kafka` | Apache Kafka | ✅ 稳定 | 消息队列、流式处理支持 |

## 🛠 安装部署

### 环境要求
- **Go 版本**: 1.19+
- **MySQL**: 5.7+ (需要开启binlog)
- **目标系统**: 根据同步类型选择对应的存储系统

### 快速安装

```bash
# 克隆项目
git clone https://github.com/coorrer/erebus.git
cd erebus

# 构建项目
make

# 运行
./erebus -f etc/config.yaml
```

## ⚙️ 配置说明

### 主配置文件示例 (config.yaml)

```yaml
Name: erebus
Log:
   Level: info
   Mode: file
   FilePath: /var/log/erebus

PositionStoragePath: /var/run/erebus/position

DataSources:
   - Name: mysql_test
     Type: mysql
     Host: 127.0.0.1
     Port: 3306
     User: root
     Password: 123456
     ServerID: 1001
   - Name: clickhouse_test
     Type: clickhouse
     Host: localhost
     Port: 9000
     Database: db_test
     User: default
     Password:

SyncTasks:
   - Name: user_data_sync
     Enabled: true
     MysqlSource: mysql_test
     ClickHouseTarget: clickhouse_test
     Description: "用户数据同步"
     Tables:
        - SourceDatabase: db_test
          SourceTable: user
          TargetDatabase: db_test
          TargetTable: user
          ColumnMappings:
             - Source: id
               Target: user_id
               Type: UInt64
               Required: true
             - Source: nickname
               Target: nickname
               Type: String
               Transform: toUpperCase
          BatchSize: 1000
          BatchTimeout: 1s
          OnConflict: "REPLACE"
```

### 字段映射配置详解

```yaml
ColumnMappings:
  # 基本映射
  - Source: "source_field"
    Target: "target_field"
    Ignore: false          # 是否忽略该字段
    Required: true         # 是否必需字段
    Default: "default_value" # 默认值
    
  # 类型转换
  - Source: "status"
    Target: "is_active"
    Type: "boolean"        # 支持: string, int, float, boolean, datetime, date
    
  # 条件过滤
  - Source: "amount"
    Target: "amount_category"
    Condition: "{{value}} > 100"  # 条件表达式
    Default: "small"
    
  # 转换函数
  - Source: "full_name"
    Target: "first_name"
    Transform: "substring({{value}}, 0, 10)"  # 内置转换函数
```

## 🔧 内置转换函数

### 字符串处理
- `toLowerCase` / `toUpperCase` - 大小写转换
- `trim` - 去除空格
- `concat(value1, value2, ...)` - 字符串连接
- `substring(value, start, length)` - 子字符串
- `replace(value, old, new)` - 字符串替换

### 时间处理
- `parseDateTimeBestEffort` - 智能时间解析
- `toUnixTimestamp` - 转换为Unix时间戳

### 条件逻辑
- `if(condition, true_value, false_value)` - 条件判断
- `default` - 默认值处理

### 类型转换
- `toBoolean` - 转换为布尔值
- `jsonStringify` - 转换为JSON字符串

## 🚀 使用示例

### 1. MySQL 到 ClickHouse 同步

```yaml
SyncTasks:
  - Name: "order_analytics"
    Type: "mysql2clickhouse"
    Source: "mysql_source"
    Target: "clickhouse_target"
    Tables:
      - SourceDatabase: "ecommerce"
        SourceTable: "orders"
        TargetDatabase: "analytics"
        TargetTable: "order_facts"
        ColumnMappings:
          - Source: "order_id"
            Target: "id"
          - Source: "customer_id"
            Target: "customer_id"
          - Source: "amount"
            Target: "amount"
            Transform: "round({{value}}, 2)"
          - Source: "status"
            Target: "order_status"
```

### 2. MySQL 到 Elasticsearch 同步

```yaml
SyncTasks:
  - Name: "product_search"
    Type: "mysql2elasticsearch"
    Source: "mysql_source"
    Target: "es_target"
    Tables:
      - SourceDatabase: "catalog"
        SourceTable: "products"
        TargetTable: "product_index"
        ColumnMappings:
          - Source: "product_name"
            Target: "name"
            Transform: "toLowerCase"
          - Source: "description"
            Target: "content"
          - Source: "price"
            Target: "price"
          - Source: "categories"
            Target: "tags"
            Transform: "jsonStringify"
```

### 3. MySQL 到 Kafka 同步

```yaml
SyncTasks:
  - Name: "user_events"
    Type: "mysql2kafka"
    Source: "mysql_source"
    Target: "kafka_target"
    Tables:
      - SourceDatabase: "user_db"
        SourceTable: "user_activities"
        TargetTable: "user-events"  # Kafka topic
        ColumnMappings:
          - Source: "user_id"
            Target: "user_id"
          - Source: "action"
            Target: "event_type"
          - Source: "timestamp"
            Target: "event_time"
            Transform: "toUnixTimestamp"
```

### 日志配置

```yaml
Log:
   Level: "info"
   Mode: "file"
   FilePath: "./logs/erebus.log"
   MaxSize: 100
   MaxBackups: 10
   MaxAge: 30
```

## 🐛 故障排除

### 常见问题

1. **Binlog 连接失败**
   - 检查 MySQL binlog 是否开启
   - 确认用户有 REPLICATION 权限

2. **同步延迟**
   - 调整批处理大小和超时时间
   - 检查网络连接和系统资源

3. **内存使用过高**
   - 减小批处理大小
   - 增加队列监控和告警

### 调试模式

```bash
# 启用调试模式
./erebus --config config.yaml --log-level debug

# 查看详细日志
tail -f logs/erebus.log
```

## 🤝 贡献指南

我们欢迎社区贡献！请参考以下步骤：

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 创建 Pull Request

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 🙏 致谢

感谢以下开源项目：
- [go-mysql](https://github.com/go-mysql-org/go-mysql) - MySQL binlog 解析
- [zeromicro/go-zero](https://github.com/zeromicro/go-zero) - 高性能 Go 框架
- 所有贡献者和用户的支持

**Erebus** - 让数据同步更简单、更可靠！ 🚀

---
*[中文版](README_zh.md) • [English Version](README.md)*