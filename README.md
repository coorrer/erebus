# Erebus - Multi-Target Real-Time Data Synchronization Platform

Erebus is a high-performance, scalable real-time data synchronization platform that supports streaming MySQL data to multiple target data storage systems.
*[中文版](README_zh.md) • [English Version](README.md)*

## 🚀 Key Features

### Core Capabilities
- **Real-time MySQL binlog-based data synchronization**
- **Multi-target support**: Synchronization to ClickHouse, MeiliSearch, Elasticsearch, and Kafka
- **Flexible data mapping**: Field-level mapping, type conversion, and complex data transformations
- **Checkpoint recovery**: Position-based management supporting fault recovery and data consistency
- **Batch processing optimization**: Intelligent batching mechanism balancing throughput and latency
- **Dead letter queue**: Failed data processing and retry mechanisms
- **Graceful startup/shutdown**: Smooth operation ensuring data integrity

### Advanced Features
- **Field-level conditional filtering**: Conditional field synchronization
- **Data type conversion**: Automatic type recognition and conversion
- **Custom transformation functions**: Support for multiple built-in transformation functions
- **Connection pool management**: Optimized connection pooling and resource management
- **Monitoring metrics**: Rich operational metrics and log output
- **Hot configuration reload**: Runtime configuration updates

## 📋 Supported Synchronization Types

| Sync Type | Target System | Status | Features |
|-----------|---------------|--------|----------|
| `mysql2clickhouse` | ClickHouse | ✅ Stable | Batch inserts, update/delete support, field mapping |
| `mysql2meilisearch` | MeiliSearch | ✅ Stable | Document indexing, real-time search support |
| `mysql2elasticsearch` | Elasticsearch | ✅ Stable | Full-text search, complex query support |
| `mysql2kafka` | Apache Kafka | ✅ Stable | Message queue, stream processing support |

## 🛠 Installation & Deployment

### Requirements
- **Go Version**: 1.19+
- **MySQL**: 5.7+ (binlog must be enabled)
- **Target Systems**: Corresponding storage systems based on synchronization type

### Quick Installation

```bash
# Clone project
git clone https://github.com/coorrer/erebus.git
cd erebus

# Build project
make

# Run
./erebus -f etc/config.yaml
```

## ⚙️ Configuration Guide

### Main Configuration Example (config.yaml)

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
     Description: "User data synchronization"
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

### Field Mapping Configuration Details

```yaml
ColumnMappings:
  # Basic mapping
  - Source: "source_field"
    Target: "target_field"
    Ignore: false          # Whether to ignore this field
    Required: true         # Whether this field is required
    Default: "default_value" # Default value
    
  # Type conversion
  - Source: "status"
    Target: "is_active"
    Type: "boolean"        # Supported: string, int, float, boolean, datetime, date
    
  # Conditional filtering
  - Source: "amount"
    Target: "amount_category"
    Condition: "{{value}} > 100"  # Conditional expression
    Default: "small"
    
  # Transformation functions
  - Source: "full_name"
    Target: "first_name"
    Transform: "substring({{value}}, 0, 10)"  # Built-in transformation functions
```

## 🔧 Built-in Transformation Functions

### String Processing
- `toLowerCase` / `toUpperCase` - Case conversion
- `trim` - Whitespace removal
- `concat(value1, value2, ...)` - String concatenation
- `substring(value, start, length)` - Substring extraction
- `replace(value, old, new)` - String replacement

### Time Processing
- `parseDateTimeBestEffort` - Intelligent datetime parsing
- `toUnixTimestamp` - Convert to Unix timestamp

### Conditional Logic
- `if(condition, true_value, false_value)` - Conditional evaluation
- `default` - Default value handling

### Type Conversion
- `toBoolean` - Convert to boolean
- `jsonStringify` - Convert to JSON string

## 🚀 Usage Examples

### 1. MySQL to ClickHouse Synchronization

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

### 2. MySQL to Elasticsearch Synchronization

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

### 3. MySQL to Kafka Synchronization

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

### Log Configuration

```yaml
Log:
  Level: "info"
  Mode: "file"
  FilePath: "./logs/erebus.log"
  MaxSize: 100
  MaxBackups: 10
  MaxAge: 30
```

## 🐛 Troubleshooting

### Common Issues

1. **Binlog Connection Failures**
    - Check if MySQL binlog is enabled
    - Verify user has REPLICATION permissions

2. **Synchronization Delay**
    - Adjust batch size and timeout settings
    - Check network connectivity and system resources

3. **High Memory Usage**
    - Reduce batch size
    - Enhance queue monitoring and alerts

### Debug Mode

```bash
# Enable debug mode
./erebus --config config.yaml --log-level debug

# View detailed logs
tail -f logs/erebus.log
```

## 🤝 Contribution Guide

We welcome community contributions! Please follow these steps:

1. Fork the project
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Create a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

Thanks to the following open-source projects:
- [go-mysql](https://github.com/go-mysql-org/go-mysql) - MySQL binlog parsing
- [zeromicro/go-zero](https://github.com/zeromicro/go-zero) - High-performance Go framework
- All contributors and users for their support

**Erebus** - Making data synchronization simpler and more reliable! 🚀
