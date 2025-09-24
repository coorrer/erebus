package datasource

import (
	"fmt"
	"sync"

	"github.com/zeromicro/go-zero/core/logx"

	"github.com/coorrer/erebus/internal/config"
	"github.com/coorrer/erebus/pkg/clickhouse"
	"github.com/coorrer/erebus/pkg/elasticsearch"
	"github.com/coorrer/erebus/pkg/mysql"
)

// Manager 数据源管理器
type Manager struct {
	datasourceConfigMap map[string]config.DataSource
	datasourceClientMap map[string]interface{}
	mu                  sync.RWMutex
}

// NewManager 创建数据源管理器
func NewManager(dataSources []config.DataSource) *Manager {
	datasourceConfigMap := make(map[string]config.DataSource)
	for _, item := range dataSources {
		datasourceConfigMap[item.Name] = item
	}
	return &Manager{
		datasourceConfigMap: datasourceConfigMap,
		datasourceClientMap: make(map[string]interface{}),
	}
}

// InitClient 初始化所有数据源
func (m *Manager) InitClient() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, ds := range m.datasourceConfigMap {
		m.GetDataSourceClient(ds.Name)
	}

	return nil
}

// GetDataSourceConfig 获取数据源配置
func (m *Manager) GetDataSourceConfig(name string) (config.DataSource, bool) {
	source, exists := m.datasourceConfigMap[name]
	return source, exists
}

// GetDataSourceClient 获取数据源
func (m *Manager) GetDataSourceClient(name string) (interface{}, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	datasourceClient, exists := m.datasourceClientMap[name]
	if exists {
		return datasourceClient, true
	}

	datasourceConfig, ok := m.datasourceConfigMap[name]
	if !ok {
		return nil, false
	}

	var source interface{}
	var err error

	switch datasourceConfig.Type {
	case config.DataSourceMysql:
		source, err = mysql.NewClient(datasourceConfig)
	case config.DataSourceClickHouse:
		source, err = clickhouse.NewClient(datasourceConfig)
	case config.DataSourceElasticsearch:
		source, err = elasticsearch.NewClient(datasourceConfig)
	default:
		err = fmt.Errorf("unsupported data source type: %s", datasourceConfig.Type)
	}

	if err != nil {
		logx.Errorf("failed to initialize data source %s: %v", datasourceConfig.Name, err)
		return nil, false
	}

	m.datasourceClientMap[datasourceConfig.Name] = source
	logx.Infof("Initialized data source: %s (%s)", datasourceConfig.Name, datasourceConfig.Type)

	return source, true
}

// Close 关闭所有数据源连接
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error
	for name, source := range m.datasourceClientMap {
		if closer, ok := source.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				errs = append(errs, fmt.Errorf("failed to close data source %s: %v", name, err))
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing data sources: %v", errs)
	}

	return nil
}
