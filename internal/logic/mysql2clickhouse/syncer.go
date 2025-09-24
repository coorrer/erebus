package mysql2clickhouse

import (
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/zeromicro/go-zero/core/logx"
	"sync"

	"github.com/coorrer/erebus/internal/config"
	"github.com/coorrer/erebus/internal/logic/position"
	"github.com/coorrer/erebus/pkg/clickhouse"
)

// Syncer Mysql到ClickHouse同步器
type Syncer struct {
	ctx             context.Context
	cancel          context.CancelFunc
	config          config.Config
	mysqlConfig     config.DataSource
	taskConfig      config.SyncTask
	canal           *canal.Canal
	chClient        *clickhouse.Client
	handler         *BinlogHandler
	positionManager *position.PositionManager
	mysqlPosition   *mysql.Position
	wg              sync.WaitGroup
	isRunning       bool
	mu              sync.RWMutex
}

// NewSyncer 创建新的Mysql到ClickHouse同步器
func NewSyncer(ctx context.Context, cfg config.Config, taskConfig config.SyncTask, sourceDataSource config.DataSource, targetDataSource config.DataSource) (*Syncer, error) {
	childCctx, cancel := context.WithCancel(ctx)

	if sourceDataSource.Type != config.DataSourceMysql {
		cancel()
		return nil, fmt.Errorf("mysql2clickhouse source datasource not support %v", sourceDataSource.Type)
	}

	if targetDataSource.Type != config.DataSourceClickHouse {
		cancel()
		return nil, fmt.Errorf("mysql2clickhouse target datasource not support %v", targetDataSource.Type)
	}

	clickhouseClient, err := clickhouse.NewClient(targetDataSource)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("初始化clickhouse客户端失败,err:%v,config:%v", err, targetDataSource)
	}

	// 初始化位置管理器
	positionManager, err := position.NewPositionManager(cfg.PositionStoragePath)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("初始化positionManager失败,err:%v,PositionStoragePath:%v", err, cfg.PositionStoragePath)
	}

	// 初始化Canal
	canalCfg := canal.NewDefaultConfig()
	canalCfg.Addr = fmt.Sprintf("%s:%d", sourceDataSource.Host, sourceDataSource.Port)
	canalCfg.User = sourceDataSource.User
	canalCfg.Password = sourceDataSource.Password
	canalCfg.ServerID = sourceDataSource.ServerID

	// 设置需要监听的表
	var includeTables []string
	for _, table := range taskConfig.Tables {
		includeTables = append(includeTables, table.SourceDatabase+"\\."+table.SourceTable)
	}
	canalCfg.IncludeTableRegex = includeTables

	c, err := canal.NewCanal(canalCfg)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create canal: %v", err)
	}

	// 尝试从保存的位置开始同步
	pos, found, err := positionManager.LoadPosition(
		taskConfig.Name,
		sourceDataSource.Host,
		sourceDataSource.Port,
		sourceDataSource.Database,
	)
	if err != nil {
		logx.Errorf("Failed to load position for task %s: %v", taskConfig.Name, err)
	}

	var mysqlPosition *mysql.Position
	if found {
		mysqlPosition = &mysql.Position{
			Name: pos.BinlogFile,
			Pos:  pos.BinlogPosition,
		}
		logx.Infof("Task %s starting from saved position: %s:%d",
			taskConfig.Name, pos.BinlogFile, pos.BinlogPosition)
	} else {
		logx.Infof("No saved position found for task %s, starting from current position",
			taskConfig.Name)
	}

	// 创建事件处理器
	handler := NewBinlogHandler(
		childCctx,
		cfg,
		clickhouseClient,
		positionManager,
		taskConfig,
		sourceDataSource,
		targetDataSource,
	)

	return &Syncer{
		ctx:             childCctx,
		cancel:          cancel,
		config:          cfg,
		mysqlConfig:     sourceDataSource,
		taskConfig:      taskConfig,
		canal:           c,
		chClient:        clickhouseClient,
		handler:         handler,
		positionManager: positionManager,
		mysqlPosition:   mysqlPosition,
		isRunning:       false,
	}, nil
}

// Type 返回任务类型
func (s *Syncer) Type() string {
	return string(config.SyncTaskMysql2ClickHouse)
}

// Name 返回任务名称
func (s *Syncer) Name() string {
	return s.taskConfig.Name
}

// Start 启动同步服务
func (s *Syncer) Start() {
	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		logx.Info("Syncer is already running")
		return
	}
	s.isRunning = true
	s.mu.Unlock()

	logx.Infof("Starting Mysql to ClickHouse syncer for task: %s", s.taskConfig.Name)

	// 设置事件处理器
	s.canal.SetEventHandler(s.handler)

	// 启动错误处理器
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
	}()

	// 启动事件处理器
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.handler.Start()
	}()

	// 启动Canal
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		logx.Infof("Starting Mysql binlog replication for task: %s", s.taskConfig.Name)

		var err error
		if s.mysqlPosition != nil {
			err = s.canal.RunFrom(*s.mysqlPosition)
		} else {
			err = s.canal.Run()
		}
		if err != nil {
			logx.Errorf("Canal run error for task %s: %v", s.taskConfig.Name, err)
		}
	}()

	logx.Infof("Mysql to ClickHouse syncer for task %s started successfully", s.taskConfig.Name)

	// 等待停止信号
	<-s.ctx.Done()
	logx.Infof("Mysql to ClickHouse syncer for task %s stopping", s.taskConfig.Name)
}

// Stop 停止同步服务
func (s *Syncer) Stop() {
	s.mu.Lock()
	if !s.isRunning {
		s.mu.Unlock()
		logx.Infof("Syncer for task %s is not running", s.taskConfig.Name)
		return
	}
	s.isRunning = false
	s.mu.Unlock()

	logx.Infof("Stopping Mysql to ClickHouse syncer for task: %s", s.taskConfig.Name)

	// 发送停止信号
	s.cancel()

	// 停止Canal
	if s.canal != nil {
		s.canal.Close()
	}

	// 停止事件处理器
	if s.handler != nil {
		s.handler.Stop()
	}

	// 等待所有goroutine完成
	s.wg.Wait()

	logx.Infof("Mysql to ClickHouse syncer for task %s stopped successfully", s.taskConfig.Name)
}
