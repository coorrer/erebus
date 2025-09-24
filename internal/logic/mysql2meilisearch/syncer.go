package mysql2meilisearch

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/zeromicro/go-zero/core/logx"

	"github.com/coorrer/erebus/internal/config"
	"github.com/coorrer/erebus/internal/logic/position"
	"github.com/coorrer/erebus/pkg/meilisearch"
)

// Syncer mysql到MeiliSearch同步器
type Syncer struct {
	ctx             context.Context
	cancel          context.CancelFunc
	config          config.Config
	mysqlConfig     config.DataSource
	taskConfig      config.SyncTask
	canal           *canal.Canal
	meiliClient     *meilisearch.Client
	handler         *BinlogHandler
	positionManager *position.PositionManager
	mysqlPosition   *mysql.Position
	wg              sync.WaitGroup
	isRunning       bool
	mu              sync.RWMutex
	stopTimeout     time.Duration
}

// NewSyncer 创建新的mysql到MeiliSearch同步器
func NewSyncer(ctx context.Context, cfg config.Config, taskConfig config.SyncTask, sourceDataSource config.DataSource, targetDataSource config.DataSource) (*Syncer, error) {
	childCtx, cancel := context.WithCancel(ctx)

	if sourceDataSource.Type != config.DataSourceMysql {
		cancel()
		return nil, fmt.Errorf("mysql2meilisearch source datasource not support %v", sourceDataSource.Type)
	}

	if targetDataSource.Type != config.DataSourceMeiliSearch {
		cancel()
		return nil, fmt.Errorf("mysql2meilisearch target datasource not support %v", targetDataSource.Type)
	}

	meiliSearchClient, err := meilisearch.NewClient(targetDataSource)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("初始化meilisearch客户端失败,err:%v,config:%v", err, targetDataSource)
	}

	// 初始化位置管理器
	positionManager := position.NewPositionManager(cfg.PositionStoragePath)

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
		logx.Errorf("failed to load position for task %s: %v", taskConfig.Name, err)
	}

	var mysqlPosition *mysql.Position
	if found {
		mysqlPosition = &mysql.Position{
			Name: pos.BinlogFile,
			Pos:  pos.BinlogPosition,
		}
		logx.Infof("task %s starting from saved position: %s:%d",
			taskConfig.Name, pos.BinlogFile, pos.BinlogPosition)
	} else {
		logx.Infof("no saved position found for task %s, starting from current position",
			taskConfig.Name)
	}

	// 创建事件处理器
	handler := NewBinlogHandler(
		childCtx,
		cfg,
		meiliSearchClient,
		positionManager,
		taskConfig,
		sourceDataSource,
		targetDataSource,
	)

	return &Syncer{
		ctx:             childCtx,
		cancel:          cancel,
		config:          cfg,
		mysqlConfig:     sourceDataSource,
		taskConfig:      taskConfig,
		canal:           c,
		meiliClient:     meiliSearchClient,
		handler:         handler,
		positionManager: positionManager,
		mysqlPosition:   mysqlPosition,
		isRunning:       false,
		stopTimeout:     10 * time.Second,
	}, nil
}

// Type 返回任务类型
func (s *Syncer) Type() string {
	return string(config.SyncTaskMysql2MeiliSearch)
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
		logx.Info("syncer is already running")
		return
	}
	s.isRunning = true
	s.mu.Unlock()

	logx.Infof("starting mysql to MeiliSearch syncer for task: %s", s.taskConfig.Name)

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
		logx.Infof("starting mysql binlog replication for task: %s", s.taskConfig.Name)

		var err error
		if s.mysqlPosition != nil {
			err = s.canal.RunFrom(*s.mysqlPosition)
		} else {
			err = s.canal.Run()
		}
		if err != nil {
			logx.Errorf("canal run error for task %s: %v", s.taskConfig.Name, err)
		}
	}()

	logx.Infof("mySQL to MeiliSearch syncer for task %s started successfully", s.taskConfig.Name)

	// 等待停止信号
	<-s.ctx.Done()
	logx.Infof("mysql to MeiliSearch syncer for task %s stopping", s.taskConfig.Name)
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

	logx.Infof("Stopping mysql to MeiliSearch syncer for task: %s", s.taskConfig.Name)

	// 发送停止信号
	s.cancel()

	// 使用带超时的等待组
	done := make(chan struct{})
	go func() {
		// 停止Canal
		if s.canal != nil {
			s.canal.Close()
			logx.Infof("Canal closed for task %s", s.taskConfig.Name)
		}

		// 停止事件处理器
		if s.handler != nil {
			s.handler.Stop()
			logx.Infof("Handler stopped for task %s", s.taskConfig.Name)
		}

		// 等待所有goroutine完成
		s.wg.Wait()

		logx.Infof("mysql to MeiliSearch syncer for task %s stopped successfully", s.taskConfig.Name)
		close(done)
	}()

	// 等待停止完成或超时
	select {
	case <-done:
		// 正常停止
	case <-time.After(s.stopTimeout):
		logx.Errorf("Stop timeout for task %s, forcing shutdown", s.taskConfig.Name)
		// 强制关闭资源
		if s.canal != nil {
			s.canal.Close()
		}
	}
}
