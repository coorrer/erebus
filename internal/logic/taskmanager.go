package logic

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/coorrer/erebus/internal/config"
	"github.com/coorrer/erebus/internal/logic/datasource"
	"github.com/zeromicro/go-zero/core/logx"
)

// TaskManager 管理所有同步任务
type TaskManager struct {
	ctx               context.Context
	cancel            context.CancelFunc
	config            config.Config
	dataSourceManager *datasource.Manager
	tasks             map[string]SyncTaskFactory
	wg                sync.WaitGroup
	isRunning         bool
	mu                sync.RWMutex
	stopTimeout       time.Duration // 添加停止超时时间
}

// NewTaskManager 创建任务管理器
func NewTaskManager(cfg config.Config) (*TaskManager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// 初始化数据源管理器
	dsManager := datasource.NewManager(cfg.DataSources)

	manager := &TaskManager{
		ctx:               ctx,
		cancel:            cancel,
		config:            cfg,
		dataSourceManager: dsManager,
		tasks:             make(map[string]SyncTaskFactory),
		isRunning:         false,
		stopTimeout:       10 * time.Second, // 设置默认停止超时时间
	}

	// 初始化所有任务
	if err := manager.initTasks(); err != nil {
		cancel()
		dsManager.Close()
		return nil, err
	}

	return manager, nil
}

// initTasks 初始化所有同步任务
func (m *TaskManager) initTasks() error {
	for _, taskConfig := range m.config.SyncTasks {
		if !taskConfig.Enabled {
			logx.Infof("Task %s is disabled, skipping", taskConfig.Name)
			continue
		}

		sourceDataSource, ok := m.dataSourceManager.GetDataSourceConfig(taskConfig.Source)
		if !ok {
			logx.Infof("source data source not exist %s, skipping", taskConfig.Source)
			continue
		}
		targetDataSource, ok := m.dataSourceManager.GetDataSourceConfig(taskConfig.Target)
		if !ok {
			logx.Infof("target data source not exist %s, skipping", taskConfig.Target)
			continue
		}

		task, err := NewSyncTaskFactory(m.ctx, m.config, taskConfig, sourceDataSource, targetDataSource)
		if err != nil {
			return fmt.Errorf("failed to create task %s: %v", taskConfig.Name, err)
		}

		m.tasks[taskConfig.Name] = task
		logx.Infof("Initialized sync task: %s (%s)", taskConfig.Name, taskConfig.Type)
	}

	return nil
}

// 添加辅助方法获取配置中的任务类型
func (m *TaskManager) getConfigTaskTypes() []config.SyncTaskType {
	var types []config.SyncTaskType
	for _, task := range m.config.SyncTasks {
		types = append(types, task.Type)
	}
	return types
}

// Start 启动所有任务
func (m *TaskManager) Start() {
	m.mu.Lock()
	if m.isRunning {
		m.mu.Unlock()
		logx.Info("Task manager is already running")
		return
	}
	m.isRunning = true
	m.mu.Unlock()

	logx.Info("Starting all sync tasks")

	for name, task := range m.tasks {
		m.wg.Add(1)
		go func(t SyncTaskFactory, taskName string) {
			defer m.wg.Done()
			logx.Infof("Starting sync task: %s", taskName)
			t.Start()
		}(task, name)
	}

	logx.Info("All sync tasks started")

	// 等待停止信号
	<-m.ctx.Done()
	logx.Info("Task manager stopping")
}

// Stop 停止所有任务
func (m *TaskManager) Stop() {
	m.mu.Lock()
	if !m.isRunning {
		m.mu.Unlock()
		logx.Info("Task manager is not running")
		return
	}
	m.isRunning = false
	m.mu.Unlock()

	logx.Info("Stopping all sync tasks")

	// 发送停止信号
	m.cancel()

	// 使用带超时的等待组
	done := make(chan struct{})
	go func() {
		// 停止所有任务
		for name, task := range m.tasks {
			logx.Infof("Stopping task: %s", name)
			m.stopTask(task)
		}

		// 等待所有任务完成
		m.wg.Wait()

		// 关闭数据源连接
		if err := m.dataSourceManager.Close(); err != nil {
			logx.Errorf("Error closing data sources: %v", err)
		}

		close(done)
	}()

	// 等待停止完成或超时
	select {
	case <-done:
		logx.Info("All sync tasks stopped successfully")
	case <-time.After(m.stopTimeout):
		logx.Error("Task manager stop timeout, forcing shutdown")
		// 记录未停止的任务
		for name, task := range m.tasks {
			if running, ok := task.(interface{ IsRunning() bool }); ok && running.IsRunning() {
				logx.Errorf("Task %s is still running", name)
			}
		}
	}
}

// stopTask 停止单个任务（带超时）
func (m *TaskManager) stopTask(task SyncTaskFactory) {
	// 使用带超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		task.Stop()
		close(done)
	}()

	select {
	case <-done:
		// 任务正常停止
	case <-ctx.Done():
		logx.Error("Task stop timeout, may not have stopped cleanly")
	}
}

// GetTask 获取特定任务
func (m *TaskManager) GetTask(name string) (SyncTaskFactory, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	task, exists := m.tasks[name]
	return task, exists
}

// GetTasksByType 按类型获取任务
func (m *TaskManager) GetTasksByType(taskType config.SyncTaskType) []SyncTaskFactory {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var tasks []SyncTaskFactory
	for _, task := range m.tasks {
		if task.Type() == string(taskType) {
			tasks = append(tasks, task)
		}
	}

	return tasks
}
