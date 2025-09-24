package position

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

type Position struct {
	BinlogFile     string `json:"binlog_file"`
	BinlogPosition uint32 `json:"binlog_position"`
	Gtid           string `json:"gtid,omitempty"`
	Timestamp      int64  `json:"timestamp"`
}

type PositionManager struct {
	storagePath string
	positions   map[string]Position // 主内存缓存
	dirtyFlags  map[string]bool     // 脏数据标记
	writeQueue  chan *writeRequest  // 异步写入队列
	mu          sync.RWMutex
	wg          sync.WaitGroup
	stopChan    chan struct{}
}

type writeRequest struct {
	key      string
	position Position
}

// 配置参数
type Config struct {
	StoragePath   string
	WriteInterval time.Duration // 写入间隔
	MaxBatchSize  int           // 最大批量大小
	BufferSize    int           // 缓冲区大小
}

const (
	DefaultWriteInterval = 2 * time.Second
	DefaultMaxBatchSize  = 100
	DefaultBufferSize    = 1000
)

func NewPositionManager(storagePath string) (*PositionManager, error) {
	return NewPositionManagerWithConfig(Config{
		StoragePath:   storagePath,
		WriteInterval: DefaultWriteInterval,
		MaxBatchSize:  DefaultMaxBatchSize,
		BufferSize:    DefaultBufferSize,
	})
}

func NewPositionManagerWithConfig(config Config) (*PositionManager, error) {
	if config.StoragePath == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			homeDir = "/tmp"
		}
		config.StoragePath = filepath.Join(homeDir, ".erebus", "sync_positions")
	}

	if config.WriteInterval <= 0 {
		config.WriteInterval = DefaultWriteInterval
	}
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = DefaultMaxBatchSize
	}
	if config.BufferSize <= 0 {
		config.BufferSize = DefaultBufferSize
	}

	// 创建存储目录
	if err := os.MkdirAll(config.StoragePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %v", err)
	}

	pm := &PositionManager{
		storagePath: config.StoragePath,
		positions:   make(map[string]Position),
		dirtyFlags:  make(map[string]bool),
		writeQueue:  make(chan *writeRequest, config.BufferSize),
		stopChan:    make(chan struct{}),
	}

	// 启动后台写入goroutine
	pm.wg.Add(1)
	go pm.backgroundWriter(config.WriteInterval, config.MaxBatchSize)

	// 启动定期全量同步goroutine（可选，用于数据安全）
	pm.wg.Add(1)
	go pm.periodicFullSync(30 * time.Second)

	logx.Infof("PositionManager started with write interval: %v, max batch size: %d",
		config.WriteInterval, config.MaxBatchSize)

	return pm, nil
}

// 后台批量写入器
func (p *PositionManager) backgroundWriter(interval time.Duration, maxBatchSize int) {
	defer p.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	batch := make(map[string]Position)

	for {
		select {
		case <-p.stopChan:
			// 程序退出时，写入所有剩余数据
			p.flushBatch(batch)
			return

		case req := <-p.writeQueue:
			// 添加到批量
			batch[req.key] = req.position

			// 如果批量达到最大大小，立即写入
			if len(batch) >= maxBatchSize {
				p.flushBatch(batch)
				batch = make(map[string]Position)
			}

		case <-ticker.C:
			// 定时写入
			if len(batch) > 0 {
				p.flushBatch(batch)
				batch = make(map[string]Position)
			}
		}
	}
}

// 批量写入文件
func (p *PositionManager) flushBatch(batch map[string]Position) {
	if len(batch) == 0 {
		return
	}

	successCount := 0
	failedKeys := make([]string, 0)

	for key, position := range batch {
		if err := p.writeToFile(key, position); err != nil {
			logx.Errorf("Failed to write position for %s: %v", key, err)
			failedKeys = append(failedKeys, key)
		} else {
			successCount++

			// 清除脏数据标记
			p.mu.Lock()
			delete(p.dirtyFlags, key)
			p.mu.Unlock()
		}
	}

	if successCount > 0 {
		logx.Infof("Batch write completed: %d success, %d failed", successCount, len(failedKeys))
	}

	// 重新加入失败的任务（指数退避重试）
	for _, key := range failedKeys {
		if position, exists := batch[key]; exists {
			// 延迟重试
			go func(k string, pos Position) {
				time.Sleep(5 * time.Second)
				select {
				case p.writeQueue <- &writeRequest{key: k, position: pos}:
				case <-p.stopChan:
				}
			}(key, position)
		}
	}
}

// 单个文件写入（带重试）
func (p *PositionManager) writeToFile(key string, position Position) error {
	data, err := json.MarshalIndent(position, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal error: %v", err)
	}

	filePath := filepath.Join(p.storagePath, key+".json")
	tmpFilePath := filepath.Join(p.storagePath, key+".tmp")

	// 重试机制
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			time.Sleep(time.Duration(i) * time.Second) // 指数退避
		}

		if err := os.WriteFile(tmpFilePath, data, 0644); err != nil {
			if i == maxRetries-1 {
				return fmt.Errorf("write error after %d retries: %v", maxRetries, err)
			}
			continue
		}

		if err := os.Rename(tmpFilePath, filePath); err != nil {
			os.Remove(tmpFilePath)
			if i == maxRetries-1 {
				return fmt.Errorf("rename error after %d retries: %v", maxRetries, err)
			}
			continue
		}

		return nil // 成功
	}

	return fmt.Errorf("unexpected error in writeToFile")
}

// 定期全量同步（确保数据安全）
func (p *PositionManager) periodicFullSync(interval time.Duration) {
	defer p.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopChan:
			// 退出时同步所有脏数据
			p.syncAllDirtyPositions()
			return
		case <-ticker.C:
			p.syncAllDirtyPositions()
		}
	}
}

// 同步所有脏数据
func (p *PositionManager) syncAllDirtyPositions() {
	p.mu.RLock()
	dirtyKeys := make([]string, 0, len(p.dirtyFlags))
	for key := range p.dirtyFlags {
		dirtyKeys = append(dirtyKeys, key)
	}
	p.mu.RUnlock()

	if len(dirtyKeys) == 0 {
		return
	}

	for _, key := range dirtyKeys {
		p.mu.RLock()
		position, exists := p.positions[key]
		p.mu.RUnlock()

		if exists {
			select {
			case p.writeQueue <- &writeRequest{key: key, position: position}:
			case <-p.stopChan:
				return
			}
		}
	}

	logx.Infof("Periodic sync: %d dirty positions queued", len(dirtyKeys))
}

// 保存位置信息（异步）
func (p *PositionManager) SavePosition(service, host string, port int, database, binlogFile string, binlogPos uint32, gtid string) error {
	key := p.generateKey(service, host, port, database)
	position := Position{
		BinlogFile:     binlogFile,
		BinlogPosition: binlogPos,
		Gtid:           gtid,
		Timestamp:      time.Now().Unix(),
	}

	// 更新内存缓存
	p.mu.Lock()
	p.positions[key] = position
	p.dirtyFlags[key] = true // 标记为脏数据
	p.mu.Unlock()

	// 异步写入队列
	select {
	case p.writeQueue <- &writeRequest{key: key, position: position}:
		// 成功加入队列
	case <-p.stopChan:
		return fmt.Errorf("position manager is stopped")
	default:
		// 队列已满，只记录警告，数据仍在内存中
		logx.Infof("Write queue full for %s, position saved in memory only", key)
	}

	logx.Debugf("Position queued for %s: %s:%d", key, binlogFile, binlogPos)
	return nil
}

// 加载位置信息
func (p *PositionManager) LoadPosition(service, host string, port int, database string) (Position, bool, error) {
	key := p.generateKey(service, host, port, database)

	// 优先从内存读取
	p.mu.RLock()
	if pos, exists := p.positions[key]; exists {
		p.mu.RUnlock()
		return pos, true, nil
	}
	p.mu.RUnlock()

	// 从文件加载（仅当内存中没有时）
	filePath := filepath.Join(p.storagePath, key+".json")
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return Position{}, false, nil
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return Position{}, false, nil // 文件读取失败不影响
	}

	var position Position
	if err := json.Unmarshal(data, &position); err != nil {
		return Position{}, false, nil // 数据解析失败不影响
	}

	// 更新到内存缓存（但不标记为脏数据）
	p.mu.Lock()
	p.positions[key] = position
	p.mu.Unlock()

	logx.Infof("Loaded position for %s: %s:%d", key, position.BinlogFile, position.BinlogPosition)
	return position, true, nil
}

// 关闭位置管理器
func (p *PositionManager) Close() error {
	close(p.stopChan)
	p.wg.Wait()
	close(p.writeQueue)
	logx.Info("PositionManager closed gracefully")
	return nil
}

// 生成唯一键（保持不变）
func (p *PositionManager) generateKey(service, host string, port int, database string) string {
	return fmt.Sprintf("%s_%s_%d_%s", service, host, port, database)
}
