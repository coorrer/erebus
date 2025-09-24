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
	positions   map[string]Position
	mu          sync.RWMutex
}

func NewPositionManager(storagePath string) *PositionManager {
	if storagePath == "" {
		storagePath = "./sync_positions"
	}

	// 创建存储目录
	if err := os.MkdirAll(storagePath, 0755); err != nil {
		logx.Errorf("Failed to create position storage directory: %v", err)
	}

	return &PositionManager{
		storagePath: storagePath,
		positions:   make(map[string]Position),
	}
}

// 生成唯一的位置标识键
func (p *PositionManager) generateKey(service, host string, port int, database string) string {
	return fmt.Sprintf("%s_%s_%d_%s", service, host, port, database)
}

// 保存位置信息到文件
func (p *PositionManager) SavePosition(service, host string, port int, database, binlogFile string, binlogPos uint32, gtid string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := p.generateKey(service, host, port, database)
	position := Position{
		BinlogFile:     binlogFile,
		BinlogPosition: binlogPos,
		Gtid:           gtid,
		Timestamp:      time.Now().Unix(),
	}

	p.positions[key] = position

	// 序列化位置信息
	data, err := json.Marshal(position)
	if err != nil {
		return fmt.Errorf("failed to marshal position: %v", err)
	}

	// 写入文件
	filePath := filepath.Join(p.storagePath, key+".json")
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write position file: %v", err)
	}

	logx.Infof("Saved position for %s: %s:%d", key, binlogFile, binlogPos)
	return nil
}

// 加载位置信息
func (p *PositionManager) LoadPosition(service, host string, port int, database string) (Position, bool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	key := p.generateKey(service, host, port, database)

	// 检查内存中是否有缓存的位置
	if pos, exists := p.positions[key]; exists {
		return pos, true, nil
	}

	// 从文件加载位置信息
	filePath := filepath.Join(p.storagePath, key+".json")
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return Position{}, false, nil
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return Position{}, false, fmt.Errorf("failed to read position file: %v", err)
	}

	var position Position
	if err := json.Unmarshal(data, &position); err != nil {
		return Position{}, false, fmt.Errorf("failed to unmarshal position: %v", err)
	}

	// 缓存到内存中
	p.positions[key] = position

	logx.Infof("Loaded position for %s: %s:%d", key, position.BinlogFile, position.BinlogPosition)
	return position, true, nil
}
