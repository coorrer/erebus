package logic

import (
	"context"
	"fmt"
	"github.com/coorrer/erebus/internal/logic/mysql2clickhouse"
	"github.com/coorrer/erebus/internal/logic/mysql2elasticsearch"
	"github.com/coorrer/erebus/internal/logic/mysql2kafka"
	"github.com/coorrer/erebus/internal/logic/mysql2meilisearch"

	"github.com/zeromicro/go-zero/core/service"

	"github.com/coorrer/erebus/internal/config"
)

// SyncTaskFactory 同步任务接口
type SyncTaskFactory interface {
	service.Service
	Type() string
	Name() string
}

func NewSyncTaskFactory(ctx context.Context, cfg config.Config, taskConfig config.SyncTask, source config.DataSource, target config.DataSource) (SyncTaskFactory, error) {
	switch taskConfig.Type {
	case config.SyncTaskMysql2ClickHouse:
		return mysql2clickhouse.NewSyncer(ctx, cfg, taskConfig, source, target)
	case config.SyncTaskMysql2MeiliSearch:
		return mysql2meilisearch.NewSyncer(ctx, cfg, taskConfig, source, target)
	case config.SyncTaskMysql2Elasticsearch:
		return mysql2elasticsearch.NewSyncer(ctx, cfg, taskConfig, source, target)
	case config.SyncTaskMysql2Kafka:
		return mysql2kafka.NewSyncer(ctx, cfg, taskConfig, source, target)
	default:
		return nil, fmt.Errorf("未找到适配器%v", taskConfig.Type)
	}
	return nil, nil
}
