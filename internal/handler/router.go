package handler

import (
	"github.com/coorrer/erebus/internal/svc"
	"github.com/zeromicro/go-zero/core/service"
)

func RegisterService(group *service.ServiceGroup, svcCtx *svc.ServiceContext) {
	// 注册任务管理器服务
	group.Add(svcCtx.TaskManager)
}
