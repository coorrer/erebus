package svc

import (
	"github.com/coorrer/erebus/internal/config"
	"github.com/coorrer/erebus/internal/logic"
)

type ServiceContext struct {
	Config      config.Config
	TaskManager *logic.TaskManager
}

func NewServiceContext(c config.Config) *ServiceContext {
	taskManager, err := logic.NewTaskManager(c)
	if err != nil {
		panic(err)
	}

	return &ServiceContext{
		Config:      c,
		TaskManager: taskManager,
	}
}
