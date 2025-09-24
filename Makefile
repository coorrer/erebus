# 主Makefile - 只实现简单的系统判断，包含文件

# 项目名称
SERVICE_NAME := erebus

# 动态版本信息
VERSION := $(shell git describe --tags --abbrev=0 2>/dev/null || echo "dev")
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
GO_VERSION := $(shell go version | cut -d' ' -f3)

# Go 配置
GO := go
GO_BUILD := $(GO) build
GO_VENDOR := -mod vendor
GO_TEST := $(GO) test
GO_LDFLAGS := -w -s \
    -X main.version=$(VERSION) \
    -X main.commit=$(COMMIT) \
    -X main.buildTime=$(BUILD_TIME) \
    -X main.goVersion=$(GO_VERSION)
CGO_ENABLED ?= 0

# 构建目录
DIST_DIR := dist
BINARY_NAME := $(SERVICE_NAME)

# 检测操作系统
ifeq ($(OS),Windows_NT)
    # Windows系统
    include Makefile.win
else
    # 检测是否为macOS
    UNAME_S := $(shell uname -s)
    ifeq ($(UNAME_S),Darwin)
        # macOS系统
        include Makefile.mac
    else
        # 默认视为Linux系统
        include Makefile.linux
    endif
endif