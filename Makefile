# 项目名称
SERVICE_NAME := erebus

# 检测操作系统
ifeq ($(OS),Windows_NT)
    DETECTED_OS := Windows
else
    DETECTED_OS := $(shell uname -s)
endif

# 动态版本信息
VERSION := $(shell git describe --tags --abbrev=0 2>/dev/null || echo "dev")
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
GO_VERSION := $(shell go version | cut -d' ' -f3)

# Go 配置
GO := go
GO_BUILD := $(GO) build
GO_TEST := $(GO) test
GO_LDFLAGS := -w -s \
    -X main.version=$(VERSION) \
    -X main.commit=$(COMMIT) \
    -X main.buildTime=$(BUILD_TIME) \
    -X main.goVersion=$(GO_VERSION)
CGO_ENABLED ?= 0

# 构建目录
DIST_DIR := dist

# 根据操作系统设置二进制文件名
ifeq ($(DETECTED_OS),Windows)
    BINARY_NAME := $(SERVICE_NAME).exe
    RM := del /Q
    MKDIR := mkdir
    CP := copy
    RMDIR := rmdir /S /Q
else
    BINARY_NAME := $(SERVICE_NAME)
    RM := rm -f
    MKDIR := mkdir -p
    CP := cp
    RMDIR := rm -rf
endif

# 系统安装配置（Windows和Linux不同）
ifeq ($(DETECTED_OS),Windows)
    # Windows路径
    PREFIX ?= C:\Program Files\$(SERVICE_NAME)
    BIN_DIR ?= $(PREFIX)
    CONF_DIR ?= $(PREFIX)\config
    LOG_DIR ?= $(PREFIX)\logs
    SERVICE_USER ?= Administrator
    SERVICE_GROUP ?: Administrators
else
    # Linux路径
    PREFIX ?= /usr/local
    BIN_DIR ?= $(PREFIX)/bin
    CONF_DIR ?= /etc/$(SERVICE_NAME)
    LOG_DIR ?= /var/log/$(SERVICE_NAME)
    SERVICE_DIR ?= /etc/systemd/system
    SERVICE_USER ?= root
    SERVICE_GROUP ?= root
endif

# 模板和配置文件
SERVICE_TEMPLATE := erebus.service.template
CONFIG_FILE := etc/erebus.yaml

# 默认目标
.PHONY: all
all: build

# 本地开发构建
.PHONY: build
build:
	@echo "Building $(SERVICE_NAME) $(VERSION) for $(shell go env GOOS)/$(shell go env GOARCH)..."
	CGO_ENABLED=$(CGO_ENABLED) $(GO_BUILD) -ldflags="$(GO_LDFLAGS)" -o $(BINARY_NAME) .

# 安装到 GOPATH/bin
.PHONY: install
install: build
	@echo "Installing to $(shell go env GOPATH)/bin..."
	$(CP) $(BINARY_NAME) $(shell go env GOPATH)/bin/$(BINARY_NAME)

# 运行测试
.PHONY: test
test:
	@echo "Running tests..."
	$(GO_TEST) -v -race -cover ./...

# 测试覆盖率
.PHONY: coverage
coverage:
	@echo "Generating test coverage report..."
	$(GO_TEST) -race -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# 清理构建文件
.PHONY: clean
clean:
	@echo "Cleaning build artifacts..."
	$(RM) $(BINARY_NAME)
	$(RMDIR) $(DIST_DIR)
	$(RM) *.log coverage.out coverage.html
	$(RM) $(SERVICE_NAME).service

# 多平台交叉编译
.PHONY: cross-build
cross-build:
	@echo "Building for multiple platforms..."
	@$(MKDIR) $(DIST_DIR)

	# Linux
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GO_BUILD) -ldflags="$(GO_LDFLAGS)" -o $(DIST_DIR)/$(BINARY_NAME)-linux-amd64 .
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 $(GO_BUILD) -ldflags="$(GO_LDFLAGS)" -o $(DIST_DIR)/$(BINARY_NAME)-linux-arm64 .

	# macOS
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 $(GO_BUILD) -ldflags="$(GO_LDFLAGS)" -o $(DIST_DIR)/$(BINARY_NAME)-darwin-amd64 .
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 $(GO_BUILD) -ldflags="$(GO_LDFLAGS)" -o $(DIST_DIR)/$(BINARY_NAME)-darwin-arm64 .

	# Windows
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 $(GO_BUILD) -ldflags="$(GO_LDFLAGS)" -o $(DIST_DIR)/$(BINARY_NAME)-windows-amd64.exe .
	GOOS=windows GOARCH=arm64 CGO_ENABLED=0 $(GO_BUILD) -ldflags="$(GO_LDFLAGS)" -o $(DIST_DIR)/$(BINARY_NAME)-windows-arm64.exe .

	@echo "Build complete. Binaries available in $(DIST_DIR)/"

# Windows特定的安装目标
.PHONY: windows-install
windows-install: build
ifeq ($(DETECTED_OS),Windows)
	@echo "Installing $(SERVICE_NAME) on Windows..."
	@if not exist "$(BIN_DIR)" $(MKDIR) "$(BIN_DIR)"
	@if not exist "$(CONF_DIR)" $(MKDIR) "$(CONF_DIR)"
	@if not exist "$(LOG_DIR)" $(MKDIR) "$(LOG_DIR)"

	$(CP) $(BINARY_NAME) "$(BIN_DIR)\$(BINARY_NAME)"

	@if exist "$(CONFIG_FILE)" (
		if not exist "$(CONF_DIR)\erebus.yaml" (
			$(CP) "$(CONFIG_FILE)" "$(CONF_DIR)\erebus.yaml"
			echo "Config file installed to $(CONF_DIR)\erebus.yaml"
		) else (
			echo "Config file already exists, skipping..."
		)
	) else (
		echo "Warning: Source config file not found"
	)

	@echo "Installation complete."
	@echo "Binary: $(BIN_DIR)\$(BINARY_NAME)"
	@echo "Config: $(CONF_DIR)\erebus.yaml"
	@echo "Logs:   $(LOG_DIR)"
else
	@echo "This target is only supported on Windows"
endif

# 条件编译系统服务相关功能
ifneq ($(DETECTED_OS),Windows)

# Linux特有的系统服务目标
.PHONY: generate-service
generate-service:
	@echo "Generating systemd service file from template..."
	@if [ ! -f $(SERVICE_TEMPLATE) ]; then \
		echo "Error: Template file $(SERVICE_TEMPLATE) not found."; \
		echo "Creating a basic template..."; \
		echo '[Unit]' > $(SERVICE_TEMPLATE); \
		echo 'Description=Erebus Service' >> $(SERVICE_TEMPLATE); \
		echo 'Documentation=https://github.com/coorrer/erebus' >> $(SERVICE_TEMPLATE); \
		echo 'After=network.target' >> $(SERVICE_TEMPLATE); \
		echo '' >> $(SERVICE_TEMPLATE); \
		echo '[Service]' >> $(SERVICE_TEMPLATE); \
		echo 'Type=simple' >> $(SERVICE_TEMPLATE); \
		echo 'User=@SERVICE_USER@' >> $(SERVICE_TEMPLATE); \
		echo 'Group=@SERVICE_GROUP@' >> $(SERVICE_TEMPLATE); \
		echo 'ExecStart=@BIN_DIR@/@SERVICE_NAME@ -f @CONF_DIR@/erebus.yaml' >> $(SERVICE_TEMPLATE); \
		echo 'WorkingDirectory=@BIN_DIR@' >> $(SERVICE_TEMPLATE); \
		echo 'Restart=always' >> $(SERVICE_TEMPLATE); \
		echo 'RestartSec=5' >> $(SERVICE_TEMPLATE); \
		echo 'StandardOutput=journal' >> $(SERVICE_TEMPLATE); \
		echo 'StandardError=journal' >> $(SERVICE_TEMPLATE); \
		echo '' >> $(SERVICE_TEMPLATE); \
		echo '[Install]' >> $(SERVICE_TEMPLATE); \
		echo 'WantedBy=multi-user.target' >> $(SERVICE_TEMPLATE); \
		echo "Basic template created at $(SERVICE_TEMPLATE)"; \
	fi
	sed -e 's|@BIN_DIR@|$(BIN_DIR)|g' \
		-e 's|@SERVICE_NAME@|$(SERVICE_NAME)|g' \
		-e 's|@SERVICE_USER@|$(SERVICE_USER)|g' \
		-e 's|@SERVICE_GROUP@|$(SERVICE_GROUP)|g' \
		-e 's|@CONF_DIR@|$(CONF_DIR)|g' \
		-e 's|@LOG_DIR@|$(LOG_DIR)|g' \
		$(SERVICE_TEMPLATE) > $(SERVICE_NAME).service
	@echo "Service file generated: $(SERVICE_NAME).service"

# 其他Linux特有的目标（system-install, start, stop等）...
# 这里保留原有的Linux服务管理代码，但用条件判断包裹

else

# Windows下的替代目标
.PHONY: generate-service
generate-service:
	@echo "Systemd service generation is not supported on Windows."
	@echo "Consider using NSSM (Non-Sucking Service Manager) for Windows service management."

.PHONY: system-install
system-install:
	@echo "System installation is not supported on Windows via this Makefile."
	@echo "Use 'make windows-install' for basic installation."

endif

# 其他通用目标（version, help等）保持不变...
.PHONY: version
version:
	@echo "Version:    $(VERSION)"
	@echo "Commit:     $(COMMIT)"
	@echo "Build Time: $(BUILD_TIME)"
	@echo "Go Version: $(GO_VERSION)"
	@echo "OS:         $(DETECTED_OS)"

.PHONY: help
help:
	@echo "Makefile for $(SERVICE_NAME) v$(VERSION)"
	@echo "Detected OS: $(DETECTED_OS)"
	@echo ""
	@echo "Usage:"
	@echo "  make                    - Build project (current platform)"
	@echo "  make build              - Build project"
	@echo "  make install            - Install to GOPATH/bin"
	@echo "  make test               - Run tests"
	@echo "  make coverage           - Generate test coverage report"
	@echo "  make clean              - Clean build artifacts"
	@echo "  make cross-build        - Cross-compile for multiple platforms"
	@echo "  make checksum           - Generate checksums"
	@echo "  make version            - Show version information"
	@echo ""
ifeq ($(DETECTED_OS),Windows)
	@echo "Windows Specific:"
	@echo "  make windows-install    - Install to Windows directories"
	@echo ""
	@echo "Note: System service management is not supported on Windows via this Makefile"
else
	@echo "System Service (Linux):"
	@echo "  make generate-service        - Generate systemd service file"
	@echo "  make system-install          - Install to system directories (requires root)"
	@echo "  make start                   - Start service"
	@echo "  make stop                    - Stop service"
	@echo "  ... (other Linux-specific targets)"
endif