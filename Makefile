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

# 系统安装配置
PREFIX ?= /usr/local
BIN_DIR ?= $(PREFIX)/bin
CONF_DIR ?= /etc/$(SERVICE_NAME)
LOG_DIR ?= /var/log/$(SERVICE_NAME)
SERVICE_DIR ?= /etc/systemd/system
SERVICE_USER ?= root
SERVICE_GROUP ?= root

# 模板和配置文件
SERVICE_TEMPLATE := erebus.service.template
CONFIG_FILE := etc/erebus.yaml

# 默认目标
.PHONY: all
all: build

# 本地开发构建
.PHONY: build
build:
	@echo "Building $(SERVICE_NAME) v$(VERSION) for $(shell go env GOOS)/$(shell go env GOARCH)..."
	CGO_ENABLED=$(CGO_ENABLED) $(GO_BUILD) -ldflags="$(GO_LDFLAGS)" -o $(BINARY_NAME) .

# 安装到 GOPATH/bin
.PHONY: install
install: build
	@echo "Installing to $(shell go env GOPATH)/bin..."
	cp $(BINARY_NAME) $(shell go env GOPATH)/bin/$(BINARY_NAME)

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
	rm -f $(BINARY_NAME)
	rm -rf $(DIST_DIR)
	rm -f *.log coverage.out coverage.html
	rm -f $(SERVICE_NAME).service

# 多平台交叉编译
.PHONY: cross-build
cross-build:
	@echo "Building for multiple platforms..."
	@mkdir -p $(DIST_DIR)

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

# 生成校验和
.PHONY: checksum
checksum: cross-build
	@echo "Generating checksums..."
	cd $(DIST_DIR) && shasum -a 256 * > $(BINARY_NAME)-$(VERSION)-checksums.txt
	@echo "Checksums generated: $(DIST_DIR)/$(BINARY_NAME)-$(VERSION)-checksums.txt"

# 生成 systemd 服务文件
.PHONY: generate-service
generate-service:
	@echo "Generating systemd service file from template..."
	@if [ ! -f $(SERVICE_TEMPLATE) ]; then \
		echo "Error: Template file $(SERVICE_TEMPLATE) not found."; \
		echo "Creating a basic template..."; \
		echo '[Unit]' > $(SERVICE_TEMPLATE); \
		echo 'Description=Erebus Service' >> $(SERVICE_TEMPLATE); \
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

# 系统安装（包含服务安装）
.PHONY: system-install
system-install: build generate-service
	@echo "Installing $(SERVICE_NAME) to system directories..."

	# 检查权限
	if [ "$(shell id -u)" -ne 0 ]; then \
		echo "Error: system installation requires root privileges"; \
		echo "Please run: sudo make system-install"; \
		exit 1; \
	fi

	# 检查服务文件是否生成
	if [ ! -f $(SERVICE_NAME).service ]; then \
		echo "Error: Service file $(SERVICE_NAME).service not found"; \
		exit 1; \
	fi

	# 创建系统用户和组
	@if ! getent group $(SERVICE_GROUP) >/dev/null 2>&1; then \
		groupadd --system $(SERVICE_GROUP); \
		echo "Created group $(SERVICE_GROUP)"; \
	fi
	@if ! id -u $(SERVICE_USER) >/dev/null 2>&1; then \
		useradd --system --no-create-home --shell /bin/false \
				-g $(SERVICE_GROUP) $(SERVICE_USER); \
		echo "Created user $(SERVICE_USER)"; \
	fi

	# 创建目录
	mkdir -p $(BIN_DIR)
	mkdir -p $(CONF_DIR)
	mkdir -p $(LOG_DIR)
	mkdir -p $(SERVICE_DIR)

	# 安装二进制文件
	install -m 755 $(BINARY_NAME) $(BIN_DIR)/$(BINARY_NAME)

	# 安装配置文件（如果不存在则安装，存在则跳过）
	if [ -f $(CONFIG_FILE) ]; then \
		if [ -f $(CONF_DIR)/erebus.yaml ]; then \
			echo "Config file $(CONF_DIR)/erebus.yaml already exists, skipping installation to preserve existing configuration."; \
			echo "If you want to update the config, run: sudo make update-config"; \
		else \
			install -m 640 $(CONFIG_FILE) $(CONF_DIR)/erebus.yaml; \
			echo "Config file installed to $(CONF_DIR)/erebus.yaml"; \
		fi; \
	else \
		echo "Warning: Source config file $(CONFIG_FILE) not found"; \
		# 如果目标配置文件也不存在，创建空文件 \
		if [ ! -f $(CONF_DIR)/erebus.yaml ]; then \
			touch $(CONF_DIR)/erebus.yaml; \
			chmod 640 $(CONF_DIR)/erebus.yaml; \
			echo "Created empty config file at $(CONF_DIR)/erebus.yaml"; \
		fi; \
	fi

	# 安装服务文件（总是覆盖，因为这是系统服务配置）
	install -m 644 $(SERVICE_NAME).service $(SERVICE_DIR)/$(SERVICE_NAME).service

	# 设置文件权限
	chown $(SERVICE_USER):$(SERVICE_GROUP) $(BIN_DIR)/$(BINARY_NAME)
	chown root:$(SERVICE_GROUP) $(CONF_DIR)/erebus.yaml
	chown $(SERVICE_USER):$(SERVICE_GROUP) $(LOG_DIR)
	chmod 755 $(LOG_DIR)

	# 重新加载systemd并启用服务
	systemctl daemon-reload
	systemctl enable $(SERVICE_NAME).service

	# 清理临时文件
	rm -f $(SERVICE_NAME).service

	@echo ""
	@echo "=== Installation Complete ==="
	@echo "Binary:        $(BIN_DIR)/$(BINARY_NAME)"
	@if [ -f $(CONF_DIR)/erebus.yaml ]; then \
		if [ -f $(CONFIG_FILE) ] && cmp -s $(CONF_DIR)/erebus.yaml $(CONFIG_FILE); then \
			echo "Config:        $(CONF_DIR)/erebus.yaml - NEWLY INSTALLED"; \
		else \
			echo "Config:        $(CONF_DIR)/erebus.yaml - EXISTING PRESERVED"; \
		fi; \
	else \
		echo "Config:        $(CONF_DIR)/erebus.yaml - NOT INSTALLED"; \
	fi
	@echo "Service:       $(SERVICE_DIR)/$(SERVICE_NAME).service"
	@echo "Log directory: $(LOG_DIR)"
	@echo "Service user:  $(SERVICE_USER):$(SERVICE_GROUP)"
	@echo ""
	@echo "Next steps:"
	@echo "  sudo systemctl start $(SERVICE_NAME)"
	@echo "  sudo systemctl status $(SERVICE_NAME)"
	@echo "  sudo journalctl -u $(SERVICE_NAME) -f"

# 专门更新配置的目标（需要手动调用）
.PHONY: update-config
update-config:
	@echo "Updating configuration file..."

	# 检查权限
	if [ "$(shell id -u)" -ne 0 ]; then \
		echo "Error: requires root privileges"; \
		echo "Please run: sudo make update-config"; \
		exit 1; \
	fi

	# 检查源配置文件是否存在
	if [ ! -f $(CONFIG_FILE) ]; then \
		echo "Error: Source config file $(CONFIG_FILE) not found"; \
		exit 1; \
	fi

	# 备份现有配置
	if [ -f $(CONF_DIR)/erebus.yaml ]; then \
		backup_file="$(CONF_DIR)/erebus.yaml.backup.$$(date +%Y%m%d%H%M%S)"; \
		cp $(CONF_DIR)/erebus.yaml $$backup_file; \
		echo "Backed up existing config to $$backup_file"; \
	fi

	# 安装新配置
	install -m 640 $(CONFIG_FILE) $(CONF_DIR)/erebus.yaml
	chown root:$(SERVICE_GROUP) $(CONF_DIR)/erebus.yaml
	echo "Configuration updated. Old config backed up."
	echo "Please review the changes and restart the service if needed:"
	echo "  sudo systemctl restart $(SERVICE_NAME)"

# 安装示例配置（不覆盖现有配置）
.PHONY: install-example-config
install-example-config:
	@echo "Installing example configuration..."

	# 检查权限
	if [ "$(shell id -u)" -ne 0 ]; then \
		echo "Error: requires root privileges"; \
		echo "Please run: sudo make install-example-config"; \
		exit 1; \
	fi

	# 检查源配置文件是否存在
	if [ ! -f $(CONFIG_FILE) ]; then \
		echo "Error: Example config file $(CONFIG_FILE) not found"; \
		exit 1; \
	fi

	# 安装为示例文件（不覆盖现有配置）
	example_file="$(CONF_DIR)/erebus.yaml.example"
	install -m 640 $(CONFIG_FILE) $$example_file
	chown root:$(SERVICE_GROUP) $$example_file
	echo "Example config installed to $$example_file"
	echo "You can compare it with your current config:"
	echo "  diff -u $(CONF_DIR)/erebus.yaml $$example_file || echo 'No existing config to compare'"

# 显示配置状态
.PHONY: config-status
config-status:
	@echo "Configuration Status:"
	@echo "  Source config:      $(CONFIG_FILE) $(if $(shell test -f $(CONFIG_FILE) && echo exists),EXISTS,NOT FOUND)"
	@echo "  Installed config:   $(CONF_DIR)/erebus.yaml $(if $(shell test -f $(CONF_DIR)/erebus.yaml && echo exists),EXISTS,NOT FOUND)"
	@if [ -f $(CONF_DIR)/erebus.yaml ] && [ -f $(CONFIG_FILE) ]; then \
		if cmp -s $(CONF_DIR)/erebus.yaml $(CONFIG_FILE); then \
			echo "  Status:             Config files are identical"; \
		else \
			echo "  Status:             Config files differ"; \
			echo "  Last modified:      $$(stat -c %y $(CONF_DIR)/erebus.yaml 2>/dev/null || echo 'Unknown')"; \
		fi; \
	elif [ -f $(CONF_DIR)/erebus.yaml ]; then \
		echo "  Status:             Using existing configuration"; \
	else \
		echo "  Status:             No configuration installed"; \
	fi

# 服务管理命令
.PHONY: start
start:
	systemctl start $(SERVICE_NAME).service
	@echo "Service started. Check status with: systemctl status $(SERVICE_NAME)"

.PHONY: stop
stop:
	systemctl stop $(SERVICE_NAME).service
	@echo "Service stopped"

.PHONY: restart
restart:
	systemctl restart $(SERVICE_NAME).service
	@echo "Service restarted"

.PHONY: status
status:
	systemctl status $(SERVICE_NAME).service

.PHONY: logs
logs:
	journalctl -u $(SERVICE_NAME).service -f

.PHONY: enable
enable:
	systemctl enable $(SERVICE_NAME).service
	@echo "Service enabled to start on boot"

.PHONY: disable
disable:
	systemctl disable $(SERVICE_NAME).service
	@echo "Service disabled from starting on boot"

# 系统卸载
.PHONY: system-uninstall
system-uninstall:
	@echo "Uninstalling $(SERVICE_NAME)..."

	# 检查权限
	if [ "$(shell id -u)" -ne 0 ]; then \
		echo "Error: uninstallation requires root privileges"; \
		echo "Please run: sudo make system-uninstall"; \
		exit 1; \
	fi

	# 停止并禁用服务
	systemctl stop $(SERVICE_NAME).service 2>/dev/null || true
	systemctl disable $(SERVICE_NAME).service 2>/dev/null || true

	# 删除服务文件
	rm -f $(SERVICE_DIR)/$(SERVICE_NAME).service

	# 删除二进制文件
	rm -f $(BIN_DIR)/$(BINARY_NAME)

	# 重新加载systemd
	systemctl daemon-reload

	@echo "Uninstallation complete. Note: Config files in $(CONF_DIR) and logs in $(LOG_DIR) were preserved."

# 完全清理（包括配置和日志）
.PHONY: system-purge
system-purge: system-uninstall
	@echo "Purging all $(SERVICE_NAME) files..."
	rm -rf $(CONF_DIR)
	rm -rf $(LOG_DIR)
	@echo "Purge complete."

# GoReleaser 相关命令
.PHONY: release-dry-run
release-dry-run:
	@echo "Running GoReleaser in dry-run mode..."
	goreleaser release --clean --snapshot --skip=publish

.PHONY: release
release:
	@echo "Creating release..."
	goreleaser release --clean

.PHONY: snapshot
snapshot:
	@echo "Creating snapshot release..."
	goreleaser release --clean --snapshot

# 显示版本信息
.PHONY: version
version:
	@echo "Version:    $(VERSION)"
	@echo "Commit:     $(COMMIT)"
	@echo "Build Time: $(BUILD_TIME)"
	@echo "Go Version: $(GO_VERSION)"

# 显示帮助信息
.PHONY: help
help:
	@echo "Makefile for $(SERVICE_NAME) v$(VERSION)"
	@echo ""
	@echo "Usage:"
	@echo "  make                    - 编译项目（当前平台）"
	@echo "  make build              - 编译项目"
	@echo "  make install            - 安装到 GOPATH/bin"
	@echo "  make test               - 运行测试"
	@echo "  make coverage           - 生成测试覆盖率报告"
	@echo "  make clean              - 清理构建文件"
	@echo "  make cross-build        - 交叉编译多平台版本"
	@echo "  make checksum           - 生成校验和文件"
	@echo "  make version            - 显示版本信息"
	@echo ""
	@echo "System Service:"
	@echo "  make generate-service        - 生成 systemd 服务文件"
	@echo "  make system-install          - 安装到系统目录（需要root）"
	@echo "  make system-uninstall        - 卸载系统服务（保留配置）"
	@echo "  make system-purge            - 完全删除（包括配置）"
	@echo "  make update-config           - 更新配置文件（备份旧配置）"
	@echo "  make install-example-config  - 安装示例配置"
	@echo "  make config-status           - 显示配置状态"
	@echo ""
	@echo "Service Management (需要root):"
	@echo "  make start             - 启动服务"
	@echo "  make stop              - 停止服务"
	@echo "  make restart           - 重启服务"
	@echo "  make status            - 查看服务状态"
	@echo "  make logs              - 查看服务日志"
	@echo "  make enable            - 启用开机自启"
	@echo "  make disable           - 禁用开机自启"
	@echo ""
	@echo "Release:"
	@echo "  make release-dry-run   - 测试发布流程"
	@echo "  make snapshot          - 创建快照版本"
	@echo "  make release           - 创建正式发布"
	@echo ""
	@echo "Variables:"
	@echo "  VERSION=$(VERSION)"
	@echo "  COMMIT=$(COMMIT)"
	@echo "  BUILD_TIME=$(BUILD_TIME)"