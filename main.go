package main

import (
	"flag"
	"fmt"
	"github.com/coorrer/erebus/internal/handler"
	"github.com/coorrer/erebus/internal/svc"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"

	"github.com/coorrer/erebus/internal/config"
)

// 版本信息变量（编译时通过 ldflags 动态注入）
var (
	version   = "dev"     // 编译时被覆盖
	commit    = "unknown" // 编译时被覆盖
	buildTime = "unknown" // 编译时被覆盖
	goVersion = "unknown" // 编译时被覆盖
)

var (
	configFile  = flag.String("f", "etc/erebus.yaml", "Specify the config file")
	showVersion = flag.Bool("v", false, "Show version information")
	showVerbose = flag.Bool("vv", false, "Show verbose version information")
)

// 获取运行时 Go 版本（真正的动态获取）
func getRuntimeGoVersion() string {
	return runtime.Version()
}

// 显示版本信息
func printVersion() {
	fmt.Printf("Erebus - Service Framework\n")
	fmt.Printf("Version:    %s\n", version)
	fmt.Printf("Commit:     %s\n", commit)
	fmt.Printf("Build Time: %s\n", buildTime)
	fmt.Printf("Go Runtime: %s\n", getRuntimeGoVersion())
}

// 显示详细版本信息（包含编译时和运行时信息）
func printVerboseVersion() {
	fmt.Printf("=== Erebus Build Information ===\n")
	fmt.Printf("Application:\n")
	fmt.Printf("  Version:    %s\n", version)
	fmt.Printf("  Commit:     %s\n", commit)
	fmt.Printf("  Build Time: %s\n", buildTime)

	fmt.Printf("\nBuild Environment:\n")
	fmt.Printf("  Go Version (build): %s\n", goVersion)
	fmt.Printf("  Go Version (runtime): %s\n", getRuntimeGoVersion())
	fmt.Printf("  OS/Arch:    %s/%s\n", runtime.GOOS, runtime.GOARCH)

	fmt.Printf("\nRuntime Information:\n")
	fmt.Printf("  Goroutines: %d\n", runtime.NumGoroutine())
	fmt.Printf("  CPUs:       %d\n", runtime.NumCPU())
}

func main() {
	flag.Parse()

	// 处理版本信息显示
	if *showVerbose {
		printVerboseVersion()
		os.Exit(0)
	}

	if *showVersion {
		printVersion()
		os.Exit(0)
	}

	// 加载配置
	var c config.Config
	conf.MustLoad(*configFile, &c)

	// 设置日志
	logx.MustSetup(c.Log)
	defer logx.Close()

	// 启动时记录版本信息
	logx.Infof("Starting Erebus version: %s, commit: %s", version, commit)
	logx.Infof("Build time: %s, Go version: %s (runtime: %s)",
		buildTime, goVersion, getRuntimeGoVersion())

	svcCtx := svc.NewServiceContext(c)

	// 创建服务组
	group := service.NewServiceGroup()
	defer group.Stop()

	handler.RegisterService(group, svcCtx)

	// 设置信号处理
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 启动服务
	go group.Start()

	// 等待终止信号
	sig := <-sigChan
	logx.Infof("Received signal %s, shutting down...", sig)

	// 优雅停止
	group.Stop()
	logx.Info("Erebus stopped successfully")
}
