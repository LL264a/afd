package logger

import (
	"io"
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	logMu          sync.Mutex
	Log            *zap.SugaredLogger
	closer         io.Closer
	currentLogFile string
)

// isTerminal reports whether f is connected to a terminal.
// 非 TTY 环境（如 systemd/journald、日志文件、管道）下不应使用 ANSI 颜色码，
// 否则日志中会出现乱码转义序列。
func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

func Init(level string, logFile string) error {
	logMu.Lock()
	defer logMu.Unlock()

	var lvl zapcore.Level
	switch level {
	case "debug":
		lvl = zapcore.DebugLevel
	case "info":
		lvl = zapcore.InfoLevel
	case "warn":
		lvl = zapcore.WarnLevel
	case "error":
		lvl = zapcore.ErrorLevel
	default:
		lvl = zapcore.InfoLevel
	}

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "time"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	if isTerminal(os.Stderr) {
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	}

	var core zapcore.Core
	var fileWriter io.Closer
	if logFile != "" {
		// 日志轮转：使用 lumberjack 按大小/数量/天数滚动，避免日志文件无限增长。
		// 文件权限由 lumberjack 在创建时设置为 0600（与其他用户隔离），保留
		// 原有的安全语义：防止同主机其他用户读取可能包含敏感请求 URL、
		// cookie 或 token 的调试日志。
		lj := &lumberjack.Logger{
			Filename:   logFile,
			MaxSize:    100, // MB
			MaxBackups: 3,
			MaxAge:     30, // days
			Compress:   true,
		}
		fileWriter = lj
		fileEncoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
		consoleEncoder := zapcore.NewConsoleEncoder(encoderConfig)

		fileCore := zapcore.NewCore(fileEncoder, zapcore.AddSync(lj), lvl)
		consoleCore := zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stderr), lvl)
		core = zapcore.NewTee(fileCore, consoleCore)
	} else {
		core = zapcore.NewCore(
			zapcore.NewConsoleEncoder(encoderConfig),
			zapcore.AddSync(os.Stderr),
			lvl,
		)
	}

	// 先创建新 logger，成功后再关闭旧的，避免新 logger 创建失败时旧 logger 已被关闭
	newLogger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel)).Sugar()

	// Drain and close any previous logger so a hot-reload of log level
	// (or log file path) does not leak the previous file descriptor.
	if Log != nil {
		_ = Log.Sync()
	}
	if closer != nil {
		_ = closer.Close()
		closer = nil
	}

	Log = newLogger
	closer = fileWriter
	currentLogFile = logFile
	return nil
}

// SetLevel 热更新日志级别，保留当前日志文件路径。
// 用于 SIGHUP 等热重载场景，无需重启进程即可调整日志级别。
func SetLevel(level string) error {
	logMu.Lock()
	prevLogFile := currentLogFile
	logMu.Unlock()
	return Init(level, prevLogFile)
}

func Sync() {
	logMu.Lock()
	defer logMu.Unlock()
	if Log != nil {
		_ = Log.Sync()
	}
}

func Close() {
	logMu.Lock()
	defer logMu.Unlock()
	if Log != nil {
		_ = Log.Sync()
	}
	if closer != nil {
		_ = closer.Close()
		closer = nil
	}
}
