package logger

import (
	"io"
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	logMu  sync.Mutex
	Log    *zap.SugaredLogger
	closer io.Closer
)

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
	encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder

	var core zapcore.Core
	var fileWriter io.Closer
	if logFile != "" {
		// Mode 0600 (not 0644) so other users on the same host cannot
		// read potentially sensitive request URLs, cookies, or tokens
		// that the logger may surface at debug level.
		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return err
		}
		fileWriter = file
		fileEncoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
		consoleEncoder := zapcore.NewConsoleEncoder(encoderConfig)

		fileCore := zapcore.NewCore(fileEncoder, zapcore.AddSync(file), lvl)
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
	return nil
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
