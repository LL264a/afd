package logger

import (
	"io"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	Log    *zap.SugaredLogger
	closer io.Closer
)

func Init(level string, logFile string) error {
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
		consoleCore := zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), lvl)
		core = zapcore.NewTee(fileCore, consoleCore)
	} else {
		core = zapcore.NewCore(
			zapcore.NewConsoleEncoder(encoderConfig),
			zapcore.AddSync(os.Stdout),
			lvl,
		)
	}

	// Drain and close any previous logger so a hot-reload of log level
	// (or log file path) does not leak the previous file descriptor.
	if Log != nil {
		_ = Log.Sync()
	}
	if closer != nil {
		_ = closer.Close()
		closer = nil
	}

	Log = zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel)).Sugar()
	closer = fileWriter
	return nil
}

func Sync() {
	if Log != nil {
		_ = Log.Sync()
	}
}

func Close() {
	if Log != nil {
		_ = Log.Sync()
	}
	if closer != nil {
		_ = closer.Close()
		closer = nil
	}
}
