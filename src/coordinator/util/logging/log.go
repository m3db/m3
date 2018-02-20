package logging

import (
	"context"
	"os"

	"github.com/pborman/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type loggerKeyType int

const (
	loggerKey loggerKeyType = iota
	rqIDKey

	undefinedID = "undefined"
)

var logger *zap.Logger

// InitWithCores is used to set up a new logger
func InitWithCores(cores []zapcore.Core) {

	consoleEncoder := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())

	highPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl >= zapcore.ErrorLevel
	})
	lowPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
		return lvl < zapcore.ErrorLevel
	})

	consoleErrors := zapcore.Lock(os.Stderr)
	consoleDebugging := zapcore.Lock(os.Stdout)

	if cores == nil {
		cores = make([]zapcore.Core, 0, 2)
	}
	cores = append(cores, zapcore.NewCore(consoleEncoder, consoleErrors, highPriority))
	cores = append(cores, zapcore.NewCore(consoleEncoder, consoleDebugging, lowPriority))

	core := zapcore.NewTee(cores...)

	logger = zap.New(core)
	defer logger.Sync()
	logger.Info("constructed a logger")
}

// NewContext returns a context has a zap logger with the extra fields added
func NewContext(ctx context.Context, fields ...zapcore.Field) context.Context {
	return context.WithValue(ctx, loggerKey, WithContext(ctx).With(fields...))
}

// NewContextWithGeneratedID returns a context with a generated id with a zap logger and an id field
func NewContextWithGeneratedID(ctx context.Context) context.Context {
	// Attach a rqID with all logs so that its simple to trace the whole call stack
	rqID := uuid.NewRandom()
	return NewContextWithID(ctx, rqID.String())
}

// NewContextWithID returns a context which has a zap logger and an id field
func NewContextWithID(ctx context.Context, id string) context.Context {
	ctxWithID := context.WithValue(ctx, rqIDKey, id)
	return context.WithValue(ctxWithID, loggerKey, WithContext(ctx).With(zap.String("rqID", id)))
}

// ReadContextID returns the context's id or "undefined"
func ReadContextID(ctx context.Context) string {
	if ctxID, ok := ctx.Value(rqIDKey).(string); ok {
		return ctxID
	}
	return undefinedID
}

// WithContext returns a zap logger with as much context as possible
func WithContext(ctx context.Context) *zap.Logger {
	if ctx == nil {
		return logger
	}
	if ctxLogger, ok := ctx.Value(loggerKey).(*zap.Logger); ok {
		return ctxLogger
	}

	return logger
}
