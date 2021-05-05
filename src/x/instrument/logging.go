// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package instrument

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type key int

var loggerKey key

// NewTestDebugLogger returns a new debug logger for tests.
func NewTestDebugLogger(t *testing.T) *zap.Logger {
	loggerConfig := zap.NewDevelopmentConfig()
	loggerConfig.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	logger, err := loggerConfig.Build()
	require.NoError(t, err)
	return logger
}

// NewTestOptions returns a set of instrument options useful for
// tests. This includes:
// - Logger built using development config with debug level set.
func NewTestOptions(t *testing.T) Options {
	logger := NewTestDebugLogger(t)
	return NewOptions().SetLogger(logger)
}

// NewContextFromLogger returns a new context with the logger added as a value.
// This allows adding request scoped fields to a logger so application code can use the request scoped logger with
// LoggerFromContext.
func NewContextFromLogger(ctx context.Context, l *zap.Logger) context.Context {
	if l == nil {
		return ctx
	}
	return context.WithValue(ctx, loggerKey, l)
}

// LoggerFromContext returns the request scoped logger from the context, or nil if not found.
func LoggerFromContext(ctx context.Context) *zap.Logger {
	l, ok := ctx.Value(loggerKey).(*zap.Logger)
	if !ok {
		return nil
	}
	return l
}
