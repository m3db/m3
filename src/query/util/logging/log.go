// Copyright (c) 2018 Uber Technologies, Inc.
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

package logging

import (
	"context"

	"github.com/pborman/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/m3db/m3/src/x/instrument"
)

type loggerKeyType int

const (
	loggerKey loggerKeyType = iota
	rqIDKey

	undefinedID = "undefined"
)

// NewContext returns a context has a zap logger with the extra fields added.
func NewContext(
	ctx context.Context,
	instrumentOpts instrument.Options,
	fields ...zapcore.Field,
) context.Context {
	return NewContextWithLogger(ctx, WithContext(ctx, instrumentOpts).With(fields...))
}

// NewContextWithLogger returns a context with the provided logger set as a context value.
func NewContextWithLogger(ctx context.Context, l *zap.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, l)
}

// NewContextWithGeneratedID returns a context with a generated id with a zap
// logger and an id field.
func NewContextWithGeneratedID(
	ctx context.Context,
	instrumentOpts instrument.Options,
) context.Context {
	// Attach a rqID with all logs so that its simple to trace the whole call stack
	rqID := uuid.NewRandom()
	return NewContextWithID(ctx, rqID.String(), instrumentOpts)
}

// NewContextWithID returns a context which has a zap logger and an id field.
func NewContextWithID(
	ctx context.Context,
	id string,
	instrumentOpts instrument.Options,
) context.Context {
	ctxWithID := context.WithValue(ctx, rqIDKey, id)
	return context.WithValue(ctxWithID, loggerKey,
		WithContext(ctx, instrumentOpts).With(zap.String("rqID", id)))
}

// ReadContextID returns the context's id or "undefined".
func ReadContextID(ctx context.Context) string {
	if ctxID, ok := ctx.Value(rqIDKey).(string); ok {
		return ctxID
	}

	return undefinedID
}

// WithContext returns a zap logger with as much context as possible.
func WithContext(ctx context.Context, instrumentOpts instrument.Options) *zap.Logger {
	if ctx == nil {
		return instrumentOpts.Logger()
	}
	if ctxLogger, ok := ctx.Value(loggerKey).(*zap.Logger); ok {
		return ctxLogger
	}

	return instrumentOpts.Logger()
}
