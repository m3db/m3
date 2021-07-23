// Copyright (c) 2020 Uber Technologies, Inc.
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

package limits

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/m3db/m3/src/x/instrument"
)

// NB: log queries that are over 1 GB.
const defaultLimit = 1024 * 1024 * 1024

type sourceLoggerBuilder struct{}

var _ SourceLoggerBuilder = (*sourceLoggerBuilder)(nil)

func (s *sourceLoggerBuilder) NewSourceLogger(
	name string,
	iOpts instrument.Options,
) SourceLogger {
	var (
		logger   = iOpts.Logger()
		debugLog = logger.Check(zapcore.DebugLevel, fmt.Sprint("source logger for", name))
	)

	if debugLog == nil {
		// NB: use empty logger.
		return &sourceLogger{}
	}

	return &sourceLogger{logger: logger.With(zap.String("limit_name", name))}
}

type sourceLogger struct {
	logger *zap.Logger
}

var _ SourceLogger = (*sourceLogger)(nil)

func (l *sourceLogger) LogSourceValue(val int64, source []byte) {
	if l.logger == nil || val < defaultLimit || len(source) == 0 {
		// NB: Don't log each source as it would be very noisy, even at debug level.
		return
	}

	l.logger.Debug("query from source exceeded size",
		zap.ByteString("source", source),
		zap.Int64("size", val),
		zap.Int64("limit", defaultLimit))
}
