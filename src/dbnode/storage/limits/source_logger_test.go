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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/m3db/m3/src/x/instrument"
)

func newTestZapHookOptions(logs *[]string) zap.Option {
	// NB: allows inspection of incoming logs by writing them to slice.
	return zap.Hooks(func(entry zapcore.Entry) error {
		*logs = append(*logs, entry.Message)
		return nil
	})
}

func TestNewSourceLoggerAboveDebugLevelDoesNotLog(t *testing.T) {
	testSourceLoggerWithLevel(t, false)
}

func TestNewSourceLoggerAtDebugLevelLogsWhenExceeded(t *testing.T) {
	testSourceLoggerWithLevel(t, true)
}

func testSourceLoggerWithLevel(t *testing.T, debugLog bool) {
	var (
		err    error
		logger *zap.Logger

		writtenLogs = []string{}
		zapHookOpts = newTestZapHookOptions(&writtenLogs)
	)

	if debugLog {
		logger, err = zap.NewDevelopment(zapHookOpts)
	} else {
		logger, err = zap.NewProduction(zapHookOpts)
	}

	require.NoError(t, err)
	var (
		iOpts        = instrument.NewOptions().SetLogger(logger)
		builder      = &sourceLoggerBuilder{}
		sourceLogger = builder.NewSourceLogger("name", iOpts)
	)

	sourceLogger.LogSourceValue(1, []byte("foo"))
	assert.Equal(t, 0, len(writtenLogs))

	sourceLogger.LogSourceValue(defaultLimit-1, []byte("defaultLimit-1"))
	assert.Equal(t, 0, len(writtenLogs))

	sourceLogger.LogSourceValue(defaultLimit, []byte("defaultLimit"))
	if debugLog {
		assert.Equal(t, 1, len(writtenLogs))
	} else {
		assert.Equal(t, 0, len(writtenLogs))
	}

	sourceLogger.LogSourceValue(math.MaxInt64, []byte("maxInt"))
	if debugLog {
		assert.Equal(t, 2, len(writtenLogs))
	} else {
		assert.Equal(t, 0, len(writtenLogs))
	}
}
