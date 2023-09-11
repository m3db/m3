// Copyright (c) 2021 Uber Technologies, Inc.
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

package log

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestLoggerConfiguration(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "logtest")
	require.NoError(t, err)

	defer tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	cfg := Configuration{
		Fields: map[string]interface{}{
			"my-field": "my-val",
		},
		Level: "error",
		File:  tmpfile.Name(),
	}

	log, err := cfg.BuildLogger()
	require.NoError(t, err)

	log.Info("should not appear")
	log.Warn("should not appear")
	log.Error("this should appear")

	b, err := ioutil.ReadAll(tmpfile)
	require.NoError(t, err)

	data := string(b)
	require.Equal(t, 1, strings.Count(data, "\n"), data)
	require.True(t, strings.Contains(data, `"msg":"this should appear"`))
	require.True(t, strings.Contains(data, `"my-field":"my-val"`))
	require.True(t, strings.Contains(data, `"level":"error"`))
}

func TestLoggerEncoderConfiguraion(t *testing.T) {
	logTime := time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	logEntry := zapcore.Entry{
		LoggerName: "main",
		Level:      zapcore.InfoLevel,
		Message:    `hello`,
		Time:       logTime,
		Stack:      "fake-stack",
		Caller:     zapcore.EntryCaller{Defined: true, File: "foo.go", Line: 42, Function: "foo.Foo"},
	}

	tests := []struct {
		name     string
		cfg      encoderConfig
		expected string
	}{
		{
			name: "empty encoder config",
			cfg:  encoderConfig{},
			expected: `{"level":"info","ts":0,"logger":"main","caller":"foo.go:42","msg":"hello",` +
				`"stacktrace":"fake-stack"}` + zapcore.DefaultLineEnding,
		},
		{
			name: "encoder custom",
			cfg: encoderConfig{
				MessageKey:     "M",
				LevelKey:       "L",
				TimeKey:        "T",
				NameKey:        "N",
				CallerKey:      "C",
				FunctionKey:    "F",
				StacktraceKey:  "S",
				LineEnding:     "\r\n",
				EncodeLevel:    "capital",
				EncodeTime:     "rfc3339nano",
				EncodeDuration: "string",
				EncodeCaller:   "short",
				EncodeName:     "full",
			},
			expected: `{"L":"INFO","T":"1970-01-01T00:00:00Z","N":"main","C":"foo.go:42","F":"foo.Foo","M":"hello",` +
				`"S":"fake-stack"}` + "\r\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logCfg := Configuration{
				EncoderConfig: tt.cfg,
			}
			ec := logCfg.newEncoderConfig()
			json := zapcore.NewJSONEncoder(ec)
			jsonOut, jsonErr := json.EncodeEntry(logEntry, nil)
			assert.NoError(t, jsonErr, "Unexpected error JSON-encoding entry")
			assert.Equal(t, tt.expected, jsonOut.String())
		})
	}
}

func TestGettingLoggerZapConfig(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "logtest")
	require.NoError(t, err)

	defer func() {
		err = tmpfile.Close()
		err = os.Remove(tmpfile.Name())
	}()

	cfg := Configuration{
		Fields: map[string]interface{}{
			"my-field": "my-val",
		},
		Level: "error",
		File:  tmpfile.Name(),
	}

	log, zapConfig, err := cfg.BuildLoggerAndReturnConfig()
	require.NoError(t, err)

	log.Info("should not appear")
	log.Warn("should not appear")
	log.Error("this should appear")

	zapConfig.Level.SetLevel(zapcore.InfoLevel)
	log.Info("info should now appear")
	log.Warn("warn should now appear")
	b, err := ioutil.ReadAll(tmpfile)
	require.NoError(t, err)

	data := string(b)
	require.Equal(t, 3, strings.Count(data, "\n"), data)
	require.True(t, strings.Contains(data, `"msg":"this should appear"`))
	require.True(t, strings.Contains(data, `"my-field":"my-val"`))
	require.True(t, strings.Contains(data, `"level":"error"`))
	require.True(t, strings.Contains(data, `"msg":"info should now appear"`))
	require.True(t, strings.Contains(data, `"msg":"warn should now appear"`))
}
