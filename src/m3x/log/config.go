// Copyright (c) 2016 Uber Technologies, Inc.
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

package xlog

import (
	"io"
	"os"
)

// Configuration defines configuration for logging.
type Configuration struct {
	File   string                 `json:"file" yaml:"file"`
	Level  string                 `json:"level" yaml:"level"`
	Fields map[string]interface{} `json:"fields" yaml:"fields"`
}

// BuildLogger builds a new Logger based on the configuration.
func (cfg Configuration) BuildLogger() (Logger, error) {
	writer := io.Writer(os.Stdout)

	if cfg.File != "" {
		fd, err := os.OpenFile(cfg.File, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}

		writer = io.MultiWriter(writer, fd)
	}

	logger := NewLogger(writer)

	if len(cfg.Level) != 0 {
		level, err := ParseLogLevel(cfg.Level)
		if err != nil {
			return nil, err
		}

		logger = NewLevelLogger(logger, level)
	}

	if len(cfg.Fields) != 0 {
		var fields []LogField
		for k, v := range cfg.Fields {
			fields = append(fields, NewLogField(k, v))
		}
		logger = logger.WithFields(fields...)
	}

	return logger, nil
}
