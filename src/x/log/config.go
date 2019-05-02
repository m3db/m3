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

package log

import (
	"fmt"

	"go.uber.org/zap"
)

// Configuration defines configuration for logging.
type Configuration struct {
	File   string                 `json:"file" yaml:"file"`
	Level  string                 `json:"level" yaml:"level"`
	Fields map[string]interface{} `json:"fields" yaml:"fields"`
}

// BuildLogger builds a new Logger based on the configuration.
func (cfg Configuration) BuildLogger() (*zap.Logger, error) {
	zc := zap.Config{
		Level:             zap.NewAtomicLevelAt(zap.InfoLevel),
		Development:       false,
		DisableCaller:     true,
		DisableStacktrace: true,
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding:         "json",
		EncoderConfig:    zap.NewProductionEncoderConfig(),
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stdout"},
		InitialFields:    cfg.Fields,
	}

	if cfg.File != "" {
		zc.OutputPaths = append(zc.OutputPaths, cfg.File)
		zc.ErrorOutputPaths = append(zc.ErrorOutputPaths, cfg.File)
	}

	if len(cfg.Level) != 0 {
		var parsedLevel zap.AtomicLevel
		if err := parsedLevel.UnmarshalText([]byte(cfg.Level)); err != nil {
			return nil, fmt.Errorf("unable to parse log level %s: %v", cfg.Level, err)
		}
		zc.Level = parsedLevel
	}

	return zc.Build()
}
