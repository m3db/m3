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
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Configuration defines configuration for logging.
type Configuration struct {
	File          string                 `json:"file" yaml:"file"`
	Level         string                 `json:"level" yaml:"level"`
	Fields        map[string]interface{} `json:"fields" yaml:"fields"`
	EncoderConfig encoderConfig          `json:"encoderConfig,omitempty" yaml:"encoderConfig,omitempty"`
}

type encoderConfig struct {
	MessageKey     string `json:"messageKey" yaml:"messageKey"`
	LevelKey       string `json:"levelKey" yaml:"levelKey"`
	TimeKey        string `json:"timeKey" yaml:"timeKey"`
	NameKey        string `json:"nameKey" yaml:"nameKey"`
	CallerKey      string `json:"callerKey" yaml:"callerKey"`
	FunctionKey    string `json:"functionKey" yaml:"functionKey"`
	StacktraceKey  string `json:"stacktraceKey" yaml:"stacktraceKey"`
	LineEnding     string `json:"lineEnding" yaml:"lineEnding"`
	EncodeLevel    string `json:"levelEncoder" yaml:"levelEncoder"`
	EncodeTime     string `json:"timeEncoder" yaml:"timeEncoder"`
	EncodeDuration string `json:"durationEncoder" yaml:"durationEncoder"`
	EncodeCaller   string `json:"callerEncoder" yaml:"callerEncoder"`
	EncodeName     string `json:"nameEncoder" yaml:"nameEncoder,omitempty"`
}

// BuildLogger builds a new Logger based on the configuration.
func (cfg Configuration) BuildLogger() (*zap.Logger, error) {
	logger, _, err := cfg.BuildLoggerAndReturnConfig()
	return logger, err
}

// BuildLoggerAndReturnConfig builds a new Logger based on the configuration and returns a zap.Config object.
func (cfg Configuration) BuildLoggerAndReturnConfig() (*zap.Logger, *zap.Config, error) {
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
		EncoderConfig:    cfg.newEncoderConfig(),
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
			return nil, nil, fmt.Errorf("unable to parse log level %s: %w", cfg.Level, err)
		}
		zc.Level = parsedLevel
	}
	logger, err := zc.Build()
	return logger, &zc, err
}

func (cfg Configuration) newEncoderConfig() zapcore.EncoderConfig {
	ec := zap.NewProductionEncoderConfig()

	if cfg.EncoderConfig.MessageKey != "" {
		ec.MessageKey = cfg.EncoderConfig.MessageKey
	}

	if cfg.EncoderConfig.LevelKey != "" {
		ec.LevelKey = cfg.EncoderConfig.LevelKey
	}

	if cfg.EncoderConfig.TimeKey != "" {
		ec.TimeKey = cfg.EncoderConfig.TimeKey
	}

	if cfg.EncoderConfig.NameKey != "" {
		ec.NameKey = cfg.EncoderConfig.NameKey
	}

	if cfg.EncoderConfig.CallerKey != "" {
		ec.CallerKey = cfg.EncoderConfig.CallerKey
	}

	if cfg.EncoderConfig.FunctionKey != "" {
		ec.FunctionKey = cfg.EncoderConfig.FunctionKey
	}

	if cfg.EncoderConfig.StacktraceKey != "" {
		ec.StacktraceKey = cfg.EncoderConfig.StacktraceKey
	}

	if cfg.EncoderConfig.LineEnding != "" {
		ec.LineEnding = cfg.EncoderConfig.LineEnding
	}

	if cfg.EncoderConfig.EncodeLevel != "" {
		var levelEncoder zapcore.LevelEncoder
		_ = levelEncoder.UnmarshalText([]byte(cfg.EncoderConfig.EncodeLevel))
		ec.EncodeLevel = levelEncoder
	}

	if cfg.EncoderConfig.EncodeTime != "" {
		var timeEncoder zapcore.TimeEncoder
		_ = timeEncoder.UnmarshalText([]byte(cfg.EncoderConfig.EncodeTime))
		ec.EncodeTime = timeEncoder
	}

	if cfg.EncoderConfig.EncodeDuration != "" {
		var durationEncoder zapcore.DurationEncoder
		_ = durationEncoder.UnmarshalText([]byte(cfg.EncoderConfig.EncodeDuration))
		ec.EncodeDuration = durationEncoder
	}

	if cfg.EncoderConfig.EncodeCaller != "" {
		var callerEncoder zapcore.CallerEncoder
		_ = callerEncoder.UnmarshalText([]byte(cfg.EncoderConfig.EncodeCaller))
		ec.EncodeCaller = callerEncoder
	}

	if cfg.EncoderConfig.EncodeName != "" {
		var nameEncoder zapcore.NameEncoder
		_ = nameEncoder.UnmarshalText([]byte(cfg.EncoderConfig.EncodeName))
		ec.EncodeName = nameEncoder
	}

	return ec
}
