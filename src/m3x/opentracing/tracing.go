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

package opentracing

import (
	"fmt"
	"io"

	"github.com/opentracing/opentracing-go"
	"github.com/uber-go/tally"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	jaegerzap "github.com/uber/jaeger-client-go/log/zap"
	jaegertally "github.com/uber/jaeger-lib/metrics/tally"
	"go.uber.org/zap"
)

// Supported tracing backends. Currently only jaeger is supported.
var (
	TracingBackendJaeger = "jaeger"
)

// TracingConfiguration configures an opentracing backend for m3query to use. Currently only jaeger is supported.
// Tracing is disabled if no backend is specified.
type TracingConfiguration struct {
	Backend string                  `yaml:"backend"`
	Jaeger  jaegercfg.Configuration `yaml:"jaeger"`
}

// NewTracer returns a tracer configured with the configuration provided by this struct. The tracer's concrete
// type is determined by cfg.Backend. Currently only `"jaeger"` is supported. `""` implies
// disabled (NoopTracer).
func (cfg *TracingConfiguration) NewTracer(defaultServiceName string, scope tally.Scope, logger *zap.Logger) (opentracing.Tracer, io.Closer, error) {
	if cfg.Backend == "" {
		return opentracing.NoopTracer{}, noopCloser{}, nil
	}

	if cfg.Backend != TracingBackendJaeger {
		return nil, nil, fmt.Errorf("unknown tracing backend: %s. Supported backends are: %s", cfg.Backend, TracingBackendJaeger)
	}

	if cfg.Jaeger.ServiceName == "" {
		cfg.Jaeger.ServiceName = defaultServiceName
	}

	jaegerLog := jaegerzap.NewLogger(logger)
	tracer, jaegerCloser, err := cfg.Jaeger.NewTracer(
		jaegercfg.Logger(jaegerLog),
		jaegercfg.Metrics(jaegertally.Wrap(scope)))

	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize jaeger: %s", err.Error())
	}

	return tracer, jaegerCloser, nil
}

type noopCloser struct{}

func (noopCloser) Close() error {
	return nil
}
