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
	"io"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	"go.uber.org/zap"
)

func TestTracingConfiguration_NewTracer(t *testing.T) {
	serviceName := "foo"
	doCall := func(cfg *TracingConfiguration) (opentracing.Tracer, io.Closer, error) {
		return cfg.NewTracer(serviceName, tally.NoopScope, zap.L())
	}

	t.Run("defaults to noop", func(t *testing.T) {
		cfg := TracingConfiguration{}
		tr, closer, err := doCall(&cfg)
		defer closer.Close()
		require.NoError(t, err)
		assert.Equal(t, opentracing.NoopTracer{}, tr)
		assert.Equal(t, noopCloser{}, closer)
	})

	t.Run("errors on non-jaeger", func(t *testing.T) {
		cfg := TracingConfiguration{
			Backend: "someone_else",
		}
		_, _, err := doCall(&cfg)
		require.EqualError(t, err, "unknown tracing backend: someone_else. Supported backends are: jaeger")
	})

	t.Run("initializes jaeger tracer", func(t *testing.T) {
		cfg := TracingConfiguration{
			Backend: TracingBackendJaeger,
		}
		tr, closer, err := doCall(&cfg)
		defer closer.Close()

		require.NoError(t, err)
		assert.IsType(t, (*jaeger.Tracer)(nil), tr)
	})

	t.Run("sets service name on empty", func(t *testing.T) {
		cfg := TracingConfiguration{
			Backend: TracingBackendJaeger,
		}
		_, closer, err := doCall(&cfg)
		defer closer.Close()

		require.NoError(t, err)
		assert.Equal(t, serviceName, cfg.Jaeger.ServiceName)
	})

	t.Run("leaves service name on non-empty", func(t *testing.T) {
		cfg := TracingConfiguration{
			Backend: TracingBackendJaeger,
			Jaeger: jaegercfg.Configuration{
				ServiceName: "other",
			},
		}
		_, closer, err := doCall(&cfg)
		defer closer.Close()

		require.NoError(t, err)
		assert.Equal(t, "other", cfg.Jaeger.ServiceName)
	})
}
