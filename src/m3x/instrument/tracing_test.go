package instrument

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
