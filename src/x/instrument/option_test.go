package instrument

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestScopeAndLoggerTagged(t *testing.T) {
	core, recorded := observer.New(zapcore.InfoLevel)
	iOpts := NewOptions().
		SetMetricsScope(tally.NewTestScope("", map[string]string{})).
		SetLogger(zap.New(core))
	iOpts = WithOptions(iOpts, WithScopeAndLoggerTagged("k", "v"))

	// Make sure the metric has tag k:v.
	iOpts.MetricsScope().Counter("a").Inc(1)

	ss := iOpts.MetricsScope().(tally.TestScope).Snapshot()
	counters, ok := ss.Counters()["a+k=v"]
	require.True(t, ok)
	require.Equal(t, map[string]string{"k": "v"}, counters.Tags())

	// Make sure the log has tag k:v.
	iOpts.Logger().Info("b")

	require.Len(t, recorded.All(), 1)
	require.Len(t, recorded.FilterField(
		zap.String("k", "v"),
	).All(), 1)
}
