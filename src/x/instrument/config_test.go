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

package instrument

import (
	"testing"

	"github.com/stretchr/testify/require"

	tallyprom "github.com/uber-go/tally/prometheus"
)

func TestPrometheusDefaults(t *testing.T) {
	sanitization := PrometheusMetricSanitization
	extended := DetailedExtendedMetrics
	cfg := MetricsConfiguration{
		Sanitization: &sanitization,
		SamplingRate: 1,
		PrometheusReporter: &tallyprom.Configuration{
			HandlerPath:   "/metrics",
			ListenAddress: "0.0.0.0:0",
			TimerType:     "histogram",
		},
		ExtendedMetrics: &extended,
	}
	_, closer, reporters, err := cfg.NewRootScopeAndReporters(
		NewRootScopeAndReportersOptions{})
	require.NoError(t, err)
	require.NotNil(t, reporters.PrometheusReporter)

	defer closer.Close()

	// Make sure populated default histogram buckets.
	numDefaultBuckets := DefaultHistogramTimerHistogramBuckets().Len()
	require.True(t, numDefaultBuckets > 0)
	require.Equal(t, numDefaultBuckets, len(cfg.PrometheusReporter.DefaultHistogramBuckets))

	// Make sure populated default summmary objectives buckets.
	numQuantiles := len(DefaultSummaryQuantileObjectives())
	require.True(t, numQuantiles > 0)
	require.Equal(t, numQuantiles, len(cfg.PrometheusReporter.DefaultSummaryObjectives))
}
