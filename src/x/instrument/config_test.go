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
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"

	"github.com/sergi/go-diff/diffmatchpatch"

	xjson "github.com/m3db/m3/src/x/json"

	"github.com/gogo/protobuf/jsonpb"
	extprom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
)

func TestPrometheusDefaults(t *testing.T) {
	cfg := newConfiguration()

	_, closer, reporters, err := cfg.NewRootScopeAndReporters(
		NewRootScopeAndReportersOptions{})
	require.NoError(t, err)
	require.NotNil(t, reporters.PrometheusReporter)

	defer closer.Close()

	// Make sure populated default histogram buckets.
	numDefaultBuckets := DefaultHistogramTimerHistogramBuckets().Len()
	require.True(t, numDefaultBuckets > 0)
	require.Equal(t, numDefaultBuckets, len(cfg.PrometheusReporter.DefaultHistogramBuckets))

	// Make sure populated default summary objectives buckets.
	numQuantiles := len(DefaultSummaryQuantileObjectives())
	require.True(t, numQuantiles > 0)
	require.Equal(t, numQuantiles, len(cfg.PrometheusReporter.DefaultSummaryObjectives))
}

func TestPrometheusExternalRegistries(t *testing.T) {
	extReg1 := PrometheusExternalRegistry{
		Registry: extprom.NewRegistry(),
		SubScope: "ext1",
	}
	extReg2 := PrometheusExternalRegistry{
		Registry: extprom.NewRegistry(),
		SubScope: "ext2",
	}

	cfg, listener := startMetricsEndpoint(t)

	scope, closer, reporters, err := cfg.NewRootScopeAndReporters(
		NewRootScopeAndReportersOptions{
			PrometheusHandlerListener: listener,
			PrometheusExternalRegistries: []PrometheusExternalRegistry{
				extReg1,
				extReg2,
			},
		})
	require.NoError(t, err)
	require.NotNil(t, reporters.PrometheusReporter)

	foo := scope.Tagged(map[string]string{
		"test": t.Name(),
	}).Counter("foo")
	foo.Inc(3)

	bar := extprom.NewCounterVec(extprom.CounterOpts{
		Name: "bar",
		Help: "bar help",
	}, []string{
		"test",
	}).With(map[string]string{
		"test": t.Name(),
	})
	extReg1.Registry.MustRegister(bar)
	bar.Inc()

	baz := extprom.NewCounterVec(extprom.CounterOpts{
		Name: "baz",
		Help: "baz help",
	}, []string{
		"test",
	}).With(map[string]string{
		"test": t.Name(),
	})
	extReg2.Registry.MustRegister(baz)
	baz.Inc()
	baz.Inc()

	// Wait for report.
	require.NoError(t, closer.Close())

	expected := map[string]xjson.Map{
		"foo": {
			"name": "foo",
			"help": "foo counter",
			"type": "COUNTER",
			"metric": xjson.Array{
				xjson.Map{
					"counter": xjson.Map{"value": 3},
					"label": xjson.Array{
						xjson.Map{
							"name":  "test",
							"value": t.Name(),
						},
					},
				},
			},
		},
		"ext1_bar": {
			"name": "ext1_bar",
			"help": "bar help",
			"type": "COUNTER",
			"metric": xjson.Array{
				xjson.Map{
					"counter": xjson.Map{"value": 1},
					"label": xjson.Array{
						xjson.Map{
							"name":  "test",
							"value": t.Name(),
						},
					},
				},
			},
		},
		"ext2_baz": {
			"name": "ext2_baz",
			"help": "baz help",
			"type": "COUNTER",
			"metric": xjson.Array{
				xjson.Map{
					"counter": xjson.Map{"value": 2},
					"label": xjson.Array{
						xjson.Map{
							"name":  "test",
							"value": t.Name(),
						},
					},
				},
			},
		},
	}

	assertMetrics(t, listener, expected)
}

func TestCommonLabelsAdded(t *testing.T) {
	extReg1 := PrometheusExternalRegistry{
		Registry: extprom.NewRegistry(),
		SubScope: "ext1",
	}

	cfg, listener := startMetricsEndpoint(t)

	scope, closer, reporters, err := cfg.NewRootScopeAndReporters(
		NewRootScopeAndReportersOptions{
			PrometheusHandlerListener:    listener,
			PrometheusExternalRegistries: []PrometheusExternalRegistry{extReg1},
			CommonLabels:                 map[string]string{"commonLabel": "commonLabelValue"},
		})
	require.NoError(t, err)
	require.NotNil(t, reporters.PrometheusReporter)

	foo := scope.Counter("foo")
	foo.Inc(3)

	bar := extprom.NewCounter(extprom.CounterOpts{Name: "bar", Help: "bar help"})
	extReg1.Registry.MustRegister(bar)
	bar.Inc()

	// Wait for report.
	require.NoError(t, closer.Close())

	expected := map[string]xjson.Map{
		"foo": {
			"name": "foo",
			"help": "foo counter",
			"type": "COUNTER",
			"metric": xjson.Array{
				xjson.Map{
					"counter": xjson.Map{"value": 3},
					"label": xjson.Array{
						xjson.Map{
							"name":  "commonLabel",
							"value": "commonLabelValue",
						},
					},
				},
			},
		},
		"ext1_bar": {
			"name": "ext1_bar",
			"help": "bar help",
			"type": "COUNTER",
			"metric": xjson.Array{
				xjson.Map{
					"counter": xjson.Map{"value": 1},
					"label": xjson.Array{
						xjson.Map{
							"name":  "commonLabel",
							"value": "commonLabelValue",
						},
					},
				},
			},
		},
	}

	assertMetrics(t, listener, expected)
}

func startMetricsEndpoint(t *testing.T) (MetricsConfiguration, net.Listener) {
	cfg := newConfiguration()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	return cfg, listener
}

func newConfiguration() MetricsConfiguration {
	sanitization := PrometheusMetricSanitization
	extended := DetailedExtendedMetrics
	cfg := MetricsConfiguration{
		Sanitization: &sanitization,
		SamplingRate: 1,
		PrometheusReporter: &PrometheusConfiguration{
			HandlerPath:   "/metrics",
			ListenAddress: "0.0.0.0:0",
			TimerType:     "histogram",
		},
		ExtendedMetrics: &extended,
	}
	return cfg
}

func assertMetrics(t *testing.T, listener net.Listener, expected map[string]xjson.Map) {
	url := fmt.Sprintf("http://%s/metrics", listener.Addr().String()) //nolint
	resp, err := http.Get(url) //nolint
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	defer resp.Body.Close()

	var parser expfmt.TextParser
	metricFamilies, err := parser.TextToMetricFamilies(resp.Body)
	require.NoError(t, err)

	expectMatch := len(expected)
	actualMatch := 0
	for k, v := range metricFamilies {
		data, err := (&jsonpb.Marshaler{}).MarshalToString(v)
		require.NoError(t, err)
		// Turn this on for debugging:
		// fmt.Printf("metric received: key=%s, value=%s\n", k, data)

		expect, ok := expected[k]
		if !ok {
			continue
		}

		// Mark matched.
		delete(expected, k)

		expectJSON := mustPrettyJSONMap(t, expect)
		actualJSON := mustPrettyJSONString(t, data)

		require.Equal(t, expectJSON, actualJSON,
			diff(expectJSON, actualJSON))
	}

	var remaining []string
	for k := range expected {
		remaining = append(remaining, k)
	}

	t.Logf("matched expected metrics: expected=%d, actual=%d",
		expectMatch, actualMatch)

	require.Equal(t, 0, len(remaining),
		fmt.Sprintf("did not match expected metrics: %v", remaining))
}

func mustPrettyJSONMap(t *testing.T, value xjson.Map) string {
	pretty, err := json.MarshalIndent(value, "", "  ")
	require.NoError(t, err)
	return string(pretty)
}

func mustPrettyJSONString(t *testing.T, str string) string {
	var unmarshalled map[string]interface{}
	err := json.Unmarshal([]byte(str), &unmarshalled)
	require.NoError(t, err)
	pretty, err := json.MarshalIndent(unmarshalled, "", "  ")
	require.NoError(t, err)
	return string(pretty)
}

func diff(expected, actual string) string {
	dmp := diffmatchpatch.New()
	diffs := dmp.DiffMain(expected, actual, false)
	return dmp.DiffPrettyText(diffs)
}
