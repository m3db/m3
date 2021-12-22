// Copyright (c) 2021  Uber Technologies, Inc.
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

// Package aggregator contains integration tests for aggregators.
package aggregator

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/integration/resources"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/x/headers"
)

const (
	// TestAggregatorDBNodeConfig is the test config for the dbnode.
	TestAggregatorDBNodeConfig = `
db: {}
coordinator: {}
`

	// TestAggregatorCoordinatorConfig is the test config for the coordinator.
	TestAggregatorCoordinatorConfig = `
listenAddress: 0.0.0.0:7202
metrics:
  scope:
    prefix: "coordinator"
  prometheus:
    handlerPath: /metrics
    listenAddress: 0.0.0.0:7303
  sanitization: prometheus
  samplingRate: 1.0
  extended: none
carbon:
  ingester:
    listenAddress: "0.0.0.0:7204"
    rules:
      - pattern: .*
        aggregation:
          type: mean
        policies:
          - resolution: 5s
            retention: 6h
downsample:
  rules:
    rollupRules:
      - name: "requests per second by status code"
        filter: "__name__:http_requests app:* status_code:* endpoint:*"
        transforms:
          - transform:
              type: "PerSecond"
          - rollup:
              metricName: "http_requests_by_status_code"
              groupBy: ["app", "status_code", "endpoint"]
              aggregations: ["Sum"]
        storagePolicies:
          - resolution: 5s
            retention: 6h
  remoteAggregator:
    client:
      type: m3msg
      m3msg:
        producer:
          writer:
            topicName: aggregator_ingest
            topicServiceOverride:
              zone: embedded
              environment: default_env
            placement:
              isStaged: true
            placementServiceOverride:
              namespaces:
                placement: /placement
            connection:
              numConnections: 4
            messagePool:
              size: 16384
              watermark:
                low: 0.2
                high: 0.5
ingest:
  ingester:
    workerPoolSize: 10000
    opPool:
      size: 10000
    retry:
      maxRetries: 3
      jitter: true
    logSampleRate: 0.01
  m3msg:
    server:
      listenAddress: "0.0.0.0:7507"
      retry:
        maxBackoff: 10s
        jitter: true
storeMetricsType: true
`

	// TestAggregatorAggregatorConfig is the test config for the aggregators.
	TestAggregatorAggregatorConfig = `
`

	// defaultCarbonPort is the default port of coordinator to receive carbon metrics.
	defaultCarbonPort = 7204
)

var (
	errEmptyResult = errors.New("empty query result")
	errQueryResult = errors.New("wrong query result")
)

// RunTest contains the logic for running the aggregator test.
func RunTest(t *testing.T, m3 resources.M3Resources) {
	t.Run("test_aggregated_graphite_metric", func(t *testing.T) {
		testAggregatedGraphiteMetric(t, m3)
	})

	t.Run("test_rollup_rule", func(t *testing.T) {
		testRollupRule(t, m3)
	})

	t.Run("test_metric_type_survives_aggregation", func(t *testing.T) {
		testMetricTypeSurvivesAggregation(t, m3)
	})
}

// testAggregatedGraphiteMetric tests the write and read of aggregated graphtie metrics.
func testAggregatedGraphiteMetric(t *testing.T, m3 resources.M3Resources) {
	var (
		carbonName         = "foo.bar.baz"
		carbonTarget       = "foo.bar.*"
		carbonLow          = float64(40)
		carbonHigh         = float64(44)
		expectedCarbonMean = float64(42)
	)

	doneCh := make(chan struct{})
	defer func() {
		doneCh <- struct{}{}
		close(doneCh)
	}()
	go func() {
		for {
			select {
			case <-doneCh:
				return
			default:
				require.NoError(t, m3.Coordinator().WriteCarbon(defaultCarbonPort, carbonName, carbonLow, time.Now()))
				require.NoError(t, m3.Coordinator().WriteCarbon(defaultCarbonPort, carbonName, carbonHigh, time.Now()))
				time.Sleep(1 * time.Second)
			}
		}
	}()

	require.NoError(t, resources.RetryWithMaxTime(func() error {
		return verifyGraphiteQuery(m3, carbonTarget, expectedCarbonMean)
	}, 2*time.Minute))
}

func verifyGraphiteQuery(m3 resources.M3Resources, target string, expected float64) error {
	datapoints, err := m3.Coordinator().GraphiteQuery(resources.GraphiteQueryRequest{
		Target: target,
		From:   time.Now().Add(-1000 * time.Second),
		Until:  time.Now(),
	})
	if err != nil {
		return err
	}
	nonNullDPs := filterNull(datapoints)
	if len(nonNullDPs) == 0 {
		return errEmptyResult
	}
	if v := *nonNullDPs[0].Value; v != expected {
		return fmt.Errorf("wrong datapoint result: expected=%f, actual=%f", expected, v)
	}
	return nil
}

func filterNull(datapoints []resources.Datapoint) []resources.Datapoint {
	nonNull := make([]resources.Datapoint, 0, len(datapoints))
	for _, dp := range datapoints {
		if dp.Value != nil {
			nonNull = append(nonNull, dp)
		}
	}
	return nonNull
}

// testRollupRule tests metrics aggregated with a rollup rule.
func testRollupRule(t *testing.T, m3 resources.M3Resources) {
	var (
		numDatapoints = 5
		resolutionSec = 5
		nowTime       = time.Now()
		initWriteTime = nowTime.Truncate(time.Duration(resolutionSec) * time.Second)
		metricName    = "http_requests"

		initVal1 = 42
		valRate1 = 22
		valInc1  = valRate1 * resolutionSec
		tags1    = map[string]string{
			"app":         "nginx_edge",
			"status_code": "500",
			"endpoint":    "/foo/bar",
		}

		initVal2 = 84
		valRate2 = 4
		valInc2  = valRate2 * resolutionSec
		tags2    = map[string]string{
			"app":         "nginx_edge",
			"status_code": "500",
			"endpoint":    "/foo/baz",
		}
	)

	for i := 0; i < numDatapoints; i++ {
		err := m3.Coordinator().WriteProm(
			metricName,
			tags1,
			[]prompb.Sample{{
				Value:     float64(initVal1 + i*valInc1),
				Timestamp: initWriteTime.Add(time.Duration(i*resolutionSec)*time.Second).Unix() * 1000,
			}},
			resources.Headers{headers.PromTypeHeader: []string{"counter"}},
		)
		require.NoError(t, err)
	}

	for i := 0; i < numDatapoints; i++ {
		err := m3.Coordinator().WriteProm(
			metricName,
			tags2,
			[]prompb.Sample{{
				Value:     float64(initVal2 + i*valInc2),
				Timestamp: initWriteTime.Add(time.Duration(i*resolutionSec)*time.Second).Unix() * 1000,
			}},
			resources.Headers{headers.PromTypeHeader: []string{"gauge"}},
		)
		require.NoError(t, err)
	}

	require.NoError(t, resources.RetryWithMaxTime(func() error {
		return verifyPromQuery(
			m3,
			`http_requests_by_status_code{endpoint="/foo/bar"}`,
			float64(valRate1),
		)
	}, 2*time.Minute))

	require.NoError(t, resources.RetryWithMaxTime(func() error {
		return verifyPromQuery(
			m3,
			`http_requests_by_status_code{endpoint="/foo/baz"}`,
			float64(valRate2),
		)
	}, 2*time.Minute))
}

func verifyPromQuery(
	m3 resources.M3Resources,
	queryStr string,
	expected float64,
) error {
	results, err := m3.Coordinator().RangeQuery(
		resources.RangeQueryRequest{
			Query: queryStr,
			Start: time.Now().Add(-1 * time.Hour),
			End:   time.Now().Add(1 * time.Hour),
			Step:  30 * time.Second,
		},
		map[string][]string{
			"M3-Metrics-Type":   {"aggregated"},
			"M3-Storage-Policy": {"5s:6h"},
		},
	)
	if err != nil {
		return err
	}
	if len(results) == 0 {
		return errEmptyResult
	}
	if len(results) > 1 {
		return errors.New("more results than expected")
	}
	if v := float64(results[0].Values[0].Value); v != expected {
		return fmt.Errorf("wrong datapoint result: expected=%f, actual=%f", expected, v)
	}
	return nil
}

// testMetricTypeSurvivesAggregation verifies that the metric type information
// is stored in db after the aggregation.
func testMetricTypeSurvivesAggregation(t *testing.T, m3 resources.M3Resources) {
	nowTime := time.Now()
	value := 42
	metricName := "metric_type_test"

	require.NoError(t, m3.Coordinator().WriteProm(
		metricName,
		map[string]string{
			"label0": "label0",
			"label1": "label1",
			"label2": "label2",
		},
		[]prompb.Sample{{
			Value:     float64(value),
			Timestamp: nowTime.Unix() * 1000,
		}},
		resources.Headers{headers.PromTypeHeader: []string{"counter"}},
	))

	node := m3.Nodes()[0]
	require.NoError(t, resources.Retry(func() error {
		res, err := node.Fetch(&rpc.FetchRequest{
			NameSpace:  "aggregated",
			ID:         `{__name__="metric_type_test",label0="label0",label1="label1",label2="label2"}`,
			RangeStart: nowTime.Add(-1 * time.Hour).Unix(),
			RangeEnd:   nowTime.Add(time.Hour).Unix(),
		})
		if err != nil {
			return err
		}
		if len(res.Datapoints) == 0 {
			return errEmptyResult
		}
		if len(res.Datapoints[0].Annotation) == 0 {
			return errQueryResult
		}
		if res.Datapoints[0].Value != float64(value) {
			return errQueryResult
		}
		return nil
	}))
}
