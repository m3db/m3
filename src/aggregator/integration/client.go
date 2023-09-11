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

package integration

import (
	"fmt"

	aggclient "github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/policy"
)

type client struct {
	aggClient aggclient.AdminClient
}

func newClient(
	aggClient aggclient.AdminClient,
) *client {
	return &client{
		aggClient: aggClient,
	}
}

func (c *client) connect() error {
	return c.aggClient.Init()
}

func (c *client) writeUntimedMetricWithMetadatas(
	mu unaggregated.MetricUnion,
	sm metadata.StagedMetadatas,
) error {
	switch mu.Type {
	case metric.CounterType:
		return c.aggClient.WriteUntimedCounter(mu.Counter(), sm)
	case metric.TimerType:
		return c.aggClient.WriteUntimedBatchTimer(mu.BatchTimer(), sm)
	case metric.GaugeType:
		return c.aggClient.WriteUntimedGauge(mu.Gauge(), sm)
	default:
		return fmt.Errorf("unrecognized metric type %v", mu.Type)
	}
}

func (c *client) writeTimedMetricWithMetadata(
	metric aggregated.Metric,
	metadata metadata.TimedMetadata,
) error {
	return c.aggClient.WriteTimed(metric, metadata)
}

func (c *client) writeForwardedMetricWithMetadata(
	metric aggregated.ForwardedMetric,
	metadata metadata.ForwardMetadata,
) error {
	return c.aggClient.WriteForwarded(metric, metadata)
}

func (c *client) writePassthroughMetricWithMetadata(
	metric aggregated.Metric,
	storagePolicy policy.StoragePolicy,
) error {
	return c.aggClient.WritePassthrough(metric, storagePolicy)
}

func (c *client) flush() error {
	return c.aggClient.Flush()
}

func (c *client) close() error {
	return c.aggClient.Close()
}
