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
	"net"
	"time"

	"github.com/m3db/m3/src/metrics/encoding"
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/policy"
)

// TODO(xichen): replace client with the actual aggregation server client.
type client struct {
	address         string
	batchSize       int
	connectTimeout  time.Duration
	protobufEncoder protobuf.UnaggregatedEncoder
	conn            net.Conn
}

func newClient(
	address string,
	batchSize int,
	connectTimeout time.Duration,
) *client {
	return &client{
		address:         address,
		batchSize:       batchSize,
		connectTimeout:  connectTimeout,
		protobufEncoder: protobuf.NewUnaggregatedEncoder(protobuf.NewUnaggregatedOptions()),
	}
}

func (c *client) connect() error {
	conn, err := net.DialTimeout("tcp", c.address, c.connectTimeout)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *client) testConnection() bool {
	if err := c.connect(); err != nil {
		return false
	}
	c.conn.Close()
	c.conn = nil
	return true
}

func (c *client) writeUntimedMetricWithMetadatas(
	mu unaggregated.MetricUnion,
	sm metadata.StagedMetadatas,
) error {
	var msg encoding.UnaggregatedMessageUnion
	switch mu.Type {
	case metric.CounterType:
		msg = encoding.UnaggregatedMessageUnion{
			Type: encoding.CounterWithMetadatasType,
			CounterWithMetadatas: unaggregated.CounterWithMetadatas{
				Counter:         mu.Counter(),
				StagedMetadatas: sm,
			}}
	case metric.TimerType:
		msg = encoding.UnaggregatedMessageUnion{
			Type: encoding.BatchTimerWithMetadatasType,
			BatchTimerWithMetadatas: unaggregated.BatchTimerWithMetadatas{
				BatchTimer:      mu.BatchTimer(),
				StagedMetadatas: sm,
			}}
	case metric.GaugeType:
		msg = encoding.UnaggregatedMessageUnion{
			Type: encoding.GaugeWithMetadatasType,
			GaugeWithMetadatas: unaggregated.GaugeWithMetadatas{
				Gauge:           mu.Gauge(),
				StagedMetadatas: sm,
			}}
	default:
		return fmt.Errorf("unrecognized metric type %v", mu.Type)
	}
	return c.writeUnaggregatedMessage(msg)
}

func (c *client) writeTimedMetricWithMetadata(
	metric aggregated.Metric,
	metadata metadata.TimedMetadata,
) error {
	msg := encoding.UnaggregatedMessageUnion{
		Type: encoding.TimedMetricWithMetadataType,
		TimedMetricWithMetadata: aggregated.TimedMetricWithMetadata{
			Metric:        metric,
			TimedMetadata: metadata,
		},
	}
	return c.writeUnaggregatedMessage(msg)
}

func (c *client) writeForwardedMetricWithMetadata(
	metric aggregated.ForwardedMetric,
	metadata metadata.ForwardMetadata,
) error {
	msg := encoding.UnaggregatedMessageUnion{
		Type: encoding.ForwardedMetricWithMetadataType,
		ForwardedMetricWithMetadata: aggregated.ForwardedMetricWithMetadata{
			ForwardedMetric: metric,
			ForwardMetadata: metadata,
		},
	}
	return c.writeUnaggregatedMessage(msg)
}

func (c *client) writePassthroughMetricWithMetadata(
	metric aggregated.Metric,
	storagePolicy policy.StoragePolicy,
) error {
	msg := encoding.UnaggregatedMessageUnion{
		Type: encoding.PassthroughMetricWithMetadataType,
		PassthroughMetricWithMetadata: aggregated.PassthroughMetricWithMetadata{
			Metric:        metric,
			StoragePolicy: storagePolicy,
		},
	}
	return c.writeUnaggregatedMessage(msg)
}

func (c *client) writeUnaggregatedMessage(
	msg encoding.UnaggregatedMessageUnion,
) error {
	encoder := c.protobufEncoder
	sizeBefore := encoder.Len()
	err := c.protobufEncoder.EncodeMessage(msg)
	if err != nil {
		encoder.Truncate(sizeBefore)
		return err
	}
	sizeAfter := encoder.Len()
	// If the buffer size is not big enough, do nothing.
	if sizeAfter < c.batchSize {
		return nil
	}
	// Otherwise we get a new buffer and copy the bytes exceeding the max
	// flush size to it, and flush out the old buffer.
	buf := encoder.Relinquish()
	encoder.Reset(buf.Bytes()[sizeBefore:sizeAfter])
	buf.Truncate(sizeBefore)
	_, err = c.conn.Write(buf.Bytes())
	buf.Close()
	return err
}

func (c *client) flush() error {
	encoder := c.protobufEncoder
	if encoder.Len() == 0 {
		return nil
	}
	buf := c.protobufEncoder.Relinquish()
	_, err := c.conn.Write(buf.Bytes())
	buf.Close()
	return err
}

func (c *client) close() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}
