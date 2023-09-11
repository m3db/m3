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

package m3msg

import (
	"fmt"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/metrics/encoding"
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/msg/consumer"
	xserver "github.com/m3db/m3/src/x/server"

	"go.uber.org/zap"
)

// NewServer creates a new M3Msg server.
func NewServer(
	address string,
	aggregator aggregator.Aggregator,
	opts Options,
) (xserver.Server, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	newMessageProcessor := func() consumer.MessageProcessor {
		// construct a new messageProcessor per consumer so the internal protos can be reused across messages on the
		// same connection.
		return &messageProcessor{
			aggregator: aggregator,
			logger:     opts.InstrumentOptions().Logger(),
		}
	}
	handler := consumer.NewMessageHandler(consumer.NewMessageProcessorFactory(newMessageProcessor), opts.ConsumerOptions())
	return xserver.NewServer(address, handler, opts.ServerOptions()), nil
}

type messageProcessor struct {
	pb         metricpb.MetricWithMetadatas
	union      encoding.UnaggregatedMessageUnion
	aggregator aggregator.Aggregator
	logger     *zap.Logger
}

func (m *messageProcessor) Process(msg consumer.Message) {
	if err := m.handleMessage(&m.pb, &m.union, msg); err != nil {
		m.logger.Error("could not process message",
			zap.Error(err),
			zap.Uint64("shard", msg.ShardID()),
			zap.String("proto", m.pb.String()))
	}
}

func (m *messageProcessor) handleMessage(
	pb *metricpb.MetricWithMetadatas,
	union *encoding.UnaggregatedMessageUnion,
	msg consumer.Message,
) error {
	defer msg.Ack()

	// Reset and reuse the protobuf message for unpacking.
	protobuf.ReuseMetricWithMetadatasProto(&m.pb)
	// Unmarshal the message.
	if err := pb.Unmarshal(msg.Bytes()); err != nil {
		return err
	}

	switch pb.Type {
	case metricpb.MetricWithMetadatas_COUNTER_WITH_METADATAS:
		err := union.CounterWithMetadatas.FromProto(pb.CounterWithMetadatas)
		if err != nil {
			return err
		}
		u := union.CounterWithMetadatas.ToUnion()
		return m.aggregator.AddUntimed(u, union.CounterWithMetadatas.StagedMetadatas)
	case metricpb.MetricWithMetadatas_BATCH_TIMER_WITH_METADATAS:
		err := union.BatchTimerWithMetadatas.FromProto(pb.BatchTimerWithMetadatas)
		if err != nil {
			return err
		}
		u := union.BatchTimerWithMetadatas.ToUnion()
		return m.aggregator.AddUntimed(u, union.BatchTimerWithMetadatas.StagedMetadatas)
	case metricpb.MetricWithMetadatas_GAUGE_WITH_METADATAS:
		err := union.GaugeWithMetadatas.FromProto(pb.GaugeWithMetadatas)
		if err != nil {
			return err
		}
		u := union.GaugeWithMetadatas.ToUnion()
		return m.aggregator.AddUntimed(u, union.GaugeWithMetadatas.StagedMetadatas)
	case metricpb.MetricWithMetadatas_FORWARDED_METRIC_WITH_METADATA:
		err := union.ForwardedMetricWithMetadata.FromProto(pb.ForwardedMetricWithMetadata)
		if err != nil {
			return err
		}
		return m.aggregator.AddForwarded(
			union.ForwardedMetricWithMetadata.ForwardedMetric,
			union.ForwardedMetricWithMetadata.ForwardMetadata)
	case metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_METADATA:
		err := union.TimedMetricWithMetadata.FromProto(pb.TimedMetricWithMetadata)
		if err != nil {
			return err
		}
		return m.aggregator.AddTimed(
			union.TimedMetricWithMetadata.Metric,
			union.TimedMetricWithMetadata.TimedMetadata)
	case metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_METADATAS:
		err := union.TimedMetricWithMetadatas.FromProto(pb.TimedMetricWithMetadatas)
		if err != nil {
			return err
		}
		return m.aggregator.AddTimedWithStagedMetadatas(
			union.TimedMetricWithMetadatas.Metric,
			union.TimedMetricWithMetadatas.StagedMetadatas)
	default:
		return fmt.Errorf("unrecognized message type: %v", pb.Type)
	}
}

func (m *messageProcessor) Close() {}
