// Copyright (c) 2018 Uber Technologies, Inc.
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

package writer

import (
	"errors"
	"strconv"
	"sync/atomic"

	"github.com/m3db/m3/src/aggregator/sharding"
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/msg/routing"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	errWriterClosed = errors.New("writer is closed")
)

type protobufWriterMetrics struct {
	writerClosed tally.Counter
	encodeErrors tally.Counter
	routeErrors  tally.Counter
}

func newProtobufWriterMetrics(scope tally.Scope) protobufWriterMetrics {
	encodeScope := scope.SubScope("encode")
	routeScope := scope.SubScope("route")
	return protobufWriterMetrics{
		writerClosed: scope.Counter("writer-closed"),
		encodeErrors: encodeScope.Counter("errors"),
		routeErrors:  routeScope.Counter("errors"),
	}
}

// protobufWriter encodes data and routes them to the backend.
// protobufWriter is not thread safe.
type protobufWriter struct {
	encoder   *protobuf.AggregatedEncoder
	p         producer.Producer
	numShards uint32

	m       aggregated.MetricWithStoragePolicy
	metrics protobufWriterMetrics

	shardFn sharding.ShardFn
	closed  bool
}

// NewProtobufWriter creates a writer that encodes metric in protobuf.
func NewProtobufWriter(
	producer producer.Producer,
	shardFn sharding.ShardFn,
	opts Options,
) Writer {
	instrumentOpts := opts.InstrumentOptions()
	w := &protobufWriter{
		encoder:   protobuf.NewAggregatedEncoder(opts.BytesPool()),
		p:         producer,
		numShards: producer.NumShards(),
		closed:    false,
		metrics:   newProtobufWriterMetrics(instrumentOpts.MetricsScope()),
		shardFn:   shardFn,
	}
	return w
}

func (w *protobufWriter) Write(mp aggregated.ChunkedMetricWithStoragePolicy) error {
	if w.closed {
		w.metrics.writerClosed.Inc(1)
		return errWriterClosed
	}

	m, shard := w.prepare(mp)
	if err := w.encoder.Encode(m); err != nil {
		w.metrics.encodeErrors.Inc(1)
		return err
	}

	if err := w.p.Produce(newMessage(shard, mp.StoragePolicy, mp.RoutingPolicy, w.encoder.Buffer())); err != nil {
		w.metrics.routeErrors.Inc(1)
		return err
	}

	return nil
}

func (w *protobufWriter) prepare(mp aggregated.ChunkedMetricWithStoragePolicy) (aggregated.MetricWithStoragePolicy, uint32) {
	// TODO(cw) Chunked metric has no 'type' field, consider adding one.
	w.m.ID = w.m.ID[:0]
	w.m.ID = append(w.m.ID, mp.Prefix...)
	w.m.ID = append(w.m.ID, mp.Data...)
	w.m.ID = append(w.m.ID, mp.Suffix...)
	w.m.Metric.TimeNanos = mp.TimeNanos
	w.m.Metric.Value = mp.Value
	w.m.Annotation = mp.ChunkedMetric.Annotation
	w.m.StoragePolicy = mp.StoragePolicy
	w.m.RoutingPolicy = mp.RoutingPolicy
	shard := w.shardFn(w.m.ID, w.numShards)
	return w.m, shard
}

func (w *protobufWriter) Flush() error {
	return nil
}

func (w *protobufWriter) Close() error {
	if w.closed {
		w.metrics.writerClosed.Inc(1)
		return errWriterClosed
	}
	// Don't close the producer here, it maybe shared by other writers.
	w.closed = true
	return nil
}

type message struct {
	shard uint32
	sp    policy.StoragePolicy
	rp    policy.RoutingPolicy
	data  protobuf.Buffer
}

func newMessage(
	shard uint32,
	sp policy.StoragePolicy,
	rp policy.RoutingPolicy,
	data protobuf.Buffer,
) producer.Message {
	return message{
		shard: shard, sp: sp, rp: rp, data: data,
	}
}

func (d message) Shard() uint32 {
	return d.shard
}

func (d message) Bytes() []byte {
	return d.data.Bytes()
}

func (d message) Size() int {
	// Use the cap of the underlying byte slice in the buffer instead of
	// the length of the byte encoded to avoid "memory leak", for example
	// when the underlying buffer is 2KB, and it only encoded 300B, if we
	// use 300 as the size, then a producer with a buffer of 3GB could be
	// actually buffering 20GB in total for the underlying buffers.
	return cap(d.data.Bytes())
}

func (d message) Finalize(producer.FinalizeReason) {
	d.data.Close()
}

type storagePolicyFilter struct {
	acceptedStoragePolicies []policy.StoragePolicy
}

// NewStoragePolicyFilter creates a new storage policy based filter.
func NewStoragePolicyFilter(
	acceptedStoragePolicies []policy.StoragePolicy,
	configSource producer.FilterFuncConfigSourceType) producer.FilterFunc {
	return producer.NewFilterFunc(
		storagePolicyFilter{acceptedStoragePolicies}.Filter,
		producer.StoragePolicyFilter,
		configSource)
}

func (f storagePolicyFilter) Filter(m producer.Message) bool {
	msg, ok := m.(message)
	if !ok {
		return true
	}
	for i := 0; i < len(f.acceptedStoragePolicies); i++ {
		if f.acceptedStoragePolicies[i].Equivalent(msg.sp) {
			return true
		}
	}
	return false
}

// RoutingPolicyFilterParams provides parameters for creating a routing policy filter.
type RoutingPolicyFilterParams struct {
	Logger               *zap.Logger
	Scope                tally.Scope
	RoutingPolicyHandler routing.PolicyHandler
	IsDefault            bool
	AllowedTrafficTypes  []string
}

// NewRoutingPolicyFilter creates a new routing policy based filter.
func NewRoutingPolicyFilter(
	p RoutingPolicyFilterParams, configSource producer.FilterFuncConfigSourceType,
) producer.FilterFunc {
	logger := p.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	cfg := &routingPolicyFilter{
		logger:                 logger,
		scope:                  p.Scope.SubScope("routing-policy-filter"),
		isDefault:              p.IsDefault,
		allowedTrafficTypes:    p.AllowedTrafficTypes,
		allowedTrafficTypeMask: 0,
	}
	p.RoutingPolicyHandler.Subscribe(cfg.onRoutingPolicyConfigUpdate)
	return producer.NewFilterFunc(cfg.Filter, producer.RoutingPolicyFilter, configSource)
}

type routingPolicyFilter struct {
	logger                 *zap.Logger
	scope                  tally.Scope
	isDefault              bool
	allowedTrafficTypes    []string
	allowedTrafficTypeMask uint64
}

func (f *routingPolicyFilter) Filter(m producer.Message) bool {
	msg, ok := m.(message)
	if !ok || msg.rp.TrafficTypes == 0 {
		return f.isDefault
	}
	mask := atomic.LoadUint64(&f.allowedTrafficTypeMask)
	return msg.rp.TrafficTypes&mask != 0
}

func (f *routingPolicyFilter) onRoutingPolicyConfigUpdate(policyConfig routing.PolicyConfig) {
	trafficTypes := policyConfig.TrafficTypes()
	f.logger.Info("updating routing policy config",
		zap.Any("received-traffic-types", trafficTypes))
	mask := uint64(0)
	for _, trafficType := range f.allowedTrafficTypes {
		bitPosition := f.resolveTrafficTypeToBitPosition(trafficTypes, trafficType)
		if bitPosition == -1 {
			f.logger.Warn("traffic type not found in routing policy config",
				zap.String("missing_traffic_type", trafficType))
			f.emitAllowedTrafficType(trafficType, true)
			continue
		}
		f.emitAllowedTrafficType(trafficType, false)
		mask |= 1 << bitPosition
	}
	atomic.StoreUint64(&f.allowedTrafficTypeMask, mask)
}

func (f *routingPolicyFilter) resolveTrafficTypeToBitPosition(
	trafficTypeMap map[string]uint64, trafficType string,
) int {
	bitPosition, ok := trafficTypeMap[trafficType]
	if !ok {
		return -1
	}
	return int(bitPosition)
}

func (f *routingPolicyFilter) emitAllowedTrafficType(
	trafficType string, missingFromRoutingPolicy bool) {
	f.scope.Tagged(map[string]string{
		"traffic-type":                trafficType,
		"missing-from-routing-policy": strconv.FormatBool(missingFromRoutingPolicy),
	}).Counter("allowed-traffic-type").Inc(1)
}
