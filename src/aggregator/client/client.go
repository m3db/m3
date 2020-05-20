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

package client

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/m3db/m3/src/aggregator/sharding"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/x/clock"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/uber-go/tally"
)

var (
	errClientIsInitializedOrClosed   = errors.New("client is already initialized or closed")
	errClientIsUninitializedOrClosed = errors.New("client is uninitialized or closed")
)

// Client is a client capable of writing different types of metrics to the aggregation clients.
type Client interface {
	// Init initializes the client.
	Init() error

	// WriteUntimedCounter writes untimed counter metrics.
	WriteUntimedCounter(
		counter unaggregated.Counter,
		metadatas metadata.StagedMetadatas,
	) error

	// WriteUntimedBatchTimer writes untimed batch timer metrics.
	WriteUntimedBatchTimer(
		batchTimer unaggregated.BatchTimer,
		metadatas metadata.StagedMetadatas,
	) error

	// WriteUntimedGauge writes untimed gauge metrics.
	WriteUntimedGauge(
		gauge unaggregated.Gauge,
		metadatas metadata.StagedMetadatas,
	) error

	// WriteTimed writes timed metrics.
	WriteTimed(
		metric aggregated.Metric,
		metadata metadata.TimedMetadata,
	) error

	// WritePassthrough writes passthrough metrics.
	WritePassthrough(
		metric aggregated.Metric,
		storagePolicy policy.StoragePolicy,
	) error

	// WriteTimedWithStagedMetadatas writes timed metrics with staged metadatas.
	WriteTimedWithStagedMetadatas(
		metric aggregated.Metric,
		metadatas metadata.StagedMetadatas,
	) error

	// Flush flushes any remaining data buffered by the client.
	Flush() error

	// Close closes the client.
	Close() error
}

// AdminClient is an administrative client capable of performing regular client operations
// as well as high-privilege operations such as internal communcations among aggregation
// servers that regular client is not permissioned to do.
type AdminClient interface {
	Client

	// WriteForwarded writes forwarded metrics.
	WriteForwarded(
		metric aggregated.ForwardedMetric,
		metadata metadata.ForwardMetadata,
	) error
}

type clientState int

const (
	clientUninitialized clientState = iota
	clientInitialized
	clientClosed
)

type clientMetrics struct {
	writeUntimedCounter    instrument.MethodMetrics
	writeUntimedBatchTimer instrument.MethodMetrics
	writeUntimedGauge      instrument.MethodMetrics
	writePassthrough       instrument.MethodMetrics
	writeForwarded         instrument.MethodMetrics
	flush                  instrument.MethodMetrics
	shardNotOwned          tally.Counter
	shardNotWriteable      tally.Counter
}

func newClientMetrics(
	scope tally.Scope,
	opts instrument.TimerOptions,
) clientMetrics {
	return clientMetrics{
		writeUntimedCounter:    instrument.NewMethodMetrics(scope, "writeUntimedCounter", opts),
		writeUntimedBatchTimer: instrument.NewMethodMetrics(scope, "writeUntimedBatchTimer", opts),
		writeUntimedGauge:      instrument.NewMethodMetrics(scope, "writeUntimedGauge", opts),
		writePassthrough:       instrument.NewMethodMetrics(scope, "writePassthrough", opts),
		writeForwarded:         instrument.NewMethodMetrics(scope, "writeForwarded", opts),
		flush:                  instrument.NewMethodMetrics(scope, "flush", opts),
		shardNotOwned:          scope.Counter("shard-not-owned"),
		shardNotWriteable:      scope.Counter("shard-not-writeable"),
	}
}

// client partitions metrics and send them via different routes based on their partitions.
type client struct {
	sync.RWMutex

	opts                 Options
	aggregatorClientType AggregatorClientType
	state                clientState

	m3msg m3msgClient

	nowFn                      clock.NowFn
	shardCutoverWarmupDuration time.Duration
	shardCutoffLingerDuration  time.Duration
	writerMgr                  instanceWriterManager
	shardFn                    sharding.ShardFn
	placementWatcher           placement.StagedPlacementWatcher

	metrics clientMetrics
}

type m3msgClient struct {
	producer    producer.Producer
	numShards   uint32
	messagePool *messagePool
}

// NewClient creates a new client.
func NewClient(opts Options) (Client, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	var (
		clientType       = opts.AggregatorClientType()
		instrumentOpts   = opts.InstrumentOptions()
		msgClient        m3msgClient
		writerMgr        instanceWriterManager
		placementWatcher placement.StagedPlacementWatcher
	)
	switch clientType {
	case M3MsgAggregatorClient:
		m3msgOpts := opts.M3MsgOptions()
		if err := m3msgOpts.Validate(); err != nil {
			return nil, err
		}

		producer := m3msgOpts.Producer()
		if err := producer.Init(); err != nil {
			return nil, err
		}

		msgClient = m3msgClient{
			producer:    producer,
			numShards:   producer.NumShards(),
			messagePool: newMessagePool(),
		}
	case LegacyAggregatorClient:
		writerMgrScope := instrumentOpts.MetricsScope().SubScope("writer-manager")
		writerMgrOpts := opts.SetInstrumentOptions(instrumentOpts.SetMetricsScope(writerMgrScope))
		writerMgr = newInstanceWriterManager(writerMgrOpts)
		onPlacementsAddedFn := func(placements []placement.Placement) {
			for _, placement := range placements {
				writerMgr.AddInstances(placement.Instances()) // nolint: errcheck
			}
		}
		onPlacementsRemovedFn := func(placements []placement.Placement) {
			for _, placement := range placements {
				writerMgr.RemoveInstances(placement.Instances()) // nolint: errcheck
			}
		}
		activeStagedPlacementOpts := placement.NewActiveStagedPlacementOptions().
			SetClockOptions(opts.ClockOptions()).
			SetOnPlacementsAddedFn(onPlacementsAddedFn).
			SetOnPlacementsRemovedFn(onPlacementsRemovedFn)
		placementWatcherOpts := opts.StagedPlacementWatcherOptions().
			SetActiveStagedPlacementOptions(activeStagedPlacementOpts)
		placementWatcher = placement.NewStagedPlacementWatcher(placementWatcherOpts)
	default:
		return nil, fmt.Errorf("unrecognized client type: %v", clientType)
	}

	return &client{
		aggregatorClientType:       clientType,
		m3msg:                      msgClient,
		opts:                       opts,
		nowFn:                      opts.ClockOptions().NowFn(),
		shardCutoverWarmupDuration: opts.ShardCutoverWarmupDuration(),
		shardCutoffLingerDuration:  opts.ShardCutoffLingerDuration(),
		writerMgr:                  writerMgr,
		shardFn:                    opts.ShardFn(),
		placementWatcher:           placementWatcher,
		metrics: newClientMetrics(instrumentOpts.MetricsScope(),
			instrumentOpts.TimerOptions()),
	}, nil
}

func (c *client) Init() error {
	c.Lock()
	defer c.Unlock()

	if c.state != clientUninitialized {
		return errClientIsInitializedOrClosed
	}

	switch c.aggregatorClientType {
	case M3MsgAggregatorClient:
		// Nothing more to do.
	case LegacyAggregatorClient:
		if err := c.placementWatcher.Watch(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unrecognized client type: %v", c.aggregatorClientType)
	}

	c.state = clientInitialized
	return nil
}

func (c *client) WriteUntimedCounter(
	counter unaggregated.Counter,
	metadatas metadata.StagedMetadatas,
) error {
	callStart := c.nowFn()
	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    counter.ToUnion(),
			metadatas: metadatas,
		},
	}
	err := c.write(counter.ID, c.nowNanos(), payload)
	c.metrics.writeUntimedCounter.ReportSuccessOrError(err, c.nowFn().Sub(callStart))
	return err
}

func (c *client) WriteUntimedBatchTimer(
	batchTimer unaggregated.BatchTimer,
	metadatas metadata.StagedMetadatas,
) error {
	callStart := c.nowFn()
	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    batchTimer.ToUnion(),
			metadatas: metadatas,
		},
	}
	err := c.write(batchTimer.ID, c.nowNanos(), payload)
	c.metrics.writeUntimedBatchTimer.ReportSuccessOrError(err, c.nowFn().Sub(callStart))
	return err
}

func (c *client) WriteUntimedGauge(
	gauge unaggregated.Gauge,
	metadatas metadata.StagedMetadatas,
) error {
	callStart := c.nowFn()
	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    gauge.ToUnion(),
			metadatas: metadatas,
		},
	}
	err := c.write(gauge.ID, c.nowNanos(), payload)
	c.metrics.writeUntimedGauge.ReportSuccessOrError(err, c.nowFn().Sub(callStart))
	return err
}

func (c *client) WriteTimed(
	metric aggregated.Metric,
	metadata metadata.TimedMetadata,
) error {
	callStart := c.nowFn()
	payload := payloadUnion{
		payloadType: timedType,
		timed: timedPayload{
			metric:   metric,
			metadata: metadata,
		},
	}
	err := c.write(metric.ID, metric.TimeNanos, payload)
	c.metrics.writeForwarded.ReportSuccessOrError(err, c.nowFn().Sub(callStart))
	return err
}

func (c *client) WritePassthrough(
	metric aggregated.Metric,
	storagePolicy policy.StoragePolicy,
) error {
	callStart := c.nowFn()
	payload := payloadUnion{
		payloadType: passthroughType,
		passthrough: passthroughPayload{
			metric:        metric,
			storagePolicy: storagePolicy,
		},
	}
	err := c.write(metric.ID, metric.TimeNanos, payload)
	c.metrics.writePassthrough.ReportSuccessOrError(err, c.nowFn().Sub(callStart))
	return err
}

func (c *client) WriteTimedWithStagedMetadatas(
	metric aggregated.Metric,
	metadatas metadata.StagedMetadatas,
) error {
	callStart := c.nowFn()
	payload := payloadUnion{
		payloadType: timedWithStagedMetadatasType,
		timedWithStagedMetadatas: timedWithStagedMetadatas{
			metric:    metric,
			metadatas: metadatas,
		},
	}
	err := c.write(metric.ID, metric.TimeNanos, payload)
	c.metrics.writeForwarded.ReportSuccessOrError(err, c.nowFn().Sub(callStart))
	return err
}

func (c *client) WriteForwarded(
	metric aggregated.ForwardedMetric,
	metadata metadata.ForwardMetadata,
) error {
	callStart := c.nowFn()
	payload := payloadUnion{
		payloadType: forwardedType,
		forwarded: forwardedPayload{
			metric:   metric,
			metadata: metadata,
		},
	}
	err := c.write(metric.ID, metric.TimeNanos, payload)
	c.metrics.writeForwarded.ReportSuccessOrError(err, c.nowFn().Sub(callStart))
	return err
}

func (c *client) Flush() error {
	var (
		callStart = c.nowFn()
		err       error
	)
	c.RLock()
	defer c.RUnlock()

	if c.state != clientInitialized {
		return errClientIsUninitializedOrClosed
	}

	switch c.aggregatorClientType {
	case LegacyAggregatorClient:
		err = c.writerMgr.Flush()
		c.metrics.flush.ReportSuccessOrError(err, c.nowFn().Sub(callStart))
	}

	return err
}

func (c *client) Close() error {
	c.Lock()
	defer c.Unlock()

	if c.state != clientInitialized {
		return errClientIsUninitializedOrClosed
	}

	var err error
	switch c.aggregatorClientType {
	case M3MsgAggregatorClient:
		c.m3msg.producer.Close(producer.WaitForConsumption)
	case LegacyAggregatorClient:
		c.placementWatcher.Unwatch() // nolint: errcheck
		err = c.writerMgr.Close()
	default:
		return fmt.Errorf("unrecognized client type: %v", c.aggregatorClientType)
	}

	c.state = clientClosed

	return err
}

func (c *client) write(metricID id.RawID, timeNanos int64, payload payloadUnion) error {
	switch c.aggregatorClientType {
	case LegacyAggregatorClient:
		return c.writeLegacy(metricID, timeNanos, payload)
	case M3MsgAggregatorClient:
		return c.writeM3Msg(metricID, timeNanos, payload)
	default:
		return fmt.Errorf("unrecognized client type: %v", c.aggregatorClientType)
	}
}

func (c *client) writeLegacy(metricID id.RawID, timeNanos int64, payload payloadUnion) error {
	c.RLock()
	if c.state != clientInitialized {
		c.RUnlock()
		return errClientIsUninitializedOrClosed
	}
	stagedPlacement, onStagedPlacementDoneFn, err := c.placementWatcher.ActiveStagedPlacement()
	if err != nil {
		c.RUnlock()
		return err
	}
	placement, onPlacementDoneFn, err := stagedPlacement.ActivePlacement()
	if err != nil {
		onStagedPlacementDoneFn()
		c.RUnlock()
		return err
	}
	var (
		shardID   = c.shardFn(metricID, uint32(placement.NumShards()))
		instances = placement.InstancesForShard(shardID)
		multiErr  = xerrors.NewMultiError()
	)
	for _, instance := range instances {
		// NB(xichen): the shard should technically always be found because the instances
		// are computed from the placement, but protect against errors here regardless.
		shard, ok := instance.Shards().Shard(shardID)
		if !ok {
			err = fmt.Errorf("instance %s does not own shard %d", instance.ID(), shardID)
			multiErr = multiErr.Add(err)
			c.metrics.shardNotOwned.Inc(1)
			continue
		}
		if !c.shouldWriteForShard(timeNanos, shard) {
			c.metrics.shardNotWriteable.Inc(1)
			continue
		}
		if err = c.writerMgr.Write(instance, shardID, payload); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	onPlacementDoneFn()
	onStagedPlacementDoneFn()
	c.RUnlock()
	return multiErr.FinalError()
}

func (c *client) writeM3Msg(metricID id.RawID, timeNanos int64, payload payloadUnion) error {
	shard := c.shardFn(metricID, c.m3msg.numShards)

	msg := c.m3msg.messagePool.Get()
	if err := msg.Encode(shard, payload); err != nil {
		msg.Finalize(producer.Dropped)
		return err
	}

	if err := c.m3msg.producer.Produce(msg); err != nil {
		msg.Finalize(producer.Dropped)
		return err
	}

	return nil
}

func (c *client) shouldWriteForShard(nowNanos int64, shard shard.Shard) bool {
	writeEarliestNanos, writeLatestNanos := c.writeTimeRangeFor(shard)
	return nowNanos >= writeEarliestNanos && nowNanos <= writeLatestNanos
}

// writeTimeRangeFor returns the time range for writes going to a given shard.
func (c *client) writeTimeRangeFor(shard shard.Shard) (int64, int64) {
	var (
		earliestNanos = int64(0)
		latestNanos   = int64(math.MaxInt64)
	)
	if cutoverNanos := shard.CutoverNanos(); cutoverNanos >= int64(c.shardCutoverWarmupDuration) {
		earliestNanos = cutoverNanos - int64(c.shardCutoverWarmupDuration)
	}
	if cutoffNanos := shard.CutoffNanos(); cutoffNanos <= math.MaxInt64-int64(c.shardCutoffLingerDuration) {
		latestNanos = cutoffNanos + int64(c.shardCutoffLingerDuration)
	}
	return earliestNanos, latestNanos
}

func (c *client) nowNanos() int64 {
	return c.nowFn().UnixNano()
}

type messagePool struct {
	pool sync.Pool
}

func newMessagePool() *messagePool {
	p := &messagePool{}
	p.pool.New = func() interface{} {
		return newMessage(p)
	}
	return p
}

func (m *messagePool) Get() *message {
	return m.pool.Get().(*message)
}

func (m *messagePool) Put(msg *message) {
	m.pool.Put(msg)
}

// Ensure message implements m3msg producer message interface.
var _ producer.Message = (*message)(nil)

type message struct {
	pool  *messagePool
	shard uint32

	metric metricpb.MetricWithMetadatas
	cm     metricpb.CounterWithMetadatas
	bm     metricpb.BatchTimerWithMetadatas
	gm     metricpb.GaugeWithMetadatas
	fm     metricpb.ForwardedMetricWithMetadata
	tm     metricpb.TimedMetricWithMetadata
	tms    metricpb.TimedMetricWithMetadatas

	buf []byte
}

func newMessage(pool *messagePool) *message {
	return &message{
		pool: pool,
	}
}

func (m *message) Encode(
	shard uint32,
	payload payloadUnion,
) error {
	m.shard = shard

	switch payload.payloadType {
	case untimedType:
		switch payload.untimed.metric.Type {
		case metric.CounterType:
			value := unaggregated.CounterWithMetadatas{
				Counter:         payload.untimed.metric.Counter(),
				StagedMetadatas: payload.untimed.metadatas,
			}
			if err := value.ToProto(&m.cm); err != nil {
				return err
			}

			m.metric = metricpb.MetricWithMetadatas{
				Type:                 metricpb.MetricWithMetadatas_COUNTER_WITH_METADATAS,
				CounterWithMetadatas: &m.cm,
			}
		case metric.TimerType:
			value := unaggregated.BatchTimerWithMetadatas{
				BatchTimer:      payload.untimed.metric.BatchTimer(),
				StagedMetadatas: payload.untimed.metadatas,
			}
			if err := value.ToProto(&m.bm); err != nil {
				return err
			}

			m.metric = metricpb.MetricWithMetadatas{
				Type:                    metricpb.MetricWithMetadatas_BATCH_TIMER_WITH_METADATAS,
				BatchTimerWithMetadatas: &m.bm,
			}
		case metric.GaugeType:
			value := unaggregated.GaugeWithMetadatas{
				Gauge:           payload.untimed.metric.Gauge(),
				StagedMetadatas: payload.untimed.metadatas,
			}
			if err := value.ToProto(&m.gm); err != nil {
				return err
			}

			m.metric = metricpb.MetricWithMetadatas{
				Type:               metricpb.MetricWithMetadatas_GAUGE_WITH_METADATAS,
				GaugeWithMetadatas: &m.gm,
			}
		default:
			return fmt.Errorf("unrecognized metric type: %v",
				payload.untimed.metric.Type)
		}
	case forwardedType:
		value := aggregated.ForwardedMetricWithMetadata{
			ForwardedMetric: payload.forwarded.metric,
			ForwardMetadata: payload.forwarded.metadata,
		}
		if err := value.ToProto(&m.fm); err != nil {
			return err
		}

		m.metric = metricpb.MetricWithMetadatas{
			Type:                        metricpb.MetricWithMetadatas_FORWARDED_METRIC_WITH_METADATA,
			ForwardedMetricWithMetadata: &m.fm,
		}
	case timedType:
		value := aggregated.TimedMetricWithMetadata{
			Metric:        payload.timed.metric,
			TimedMetadata: payload.timed.metadata,
		}
		if err := value.ToProto(&m.tm); err != nil {
			return err
		}

		m.metric = metricpb.MetricWithMetadatas{
			Type:                    metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_METADATA,
			TimedMetricWithMetadata: &m.tm,
		}
	case timedWithStagedMetadatasType:
		value := aggregated.TimedMetricWithMetadatas{
			Metric:          payload.timedWithStagedMetadatas.metric,
			StagedMetadatas: payload.timedWithStagedMetadatas.metadatas,
		}
		if err := value.ToProto(&m.tms); err != nil {
			return err
		}

		m.metric = metricpb.MetricWithMetadatas{
			Type:                     metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_METADATAS,
			TimedMetricWithMetadatas: &m.tms,
		}
	default:
		return fmt.Errorf("unrecognized payload type: %v",
			payload.payloadType)
	}

	size := m.metric.Size()
	if size > cap(m.buf) {
		const growthFactor = 2
		m.buf = make([]byte, int(growthFactor*float64(size)))
	}

	// Resize buffer to exactly how long we need for marshalling.
	m.buf = m.buf[:size]

	_, err := m.metric.MarshalTo(m.buf)
	return err
}

func (m *message) Shard() uint32 {
	return m.shard
}

func (m *message) Bytes() []byte {
	return m.buf
}

func (m *message) Size() int {
	return len(m.buf)
}

func (m *message) Finalize(reason producer.FinalizeReason) {
	// Return to pool.
	m.pool.Put(m)
}
