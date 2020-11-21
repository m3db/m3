package client

import (
	"fmt"
	"math"
	"time"

	"github.com/m3db/m3/src/aggregator/sharding"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/x/clock"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/uber-go/tally"
)

// LegacyClient is raw Protobuf over TCP M3 Aggregator client
type LegacyClient struct {
	nowFn                      clock.NowFn
	shardCutoverWarmupDuration time.Duration
	shardCutoffLingerDuration  time.Duration
	writerMgr                  instanceWriterManager
	shardFn                    sharding.ShardFn
	placementWatcher           placement.StagedPlacementWatcher
	metrics                    legacyClientMetrics
}

// NewLegacyClient returns new Protobuf over TCP M3 Aggregator client
func NewLegacyClient(opts Options) (*LegacyClient, error) {
	c := &LegacyClient{
		metrics: newLegacyClientMetrics(nil),
	}
	if err := c.placementWatcher.Watch(); err != nil {
		return nil, err
	}
	return c, nil
}

// Init does nothing for LegacyClient, just satisfies Client interface
func (c *LegacyClient) Init() error {
	return nil
}

// WriteUntimedCounter writes untimed counter metrics.
func (c *LegacyClient) WriteUntimedCounter(
	counter unaggregated.Counter,
	metadatas metadata.StagedMetadatas,
) error {
	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    counter.ToUnion(),
			metadatas: metadatas,
		},
	}

	c.metrics.writeUntimedCounter.Inc(1)
	return c.write(counter.ID, c.nowFn().UnixNano(), payload)
}

// WriteUntimedBatchTimer writes untimed batch timer metrics.
func (c *LegacyClient) WriteUntimedBatchTimer(
	batchTimer unaggregated.BatchTimer,
	metadatas metadata.StagedMetadatas,
) error {
	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    batchTimer.ToUnion(),
			metadatas: metadatas,
		},
	}

	c.metrics.writeUntimedBatchTimer.Inc(1)
	return c.write(batchTimer.ID, c.nowFn().UnixNano(), payload)
}

// WriteUntimedGauge writes untimed gauge metrics.
func (c *LegacyClient) WriteUntimedGauge(
	gauge unaggregated.Gauge,
	metadatas metadata.StagedMetadatas,
) error {
	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    gauge.ToUnion(),
			metadatas: metadatas,
		},
	}

	c.metrics.writeUntimedGauge.Inc(1)
	return c.write(gauge.ID, c.nowFn().UnixNano(), payload)
}

// WriteTimed writes timed metrics.
func (c *LegacyClient) WriteTimed(
	metric aggregated.Metric,
	metadata metadata.TimedMetadata,
) error {
	payload := payloadUnion{
		payloadType: timedType,
		timed: timedPayload{
			metric:   metric,
			metadata: metadata,
		},
	}

	c.metrics.writeForwarded.Inc(1)
	return c.write(metric.ID, metric.TimeNanos, payload)
}

// WritePassthrough writes passthrough metrics.
func (c *LegacyClient) WritePassthrough(
	metric aggregated.Metric,
	storagePolicy policy.StoragePolicy,
) error {
	payload := payloadUnion{
		payloadType: passthroughType,
		passthrough: passthroughPayload{
			metric:        metric,
			storagePolicy: storagePolicy,
		},
	}

	c.metrics.writePassthrough.Inc(1)
	return c.write(metric.ID, metric.TimeNanos, payload)
}

// WriteTimedWithStagedMetadatas writes timed metrics with staged metadatas.
func (c *LegacyClient) WriteTimedWithStagedMetadatas(
	metric aggregated.Metric,
	metadatas metadata.StagedMetadatas,
) error {
	payload := payloadUnion{
		payloadType: timedWithStagedMetadatasType,
		timedWithStagedMetadatas: timedWithStagedMetadatas{
			metric:    metric,
			metadatas: metadatas,
		},
	}

	c.metrics.writeForwarded.Inc(1)
	return c.write(metric.ID, metric.TimeNanos, payload)
}

// WriteForwarded writes forwarded metrics.
func (c *LegacyClient) WriteForwarded(
	metric aggregated.ForwardedMetric,
	metadata metadata.ForwardMetadata,
) error {
	payload := payloadUnion{
		payloadType: forwardedType,
		forwarded: forwardedPayload{
			metric:   metric,
			metadata: metadata,
		},
	}

	c.metrics.writeForwarded.Inc(1)
	return c.write(metric.ID, metric.TimeNanos, payload)
}

// Flush flushes any remaining data buffered by the client.
func (c *LegacyClient) Flush() error {
	c.metrics.flush.Inc(1)
	return c.writerMgr.Flush()
}

// Close closes the client.
func (c *LegacyClient) Close() error {
	c.placementWatcher.Unwatch() // nolint: errcheck
	// writerMgr errors out if trying to close twice
	return c.writerMgr.Close()
}

func (c *LegacyClient) write(
	metricID id.RawID,
	timeNanos int64,
	payload payloadUnion,
) error {
	stagedPlacement, onStagedPlacementDoneFn, err := c.placementWatcher.ActiveStagedPlacement()
	if err != nil {
		return err
	}
	placement, onPlacementDoneFn, err := stagedPlacement.ActivePlacement()
	if err != nil {
		onStagedPlacementDoneFn()
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
	return multiErr.FinalError()
}

func (c *LegacyClient) shouldWriteForShard(nowNanos int64, shard shard.Shard) bool {
	writeEarliestNanos, writeLatestNanos := c.writeTimeRangeFor(shard)
	return nowNanos >= writeEarliestNanos && nowNanos <= writeLatestNanos
}

// writeTimeRangeFor returns the time range for writes going to a given shard.
func (c *LegacyClient) writeTimeRangeFor(shard shard.Shard) (int64, int64) {
	var (
		cutoverNanos  = shard.CutoverNanos()
		cutoffNanos   = shard.CutoffNanos()
		earliestNanos = int64(0)
		latestNanos   = int64(math.MaxInt64)
	)

	if cutoverNanos >= int64(c.shardCutoverWarmupDuration) {
		earliestNanos = cutoverNanos - int64(c.shardCutoverWarmupDuration)
	}

	if cutoffNanos <= math.MaxInt64-int64(c.shardCutoffLingerDuration) {
		latestNanos = cutoffNanos + int64(c.shardCutoffLingerDuration)
	}
	return earliestNanos, latestNanos
}

type legacyClientMetrics struct {
	writeUntimedCounter    tally.Counter
	writeUntimedBatchTimer tally.Counter
	writeUntimedGauge      tally.Counter
	writePassthrough       tally.Counter
	writeForwarded         tally.Counter
	flush                  tally.Counter
	shardNotOwned          tally.Counter
	shardNotWriteable      tally.Counter
}

func newLegacyClientMetrics(
	scope tally.Scope,
) legacyClientMetrics {
	return legacyClientMetrics{
		writeUntimedCounter:    scope.Counter("writeUntimedCounter"),
		writeUntimedBatchTimer: scope.Counter("writeUntimedBatchTimer"),
		writeUntimedGauge:      scope.Counter("writeUntimedGauge"),
		writePassthrough:       scope.Counter("writePassthrough"),
		writeForwarded:         scope.Counter("writeForwarded"),
		flush:                  scope.Counter("flush"),
		shardNotOwned:          scope.Counter("shard-not-owned"),
		shardNotWriteable:      scope.Counter("shard-not-writeable"),
	}
}
