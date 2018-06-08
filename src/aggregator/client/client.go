package client

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3x/clock"
	xerrors "github.com/m3db/m3x/errors"
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

	// Flush flushes any remaining data buffered by the client.
	Flush() error

	// Close closes the client.
	Close() error
}

type clientState int

const (
	clientUninitialized clientState = iota
	clientInitialized
	clientClosed
)

// client partitions metrics and send them via different routes based on their partitions.
type client struct {
	sync.RWMutex

	opts                       Options
	nowFn                      clock.NowFn
	shardCutoverWarmupDuration time.Duration
	shardCutoffLingerDuration  time.Duration
	writerMgr                  instanceWriterManager
	shardFn                    ShardFn
	placementWatcher           placement.StagedPlacementWatcher
	state                      clientState
}

// NewClient creates a new client.
func NewClient(opts Options) Client {
	var (
		instrumentOpts = opts.InstrumentOptions()
		writerScope    = instrumentOpts.MetricsScope().SubScope("writer")
		writerOpts     = opts.SetInstrumentOptions(instrumentOpts.SetMetricsScope(writerScope))
		writerMgr      = newInstanceWriterManager(writerOpts)
	)
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
	placementWatcherOpts := opts.StagedPlacementWatcherOptions().SetActiveStagedPlacementOptions(activeStagedPlacementOpts)
	placementWatcher := placement.NewStagedPlacementWatcher(placementWatcherOpts)

	return &client{
		opts:  opts,
		nowFn: opts.ClockOptions().NowFn(),
		shardCutoverWarmupDuration: opts.ShardCutoverWarmupDuration(),
		shardCutoffLingerDuration:  opts.ShardCutoffLingerDuration(),
		writerMgr:                  writerMgr,
		shardFn:                    opts.ShardFn(),
		placementWatcher:           placementWatcher,
	}
}

func (c *client) Init() error {
	c.Lock()
	defer c.Unlock()

	if c.state != clientUninitialized {
		return errClientIsInitializedOrClosed
	}
	c.state = clientInitialized
	return c.placementWatcher.Watch()
}

func (c *client) WriteUntimedCounter(
	counter unaggregated.Counter,
	metadatas metadata.StagedMetadatas,
) error {
	return c.writeUntimed(counter.ToUnion(), metadatas)
}

func (c *client) WriteUntimedBatchTimer(
	batchTimer unaggregated.BatchTimer,
	metadatas metadata.StagedMetadatas,
) error {
	return c.writeUntimed(batchTimer.ToUnion(), metadatas)
}

func (c *client) WriteUntimedGauge(
	gauge unaggregated.Gauge,
	metadatas metadata.StagedMetadatas,
) error {
	return c.writeUntimed(gauge.ToUnion(), metadatas)
}

func (c *client) Flush() error {
	c.RLock()
	if c.state != clientInitialized {
		c.RUnlock()
		return errClientIsUninitializedOrClosed
	}
	err := c.writerMgr.Flush()
	c.RUnlock()
	return err
}

func (c *client) Close() error {
	c.Lock()
	defer c.Unlock()

	if c.state != clientInitialized {
		return errClientIsUninitializedOrClosed
	}
	c.state = clientClosed
	c.placementWatcher.Unwatch() // nolint: errcheck
	return c.writerMgr.Close()
}

func (c *client) writeUntimed(
	metric unaggregated.MetricUnion,
	metadatas metadata.StagedMetadatas,
) error {
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
		numShards = placement.NumShards()
		shardID   = c.shardFn(metric.ID, numShards)
		instances = placement.InstancesForShard(shardID)
		nowNanos  = c.nowFn().UnixNano()
		multiErr  = xerrors.NewMultiError()
	)
	for _, instance := range instances {
		// NB(xichen): the shard should technically always be found because the instances
		// are computed from the placement, but protect against errors here regardless.
		shard, ok := instance.Shards().Shard(shardID)
		if !ok {
			err = fmt.Errorf("instance %s does not own shard %d", instance.ID(), shardID)
			multiErr = multiErr.Add(err)
			continue
		}
		if !c.shouldWriteForShard(nowNanos, shard) {
			continue
		}
		if err = c.writerMgr.WriteUntimed(instance, shardID, metric, metadatas); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	onPlacementDoneFn()
	onStagedPlacementDoneFn()
	c.RUnlock()
	return multiErr.FinalError()
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
