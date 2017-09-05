// Copyright (c) 2017 Uber Technologies, Inc.
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

package msgpack

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3collector/backend"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/errors"
)

var (
	errServerIsOpenOrClosed    = errors.New("server is already open or closed")
	errServerIsNotOpenOrClosed = errors.New("server is not open or closed")
)

type serverState int

const (
	serverNotOpen serverState = iota
	serverOpen
	serverClosed
)

// server partitions metrics and send them via different routes based on their partitions.
type server struct {
	sync.RWMutex

	opts                       ServerOptions
	nowFn                      clock.NowFn
	shardCutoverWarmupDuration time.Duration
	shardCutoffLingerDuration  time.Duration
	writerMgr                  instanceWriterManager
	shardFn                    ShardFn
	placementWatcher           placement.StagedPlacementWatcher
	state                      serverState
}

// NewServer creates a new server.
func NewServer(opts ServerOptions) backend.Server {
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

	return &server{
		opts:  opts,
		nowFn: opts.ClockOptions().NowFn(),
		shardCutoverWarmupDuration: opts.ShardCutoverWarmupDuration(),
		shardCutoffLingerDuration:  opts.ShardCutoffLingerDuration(),
		writerMgr:                  writerMgr,
		shardFn:                    opts.ShardFn(),
		placementWatcher:           placementWatcher,
	}
}

func (s *server) Open() error {
	s.Lock()
	defer s.Unlock()

	if s.state != serverNotOpen {
		return errServerIsOpenOrClosed
	}
	s.state = serverOpen
	return s.placementWatcher.Watch()
}

func (s *server) WriteCounterWithPoliciesList(
	id []byte,
	val int64,
	pl policy.PoliciesList,
) error {
	mu := unaggregated.MetricUnion{
		Type:       unaggregated.CounterType,
		ID:         id,
		CounterVal: val,
	}
	return s.write(id, mu, pl)
}

func (s *server) WriteBatchTimerWithPoliciesList(
	id []byte,
	val []float64,
	pl policy.PoliciesList,
) error {
	mu := unaggregated.MetricUnion{
		Type:          unaggregated.BatchTimerType,
		ID:            id,
		BatchTimerVal: val,
	}
	return s.write(id, mu, pl)
}

func (s *server) WriteGaugeWithPoliciesList(
	id []byte,
	val float64,
	pl policy.PoliciesList,
) error {
	mu := unaggregated.MetricUnion{
		Type:     unaggregated.GaugeType,
		ID:       id,
		GaugeVal: val,
	}
	return s.write(id, mu, pl)
}

func (s *server) Flush() error {
	s.RLock()
	if s.state != serverOpen {
		s.RUnlock()
		return errServerIsNotOpenOrClosed
	}
	err := s.writerMgr.Flush()
	s.RUnlock()
	return err
}

func (s *server) Close() error {
	s.Lock()
	defer s.Unlock()

	if s.state != serverOpen {
		return errServerIsNotOpenOrClosed
	}
	s.state = serverClosed
	s.placementWatcher.Unwatch() // nolint: errcheck
	return s.writerMgr.Close()
}

func (s *server) write(
	id []byte,
	mu unaggregated.MetricUnion,
	pl policy.PoliciesList,
) error {
	s.RLock()
	if s.state != serverOpen {
		s.RUnlock()
		return errServerIsNotOpenOrClosed
	}
	stagedPlacement, onStagedPlacementDoneFn, err := s.placementWatcher.ActiveStagedPlacement()
	if err != nil {
		s.RUnlock()
		return err
	}
	placement, onPlacementDoneFn, err := stagedPlacement.ActivePlacement()
	if err != nil {
		onStagedPlacementDoneFn()
		s.RUnlock()
		return err
	}
	var (
		numShards = placement.NumShards()
		shardID   = s.shardFn(id, numShards)
		instances = placement.InstancesForShard(shardID)
		nowNanos  = s.nowFn().UnixNano()
		multiErr  = xerrors.NewMultiError()
	)
	for _, instance := range instances {
		// NB(xichen): the shard should technically always be found because the instances
		// are computed from the placement, but protect against errors here regardless.
		shard, ok := instance.Shards().Shard(shardID)
		if !ok {
			err := fmt.Errorf("instance %s does not own shard %d", instance.ID(), shardID)
			multiErr = multiErr.Add(err)
			continue
		}
		if !s.shouldWriteForShard(nowNanos, shard) {
			continue
		}
		if err := s.writerMgr.WriteTo(instance, shardID, mu, pl); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	onPlacementDoneFn()
	onStagedPlacementDoneFn()
	s.RUnlock()
	return err
}

func (s *server) shouldWriteForShard(nowNanos int64, shard shard.Shard) bool {
	writeEarliestNanos, writeLatestNanos := s.writeTimeRangeFor(shard)
	return nowNanos >= writeEarliestNanos && nowNanos <= writeLatestNanos
}

// writeTimeRangeFor returns the time range for writes going to a given shard.
func (s *server) writeTimeRangeFor(shard shard.Shard) (int64, int64) {
	var (
		earliestNanos = int64(0)
		latestNanos   = int64(math.MaxInt64)
	)
	if cutoverNanos := shard.CutoverNanos(); cutoverNanos >= int64(s.shardCutoverWarmupDuration) {
		earliestNanos = cutoverNanos - int64(s.shardCutoverWarmupDuration)
	}
	if cutoffNanos := shard.CutoffNanos(); cutoffNanos <= math.MaxInt64-int64(s.shardCutoffLingerDuration) {
		latestNanos = cutoffNanos + int64(s.shardCutoffLingerDuration)
	}
	return earliestNanos, latestNanos
}
