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
	"sync"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3collector/backend"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
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

	opts             ServerOptions
	writerMgr        instanceWriterManager
	shardFn          ShardFn
	placementWatcher services.StagedPlacementWatcher
	state            serverState
}

// NewServer creates a new server.
func NewServer(opts ServerOptions) backend.Server {
	var (
		instrumentOpts = opts.InstrumentOptions()
		writerScope    = instrumentOpts.MetricsScope().SubScope("writer")
		writerOpts     = opts.SetInstrumentOptions(instrumentOpts.SetMetricsScope(writerScope))
		writerMgr      = newInstanceWriterManager(writerOpts)
	)
	onPlacementsAddedFn := func(placements []services.Placement) {
		for _, placement := range placements {
			writerMgr.AddInstances(placement.Instances()) // nolint: errcheck
		}
	}
	onPlacementsRemovedFn := func(placements []services.Placement) {
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
		opts:             opts,
		writerMgr:        writerMgr,
		shardFn:          opts.ShardFn(),
		placementWatcher: placementWatcher,
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
	numShards := placement.NumShards()
	shard := s.shardFn(id, numShards)
	instances := placement.InstancesForShard(shard)
	err = s.writerMgr.WriteTo(instances, shard, mu, pl)
	onPlacementDoneFn()
	onStagedPlacementDoneFn()
	s.RUnlock()
	return err
}
