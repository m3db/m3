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

package aggregator

import (
	"errors"
	"sync"

	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3x/clock"

	"github.com/uber-go/tally"
)

var (
	// ErrInstanceNotFoundInPlacement is returned when instance is not found in placement.
	ErrInstanceNotFoundInPlacement = errors.New("instance not found in placement")

	errPlacementManagerNotOpenOrClosed = errors.New("placement manager not open or closed")
	errPlacementManagerOpenOrClosed    = errors.New("placement manager already open or closed")
)

// PlacementManager manages agg tier placements.
type PlacementManager interface {
	// Open opens the placement manager.
	Open() error

	// Placement returns the active staged placement and the active placement.
	Placement() (placement.ActiveStagedPlacement, placement.Placement, error)

	// Instance returns the current instance in the current placement.
	Instance() (placement.Instance, error)

	// InstanceFrom returns the current instance from the given placement.
	InstanceFrom(placement placement.Placement) (placement.Instance, error)

	// HasReplacementInstance returns true if there is an instance in the same group replacing
	// the current instance, and false otherwise.
	HasReplacementInstance() (bool, error)

	// Shards returns the current shards owned by the instance.
	Shards() (shard.Shards, error)

	// Close closes the placement manager.
	Close() error
}

type placementManagerMetrics struct {
	activeStagedPlacementErrors tally.Counter
	activePlacementErrors       tally.Counter
	instanceNotFound            tally.Counter
}

func newPlacementManagerMetrics(scope tally.Scope) placementManagerMetrics {
	return placementManagerMetrics{
		activeStagedPlacementErrors: scope.Counter("active-staged-placement-errors"),
		activePlacementErrors:       scope.Counter("active-placement-errors"),
		instanceNotFound:            scope.Counter("instance-not-found"),
	}
}

type placementManagerState int

const (
	placementManagerNotOpen placementManagerState = iota
	placementManagerOpen
	placementManagerClosed
)

type placementManager struct {
	sync.RWMutex

	nowFn            clock.NowFn
	instanceID       string
	placementWatcher placement.StagedPlacementWatcher

	state   placementManagerState
	metrics placementManagerMetrics
}

// NewPlacementManager creates a new placement manager.
func NewPlacementManager(opts PlacementManagerOptions) PlacementManager {
	instrumentOpts := opts.InstrumentOptions()
	return &placementManager{
		nowFn:            opts.ClockOptions().NowFn(),
		instanceID:       opts.InstanceID(),
		placementWatcher: opts.StagedPlacementWatcher(),
		metrics:          newPlacementManagerMetrics(instrumentOpts.MetricsScope()),
	}
}

func (mgr *placementManager) Open() error {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.state != placementManagerNotOpen {
		return errPlacementManagerOpenOrClosed
	}
	if err := mgr.placementWatcher.Watch(); err != nil {
		return err
	}
	mgr.state = placementManagerOpen
	return nil
}

func (mgr *placementManager) Placement() (placement.ActiveStagedPlacement, placement.Placement, error) {
	mgr.RLock()
	stagedPlacement, placement, err := mgr.placementWithLock()
	mgr.RUnlock()
	return stagedPlacement, placement, err
}

func (mgr *placementManager) Instance() (placement.Instance, error) {
	mgr.RLock()
	instance, err := mgr.instanceWithLock()
	mgr.RUnlock()
	return instance, err
}

func (mgr *placementManager) InstanceFrom(placement placement.Placement) (placement.Instance, error) {
	return mgr.instanceFrom(placement)
}

// TODO(xichen): move the method to placement interface.
func (mgr *placementManager) HasReplacementInstance() (bool, error) {
	_, placement, err := mgr.Placement()
	if err != nil {
		return false, err
	}
	currInstance, err := mgr.instanceFrom(placement)
	if err != nil {
		return false, err
	}
	currShardSetID := currInstance.ShardSetID()
	currShards := currInstance.Shards().All()
	for _, currShard := range currShards {
		if currShard.State() != shard.Leaving {
			return false, nil
		}
	}
	allInstances := placement.Instances()
	for _, instance := range allInstances {
		if instance.ShardSetID() != currShardSetID || instance.ID() == mgr.instanceID {
			continue
		}
		otherShards := instance.Shards().All()
		if len(otherShards) != len(currShards) {
			continue
		}
		match := true
		for i := 0; i < len(otherShards); i++ {
			if otherShards[i].State() != shard.Initializing ||
				otherShards[i].ID() != currShards[i].ID() ||
				otherShards[i].CutoverNanos() != currShards[i].CutoffNanos() {
				match = false
				break
			}
		}
		if match {
			return true, nil
		}
	}
	return false, nil
}

func (mgr *placementManager) Shards() (shard.Shards, error) {
	mgr.RLock()
	instance, err := mgr.instanceWithLock()
	mgr.RUnlock()
	if err != nil {
		return nil, err
	}
	return instance.Shards(), nil
}

func (mgr *placementManager) Close() error {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.state != placementManagerOpen {
		return errPlacementManagerNotOpenOrClosed
	}
	if err := mgr.placementWatcher.Unwatch(); err != nil {
		return err
	}
	mgr.state = placementManagerClosed
	return nil
}

func (mgr *placementManager) placementWithLock() (placement.ActiveStagedPlacement, placement.Placement, error) {
	if mgr.state != placementManagerOpen {
		return nil, nil, errPlacementManagerNotOpenOrClosed
	}

	// NB(xichen): avoid using defer here because this is called on the write path
	// for every incoming metric and defered return and func execution is expensive.
	stagedPlacement, onStagedPlacementDoneFn, err := mgr.placementWatcher.ActiveStagedPlacement()
	if err != nil {
		mgr.metrics.activeStagedPlacementErrors.Inc(1)
		return nil, nil, err
	}
	placement, onPlacementDoneFn, err := stagedPlacement.ActivePlacement()
	if err != nil {
		onStagedPlacementDoneFn()
		mgr.metrics.activePlacementErrors.Inc(1)
		return nil, nil, err
	}
	onPlacementDoneFn()
	onStagedPlacementDoneFn()
	return stagedPlacement, placement, nil
}

func (mgr *placementManager) instanceWithLock() (placement.Instance, error) {
	_, placement, err := mgr.placementWithLock()
	if err != nil {
		return nil, err
	}
	return mgr.instanceFrom(placement)
}

func (mgr *placementManager) instanceFrom(placement placement.Placement) (placement.Instance, error) {
	instance, found := placement.Instance(mgr.instanceID)
	if !found {
		mgr.metrics.instanceNotFound.Inc(1)
		return nil, ErrInstanceNotFoundInPlacement
	}
	return instance, nil
}
