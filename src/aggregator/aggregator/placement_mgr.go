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

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/x/clock"

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

	// InstanceID returns the configured instance ID.
	InstanceID() string

	// Placement returns the current placement.
	Placement() (placement.Placement, error)

	// Instance returns the current instance in the current placement.
	Instance() (placement.Instance, error)

	// InstanceFrom returns the current instance from the given placement.
	InstanceFrom(placement placement.Placement) (placement.Instance, error)

	// HasReplacementInstance returns true if there is an instance in the same group replacing
	// the current instance, and false otherwise.
	HasReplacementInstance() (bool, error)

	// Shards returns the current shards owned by the instance.
	Shards() (shard.Shards, error)

	// C returns a channel that can be used to subscribe for updates
	C() <-chan struct{}

	// Close closes the placement manager.
	Close() error
}

type placementManagerMetrics struct {
	activePlacementErrors tally.Counter
	instanceNotFound      tally.Counter
	updates               tally.Counter
}

func newPlacementManagerMetrics(scope tally.Scope) placementManagerMetrics {
	return placementManagerMetrics{
		activePlacementErrors: scope.Counter("active-placement-errors"),
		instanceNotFound:      scope.Counter("instance-not-found"),
		updates:               scope.Counter("placement-updates"),
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
	placementWatcher placement.Watcher

	state   placementManagerState
	metrics placementManagerMetrics
	ch      chan struct{}
}

// NewPlacementManager creates a new placement manager.
func NewPlacementManager(opts PlacementManagerOptions) PlacementManager {
	instrumentOpts := opts.InstrumentOptions()
	mgr := &placementManager{
		nowFn:      opts.ClockOptions().NowFn(),
		instanceID: opts.InstanceID(),
		ch:         make(chan struct{}, 1),
		metrics:    newPlacementManagerMetrics(instrumentOpts.MetricsScope()),
	}
	mgr.placementWatcher = placement.NewPlacementsWatcher(
		opts.WatcherOptions().SetOnPlacementChangedFn(mgr.process))

	return mgr
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

func (mgr *placementManager) C() <-chan struct{} {
	return mgr.ch
}

func (mgr *placementManager) InstanceID() string {
	mgr.RLock()
	value := mgr.instanceID
	mgr.RUnlock()
	return value
}

func (mgr *placementManager) Placement() (placement.Placement, error) {
	mgr.RLock()
	placement, err := mgr.placementWithLock()
	mgr.RUnlock()
	return placement, err
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
	placement, err := mgr.Placement()
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

func (mgr *placementManager) placementWithLock() (placement.Placement, error) {
	if mgr.state != placementManagerOpen {
		return nil, errPlacementManagerNotOpenOrClosed
	}

	placement, err := mgr.placementWatcher.Get()
	if err != nil {
		mgr.metrics.activePlacementErrors.Inc(1)
		return nil, err
	}
	return placement, nil
}

func (mgr *placementManager) instanceWithLock() (placement.Instance, error) {
	placement, err := mgr.placementWithLock()
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

func (mgr *placementManager) process(_, _ placement.Placement) {
	select {
	case mgr.ch <- struct{}{}:
	default:
	}
}
