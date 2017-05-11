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

	schema "github.com/m3db/m3cluster/generated/proto/placement"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/util/runtime"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/clock"
)

var (
	errNilValue                  = errors.New("nil value received")
	errTopologyIsOpenOrClosed    = errors.New("topology is already open or closed")
	errTopologyIsNotOpenOrClosed = errors.New("topology is not open or closed")
	errNoEligiblePlacement       = errors.New("no eligible placement")
)

type topologyState int

const (
	topologyNotOpen topologyState = iota
	topologyOpen
	topologyClosed
)

// topology stores topology information.
type topology interface {
	// Open opens the topology.
	Open() error

	// Route routes a metric alongside its policies list.
	Route(mu unaggregated.MetricUnion, pl policy.PoliciesList) error

	// Close closes the topology.
	Close() error
}

type topo struct {
	sync.RWMutex
	runtime.Value

	serverOpts    ServerOptions
	placementOpts placementOptions
	nowFn         clock.NowFn

	state     topologyState
	proto     *schema.PlacementSnapshots
	placement placement
}

// newTopology creates a new topology.
func newTopology(writerMgr instanceWriterManager, opts ServerOptions) topology {
	clockOpts := opts.ClockOptions()
	topology := &topo{
		serverOpts: opts,
		nowFn:      clockOpts.NowFn(),
		proto:      &schema.PlacementSnapshots{},
	}
	topologyOpts := opts.TopologyOptions()
	topology.placementOpts = newPlacementOptions().
		SetClockOptions(clockOpts).
		SetHashGenFn(topologyOpts.HashGenFn()).
		SetInstanceWriterManager(writerMgr)
	valueOpts := runtime.NewOptions().
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetInitWatchTimeout(topologyOpts.InitWatchTimeout()).
		SetKVStore(topologyOpts.KVStore()).
		SetUnmarshalFn(topology.toPlacement).
		SetProcessFn(topology.process)
	topology.Value = runtime.NewValue(topologyOpts.TopologyKey(), valueOpts)
	return topology
}

func (t *topo) Open() error {
	t.Lock()
	if t.state != topologyNotOpen {
		return errTopologyIsOpenOrClosed
	}
	t.state = topologyOpen
	t.Unlock()

	// NB(xichen): we watch the topology updates outside the lock because
	// otherwise the initial update will trigger the process() callback,
	// which attempts to acquire the same lock, causing a deadlock.
	return t.Watch()
}

func (t *topo) Route(mu unaggregated.MetricUnion, pl policy.PoliciesList) error {
	t.RLock()
	if t.state != topologyOpen {
		t.RUnlock()
		return errTopologyIsNotOpenOrClosed
	}
	err := t.placement.Route(mu, pl)
	t.RUnlock()
	return err
}

func (t *topo) Close() error {
	t.Lock()
	if t.state != topologyOpen {
		t.Unlock()
		return errTopologyIsNotOpenOrClosed
	}
	t.state = topologyClosed
	if t.placement != nil {
		t.placement.Close()
	}
	t.placement = nil
	t.Unlock()

	// NB(xichen): we unwatch the topology updates outside the lock to avoid deadlock
	// due to topology contending for the runtime value lock and the runtime updating
	// goroutine attempting to acquire topology lock.
	t.Value.Unwatch()
	return nil
}

func (t *topo) toPlacement(value kv.Value) (interface{}, error) {
	t.Lock()
	defer t.Unlock()

	if t.state != topologyOpen {
		return nil, errTopologyIsNotOpenOrClosed
	}
	if value == nil {
		return nil, errNilValue
	}
	t.proto.Reset()
	if err := value.Unmarshal(t.proto); err != nil {
		return nil, err
	}
	version := value.Version()
	return newPlacementSnapshots(version, t.proto, t.placementOpts)
}

func (t *topo) process(value interface{}) error {
	t.Lock()
	defer t.Unlock()

	if t.state != topologyOpen {
		return errTopologyIsNotOpenOrClosed
	}
	ps := value.(*placementSnapshots)
	placement := ps.ActivePlacement(t.nowFn())
	if t.placement != nil {
		t.placement.Close()
	}
	t.placement = placement
	return nil
}
