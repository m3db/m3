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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	schema "github.com/m3db/m3cluster/generated/proto/placement"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/clock"
)

var (
	errNilPlacementSchema            = errors.New("nil placement schema")
	errNilPlacementSnapshotsSchema   = errors.New("nil placement snapshots schema")
	errNoApplicablePlacementSnapshot = errors.New("no applicable placement snapshot found")
	errPlacementClosed               = errors.New("placement is closed")
)

// placement stores placement information.
type placement interface {
	// Route routes a metric alongside its policies list.
	Route(mu unaggregated.MetricUnion, pl policy.PoliciesList) error

	// Close closes the topology.
	Close() error
}

type activePlacement struct {
	sync.RWMutex

	snapshots []*placementSnapshot
	nowFn     clock.NowFn
	writerMgr instanceWriterManager
	expiring  int32
	closed    bool
}

func newActivePlacement(snapshots []*placementSnapshot, opts placementOptions) placement {
	writerMgr := opts.InstanceWriterManager()
	for _, snapshot := range snapshots {
		writerMgr.AddInstances(snapshot.Instances())
	}
	return &activePlacement{
		snapshots: snapshots,
		nowFn:     opts.ClockOptions().NowFn(),
		writerMgr: writerMgr,
	}
}

func (p *activePlacement) Route(mu unaggregated.MetricUnion, pl policy.PoliciesList) error {
	p.RLock()
	snapshot, err := p.activeSnapshotWithLock(p.nowFn())
	if err != nil {
		p.RUnlock()
		return err
	}
	shard := snapshot.Shard(mu.ID)
	instances := snapshot.InstancesForShard(shard)
	err = p.writerMgr.WriteTo(instances, shard, mu, pl)
	p.RUnlock()
	return err
}

func (p *activePlacement) Close() error {
	p.Lock()
	defer p.Unlock()

	if p.closed {
		return errPlacementClosed
	}
	for _, snapshot := range p.snapshots {
		p.writerMgr.RemoveInstances(snapshot.Instances())
	}
	p.snapshots = nil
	return nil
}

func (p *activePlacement) activeSnapshotWithLock(t time.Time) (*placementSnapshot, error) {
	if p.closed {
		return nil, errPlacementClosed
	}
	idx := p.activeIndexWithLock(t)
	if idx < 0 {
		return nil, errNoApplicablePlacementSnapshot
	}
	snapshot := p.snapshots[idx]
	// If the snapshot that's in effect is not the first placment, expire the stale ones.
	if idx > 0 && atomic.CompareAndSwapInt32(&p.expiring, 0, 1) {
		go p.expire()
	}
	return snapshot, nil
}

// activeIndexWithLock finds the index of the last snapshot whose cutover time is no
// later than t (a.k.a. the active snapshot). The cutover times of the snapshots are
// sorted in ascending order (i.e., earliest time first).
func (p *activePlacement) activeIndexWithLock(t time.Time) int {
	timeNanos := t.UnixNano()
	idx := 0
	for idx < len(p.snapshots) && p.snapshots[idx].cutoverNanos <= timeNanos {
		idx++
	}
	idx--
	return idx
}

func (p *activePlacement) expire() {
	// NB(xichen): this improves readability at the slight cost of lambda capture
	// because this code path is triggered very infrequently.
	cleanup := func() {
		p.Unlock()
		atomic.StoreInt32(&p.expiring, 0)
	}
	p.Lock()
	defer cleanup()

	if p.closed {
		return
	}
	idx := p.activeIndexWithLock(p.nowFn())
	if idx <= 0 {
		return
	}
	for i := 0; i < idx; i++ {
		p.writerMgr.RemoveInstances(p.snapshots[i].Instances())
	}
	n := copy(p.snapshots[0:], p.snapshots[idx:])
	for i := n; i < len(p.snapshots); i++ {
		p.snapshots[i] = nil
	}
	p.snapshots = p.snapshots[:n]
}

type placementSnapshots struct {
	version   int
	snapshots []*placementSnapshot
	opts      placementOptions
}

func newPlacementSnapshots(
	version int,
	p *schema.PlacementSnapshots,
	opts placementOptions,
) (*placementSnapshots, error) {
	if p == nil {
		return nil, errNilPlacementSnapshotsSchema
	}
	snapshots := make([]*placementSnapshot, 0, len(p.Snapshots))
	for _, snapshot := range p.Snapshots {
		ps, err := newPlacementSnapshot(snapshot, opts)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, ps)
	}
	return &placementSnapshots{
		version:   version,
		snapshots: snapshots,
		opts:      opts,
	}, nil
}

func (pss *placementSnapshots) ActivePlacement(t time.Time) placement {
	timeNanos := t.UnixNano()
	idx := len(pss.snapshots) - 1
	for idx >= 0 && pss.snapshots[idx].cutoverNanos > timeNanos {
		idx--
	}
	if idx < 0 {
		return newActivePlacement(pss.snapshots, pss.opts)
	}
	return newActivePlacement(pss.snapshots[idx:], pss.opts)
}

type placementSnapshot struct {
	numShards        int
	hashFn           HashFn
	cutoverNanos     int64
	instancesByShard map[uint32][]instance
	instances        []instance
}

func newPlacementSnapshot(p *schema.Placement, opts placementOptions) (*placementSnapshot, error) {
	if p == nil {
		return nil, errNilPlacementSchema
	}
	numShards := int(p.NumShards)
	hashGenFn := opts.HashGenFn()
	instances := make([]instance, 0, len(p.Instances))
	instancesByShard := make(map[uint32][]instance, numShards)
	for _, sInstance := range p.Instances {
		instance := newInstance(sInstance.Id, sInstance.Endpoint)
		for _, shard := range sInstance.Shards {
			instancesByShard[shard.Id] = append(instancesByShard[shard.Id], instance)
		}
		instances = append(instances, instance)
	}

	// Sort the instances by their ids for deterministic ordering.
	for _, instances := range instancesByShard {
		sort.Sort(instancesByIDAsc(instances))
	}
	sort.Sort(instancesByIDAsc(instances))

	return &placementSnapshot{
		numShards:        numShards,
		hashFn:           hashGenFn(numShards),
		cutoverNanos:     p.CutoverTime,
		instancesByShard: instancesByShard,
		instances:        instances,
	}, nil
}

func (p *placementSnapshot) NumShards() int         { return p.numShards }
func (p *placementSnapshot) CutoverNanos() int64    { return p.cutoverNanos }
func (p *placementSnapshot) Shard(id []byte) uint32 { return p.hashFn(id) }
func (p *placementSnapshot) Instances() []instance  { return p.instances }

func (p *placementSnapshot) InstancesForShard(shard uint32) []instance {
	return p.instancesByShard[shard]
}
