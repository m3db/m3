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
	"testing"
	"time"

	schema "github.com/m3db/m3cluster/generated/proto/placement"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"

	"github.com/stretchr/testify/require"
)

var (
	testPlacementVersion  = 2
	testPlacementInstance = newInstance("testInstanceID", "testInstanceAddress")
	testPlacementSchemas  = []*schema.Placement{
		&schema.Placement{
			NumShards:   4,
			CutoverTime: 12345,
			Instances: map[string]*schema.Instance{
				"instance1": &schema.Instance{
					Id:       "instance1",
					Endpoint: "instance1_endpoint",
					Shards: []*schema.Shard{
						&schema.Shard{Id: 0},
						&schema.Shard{Id: 1},
					},
				},
				"instance2": &schema.Instance{
					Id:       "instance2",
					Endpoint: "instance2_endpoint",
					Shards: []*schema.Shard{
						&schema.Shard{Id: 2},
						&schema.Shard{Id: 3},
					},
				},
				"instance3": &schema.Instance{
					Id:       "instance3",
					Endpoint: "instance3_endpoint",
					Shards: []*schema.Shard{
						&schema.Shard{Id: 0},
						&schema.Shard{Id: 1},
					},
				},
				"instance4": &schema.Instance{
					Id:       "instance4",
					Endpoint: "instance4_endpoint",
					Shards: []*schema.Shard{
						&schema.Shard{Id: 2},
						&schema.Shard{Id: 3},
					},
				},
			},
		},
		&schema.Placement{
			NumShards:   4,
			CutoverTime: 67890,
			Instances: map[string]*schema.Instance{
				"instance1": &schema.Instance{
					Id:       "instance1",
					Endpoint: "instance1_endpoint",
					Shards: []*schema.Shard{
						&schema.Shard{Id: 0},
						&schema.Shard{Id: 1},
						&schema.Shard{Id: 2},
						&schema.Shard{Id: 3},
					},
				},
			},
		},
	}
	testPlacementSnapshotsSchema = &schema.PlacementSnapshots{
		Snapshots: testPlacementSchemas,
	}
	testPlacementSnapshots = []*placementSnapshot{
		&placementSnapshot{
			numShards: 4,
			hashFn:    defaultHashGen(4),
			cutoverNs: 12345,
			instancesByShard: map[uint32][]instance{
				0: []instance{
					newInstance("instance1", "instance1_endpoint"),
					newInstance("instance3", "instance3_endpoint"),
				},
				1: []instance{
					newInstance("instance1", "instance1_endpoint"),
					newInstance("instance3", "instance3_endpoint"),
				},
				2: []instance{
					newInstance("instance2", "instance2_endpoint"),
					newInstance("instance4", "instance4_endpoint"),
				},
				3: []instance{
					newInstance("instance2", "instance2_endpoint"),
					newInstance("instance4", "instance4_endpoint"),
				},
			},
			instances: []instance{
				newInstance("instance1", "instance1_endpoint"),
				newInstance("instance2", "instance2_endpoint"),
				newInstance("instance3", "instance3_endpoint"),
				newInstance("instance4", "instance4_endpoint"),
			},
		},
		&placementSnapshot{
			numShards: 4,
			hashFn:    defaultHashGen(4),
			cutoverNs: 67890,
			instancesByShard: map[uint32][]instance{
				0: []instance{
					newInstance("instance1", "instance1_endpoint"),
				},
				1: []instance{
					newInstance("instance1", "instance1_endpoint"),
				},
				2: []instance{
					newInstance("instance1", "instance1_endpoint"),
				},
				3: []instance{
					newInstance("instance1", "instance1_endpoint"),
				},
			},
			instances: []instance{
				newInstance("instance1", "instance1_endpoint"),
			},
		},
	}
)

func TestNewActivePlacement(t *testing.T) {
	var allInstances [][]instance
	mgr := &mockInstanceWriterManager{
		addInstancesFn: func(instances []instance) error {
			allInstances = append(allInstances, instances)
			return nil
		},
	}
	opts := newPlacementOptions().SetInstanceWriterManager(mgr)
	ap := newActivePlacement(testPlacementSnapshots, opts).(*activePlacement)
	require.Equal(t, len(testPlacementSnapshots), len(allInstances))
	require.Equal(t, len(testPlacementSnapshots), len(ap.snapshots))
	for i := 0; i < len(testPlacementSnapshots); i++ {
		require.Equal(t, allInstances[i], testPlacementSnapshots[i].Instances())
		validateSnapshot(t, testPlacementSnapshots[i], ap.snapshots[i])
	}
}

func TestActivePlacementRoutePlacementClosed(t *testing.T) {
	p := &activePlacement{
		snapshots: append([]*placementSnapshot{}, testPlacementSnapshots...),
		nowFn:     time.Now,
		closed:    true,
	}
	err := p.Route(testCounter, testVersionedPolicies)
	require.Equal(t, errPlacementClosed, err)
}

func TestActivePlacementRouteNoPlacementFound(t *testing.T) {
	p := &activePlacement{
		snapshots: append([]*placementSnapshot{}, testPlacementSnapshots...),
		nowFn:     func() time.Time { return time.Unix(0, 0) },
	}
	err := p.Route(testCounter, testVersionedPolicies)
	require.Equal(t, errNoApplicablePlacementSnapshot, err)
}

func TestActivePlacementRoutePlacementFoundWithExpiry(t *testing.T) {
	var (
		removedInstances         [][]instance
		writeToInstances         [][]instance
		writeToShard             uint32
		writeToMetricUnion       unaggregated.MetricUnion
		writeToVersionedPolicies policy.VersionedPolicies
	)
	p := &activePlacement{
		writerMgr: &mockInstanceWriterManager{
			removeInstancesFn: func(instances []instance) error {
				removedInstances = append(removedInstances, instances)
				return nil
			},
			writeToFn: func(
				instances []instance,
				shard uint32,
				mu unaggregated.MetricUnion,
				vp policy.VersionedPolicies,
			) error {
				writeToInstances = append(writeToInstances, instances)
				writeToShard = shard
				writeToMetricUnion = mu
				writeToVersionedPolicies = vp
				return nil
			},
		},
		snapshots: append([]*placementSnapshot{}, testPlacementSnapshots...),
		nowFn:     func() time.Time { return time.Unix(0, 99999) },
	}
	err := p.Route(testCounter, testVersionedPolicies)
	require.NoError(t, err)
	require.Equal(t, 1, len(writeToInstances))
	require.Equal(t, testPlacementSnapshots[1].instancesByShard[0], writeToInstances[0])
	require.Equal(t, uint32(0), writeToShard)
	require.Equal(t, testCounter, writeToMetricUnion)
	require.Equal(t, testVersionedPolicies, writeToVersionedPolicies)

	for {
		p.RLock()
		numSnapshots := len(p.snapshots)
		p.RUnlock()
		if numSnapshots == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	validateSnapshot(t, testPlacementSnapshots[1], p.snapshots[0])
	require.Equal(t, 1, len(removedInstances))
	require.Equal(t, testPlacementSnapshots[0].Instances(), removedInstances[0])
}

func TestActivePlacementCloseAlreadyClosed(t *testing.T) {
	p := &activePlacement{
		snapshots: append([]*placementSnapshot{}, testPlacementSnapshots...),
		nowFn:     time.Now,
		closed:    true,
	}
	require.Equal(t, errPlacementClosed, p.Close())
}

func TestActivePlacementCloseSuccess(t *testing.T) {
	var (
		addedInstances   [][]instance
		removedInstances [][]instance
	)
	writerMgr := &mockInstanceWriterManager{
		addInstancesFn: func(instances []instance) error {
			addedInstances = append(addedInstances, instances)
			return nil
		},
		removeInstancesFn: func(instances []instance) error {
			removedInstances = append(removedInstances, instances)
			return nil
		},
	}
	opts := newPlacementOptions().SetInstanceWriterManager(writerMgr)
	p := newActivePlacement(testPlacementSnapshots, opts)
	require.NoError(t, p.Close())
	require.Equal(t, 2, len(addedInstances))
	require.Equal(t, 2, len(removedInstances))
	for i := 0; i < 2; i++ {
		require.Equal(t, testPlacementSnapshots[i].Instances(), addedInstances[i])
		require.Equal(t, testPlacementSnapshots[i].Instances(), removedInstances[i])
	}
}

func TestActivePlacementExpireAlreadyClosed(t *testing.T) {
	var removedInstances [][]instance
	p := &activePlacement{
		writerMgr: &mockInstanceWriterManager{
			removeInstancesFn: func(instances []instance) error {
				removedInstances = append(removedInstances, instances)
				return nil
			},
		},
		snapshots: append([]*placementSnapshot{}, testPlacementSnapshots...),
		nowFn:     func() time.Time { return time.Unix(0, 99999) },
		expiring:  1,
		closed:    true,
	}
	p.expire()
	require.Equal(t, int32(0), p.expiring)
	require.Nil(t, removedInstances)
}

func TestActivePlacementExpireAlreadyExpired(t *testing.T) {
	var removedInstances [][]instance
	p := &activePlacement{
		writerMgr: &mockInstanceWriterManager{
			removeInstancesFn: func(instances []instance) error {
				removedInstances = append(removedInstances, instances)
				return nil
			},
		},
		snapshots: append([]*placementSnapshot{}, testPlacementSnapshots...),
		nowFn:     func() time.Time { return time.Unix(0, 0) },
		expiring:  1,
	}
	p.expire()
	require.Equal(t, int32(0), p.expiring)
	require.Nil(t, removedInstances)
}

func TestActivePlacementExpireSuccess(t *testing.T) {
	var removedInstances [][]instance
	p := &activePlacement{
		writerMgr: &mockInstanceWriterManager{
			removeInstancesFn: func(instances []instance) error {
				removedInstances = append(removedInstances, instances)
				return nil
			},
		},
		snapshots: append([]*placementSnapshot{}, testPlacementSnapshots...),
		nowFn:     func() time.Time { return time.Unix(0, 99999) },
		expiring:  1,
	}
	p.expire()
	require.Equal(t, int32(0), p.expiring)
	require.Equal(t, [][]instance{testPlacementSnapshots[0].Instances()}, removedInstances)
	require.Equal(t, 1, len(p.snapshots))
	validateSnapshot(t, testPlacementSnapshots[1], p.snapshots[0])
}

func TestPlacementSnapshotsSchemaNilSchema(t *testing.T) {
	_, err := newPlacementSnapshots(1, nil, newPlacementOptions())
	require.Equal(t, errNilPlacementSnapshotsSchema, err)
}

func TestPlacementSnapshotsSchemaValidSchema(t *testing.T) {
	pss, err := newPlacementSnapshots(1, testPlacementSnapshotsSchema, newPlacementOptions())
	require.NoError(t, err)
	require.Equal(t, 1, pss.version)
	require.Equal(t, len(pss.snapshots), len(testPlacementSnapshotsSchema.Snapshots))
	for i := 0; i < len(testPlacementSnapshotsSchema.Snapshots); i++ {
		validateSnapshot(t, testPlacementSnapshots[i], pss.snapshots[i])
	}
}

func TestPlacementSnapshotsSchemaActivePlacement(t *testing.T) {
	mgr := &mockInstanceWriterManager{
		addInstancesFn: func(instances []instance) error { return nil },
	}
	opts := newPlacementOptions().SetInstanceWriterManager(mgr)
	pss, err := newPlacementSnapshots(1, testPlacementSnapshotsSchema, opts)
	require.NoError(t, err)

	for _, input := range []struct {
		t         time.Time
		snapshots []*placementSnapshot
	}{
		{t: time.Unix(0, 0), snapshots: pss.snapshots[:]},
		{t: time.Unix(0, 20000), snapshots: pss.snapshots[:]},
		{t: time.Unix(0, 99999), snapshots: pss.snapshots[1:]},
	} {
		ap := pss.ActivePlacement(input.t)
		require.Equal(t, input.snapshots, ap.(*activePlacement).snapshots)
	}
}

func TestPlacementSnapshotNilSchema(t *testing.T) {
	_, err := newPlacementSnapshot(nil, newPlacementOptions())
	require.Equal(t, errNilPlacementSchema, err)
}

func TestPlacementSnapshotValidSchema(t *testing.T) {
	ps, err := newPlacementSnapshot(testPlacementSchemas[0], newPlacementOptions())
	require.NoError(t, err)
	validateSnapshot(t, testPlacementSnapshots[0], ps)
}

func validateSnapshot(t *testing.T, expected *placementSnapshot, actual *placementSnapshot) {
	require.Equal(t, expected.cutoverNs, actual.CutoverNs())
	require.Equal(t, expected.numShards, actual.NumShards())
	require.Equal(t, expected.hashFn([]byte("foo")), actual.Shard([]byte("foo")))
	require.Equal(t, expected.instances, actual.Instances())
	for shard := uint32(0); shard < uint32(actual.NumShards()); shard++ {
		require.Equal(t, expected.instancesByShard[shard], actual.InstancesForShard(shard))
	}
}

type addInstancesFn func(instances []instance) error
type removeInstancesFn func(instances []instance) error
type writeToFn func([]instance, uint32, unaggregated.MetricUnion, policy.VersionedPolicies) error

type mockInstanceWriterManager struct {
	addInstancesFn    addInstancesFn
	removeInstancesFn removeInstancesFn
	writeToFn         writeToFn
	flushFn           mockFlushFn
}

func (mgr *mockInstanceWriterManager) AddInstances(instances []instance) error {
	return mgr.addInstancesFn(instances)
}

func (mgr *mockInstanceWriterManager) RemoveInstances(instances []instance) error {
	return mgr.removeInstancesFn(instances)
}

func (mgr *mockInstanceWriterManager) WriteTo(
	instances []instance,
	shard uint32,
	mu unaggregated.MetricUnion,
	vp policy.VersionedPolicies,
) error {
	return mgr.writeToFn(instances, shard, mu, vp)
}

func (mgr *mockInstanceWriterManager) Flush() error { return mgr.flushFn() }
func (mgr *mockInstanceWriterManager) Close() error { return nil }
