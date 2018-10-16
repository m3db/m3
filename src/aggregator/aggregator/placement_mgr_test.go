// Copyright (c) 2016 Uber Technologies, Inc.
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
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placement"

	"github.com/stretchr/testify/require"
)

const (
	testInstanceID1 = "testInstance1"
	testInstanceID2 = "testInstance2"
	testInstanceID3 = "testInstance3"
)

var (
	testPlacementsProto = []*placementpb.Placement{
		&placementpb.Placement{
			NumShards:   4,
			CutoverTime: 0,
			Instances: map[string]*placementpb.Instance{
				testInstanceID1: &placementpb.Instance{
					Id:       testInstanceID1,
					Endpoint: testInstanceID1,
					Shards: []*placementpb.Shard{
						&placementpb.Shard{Id: 0, State: placementpb.ShardState_INITIALIZING},
						&placementpb.Shard{Id: 1, State: placementpb.ShardState_INITIALIZING},
						&placementpb.Shard{Id: 2, State: placementpb.ShardState_INITIALIZING},
						&placementpb.Shard{Id: 3, State: placementpb.ShardState_INITIALIZING},
					},
				},
			},
		},
		&placementpb.Placement{
			NumShards:   4,
			CutoverTime: 10000,
			Instances: map[string]*placementpb.Instance{
				testInstanceID1: &placementpb.Instance{
					Id:       testInstanceID1,
					Endpoint: testInstanceID1,
					Shards: []*placementpb.Shard{
						&placementpb.Shard{Id: 0, State: placementpb.ShardState_INITIALIZING},
						&placementpb.Shard{Id: 1, State: placementpb.ShardState_INITIALIZING},
					},
					ShardSetId: 0,
				},
				testInstanceID2: &placementpb.Instance{
					Id:       testInstanceID2,
					Endpoint: testInstanceID2,
					Shards: []*placementpb.Shard{
						&placementpb.Shard{Id: 2, State: placementpb.ShardState_INITIALIZING},
						&placementpb.Shard{Id: 3, State: placementpb.ShardState_INITIALIZING},
					},
					ShardSetId: 1,
				},
			},
		},
	}
	testStagedPlacementProto = &placementpb.PlacementSnapshots{
		Snapshots: testPlacementsProto,
	}
)

func TestPlacementManagerOpenAlreadyOpen(t *testing.T) {
	mgr, _ := testPlacementManager(t)
	mgr.state = placementManagerOpen
	require.Equal(t, errPlacementManagerOpenOrClosed, mgr.Open())
}

func TestPlacementManagerOpenSuccess(t *testing.T) {
	mgr, _ := testPlacementManager(t)
	require.NoError(t, mgr.Open())
	require.Equal(t, placementManagerOpen, mgr.state)
}

func TestPlacementManagerPlacementNotOpen(t *testing.T) {
	mgr, _ := testPlacementManager(t)
	_, _, err := mgr.Placement()
	require.Equal(t, errPlacementManagerNotOpenOrClosed, err)
}

func TestPlacementManagerPlacement(t *testing.T) {
	mgr, store := testPlacementManager(t)
	require.NoError(t, mgr.Open())

	// Wait for change to propagate.
	_, err := store.Set(testPlacementKey, testStagedPlacementProto)
	require.NoError(t, err)
	var placement placement.Placement
	for {
		_, placement, err = mgr.Placement()
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	require.Equal(t, int64(10000), placement.CutoverNanos())
	require.Equal(t, []uint32{0, 1, 2, 3}, placement.Shards())
}

func TestPlacementManagerInstanceNotFound(t *testing.T) {
	mgr, store := testPlacementManager(t)
	require.NoError(t, mgr.Open())

	// Wait for change to propagate.
	_, err := store.Set(testPlacementKey, testStagedPlacementProto)
	require.NoError(t, err)
	for {
		_, err := mgr.Instance()
		if err == ErrInstanceNotFoundInPlacement {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPlacementManagerInstanceFound(t *testing.T) {
	mgr, store := testPlacementManager(t)
	mgr.instanceID = testInstanceID1
	require.NoError(t, mgr.Open())

	// Wait for change to propagate.
	_, err := store.Set(testPlacementKey, testStagedPlacementProto)
	require.NoError(t, err)
	for {
		instance, err := mgr.Instance()
		if err == nil {
			require.Equal(t, testInstanceID1, instance.ID())
			require.Equal(t, uint32(0), instance.ShardSetID())
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPlacementHasReplacementInstance(t *testing.T) {
	protos := []*placementpb.PlacementSnapshots{
		&placementpb.PlacementSnapshots{
			Snapshots: []*placementpb.Placement{
				&placementpb.Placement{
					NumShards:   4,
					CutoverTime: 100,
					Instances: map[string]*placementpb.Instance{
						testInstanceID1: &placementpb.Instance{
							Id:       testInstanceID1,
							Endpoint: testInstanceID1,
							Shards: []*placementpb.Shard{
								&placementpb.Shard{Id: 0, State: placementpb.ShardState_INITIALIZING, CutoffNanos: 1000},
								&placementpb.Shard{Id: 1, State: placementpb.ShardState_INITIALIZING, CutoffNanos: 1000},
							},
							ShardSetId: 0,
						},
						testInstanceID2: &placementpb.Instance{
							Id:       testInstanceID2,
							Endpoint: testInstanceID2,
							Shards: []*placementpb.Shard{
								&placementpb.Shard{Id: 0, State: placementpb.ShardState_LEAVING, CutoverNanos: 1000},
								&placementpb.Shard{Id: 1, State: placementpb.ShardState_LEAVING, CutoverNanos: 1000},
							},
							ShardSetId: 0,
						},
					},
				},
			},
		},
		&placementpb.PlacementSnapshots{
			Snapshots: []*placementpb.Placement{
				&placementpb.Placement{
					NumShards:   4,
					CutoverTime: 200,
					Instances: map[string]*placementpb.Instance{
						testInstanceID1: &placementpb.Instance{
							Id:       testInstanceID1,
							Endpoint: testInstanceID1,
							Shards: []*placementpb.Shard{
								&placementpb.Shard{Id: 0, State: placementpb.ShardState_LEAVING, CutoffNanos: 1000},
								&placementpb.Shard{Id: 1, State: placementpb.ShardState_LEAVING, CutoffNanos: 1000},
							},
							ShardSetId: 0,
						},
						testInstanceID2: &placementpb.Instance{
							Id:       testInstanceID2,
							Endpoint: testInstanceID2,
							Shards: []*placementpb.Shard{
								&placementpb.Shard{Id: 0, State: placementpb.ShardState_INITIALIZING, CutoverNanos: 1000},
							},
							ShardSetId: 0,
						},
					},
				},
			},
		},
		&placementpb.PlacementSnapshots{
			Snapshots: []*placementpb.Placement{
				&placementpb.Placement{
					NumShards:   4,
					CutoverTime: 300,
					Instances: map[string]*placementpb.Instance{
						testInstanceID1: &placementpb.Instance{
							Id:       testInstanceID1,
							Endpoint: testInstanceID1,
							Shards: []*placementpb.Shard{
								&placementpb.Shard{Id: 0, State: placementpb.ShardState_LEAVING, CutoffNanos: 1000},
								&placementpb.Shard{Id: 1, State: placementpb.ShardState_LEAVING, CutoffNanos: 1000},
							},
							ShardSetId: 0,
						},
						testInstanceID2: &placementpb.Instance{
							Id:       testInstanceID2,
							Endpoint: testInstanceID2,
							Shards: []*placementpb.Shard{
								&placementpb.Shard{Id: 0, State: placementpb.ShardState_INITIALIZING, CutoverNanos: 1000},
								&placementpb.Shard{Id: 1, State: placementpb.ShardState_INITIALIZING, CutoverNanos: 800},
							},
							ShardSetId: 0,
						},
					},
				},
			},
		},
		&placementpb.PlacementSnapshots{
			Snapshots: []*placementpb.Placement{
				&placementpb.Placement{
					NumShards:   4,
					CutoverTime: 400,
					Instances: map[string]*placementpb.Instance{
						testInstanceID1: &placementpb.Instance{
							Id:       testInstanceID1,
							Endpoint: testInstanceID1,
							Shards: []*placementpb.Shard{
								&placementpb.Shard{Id: 0, State: placementpb.ShardState_LEAVING, CutoffNanos: 1000},
								&placementpb.Shard{Id: 1, State: placementpb.ShardState_LEAVING, CutoffNanos: 1000},
							},
							ShardSetId: 0,
						},
						testInstanceID2: &placementpb.Instance{
							Id:       testInstanceID2,
							Endpoint: testInstanceID2,
							Shards: []*placementpb.Shard{
								&placementpb.Shard{Id: 0, State: placementpb.ShardState_INITIALIZING, CutoverNanos: 1000},
								&placementpb.Shard{Id: 1, State: placementpb.ShardState_INITIALIZING, CutoverNanos: 1000},
							},
							ShardSetId: 1,
						},
						testInstanceID3: &placementpb.Instance{
							Id:       testInstanceID3,
							Endpoint: testInstanceID3,
							Shards: []*placementpb.Shard{
								&placementpb.Shard{Id: 0, State: placementpb.ShardState_INITIALIZING, CutoverNanos: 1000},
								&placementpb.Shard{Id: 1, State: placementpb.ShardState_INITIALIZING, CutoverNanos: 1000},
							},
							ShardSetId: 0,
						},
					},
				},
			},
		},
	}
	expected := []bool{false, false, false, true}
	mgr, store := testPlacementManager(t)
	mgr.instanceID = testInstanceID1
	require.NoError(t, mgr.Open())

	for i, proto := range protos {
		_, err := store.Set(testPlacementKey, proto)
		require.NoError(t, err)
		for {
			_, p, err := mgr.Placement()
			if err == nil && p.CutoverNanos() == proto.Snapshots[0].CutoverTime {
				res, err := mgr.HasReplacementInstance()
				require.NoError(t, err)
				require.Equal(t, expected[i], res)
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestPlacementManagerShards(t *testing.T) {
	mgr, store := testPlacementManager(t)
	mgr.instanceID = testInstanceID1
	require.NoError(t, mgr.Open())

	// Wait for change to propagate.
	_, err := store.Set(testPlacementKey, testStagedPlacementProto)
	require.NoError(t, err)
	for {
		shards, err := mgr.Shards()
		if err == nil {
			require.Equal(t, []uint32{0, 1}, shards.AllIDs())
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPlacementClose(t *testing.T) {
	mgr, _ := testPlacementManager(t)
	require.NoError(t, mgr.Open())
	require.NoError(t, mgr.Close())
	require.Equal(t, placementManagerClosed, mgr.state)
}

func testPlacementManager(t *testing.T) (*placementManager, kv.Store) {
	watcher, store := testPlacementWatcherWithPlacementProto(t, testPlacementKey, testStagedPlacementProto)
	opts := NewPlacementManagerOptions().
		SetInstanceID(testInstanceID).
		SetStagedPlacementWatcher(watcher)
	placementManager := NewPlacementManager(opts).(*placementManager)
	return placementManager, store
}
