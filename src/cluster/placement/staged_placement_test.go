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

package placement

import (
	"testing"
	"time"

	"github.com/m3db/m3cluster/generated/proto/placementpb"
	"github.com/m3db/m3cluster/shard"

	"github.com/stretchr/testify/require"
)

var (
	testPlacementsProto = []*placementpb.Placement{
		&placementpb.Placement{
			NumShards:   4,
			CutoverTime: 12345,
			Instances: map[string]*placementpb.Instance{
				"instance1": &placementpb.Instance{
					Id:       "instance1",
					Endpoint: "instance1_endpoint",
					Shards: []*placementpb.Shard{
						&placementpb.Shard{Id: 0},
						&placementpb.Shard{Id: 1},
					},
				},
				"instance2": &placementpb.Instance{
					Id:       "instance2",
					Endpoint: "instance2_endpoint",
					Shards: []*placementpb.Shard{
						&placementpb.Shard{Id: 2},
						&placementpb.Shard{Id: 3},
					},
				},
				"instance3": &placementpb.Instance{
					Id:       "instance3",
					Endpoint: "instance3_endpoint",
					Shards: []*placementpb.Shard{
						&placementpb.Shard{Id: 0},
						&placementpb.Shard{Id: 1},
					},
				},
				"instance4": &placementpb.Instance{
					Id:       "instance4",
					Endpoint: "instance4_endpoint",
					Shards: []*placementpb.Shard{
						&placementpb.Shard{Id: 2},
						&placementpb.Shard{Id: 3},
					},
				},
			},
		},
		&placementpb.Placement{
			NumShards:   4,
			CutoverTime: 67890,
			Instances: map[string]*placementpb.Instance{
				"instance1": &placementpb.Instance{
					Id:       "instance1",
					Endpoint: "instance1_endpoint",
					Shards: []*placementpb.Shard{
						&placementpb.Shard{Id: 0},
						&placementpb.Shard{Id: 1},
						&placementpb.Shard{Id: 2},
						&placementpb.Shard{Id: 3},
					},
				},
			},
		},
	}
	testStagedPlacementProto = &placementpb.PlacementSnapshots{
		Snapshots: testPlacementsProto,
	}
	testActivePlacements = []Placement{
		&placement{
			shards:       []uint32{0, 1, 2, 3},
			cutoverNanos: 12345,
			instancesByShard: map[uint32][]Instance{
				0: []Instance{
					NewInstance().
						SetID("instance1").
						SetEndpoint("instance1_endpoint").
						SetShards(shard.NewShards([]shard.Shard{
							shard.NewShard(0).SetState(shard.Initializing),
							shard.NewShard(1).SetState(shard.Initializing),
						})),
					NewInstance().
						SetID("instance3").
						SetEndpoint("instance3_endpoint").
						SetShards(shard.NewShards([]shard.Shard{
							shard.NewShard(0).SetState(shard.Initializing),
							shard.NewShard(1).SetState(shard.Initializing),
						})),
				},
				1: []Instance{
					NewInstance().
						SetID("instance1").
						SetEndpoint("instance1_endpoint").
						SetShards(shard.NewShards([]shard.Shard{
							shard.NewShard(0).SetState(shard.Initializing),
							shard.NewShard(1).SetState(shard.Initializing),
						})),
					NewInstance().
						SetID("instance3").
						SetEndpoint("instance3_endpoint").
						SetShards(shard.NewShards([]shard.Shard{
							shard.NewShard(0).SetState(shard.Initializing),
							shard.NewShard(1).SetState(shard.Initializing),
						})),
				},
				2: []Instance{
					NewInstance().
						SetID("instance2").
						SetEndpoint("instance2_endpoint").
						SetShards(shard.NewShards([]shard.Shard{
							shard.NewShard(2).SetState(shard.Initializing),
							shard.NewShard(3).SetState(shard.Initializing),
						})),
					NewInstance().
						SetID("instance4").
						SetEndpoint("instance4_endpoint").
						SetShards(shard.NewShards([]shard.Shard{
							shard.NewShard(2).SetState(shard.Initializing),
							shard.NewShard(3).SetState(shard.Initializing),
						})),
				},
				3: []Instance{
					NewInstance().
						SetID("instance2").
						SetEndpoint("instance2_endpoint").
						SetShards(shard.NewShards([]shard.Shard{
							shard.NewShard(2).SetState(shard.Initializing),
							shard.NewShard(3).SetState(shard.Initializing),
						})),
					NewInstance().
						SetID("instance4").
						SetEndpoint("instance4_endpoint").
						SetShards(shard.NewShards([]shard.Shard{
							shard.NewShard(2).SetState(shard.Initializing),
							shard.NewShard(3).SetState(shard.Initializing),
						})),
				},
			},
			instances: map[string]Instance{
				"instance1": NewInstance().
					SetID("instance1").
					SetEndpoint("instance1_endpoint").
					SetShards(shard.NewShards([]shard.Shard{
						shard.NewShard(0).SetState(shard.Initializing),
						shard.NewShard(1).SetState(shard.Initializing),
					})),
				"instance2": NewInstance().
					SetID("instance2").
					SetEndpoint("instance2_endpoint").
					SetShards(shard.NewShards([]shard.Shard{
						shard.NewShard(2).SetState(shard.Initializing),
						shard.NewShard(3).SetState(shard.Initializing),
					})),
				"instance3": NewInstance().
					SetID("instance3").
					SetEndpoint("instance3_endpoint").
					SetShards(shard.NewShards([]shard.Shard{
						shard.NewShard(0).SetState(shard.Initializing),
						shard.NewShard(1).SetState(shard.Initializing),
					})),
				"instance4": NewInstance().
					SetID("instance4").
					SetEndpoint("instance4_endpoint").
					SetShards(shard.NewShards([]shard.Shard{
						shard.NewShard(2).SetState(shard.Initializing),
						shard.NewShard(3).SetState(shard.Initializing),
					})),
			},
		},
		&placement{
			shards:       []uint32{0, 1, 2, 3},
			cutoverNanos: 67890,
			instancesByShard: map[uint32][]Instance{
				0: []Instance{
					NewInstance().
						SetID("instance1").
						SetEndpoint("instance1_endpoint").
						SetShards(shard.NewShards([]shard.Shard{
							shard.NewShard(0).SetState(shard.Initializing),
							shard.NewShard(1).SetState(shard.Initializing),
							shard.NewShard(2).SetState(shard.Initializing),
							shard.NewShard(3).SetState(shard.Initializing),
						})),
				},
				1: []Instance{
					NewInstance().
						SetID("instance1").
						SetEndpoint("instance1_endpoint").
						SetShards(shard.NewShards([]shard.Shard{
							shard.NewShard(0).SetState(shard.Initializing),
							shard.NewShard(1).SetState(shard.Initializing),
							shard.NewShard(2).SetState(shard.Initializing),
							shard.NewShard(3).SetState(shard.Initializing),
						})),
				},
				2: []Instance{
					NewInstance().
						SetID("instance1").
						SetEndpoint("instance1_endpoint").
						SetShards(shard.NewShards([]shard.Shard{
							shard.NewShard(0).SetState(shard.Initializing),
							shard.NewShard(1).SetState(shard.Initializing),
							shard.NewShard(2).SetState(shard.Initializing),
							shard.NewShard(3).SetState(shard.Initializing),
						})),
				},
				3: []Instance{
					NewInstance().
						SetID("instance1").
						SetEndpoint("instance1_endpoint").
						SetShards(shard.NewShards([]shard.Shard{
							shard.NewShard(0).SetState(shard.Initializing),
							shard.NewShard(1).SetState(shard.Initializing),
							shard.NewShard(2).SetState(shard.Initializing),
							shard.NewShard(3).SetState(shard.Initializing),
						})),
				},
			},
			instances: map[string]Instance{
				"instance1": NewInstance().
					SetID("instance1").
					SetEndpoint("instance1_endpoint").
					SetShards(shard.NewShards([]shard.Shard{
						shard.NewShard(0).SetState(shard.Initializing),
						shard.NewShard(1).SetState(shard.Initializing),
						shard.NewShard(2).SetState(shard.Initializing),
						shard.NewShard(3).SetState(shard.Initializing),
					})),
			},
		},
	}
)

func TestNewActiveStagedPlacement(t *testing.T) {
	var allInstances [][]Instance
	opts := NewActiveStagedPlacementOptions().SetOnPlacementsAddedFn(
		func(placements []Placement) {
			for _, placement := range placements {
				allInstances = append(allInstances, placement.Instances())
			}
		},
	)
	ap := newActiveStagedPlacement(testActivePlacements, opts).(*activeStagedPlacement)
	require.Equal(t, len(testActivePlacements), len(allInstances))
	require.Equal(t, len(testActivePlacements), len(ap.placements))
	for i := 0; i < len(testActivePlacements); i++ {
		require.Equal(t, allInstances[i], testActivePlacements[i].Instances())
		validateSnapshot(t, testActivePlacements[i], ap.placements[i])
	}
}

func TestActiveStagedPlacementActivePlacementClosed(t *testing.T) {
	p := &activeStagedPlacement{
		placements: append([]Placement{}, testActivePlacements...),
		nowFn:      time.Now,
		closed:     true,
	}
	_, _, err := p.ActivePlacement()
	require.Equal(t, errActiveStagedPlacementClosed, err)
}

func TestActiveStagedPlacementNoApplicablePlacementFound(t *testing.T) {
	p := &activeStagedPlacement{
		placements: append([]Placement{}, testActivePlacements...),
		nowFn:      func() time.Time { return time.Unix(0, 0) },
	}
	_, _, err := p.ActivePlacement()
	require.Equal(t, errNoApplicablePlacement, err)
}

func TestActiveStagedPlacementActivePlacementFoundWithExpiry(t *testing.T) {
	var removedInstances [][]Instance
	p := &activeStagedPlacement{
		placements: append([]Placement{}, testActivePlacements...),
		nowFn:      func() time.Time { return time.Unix(0, 99999) },
		onPlacementsRemovedFn: func(placements []Placement) {
			for _, placement := range placements {
				removedInstances = append(removedInstances, placement.Instances())
			}
		},
	}
	p.doneFn = p.onPlacementDone
	placement, doneFn, err := p.ActivePlacement()
	require.NoError(t, err)
	require.Equal(t, testActivePlacements[1], placement)
	doneFn()

	for {
		p.RLock()
		numPlacements := len(p.placements)
		p.RUnlock()
		if numPlacements == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	validateSnapshot(t, testActivePlacements[1], p.placements[0])
	require.Equal(t, 1, len(removedInstances))
	require.Equal(t, testActivePlacements[0].Instances(), removedInstances[0])
}

func TestActiveStagedPlacementCloseAlreadyClosed(t *testing.T) {
	p := &activeStagedPlacement{
		placements: append([]Placement{}, testActivePlacements...),
		nowFn:      time.Now,
		closed:     true,
	}
	require.Equal(t, errActiveStagedPlacementClosed, p.Close())
}

func TestActiveStagedPlacementCloseSuccess(t *testing.T) {
	var (
		addedInstances   [][]Instance
		removedInstances [][]Instance
	)
	opts := NewActiveStagedPlacementOptions().
		SetOnPlacementsAddedFn(func(placements []Placement) {
			for _, placement := range placements {
				addedInstances = append(addedInstances, placement.Instances())
			}
		}).
		SetOnPlacementsRemovedFn(func(placements []Placement) {
			for _, placement := range placements {
				removedInstances = append(removedInstances, placement.Instances())
			}
		})
	p := newActiveStagedPlacement(testActivePlacements, opts)
	require.NoError(t, p.Close())
	require.Equal(t, 2, len(addedInstances))
	require.Equal(t, 2, len(removedInstances))
	for i := 0; i < 2; i++ {
		require.Equal(t, testActivePlacements[i].Instances(), addedInstances[i])
		require.Equal(t, testActivePlacements[i].Instances(), removedInstances[i])
	}
}

func TestActiveStagedPlacementExpireAlreadyClosed(t *testing.T) {
	var removedInstances [][]Instance
	p := &activeStagedPlacement{
		placements: append([]Placement{}, testActivePlacements...),
		nowFn:      func() time.Time { return time.Unix(0, 99999) },
		expiring:   1,
		closed:     true,
		onPlacementsRemovedFn: func(placements []Placement) {
			for _, placement := range placements {
				removedInstances = append(removedInstances, placement.Instances())
			}
		},
	}
	p.expire()
	require.Equal(t, int32(0), p.expiring)
	require.Nil(t, removedInstances)
}

func TestActiveStagedPlacementExpireAlreadyExpired(t *testing.T) {
	var removedInstances [][]Instance
	p := &activeStagedPlacement{
		placements: append([]Placement{}, testActivePlacements...),
		nowFn:      func() time.Time { return time.Unix(0, 0) },
		expiring:   1,
		onPlacementsRemovedFn: func(placements []Placement) {
			for _, placement := range placements {
				removedInstances = append(removedInstances, placement.Instances())
			}
		},
	}
	p.expire()
	require.Equal(t, int32(0), p.expiring)
	require.Nil(t, removedInstances)
}

func TestActiveStagedPlacementExpireSuccess(t *testing.T) {
	var removedInstances [][]Instance
	p := &activeStagedPlacement{
		placements: append([]Placement{}, testActivePlacements...),
		nowFn:      func() time.Time { return time.Unix(0, 99999) },
		expiring:   1,
		onPlacementsRemovedFn: func(placements []Placement) {
			for _, placement := range placements {
				removedInstances = append(removedInstances, placement.Instances())
			}
		},
	}
	p.expire()
	require.Equal(t, int32(0), p.expiring)
	require.Equal(t, [][]Instance{testActivePlacements[0].Instances()}, removedInstances)
	require.Equal(t, 1, len(p.placements))
	validateSnapshot(t, testActivePlacements[1], p.placements[0])
}

func TestStagedPlacementNilProto(t *testing.T) {
	_, err := NewStagedPlacementFromProto(1, nil, NewActiveStagedPlacementOptions())
	require.Equal(t, errNilPlacementSnapshotsProto, err)
}

func TestStagedPlacementValidProto(t *testing.T) {
	sp, err := NewStagedPlacementFromProto(1, testStagedPlacementProto, NewActiveStagedPlacementOptions())
	require.NoError(t, err)
	pss := sp.(*stagedPlacement)
	require.Equal(t, 1, pss.Version())
	require.Equal(t, len(pss.placements), len(testStagedPlacementProto.Snapshots))
	for i := 0; i < len(testStagedPlacementProto.Snapshots); i++ {
		validateSnapshot(t, testActivePlacements[i], pss.placements[i])
	}
}

func TestStagedPlacementRoundtrip(t *testing.T) {
	sp, err := NewStagedPlacementFromProto(1, testStagedPlacementProto, nil)
	require.NoError(t, err)
	actual, err := sp.Proto()
	require.NoError(t, err)
	require.Equal(t, testStagedPlacementProto, actual)
}

func TestStagedPlacementActiveStagedPlacement(t *testing.T) {
	opts := NewActiveStagedPlacementOptions()
	sp, err := NewStagedPlacementFromProto(1, testStagedPlacementProto, opts)
	require.NoError(t, err)
	pss := sp.(*stagedPlacement)

	for _, input := range []struct {
		t          int64
		placements []Placement
	}{
		{t: 0, placements: pss.placements[:]},
		{t: 20000, placements: pss.placements[:]},
		{t: 99999, placements: pss.placements[1:]},
	} {
		ap := pss.ActiveStagedPlacement(input.t)
		require.Equal(t, input.placements, ap.(*activeStagedPlacement).placements)
	}
}

func validateSnapshot(
	t *testing.T,
	expected Placement,
	actual Placement,
) {
	require.Equal(t, expected.CutoverNanos(), actual.CutoverNanos())
	require.Equal(t, expected.NumShards(), actual.NumShards())
	require.Equal(t, expected.Instances(), actual.Instances())
	for shard := uint32(0); shard < uint32(actual.NumShards()); shard++ {
		require.Equal(t, expected.InstancesForShard(shard), actual.InstancesForShard(shard))
	}
}
