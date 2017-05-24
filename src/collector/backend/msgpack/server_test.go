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
	"testing"
	"time"

	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testCounter = unaggregated.MetricUnion{
		Type:       unaggregated.CounterType,
		ID:         []byte("foo"),
		CounterVal: 1234,
	}
	testBatchTimer = unaggregated.MetricUnion{
		Type:          unaggregated.BatchTimerType,
		ID:            []byte("foo"),
		BatchTimerVal: []float64{222.22, 345.67, 901.23345},
	}
	testGauge = unaggregated.MetricUnion{
		Type:     unaggregated.GaugeType,
		ID:       []byte("foo"),
		GaugeVal: 123.456,
	}
	testPoliciesList = policy.PoliciesList{
		policy.NewStagedPolicies(
			100,
			false,
			[]policy.Policy{
				policy.NewPolicy(20*time.Second, xtime.Second, 6*time.Hour),
				policy.NewPolicy(time.Minute, xtime.Minute, 2*24*time.Hour),
				policy.NewPolicy(10*time.Minute, xtime.Minute, 25*24*time.Hour),
			},
		),
		policy.NewStagedPolicies(
			200,
			true,
			[]policy.Policy{
				policy.NewPolicy(time.Second, xtime.Second, time.Hour),
			},
		),
	}
	testPlacementInstances = []services.PlacementInstance{
		placement.NewInstance().
			SetID("instance1").
			SetEndpoint("instance1_endpoint").
			SetShards(shard.NewShards([]shard.Shard{
				shard.NewShard(0).SetState(shard.Initializing),
				shard.NewShard(1).SetState(shard.Initializing),
			})),
		placement.NewInstance().
			SetID("instance2").
			SetEndpoint("instance2_endpoint").
			SetShards(shard.NewShards([]shard.Shard{
				shard.NewShard(2).SetState(shard.Initializing),
				shard.NewShard(3).SetState(shard.Initializing),
			})),
		placement.NewInstance().
			SetID("instance3").
			SetEndpoint("instance3_endpoint").
			SetShards(shard.NewShards([]shard.Shard{
				shard.NewShard(0).SetState(shard.Initializing),
				shard.NewShard(1).SetState(shard.Initializing),
			})),
		placement.NewInstance().
			SetID("instance4").
			SetEndpoint("instance4_endpoint").
			SetShards(shard.NewShards([]shard.Shard{
				shard.NewShard(2).SetState(shard.Initializing),
				shard.NewShard(3).SetState(shard.Initializing),
			})),
	}
	testPlacement = placement.NewPlacement().
			SetVersion(1).
			SetCutoverNanos(12345).
			SetShards([]uint32{0, 1, 2, 3}).
			SetInstances(testPlacementInstances)
)

func TestServerOpenNotOpenOrClosed(t *testing.T) {
	s := NewServer(testServerOptions()).(*server)

	s.state = serverOpen
	require.Equal(t, errServerIsOpenOrClosed, s.Open())

	s.state = serverClosed
	require.Equal(t, errServerIsOpenOrClosed, s.Open())
}

func TestServerOpenWatcherWatchError(t *testing.T) {
	errTestWatcherWatch := errors.New("error watching")
	s := NewServer(testServerOptions()).(*server)
	s.placementWatcher = &mockWatcher{
		watchFn: func() error { return errTestWatcherWatch },
	}
	require.Equal(t, errTestWatcherWatch, s.Open())
}

func TestServerOpenSuccess(t *testing.T) {
	s := NewServer(testServerOptions()).(*server)
	s.placementWatcher = &mockWatcher{
		watchFn: func() error { return nil },
	}
	require.NoError(t, s.Open())
	require.Equal(t, serverOpen, s.state)
}

func TestServerWriteCounterWithPoliciesListClosed(t *testing.T) {
	s := NewServer(testServerOptions()).(*server)
	s.state = serverNotOpen
	require.Equal(t, errServerIsNotOpenOrClosed, s.WriteCounterWithPoliciesList(
		testCounter.ID,
		testCounter.CounterVal,
		testPoliciesList,
	))
}

func TestServerWriteCounterWithPoliciesListActiveStagedPlacementError(t *testing.T) {
	errActiveStagedPlacementError := errors.New("error active staged placement")
	s := NewServer(testServerOptions()).(*server)
	s.state = serverOpen
	s.placementWatcher = &mockWatcher{
		activeStagedPlacementFn: func() (services.ActiveStagedPlacement, services.DoneFn, error) {
			return nil, nil, errActiveStagedPlacementError
		},
	}
	require.Equal(t, errActiveStagedPlacementError, s.WriteCounterWithPoliciesList(
		testCounter.ID,
		testCounter.CounterVal,
		testPoliciesList,
	))
}

func TestServerWriteCounterWithPoliciesListActivePlacementError(t *testing.T) {
	errActivePlacementError := errors.New("error active placement")
	s := NewServer(testServerOptions()).(*server)
	s.state = serverOpen
	s.placementWatcher = &mockWatcher{
		activeStagedPlacementFn: func() (services.ActiveStagedPlacement, services.DoneFn, error) {
			activeStagedPlacement := &mockActiveStagedPlacement{
				activePlacementFn: func() (services.Placement, services.DoneFn, error) {
					return nil, nil, errActivePlacementError
				},
			}
			noOpDoneFn := func() {}
			return activeStagedPlacement, noOpDoneFn, nil
		},
	}
	require.Equal(t, errActivePlacementError, s.WriteCounterWithPoliciesList(
		testCounter.ID,
		testCounter.CounterVal,
		testPoliciesList,
	))
}

func TestServerWriteCounterWithPoliciesListSuccess(t *testing.T) {
	var (
		instancesRes []services.PlacementInstance
		shardRes     uint32
		muRes        unaggregated.MetricUnion
		plRes        policy.PoliciesList
	)
	s := NewServer(testServerOptions()).(*server)
	s.state = serverOpen
	s.writerMgr = &mockInstanceWriterManager{
		writeToFn: func(
			instances []services.PlacementInstance,
			shard uint32,
			mu unaggregated.MetricUnion,
			pl policy.PoliciesList,
		) error {
			instancesRes = instances
			shardRes = shard
			muRes = mu
			plRes = pl
			return nil
		},
	}
	s.placementWatcher = &mockWatcher{
		activeStagedPlacementFn: func() (services.ActiveStagedPlacement, services.DoneFn, error) {
			noOpDoneFn := func() {}
			activeStagedPlacement := &mockActiveStagedPlacement{
				activePlacementFn: func() (services.Placement, services.DoneFn, error) {
					return testPlacement, noOpDoneFn, nil
				},
			}
			return activeStagedPlacement, noOpDoneFn, nil
		},
	}
	require.NoError(t, s.WriteCounterWithPoliciesList(
		testCounter.ID,
		testCounter.CounterVal,
		testPoliciesList,
	))
	expectedInstances := []services.PlacementInstance{
		testPlacementInstances[0],
		testPlacementInstances[2],
	}
	require.Equal(t, expectedInstances, instancesRes)
	require.Equal(t, uint32(1), shardRes)
	require.Equal(t, testCounter, muRes)
	require.Equal(t, testPoliciesList, plRes)
}

func TestServerWriteBatchTimerWithPoliciesListClosed(t *testing.T) {
	s := NewServer(testServerOptions()).(*server)
	s.state = serverNotOpen
	require.Equal(t, errServerIsNotOpenOrClosed, s.WriteBatchTimerWithPoliciesList(
		testBatchTimer.ID,
		testBatchTimer.BatchTimerVal,
		testPoliciesList,
	))
}

func TestServerWriteBatchTimerWithPoliciesListActiveStagedPlacementError(t *testing.T) {
	errActiveStagedPlacementError := errors.New("error active staged placement")
	s := NewServer(testServerOptions()).(*server)
	s.state = serverOpen
	s.placementWatcher = &mockWatcher{
		activeStagedPlacementFn: func() (services.ActiveStagedPlacement, services.DoneFn, error) {
			return nil, nil, errActiveStagedPlacementError
		},
	}
	require.Equal(t, errActiveStagedPlacementError, s.WriteBatchTimerWithPoliciesList(
		testBatchTimer.ID,
		testBatchTimer.BatchTimerVal,
		testPoliciesList,
	))
}

func TestServerWriteBatchTimerWithPoliciesListActivePlacementError(t *testing.T) {
	errActivePlacementError := errors.New("error active placement")
	s := NewServer(testServerOptions()).(*server)
	s.state = serverOpen
	s.placementWatcher = &mockWatcher{
		activeStagedPlacementFn: func() (services.ActiveStagedPlacement, services.DoneFn, error) {
			activeStagedPlacement := &mockActiveStagedPlacement{
				activePlacementFn: func() (services.Placement, services.DoneFn, error) {
					return nil, nil, errActivePlacementError
				},
			}
			noOpDoneFn := func() {}
			return activeStagedPlacement, noOpDoneFn, nil
		},
	}
	require.Equal(t, errActivePlacementError, s.WriteBatchTimerWithPoliciesList(
		testBatchTimer.ID,
		testBatchTimer.BatchTimerVal,
		testPoliciesList,
	))
}

func TestServerWriteBatchTimerWithPoliciesListSuccess(t *testing.T) {
	var (
		instancesRes []services.PlacementInstance
		shardRes     uint32
		muRes        unaggregated.MetricUnion
		plRes        policy.PoliciesList
	)
	s := NewServer(testServerOptions()).(*server)
	s.state = serverOpen
	s.writerMgr = &mockInstanceWriterManager{
		writeToFn: func(
			instances []services.PlacementInstance,
			shard uint32,
			mu unaggregated.MetricUnion,
			pl policy.PoliciesList,
		) error {
			instancesRes = instances
			shardRes = shard
			muRes = mu
			plRes = pl
			return nil
		},
	}
	s.placementWatcher = &mockWatcher{
		activeStagedPlacementFn: func() (services.ActiveStagedPlacement, services.DoneFn, error) {
			noOpDoneFn := func() {}
			activeStagedPlacement := &mockActiveStagedPlacement{
				activePlacementFn: func() (services.Placement, services.DoneFn, error) {
					return testPlacement, noOpDoneFn, nil
				},
			}
			return activeStagedPlacement, noOpDoneFn, nil
		},
	}
	require.NoError(t, s.WriteBatchTimerWithPoliciesList(
		testBatchTimer.ID,
		testBatchTimer.BatchTimerVal,
		testPoliciesList,
	))
	expectedInstances := []services.PlacementInstance{
		testPlacementInstances[0],
		testPlacementInstances[2],
	}
	require.Equal(t, expectedInstances, instancesRes)
	require.Equal(t, uint32(1), shardRes)
	require.Equal(t, testBatchTimer, muRes)
	require.Equal(t, testPoliciesList, plRes)
}

func TestServerWriteGaugeWithPoliciesListClosed(t *testing.T) {
	s := NewServer(testServerOptions()).(*server)
	s.state = serverNotOpen
	require.Equal(t, errServerIsNotOpenOrClosed, s.WriteGaugeWithPoliciesList(
		testGauge.ID,
		testGauge.GaugeVal,
		testPoliciesList,
	))
}

func TestServerWriteGaugeWithPoliciesListActiveStagedPlacementError(t *testing.T) {
	errActiveStagedPlacementError := errors.New("error active staged placement")
	s := NewServer(testServerOptions()).(*server)
	s.state = serverOpen
	s.placementWatcher = &mockWatcher{
		activeStagedPlacementFn: func() (services.ActiveStagedPlacement, services.DoneFn, error) {
			return nil, nil, errActiveStagedPlacementError
		},
	}
	require.Equal(t, errActiveStagedPlacementError, s.WriteGaugeWithPoliciesList(
		testGauge.ID,
		testGauge.GaugeVal,
		testPoliciesList,
	))
}

func TestServerWriteGaugeWithPoliciesListActivePlacementError(t *testing.T) {
	errActivePlacementError := errors.New("error active placement")
	s := NewServer(testServerOptions()).(*server)
	s.state = serverOpen
	s.placementWatcher = &mockWatcher{
		activeStagedPlacementFn: func() (services.ActiveStagedPlacement, services.DoneFn, error) {
			activeStagedPlacement := &mockActiveStagedPlacement{
				activePlacementFn: func() (services.Placement, services.DoneFn, error) {
					return nil, nil, errActivePlacementError
				},
			}
			noOpDoneFn := func() {}
			return activeStagedPlacement, noOpDoneFn, nil
		},
	}
	require.Equal(t, errActivePlacementError, s.WriteGaugeWithPoliciesList(
		testGauge.ID,
		testGauge.GaugeVal,
		testPoliciesList,
	))
}

func TestServerWriteGaugeWithPoliciesListSuccess(t *testing.T) {
	var (
		instancesRes []services.PlacementInstance
		shardRes     uint32
		muRes        unaggregated.MetricUnion
		plRes        policy.PoliciesList
	)
	s := NewServer(testServerOptions()).(*server)
	s.state = serverOpen
	s.writerMgr = &mockInstanceWriterManager{
		writeToFn: func(
			instances []services.PlacementInstance,
			shard uint32,
			mu unaggregated.MetricUnion,
			pl policy.PoliciesList,
		) error {
			instancesRes = instances
			shardRes = shard
			muRes = mu
			plRes = pl
			return nil
		},
	}
	s.placementWatcher = &mockWatcher{
		activeStagedPlacementFn: func() (services.ActiveStagedPlacement, services.DoneFn, error) {
			noOpDoneFn := func() {}
			activeStagedPlacement := &mockActiveStagedPlacement{
				activePlacementFn: func() (services.Placement, services.DoneFn, error) {
					return testPlacement, noOpDoneFn, nil
				},
			}
			return activeStagedPlacement, noOpDoneFn, nil
		},
	}
	require.NoError(t, s.WriteGaugeWithPoliciesList(
		testGauge.ID,
		testGauge.GaugeVal,
		testPoliciesList,
	))
	expectedInstances := []services.PlacementInstance{
		testPlacementInstances[0],
		testPlacementInstances[2],
	}
	require.Equal(t, expectedInstances, instancesRes)
	require.Equal(t, uint32(1), shardRes)
	require.Equal(t, testGauge, muRes)
	require.Equal(t, testPoliciesList, plRes)
}

func TestServerFlushClosed(t *testing.T) {
	s := NewServer(testServerOptions()).(*server)
	s.state = serverClosed
	require.Equal(t, errServerIsNotOpenOrClosed, s.Flush())
}

func TestServerFlushError(t *testing.T) {
	errTestFlush := errors.New("test flush error")
	opts := testServerOptions()
	s := NewServer(opts).(*server)
	s.state = serverOpen
	s.writerMgr = &mockInstanceWriterManager{
		flushFn: func() error { return errTestFlush },
	}
	require.Equal(t, errTestFlush, s.Flush())
}

func TestServerFlushSuccess(t *testing.T) {
	opts := testServerOptions()
	s := NewServer(opts).(*server)
	s.state = serverOpen
	s.writerMgr = &mockInstanceWriterManager{
		flushFn: func() error { return nil },
	}
	require.NoError(t, s.Flush())
}

func TestServerCloseNotOpenOrAlreadyClosed(t *testing.T) {
	s := NewServer(testServerOptions()).(*server)

	s.state = serverNotOpen
	require.Equal(t, errServerIsNotOpenOrClosed, s.Close())

	s.state = serverClosed
	require.Equal(t, errServerIsNotOpenOrClosed, s.Close())
}

func TestServerCloseSuccess(t *testing.T) {
	s := NewServer(testServerOptions()).(*server)
	s.state = serverOpen
	require.NoError(t, s.Close())
}

func testPlacementWatcherOptions() services.StagedPlacementWatcherOptions {
	return placement.NewStagedPlacementWatcherOptions().
		SetInitWatchTimeout(100 * time.Millisecond)
}

func testServerOptions() ServerOptions {
	return NewServerOptions().
		SetClockOptions(clock.NewOptions()).
		SetConnectionOptions(testConnectionOptions()).
		SetInstrumentOptions(instrument.NewOptions()).
		SetShardFn(func(id []byte, numShards int) uint32 { return 1 }).
		SetInstanceQueueSize(10)
}

type watchFn func() error
type activePlacementFn func() (services.Placement, services.DoneFn, error)
type activeStagedPlacementFn func() (services.ActiveStagedPlacement, services.DoneFn, error)

type mockActiveStagedPlacement struct {
	activePlacementFn activePlacementFn
}

func (mp *mockActiveStagedPlacement) ActivePlacement() (services.Placement, services.DoneFn, error) {
	return mp.activePlacementFn()
}

func (mp *mockActiveStagedPlacement) Close() error { return nil }

type mockWatcher struct {
	watchFn                 watchFn
	activeStagedPlacementFn activeStagedPlacementFn
}

func (mw *mockWatcher) Watch() error { return mw.watchFn() }

func (mw *mockWatcher) ActiveStagedPlacement() (services.ActiveStagedPlacement, services.DoneFn, error) {
	return mw.activeStagedPlacementFn()
}

func (mw *mockWatcher) Unwatch() error { return nil }

type writeToFn func(
	[]services.PlacementInstance,
	uint32,
	unaggregated.MetricUnion,
	policy.PoliciesList,
) error

type mockInstanceWriterManager struct {
	writeToFn writeToFn
	flushFn   mockFlushFn
}

func (mgr *mockInstanceWriterManager) AddInstances(instances []services.PlacementInstance) error {
	return nil
}

func (mgr *mockInstanceWriterManager) RemoveInstances(instances []services.PlacementInstance) error {
	return nil
}

func (mgr *mockInstanceWriterManager) WriteTo(
	instances []services.PlacementInstance,
	shard uint32,
	mu unaggregated.MetricUnion,
	pl policy.PoliciesList,
) error {
	return mgr.writeToFn(instances, shard, mu, pl)
}

func (mgr *mockInstanceWriterManager) Flush() error { return mgr.flushFn() }
func (mgr *mockInstanceWriterManager) Close() error { return nil }
