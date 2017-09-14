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
	"math"
	"testing"
	"time"

	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/shard"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testNowNanos     = time.Now().UnixNano()
	testCutoverNanos = testNowNanos - int64(time.Minute)
	testCutoffNanos  = testNowNanos + int64(time.Hour)
	testCounter      = unaggregated.MetricUnion{
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
				policy.NewPolicy(policy.NewStoragePolicy(20*time.Second, xtime.Second, 6*time.Hour), policy.DefaultAggregationID),
				policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 2*24*time.Hour), policy.DefaultAggregationID),
				policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 25*24*time.Hour), policy.DefaultAggregationID),
			},
		),
		policy.NewStagedPolicies(
			200,
			true,
			[]policy.Policy{
				policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour), policy.DefaultAggregationID),
			},
		),
	}
	testPlacementInstances = []placement.Instance{
		placement.NewInstance().
			SetID("instance1").
			SetEndpoint("instance1_endpoint").
			SetShards(shard.NewShards([]shard.Shard{
				shard.NewShard(0).
					SetState(shard.Initializing).
					SetCutoverNanos(testCutoverNanos).
					SetCutoffNanos(testCutoffNanos),
				shard.NewShard(1).
					SetState(shard.Initializing).
					SetCutoverNanos(testCutoverNanos).
					SetCutoffNanos(testCutoffNanos),
			})),
		placement.NewInstance().
			SetID("instance2").
			SetEndpoint("instance2_endpoint").
			SetShards(shard.NewShards([]shard.Shard{
				shard.NewShard(2).
					SetState(shard.Initializing).
					SetCutoverNanos(testCutoverNanos).
					SetCutoffNanos(testCutoffNanos),
				shard.NewShard(3).
					SetState(shard.Initializing).
					SetCutoverNanos(testCutoverNanos).
					SetCutoffNanos(testCutoffNanos),
			})),
		placement.NewInstance().
			SetID("instance3").
			SetEndpoint("instance3_endpoint").
			SetShards(shard.NewShards([]shard.Shard{
				shard.NewShard(0).
					SetState(shard.Initializing).
					SetCutoverNanos(testCutoverNanos).
					SetCutoffNanos(testCutoffNanos),
				shard.NewShard(1).
					SetState(shard.Initializing).
					SetCutoverNanos(testCutoverNanos).
					SetCutoffNanos(testCutoffNanos),
			})),
		placement.NewInstance().
			SetID("instance4").
			SetEndpoint("instance4_endpoint").
			SetShards(shard.NewShards([]shard.Shard{
				shard.NewShard(2).
					SetState(shard.Initializing).
					SetCutoverNanos(testCutoverNanos).
					SetCutoffNanos(testCutoffNanos),
				shard.NewShard(3).
					SetState(shard.Initializing).
					SetCutoverNanos(testCutoverNanos).
					SetCutoffNanos(testCutoffNanos),
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

func TestServerWriteMetricWithPoliciesListClosed(t *testing.T) {
	s := NewServer(testServerOptions()).(*server)
	s.state = serverNotOpen
	for _, input := range []unaggregated.MetricUnion{testCounter, testBatchTimer, testGauge} {
		var err error
		switch input.Type {
		case unaggregated.CounterType:
			err = s.WriteCounterWithPoliciesList(input.ID, input.CounterVal, testPoliciesList)
		case unaggregated.BatchTimerType:
			err = s.WriteBatchTimerWithPoliciesList(input.ID, input.BatchTimerVal, testPoliciesList)
		case unaggregated.GaugeType:
			err = s.WriteGaugeWithPoliciesList(input.ID, input.GaugeVal, testPoliciesList)
		}
		require.Equal(t, errServerIsNotOpenOrClosed, err)
	}
}

func TestServerWriteMetricWithPoliciesListActiveStagedPlacementError(t *testing.T) {
	errActiveStagedPlacementError := errors.New("error active staged placement")
	s := NewServer(testServerOptions()).(*server)
	s.state = serverOpen
	s.placementWatcher = &mockWatcher{
		activeStagedPlacementFn: func() (placement.ActiveStagedPlacement, placement.DoneFn, error) {
			return nil, nil, errActiveStagedPlacementError
		},
	}
	for _, input := range []unaggregated.MetricUnion{testCounter, testBatchTimer, testGauge} {
		var err error
		switch input.Type {
		case unaggregated.CounterType:
			err = s.WriteCounterWithPoliciesList(input.ID, input.CounterVal, testPoliciesList)
		case unaggregated.BatchTimerType:
			err = s.WriteBatchTimerWithPoliciesList(input.ID, input.BatchTimerVal, testPoliciesList)
		case unaggregated.GaugeType:
			err = s.WriteGaugeWithPoliciesList(input.ID, input.GaugeVal, testPoliciesList)
		}
		require.Equal(t, errActiveStagedPlacementError, err)
	}
}

func TestServerWriteMetricWithPoliciesListActivePlacementError(t *testing.T) {
	errActivePlacementError := errors.New("error active placement")
	s := NewServer(testServerOptions()).(*server)
	s.state = serverOpen
	s.placementWatcher = &mockWatcher{
		activeStagedPlacementFn: func() (placement.ActiveStagedPlacement, placement.DoneFn, error) {
			activeStagedPlacement := &mockActiveStagedPlacement{
				activePlacementFn: func() (placement.Placement, placement.DoneFn, error) {
					return nil, nil, errActivePlacementError
				},
			}
			noOpDoneFn := func() {}
			return activeStagedPlacement, noOpDoneFn, nil
		},
	}
	for _, input := range []unaggregated.MetricUnion{testCounter, testBatchTimer, testGauge} {
		var err error
		switch input.Type {
		case unaggregated.CounterType:
			err = s.WriteCounterWithPoliciesList(input.ID, input.CounterVal, testPoliciesList)
		case unaggregated.BatchTimerType:
			err = s.WriteBatchTimerWithPoliciesList(input.ID, input.BatchTimerVal, testPoliciesList)
		case unaggregated.GaugeType:
			err = s.WriteGaugeWithPoliciesList(input.ID, input.GaugeVal, testPoliciesList)
		}
		require.Equal(t, errActivePlacementError, err)
	}
}

func TestServerWriteMetricWithPoliciesListSuccess(t *testing.T) {
	var (
		instancesRes []placement.Instance
		shardRes     uint32
		muRes        unaggregated.MetricUnion
		plRes        policy.PoliciesList
	)
	s := NewServer(testServerOptions()).(*server)
	s.state = serverOpen
	s.nowFn = func() time.Time { return time.Unix(0, testNowNanos) }
	s.writerMgr = &mockInstanceWriterManager{
		writeToFn: func(
			instance placement.Instance,
			shard uint32,
			mu unaggregated.MetricUnion,
			pl policy.PoliciesList,
		) error {
			instancesRes = append(instancesRes, instance)
			shardRes = shard
			muRes = mu
			plRes = pl
			return nil
		},
	}
	s.placementWatcher = &mockWatcher{
		activeStagedPlacementFn: func() (placement.ActiveStagedPlacement, placement.DoneFn, error) {
			noOpDoneFn := func() {}
			activeStagedPlacement := &mockActiveStagedPlacement{
				activePlacementFn: func() (placement.Placement, placement.DoneFn, error) {
					return testPlacement, noOpDoneFn, nil
				},
			}
			return activeStagedPlacement, noOpDoneFn, nil
		},
	}

	expectedInstances := []placement.Instance{
		testPlacementInstances[0],
		testPlacementInstances[2],
	}
	for _, input := range []unaggregated.MetricUnion{testCounter, testBatchTimer, testGauge} {
		// Reset states in each iteration.
		instancesRes = instancesRes[:0]
		shardRes = 0
		muRes = unaggregated.MetricUnion{}
		plRes = plRes[:0]

		var err error
		switch input.Type {
		case unaggregated.CounterType:
			err = s.WriteCounterWithPoliciesList(input.ID, input.CounterVal, testPoliciesList)
		case unaggregated.BatchTimerType:
			err = s.WriteBatchTimerWithPoliciesList(input.ID, input.BatchTimerVal, testPoliciesList)
		case unaggregated.GaugeType:
			err = s.WriteGaugeWithPoliciesList(input.ID, input.GaugeVal, testPoliciesList)
		}

		require.NoError(t, err)
		require.Equal(t, expectedInstances, instancesRes)
		require.Equal(t, uint32(1), shardRes)
		require.Equal(t, input, muRes)
		require.Equal(t, testPoliciesList, plRes)
	}
}

func TestServerWriteMetricWithPoliciesListPartialError(t *testing.T) {
	var (
		instancesRes     []placement.Instance
		shardRes         uint32
		muRes            unaggregated.MetricUnion
		plRes            policy.PoliciesList
		errInstanceWrite = errors.New("instance write error")
	)
	s := NewServer(testServerOptions()).(*server)
	s.state = serverOpen
	s.nowFn = func() time.Time { return time.Unix(0, testNowNanos) }
	s.writerMgr = &mockInstanceWriterManager{
		writeToFn: func(
			instance placement.Instance,
			shard uint32,
			mu unaggregated.MetricUnion,
			pl policy.PoliciesList,
		) error {
			if instance.ID() == testPlacementInstances[0].ID() {
				return errInstanceWrite
			}
			instancesRes = append(instancesRes, instance)
			shardRes = shard
			muRes = mu
			plRes = pl
			return nil
		},
	}
	s.placementWatcher = &mockWatcher{
		activeStagedPlacementFn: func() (placement.ActiveStagedPlacement, placement.DoneFn, error) {
			noOpDoneFn := func() {}
			activeStagedPlacement := &mockActiveStagedPlacement{
				activePlacementFn: func() (placement.Placement, placement.DoneFn, error) {
					return testPlacement, noOpDoneFn, nil
				},
			}
			return activeStagedPlacement, noOpDoneFn, nil
		},
	}

	expectedInstances := []placement.Instance{
		testPlacementInstances[2],
	}
	err := s.WriteCounterWithPoliciesList(testCounter.ID, testCounter.CounterVal, testPoliciesList)
	require.NoError(t, err)
	require.Equal(t, expectedInstances, instancesRes)
	require.Equal(t, uint32(1), shardRes)
	require.Equal(t, testCounter, muRes)
	require.Equal(t, testPoliciesList, plRes)
}

func TestServerWriteMetricWithPoliciesListBeforeShardCutover(t *testing.T) {
	var instancesRes []placement.Instance
	s := NewServer(testServerOptions()).(*server)
	s.shardCutoverWarmupDuration = time.Second
	s.state = serverOpen
	s.nowFn = func() time.Time { return time.Unix(0, testCutoverNanos-1).Add(-time.Second) }
	s.writerMgr = &mockInstanceWriterManager{
		writeToFn: func(
			instance placement.Instance,
			shard uint32,
			mu unaggregated.MetricUnion,
			pl policy.PoliciesList,
		) error {
			instancesRes = append(instancesRes, instance)
			return nil
		},
	}
	s.placementWatcher = &mockWatcher{
		activeStagedPlacementFn: func() (placement.ActiveStagedPlacement, placement.DoneFn, error) {
			noOpDoneFn := func() {}
			activeStagedPlacement := &mockActiveStagedPlacement{
				activePlacementFn: func() (placement.Placement, placement.DoneFn, error) {
					return testPlacement, noOpDoneFn, nil
				},
			}
			return activeStagedPlacement, noOpDoneFn, nil
		},
	}

	err := s.WriteCounterWithPoliciesList(testCounter.ID, testCounter.CounterVal, testPoliciesList)
	require.NoError(t, err)
	require.Nil(t, instancesRes)
}

func TestServerWriteMetricWithPoliciesListAfterShardCutoff(t *testing.T) {
	var instancesRes []placement.Instance
	s := NewServer(testServerOptions()).(*server)
	s.shardCutoffLingerDuration = time.Second
	s.state = serverOpen
	s.nowFn = func() time.Time { return time.Unix(0, testCutoffNanos+1).Add(time.Second) }
	s.writerMgr = &mockInstanceWriterManager{
		writeToFn: func(
			instance placement.Instance,
			shard uint32,
			mu unaggregated.MetricUnion,
			pl policy.PoliciesList,
		) error {
			instancesRes = append(instancesRes, instance)
			return nil
		},
	}
	s.placementWatcher = &mockWatcher{
		activeStagedPlacementFn: func() (placement.ActiveStagedPlacement, placement.DoneFn, error) {
			noOpDoneFn := func() {}
			activeStagedPlacement := &mockActiveStagedPlacement{
				activePlacementFn: func() (placement.Placement, placement.DoneFn, error) {
					return testPlacement, noOpDoneFn, nil
				},
			}
			return activeStagedPlacement, noOpDoneFn, nil
		},
	}

	err := s.WriteCounterWithPoliciesList(testCounter.ID, testCounter.CounterVal, testPoliciesList)
	require.NoError(t, err)
	require.Nil(t, instancesRes)
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

func TestServerWriteTimeRangeFor(t *testing.T) {
	s := NewServer(testServerOptions()).(*server)
	testShard := shard.NewShard(0).SetState(shard.Initializing)
	for _, input := range []struct {
		cutoverNanos     int64
		cutoffNanos      int64
		expectedEarliest int64
		expectedLatest   int64
	}{
		{
			cutoverNanos:     0,
			cutoffNanos:      int64(math.MaxInt64),
			expectedEarliest: 0,
			expectedLatest:   int64(math.MaxInt64),
		},
		{
			cutoverNanos:     testNowNanos,
			cutoffNanos:      int64(math.MaxInt64),
			expectedEarliest: testNowNanos - int64(time.Minute),
			expectedLatest:   int64(math.MaxInt64),
		},
		{
			cutoverNanos:     0,
			cutoffNanos:      testNowNanos,
			expectedEarliest: 0,
			expectedLatest:   testNowNanos + int64(10*time.Minute),
		},
	} {
		testShard = testShard.SetCutoverNanos(input.cutoverNanos).SetCutoffNanos(input.cutoffNanos)
		earliest, latest := s.writeTimeRangeFor(testShard)
		require.Equal(t, input.expectedEarliest, earliest)
		require.Equal(t, input.expectedLatest, latest)
	}
}

func testServerOptions() ServerOptions {
	return NewServerOptions().
		SetClockOptions(clock.NewOptions()).
		SetConnectionOptions(testConnectionOptions()).
		SetInstrumentOptions(instrument.NewOptions()).
		SetShardFn(func(id []byte, numShards int) uint32 { return 1 }).
		SetInstanceQueueSize(10).
		SetMaxTimerBatchSize(140).
		SetShardCutoverWarmupDuration(time.Minute).
		SetShardCutoffLingerDuration(10 * time.Minute)
}

type watchFn func() error
type activePlacementFn func() (placement.Placement, placement.DoneFn, error)
type activeStagedPlacementFn func() (placement.ActiveStagedPlacement, placement.DoneFn, error)

type mockActiveStagedPlacement struct {
	activePlacementFn activePlacementFn
}

func (mp *mockActiveStagedPlacement) ActivePlacement() (placement.Placement, placement.DoneFn, error) {
	return mp.activePlacementFn()
}

func (mp *mockActiveStagedPlacement) Close() error { return nil }

type mockWatcher struct {
	watchFn                 watchFn
	activeStagedPlacementFn activeStagedPlacementFn
}

func (mw *mockWatcher) Watch() error { return mw.watchFn() }

func (mw *mockWatcher) ActiveStagedPlacement() (placement.ActiveStagedPlacement, placement.DoneFn, error) {
	return mw.activeStagedPlacementFn()
}

func (mw *mockWatcher) Unwatch() error { return nil }

type writeToFn func(
	placement.Instance,
	uint32,
	unaggregated.MetricUnion,
	policy.PoliciesList,
) error

type mockInstanceWriterManager struct {
	writeToFn writeToFn
	flushFn   mockFlushFn
}

func (mgr *mockInstanceWriterManager) AddInstances(instances []placement.Instance) error {
	return nil
}

func (mgr *mockInstanceWriterManager) RemoveInstances(instances []placement.Instance) error {
	return nil
}

func (mgr *mockInstanceWriterManager) WriteTo(
	instance placement.Instance,
	shard uint32,
	mu unaggregated.MetricUnion,
	pl policy.PoliciesList,
) error {
	return mgr.writeToFn(instance, shard, mu, pl)
}

func (mgr *mockInstanceWriterManager) Flush() error { return mgr.flushFn() }
func (mgr *mockInstanceWriterManager) Close() error { return nil }
