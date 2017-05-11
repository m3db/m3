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
)

func TestServerOpenNotOpenOrClosed(t *testing.T) {
	s := NewServer(testServerOptions()).(*server)

	s.state = serverOpen
	require.Equal(t, errServerIsOpenOrClosed, s.Open())

	s.state = serverClosed
	require.Equal(t, errServerIsOpenOrClosed, s.Open())
}

func TestServerOpenTopologyOpenError(t *testing.T) {
	errTestTopologyOpen := errors.New("error opening topology")
	s := NewServer(testServerOptions()).(*server)
	s.topology = &mockTopology{
		openFn: func() error { return errTestTopologyOpen },
	}
	require.Equal(t, errTestTopologyOpen, s.Open())
}

func TestServerOpenSuccess(t *testing.T) {
	s := NewServer(testServerOptions()).(*server)
	s.topology = &mockTopology{
		openFn: func() error { return nil },
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

func TestServerWriteCounterWithPoliciesListSuccess(t *testing.T) {
	s := NewServer(testServerOptions()).(*server)
	s.state = serverOpen
	var (
		muRes unaggregated.MetricUnion
		plRes policy.PoliciesList
	)
	s.topology = &mockTopology{
		routeFn: func(mu unaggregated.MetricUnion, pl policy.PoliciesList) error {
			muRes = mu
			plRes = pl
			return nil
		},
	}
	require.NoError(t, s.WriteCounterWithPoliciesList(
		testCounter.ID,
		testCounter.CounterVal,
		testPoliciesList,
	))
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

func TestServerWriteBatchTimerWithPoliciesListSuccess(t *testing.T) {
	s := NewServer(testServerOptions()).(*server)
	s.state = serverOpen
	var (
		muRes unaggregated.MetricUnion
		plRes policy.PoliciesList
	)
	s.topology = &mockTopology{
		routeFn: func(mu unaggregated.MetricUnion, pl policy.PoliciesList) error {
			muRes = mu
			plRes = pl
			return nil
		},
	}
	require.NoError(t, s.WriteBatchTimerWithPoliciesList(
		testBatchTimer.ID,
		testBatchTimer.BatchTimerVal,
		testPoliciesList,
	))
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

func TestServerWriteGaugeWithPoliciesListSuccess(t *testing.T) {
	s := NewServer(testServerOptions()).(*server)
	s.state = serverOpen
	var (
		muRes unaggregated.MetricUnion
		plRes policy.PoliciesList
	)
	s.topology = &mockTopology{
		routeFn: func(mu unaggregated.MetricUnion, pl policy.PoliciesList) error {
			muRes = mu
			plRes = pl
			return nil
		},
	}
	require.NoError(t, s.WriteGaugeWithPoliciesList(
		testGauge.ID,
		testGauge.GaugeVal,
		testPoliciesList,
	))
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

func testServerOptions() ServerOptions {
	return NewServerOptions().
		SetClockOptions(clock.NewOptions()).
		SetConnectionOptions(testConnectionOptions()).
		SetInstrumentOptions(instrument.NewOptions()).
		SetTopologyOptions(testTopologyOptions()).
		SetInstanceQueueSize(10)
}

type openFn func() error

type mockTopology struct {
	openFn  openFn
	routeFn routeFn
}

func (mt *mockTopology) Open() error { return mt.openFn() }

func (mt *mockTopology) Route(mu unaggregated.MetricUnion, pl policy.PoliciesList) error {
	return mt.routeFn(mu, pl)
}

func (mt *mockTopology) Close() error { return nil }
