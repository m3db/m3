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
	testVersionedPolicies = policy.CustomVersionedPolicies(
		2,
		time.Now(),
		[]policy.Policy{
			policy.NewPolicy(20*time.Second, xtime.Second, 6*time.Hour),
			policy.NewPolicy(time.Minute, xtime.Minute, 2*24*time.Hour),
			policy.NewPolicy(10*time.Minute, xtime.Minute, 25*24*time.Hour),
		},
	)
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

func TestServerWriteCounterWithPoliciesClosed(t *testing.T) {
	s := NewServer(testServerOptions()).(*server)
	s.state = serverNotOpen
	require.Equal(t, errServerIsNotOpenOrClosed, s.WriteCounterWithPolicies(
		testCounter.ID,
		testCounter.CounterVal,
		testVersionedPolicies,
	))
}

func TestServerWriteCounterWithPoliciesSuccess(t *testing.T) {
	s := NewServer(testServerOptions()).(*server)
	s.state = serverOpen
	var (
		muRes unaggregated.MetricUnion
		vpRes policy.VersionedPolicies
	)
	s.topology = &mockTopology{
		routeFn: func(mu unaggregated.MetricUnion, vp policy.VersionedPolicies) error {
			muRes = mu
			vpRes = vp
			return nil
		},
	}
	require.NoError(t, s.WriteCounterWithPolicies(
		testCounter.ID,
		testCounter.CounterVal,
		testVersionedPolicies,
	))
	require.Equal(t, testCounter, muRes)
	require.Equal(t, testVersionedPolicies, vpRes)
}

func TestServerWriteBatchTimerWithPoliciesClosed(t *testing.T) {
	s := NewServer(testServerOptions()).(*server)
	s.state = serverNotOpen
	require.Equal(t, errServerIsNotOpenOrClosed, s.WriteBatchTimerWithPolicies(
		testBatchTimer.ID,
		testBatchTimer.BatchTimerVal,
		testVersionedPolicies,
	))
}

func TestServerWriteBatchTimerWithPoliciesSuccess(t *testing.T) {
	s := NewServer(testServerOptions()).(*server)
	s.state = serverOpen
	var (
		muRes unaggregated.MetricUnion
		vpRes policy.VersionedPolicies
	)
	s.topology = &mockTopology{
		routeFn: func(mu unaggregated.MetricUnion, vp policy.VersionedPolicies) error {
			muRes = mu
			vpRes = vp
			return nil
		},
	}
	require.NoError(t, s.WriteBatchTimerWithPolicies(
		testBatchTimer.ID,
		testBatchTimer.BatchTimerVal,
		testVersionedPolicies,
	))
	require.Equal(t, testBatchTimer, muRes)
	require.Equal(t, testVersionedPolicies, vpRes)
}

func TestServerWriteGaugeWithPoliciesClosed(t *testing.T) {
	s := NewServer(testServerOptions()).(*server)
	s.state = serverNotOpen
	require.Equal(t, errServerIsNotOpenOrClosed, s.WriteGaugeWithPolicies(
		testGauge.ID,
		testGauge.GaugeVal,
		testVersionedPolicies,
	))
}

func TestServerWriteGaugeWithPoliciesSuccess(t *testing.T) {
	s := NewServer(testServerOptions()).(*server)
	s.state = serverOpen
	var (
		muRes unaggregated.MetricUnion
		vpRes policy.VersionedPolicies
	)
	s.topology = &mockTopology{
		routeFn: func(mu unaggregated.MetricUnion, vp policy.VersionedPolicies) error {
			muRes = mu
			vpRes = vp
			return nil
		},
	}
	require.NoError(t, s.WriteGaugeWithPolicies(
		testGauge.ID,
		testGauge.GaugeVal,
		testVersionedPolicies,
	))
	require.Equal(t, testGauge, muRes)
	require.Equal(t, testVersionedPolicies, vpRes)
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

func (mt *mockTopology) Route(mu unaggregated.MetricUnion, vp policy.VersionedPolicies) error {
	return mt.routeFn(mu, vp)
}

func (mt *mockTopology) Close() error { return nil }
