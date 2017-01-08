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

package server

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3aggregator/aggregator/mock"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/retry"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testCounterWithPolicies = unaggregated.CounterWithPolicies{
		Counter: unaggregated.Counter{
			ID:    []byte("foo"),
			Value: 123,
		},
		VersionedPolicies: policy.DefaultVersionedPolicies,
	}
	testBatchTimerWithPolicies = unaggregated.BatchTimerWithPolicies{
		BatchTimer: unaggregated.BatchTimer{
			ID:     []byte("bar"),
			Values: []float64{1.0, 2.0, 3.0},
		},
		VersionedPolicies: policy.VersionedPolicies{
			Version: 1,
			Cutover: time.Now(),
			Policies: []policy.Policy{
				{
					Resolution: policy.Resolution{Window: time.Duration(1), Precision: xtime.Second},
					Retention:  policy.Retention(time.Hour),
				},
			},
		},
	}
	testGaugeWithPolicies = unaggregated.GaugeWithPolicies{
		Gauge: unaggregated.Gauge{
			ID:    []byte("baz"),
			Value: 456.780,
		},
		VersionedPolicies: policy.DefaultVersionedPolicies,
	}
)

func testAggregationServerOptions() Options {
	iteratorPool := msgpack.NewUnaggregatedIteratorPool(nil)
	iteratorOpts := msgpack.NewUnaggregatedIteratorOptions().SetIteratorPool(iteratorPool)
	iteratorPool.Init(func() msgpack.UnaggregatedIterator {
		return msgpack.NewUnaggregatedIterator(nil, iteratorOpts)
	})

	opts := NewOptions()
	return opts.
		SetPacketQueueSize(1024).
		SetWorkerPoolSize(2).
		SetIteratorPool(iteratorPool).
		SetRetrier(xretry.NewRetrier(xretry.NewOptions().SetMaxRetries(2))).
		SetInstrumentOptions(opts.InstrumentOptions().SetReportInterval(time.Second))
}

func testAggregationServer(l net.Listener) (*Server, *int32, *int32, *int32, *int32) {
	var (
		numAddedConns   int32
		numRemovedConns int32
		numHandledConns int32
		numPackets      int32
	)

	opts := testAggregationServerOptions()
	agg := mock.NewAggregator()
	s := NewServer(l, agg, opts)

	s.addConnectionFn = func(conn net.Conn) bool {
		ret := s.addConnection(conn)
		atomic.AddInt32(&numAddedConns, 1)
		return ret
	}

	s.removeConnectionFn = func(conn net.Conn) {
		s.removeConnection(conn)
		atomic.AddInt32(&numRemovedConns, 1)
	}

	s.handleConnectionFn = func(conn net.Conn) {
		s.handleConnection(conn)
		atomic.AddInt32(&numHandledConns, 1)
	}

	s.processPacketFn = func(p packet) {
		s.processPacket(p)
		atomic.AddInt32(&numPackets, 1)
	}

	return s, &numAddedConns, &numRemovedConns, &numHandledConns, &numPackets
}

func TestAggregationServerListenAndClose(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s, numAddedConns, numRemovedConns, numHandledConns, numPackets := testAggregationServer(l)

	var (
		numClients     = 9
		wgClient       sync.WaitGroup
		expectedResult mock.SnapshotResult
	)

	// Start server
	go s.ListenAndServe()

	// Now establish multiple connections and send data to the server
	for i := 0; i < numClients; i++ {
		wgClient.Add(1)

		// Add test metrics to expected result
		expectedResult.CountersWithPolicies = append(expectedResult.CountersWithPolicies, testCounterWithPolicies)
		expectedResult.BatchTimersWithPolicies = append(expectedResult.BatchTimersWithPolicies, testBatchTimerWithPolicies)
		expectedResult.GaugesWithPolicies = append(expectedResult.GaugesWithPolicies, testGaugeWithPolicies)

		go func() {
			defer wgClient.Done()

			conn, err := net.Dial("tcp", l.Addr().String())
			require.NoError(t, err)

			encoder := msgpack.NewUnaggregatedEncoder(msgpack.NewPooledBufferedEncoder(nil))
			encoder.EncodeCounterWithPolicies(testCounterWithPolicies)
			encoder.EncodeBatchTimerWithPolicies(testBatchTimerWithPolicies)
			encoder.EncodeGaugeWithPolicies(testGaugeWithPolicies)

			_, err = conn.Write(encoder.Encoder().Bytes())
			require.NoError(t, err)
		}()
	}

	// Wait for all connections to be added
	for atomic.LoadInt32(numPackets) < int32(numClients)*3 {
	}

	// Close the server
	s.Close()

	// Assert the number of connections match expectations
	require.Equal(t, int32(numClients), atomic.LoadInt32(numAddedConns))
	require.Equal(t, int32(numClients), atomic.LoadInt32(numRemovedConns))
	require.Equal(t, int32(numClients), atomic.LoadInt32(numHandledConns))

	// Assert the snapshot match expectations
	require.Equal(t, expectedResult, s.aggregator.(mock.Aggregator).Snapshot())
}
