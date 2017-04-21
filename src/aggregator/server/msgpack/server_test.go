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

package msgpack

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

const (
	testListenAddress = "127.0.0.1:0"
)

var (
	testCounter = unaggregated.MetricUnion{
		Type:       unaggregated.CounterType,
		ID:         []byte("foo"),
		CounterVal: 123,
	}
	testBatchTimer = unaggregated.MetricUnion{
		Type:          unaggregated.BatchTimerType,
		ID:            []byte("bar"),
		BatchTimerVal: []float64{1.0, 2.0, 3.0},
	}
	testGauge = unaggregated.MetricUnion{
		Type:     unaggregated.GaugeType,
		ID:       []byte("baz"),
		GaugeVal: 456.780,
	}
	testDefaultVersionedPolices = policy.DefaultVersionedPolicies(
		1,
		time.Now(),
	)
	testCounterWithPolicies = unaggregated.CounterWithPolicies{
		Counter:           testCounter.Counter(),
		VersionedPolicies: testDefaultVersionedPolices,
	}
	testBatchTimerWithPolicies = unaggregated.BatchTimerWithPolicies{
		BatchTimer: testBatchTimer.BatchTimer(),
		VersionedPolicies: policy.CustomVersionedPolicies(
			1,
			time.Now(),
			[]policy.Policy{
				policy.NewPolicy(time.Duration(1), xtime.Second, time.Hour),
			},
		),
	}
	testGaugeWithPolicies = unaggregated.GaugeWithPolicies{
		Gauge:             testGauge.Gauge(),
		VersionedPolicies: testDefaultVersionedPolices,
	}
)

func testServerOptions() Options {
	iteratorPool := msgpack.NewUnaggregatedIteratorPool(nil)
	iteratorOpts := msgpack.NewUnaggregatedIteratorOptions().SetIteratorPool(iteratorPool)
	iteratorPool.Init(func() msgpack.UnaggregatedIterator {
		return msgpack.NewUnaggregatedIterator(nil, iteratorOpts)
	})

	opts := NewOptions()
	return opts.
		SetIteratorPool(iteratorPool).
		SetRetrier(xretry.NewRetrier(xretry.NewOptions().SetMaxRetries(2))).
		SetInstrumentOptions(opts.InstrumentOptions().SetReportInterval(time.Second))
}

func testServer(addr string) (*server, mock.Aggregator, *int32, *int32, *int32) {
	var (
		numAdded   int32
		numRemoved int32
		numHandled int32
	)

	opts := testServerOptions()
	agg := mock.NewAggregator()
	s := NewServer(addr, agg, opts).(*server)

	s.addConnectionFn = func(conn net.Conn) bool {
		atomic.AddInt32(&numAdded, 1)
		ret := s.addConnection(conn)
		return ret
	}

	s.removeConnectionFn = func(conn net.Conn) {
		atomic.AddInt32(&numRemoved, 1)
		s.removeConnection(conn)
	}

	s.handleConnectionFn = func(conn net.Conn) {
		atomic.AddInt32(&numHandled, 1)
		s.handleConnection(conn)
	}

	return s, agg, &numAdded, &numRemoved, &numHandled
}

func TestServerListenAndClose(t *testing.T) {
	s, agg, numAdded, numRemoved, numHandled := testServer(testListenAddress)

	var (
		numClients     = 9
		wgClient       sync.WaitGroup
		expectedResult mock.SnapshotResult
	)

	// Start server
	err := s.ListenAndServe()
	require.NoError(t, err)
	listenAddr := s.listener.Addr().String()

	// Now establish multiple connections and send data to the server
	for i := 0; i < numClients; i++ {
		wgClient.Add(1)

		// Add test metrics to expected result
		expectedResult.CountersWithPolicies = append(expectedResult.CountersWithPolicies, testCounterWithPolicies)
		expectedResult.BatchTimersWithPolicies = append(expectedResult.BatchTimersWithPolicies, testBatchTimerWithPolicies)
		expectedResult.GaugesWithPolicies = append(expectedResult.GaugesWithPolicies, testGaugeWithPolicies)

		go func() {
			defer wgClient.Done()

			conn, err := net.Dial("tcp", listenAddr)
			require.NoError(t, err)

			encoder := msgpack.NewUnaggregatedEncoder(msgpack.NewPooledBufferedEncoder(nil))
			encoder.EncodeCounterWithPolicies(testCounterWithPolicies)
			encoder.EncodeBatchTimerWithPolicies(testBatchTimerWithPolicies)
			encoder.EncodeGaugeWithPolicies(testGaugeWithPolicies)

			_, err = conn.Write(encoder.Encoder().Bytes())
			require.NoError(t, err)
		}()
	}

	// Wait for all metrics to be processed
	for agg.NumMetricsAdded() < numClients*3 {
		time.Sleep(100 * time.Millisecond)
	}

	// Close the server
	s.Close()

	// Assert the number of connections match expectations
	require.Equal(t, int32(numClients), atomic.LoadInt32(numAdded))
	require.Equal(t, int32(numClients), atomic.LoadInt32(numRemoved))
	require.Equal(t, int32(numClients), atomic.LoadInt32(numHandled))

	// Assert the snapshot match expectations
	require.Equal(t, expectedResult, agg.Snapshot())
}
