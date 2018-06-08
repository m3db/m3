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

// TODO(xichen): revive the test once the encoder APIs are added.
/*
import (
	"time"

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/retry"
	xserver "github.com/m3db/m3x/server"
	xtime "github.com/m3db/m3x/time"
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
	testDefaultMetadatas = metadata.DefaultStagedMetadatas
	testCustomMetadatas  = metadata.StagedMetadatas{
		{
			CutoverNanos: time.Now().UnixNano(),
			Tombstoned:   false,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: aggregation.DefaultID,
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
							policy.NewStoragePolicy(time.Minute, xtime.Minute, 2*24*time.Hour),
							policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 30*24*time.Hour),
						},
					},
				},
			},
		},
	}
	testCounterWithMetadatas = unaggregated.CounterWithMetadatas{
		Counter:         testCounter.Counter(),
		StagedMetadatas: testDefaultMetadatas,
	}
	testBatchTimerWithMetadatas = unaggregated.BatchTimerWithMetadatas{
		BatchTimer:      testBatchTimer.BatchTimer(),
		StagedMetadatas: testCustomMetadatas,
	}
	testGaugeWithMetadatas = unaggregated.GaugeWithMetadatas{
		Gauge:           testGauge.Gauge(),
		StagedMetadatas: testCustomMetadatas,
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
		SetServerOptions(xserver.NewOptions().SetRetryOptions(retry.NewOptions().SetMaxRetries(2))).
		SetInstrumentOptions(opts.InstrumentOptions().SetReportInterval(time.Second))
}

func TestHandleUnaggregatedMsgpack(t *testing.T) {
	agg := capture.NewAggregator()
	h := NewHandler(agg, testServerOptions())

	var (
		numClients     = 9
		wgClient       sync.WaitGroup
		expectedResult capture.SnapshotResult
	)

	listener, err := net.Listen("tcp", testListenAddress)
	require.NoError(t, err)

	s := xserver.NewServer(testListenAddress, h, xserver.NewOptions())
	// Start server.
	require.NoError(t, s.Serve(listener))

	// Now establish multiple connections and send data to the server.
	for i := 0; i < numClients; i++ {
		wgClient.Add(1)

		// Add test metrics to expected result.
		expectedResult.CountersWithMetadatas = append(expectedResult.CountersWithMetadatas, testCounterWithMetadatas)
		expectedResult.BatchTimersWithMetadatas = append(expectedResult.BatchTimersWithMetadatas, testBatchTimerWithMetadatas)
		expectedResult.GaugesWithMetadatas = append(expectedResult.GaugesWithMetadatas, testGaugeWithMetadatas)

		go func() {
			defer wgClient.Done()

			conn, err := net.Dial("tcp", listener.Addr().String())
			require.NoError(t, err)

			encoder := msgpack.NewUnaggregatedEncoder(msgpack.NewPooledBufferedEncoder(nil))
			encoder.EncodeCounterWithPoliciesList(testCounterWithPoliciesList)
			encoder.EncodeBatchTimerWithPoliciesList(testBatchTimerWithPoliciesList)
			encoder.EncodeGaugeWithPoliciesList(testGaugeWithPoliciesList)

			_, err = conn.Write(encoder.Encoder().Bytes())
			require.NoError(t, err)
		}()
	}

	// Wait for all metrics to be processed.
	for agg.NumMetricsAdded() < numClients*3 {
		time.Sleep(50 * time.Millisecond)
	}

	// Close the server.
	s.Close()

	// Assert the snapshot match expectations.
	require.Equal(t, expectedResult, agg.Snapshot())
}
*/
