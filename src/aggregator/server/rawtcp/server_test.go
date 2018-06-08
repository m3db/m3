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

package rawtcp

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3aggregator/aggregator/capture"
	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/encoding"
	"github.com/m3db/m3metrics/encoding/msgpack"
	"github.com/m3db/m3metrics/encoding/protobuf"
	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/pipeline"
	"github.com/m3db/m3metrics/pipeline/applied"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/retry"
	xserver "github.com/m3db/m3x/server"
	xtime "github.com/m3db/m3x/time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
)

const (
	testListenAddress = "127.0.0.1:0"
)

var (
	testNowNanos = time.Now().UnixNano()
	testCounter  = unaggregated.MetricUnion{
		Type:       metric.CounterType,
		ID:         []byte("testCounter"),
		CounterVal: 123,
	}
	testBatchTimer = unaggregated.MetricUnion{
		Type:          metric.TimerType,
		ID:            []byte("testBatchTimer"),
		BatchTimerVal: []float64{1.0, 2.0, 3.0},
	}
	testGauge = unaggregated.MetricUnion{
		Type:     metric.GaugeType,
		ID:       []byte("testGauge"),
		GaugeVal: 456.780,
	}
	testForwarded = aggregated.Metric{
		Type:      metric.CounterType,
		ID:        []byte("testForwarded"),
		TimeNanos: 12345,
		Value:     908,
	}
	testDefaultPoliciesList = policy.DefaultPoliciesList
	testCustomPoliciesList  = policy.PoliciesList{
		policy.NewStagedPolicies(
			testNowNanos,
			false,
			[]policy.Policy{
				policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour), aggregation.MustCompressTypes(aggregation.Min)),
				policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 2*24*time.Hour), aggregation.DefaultID),
				policy.NewPolicy(policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 30*24*time.Hour), aggregation.DefaultID),
			},
		),
	}
	testDefaultMetadatas = metadata.DefaultStagedMetadatas
	testCustomMetadatas  = metadata.StagedMetadatas{
		{
			CutoverNanos: testNowNanos,
			Tombstoned:   false,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: aggregation.MustCompressTypes(aggregation.Min),
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 6*time.Hour),
						},
					},
					{
						AggregationID: aggregation.DefaultID,
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Minute, xtime.Minute, 2*24*time.Hour),
							policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 30*24*time.Hour),
						},
					},
				},
			},
		},
	}
	testForwardMetadata = metadata.ForwardMetadata{
		AggregationID: aggregation.DefaultID,
		StoragePolicy: policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour),
		Pipeline: applied.NewPipeline([]applied.OpUnion{
			{
				Type: pipeline.RollupOpType,
				Rollup: applied.RollupOp{
					ID:            []byte("foo"),
					AggregationID: aggregation.MustCompressTypes(aggregation.Count),
				},
			},
		}),
		SourceID:          []byte("testForwardSource"),
		NumForwardedTimes: 3,
	}
	testCounterWithPoliciesList = unaggregated.CounterWithPoliciesList{
		Counter:      testCounter.Counter(),
		PoliciesList: testDefaultPoliciesList,
	}
	testBatchTimerWithPoliciesList = unaggregated.BatchTimerWithPoliciesList{
		BatchTimer:   testBatchTimer.BatchTimer(),
		PoliciesList: testCustomPoliciesList,
	}
	testGaugeWithPoliciesList = unaggregated.GaugeWithPoliciesList{
		Gauge:        testGauge.Gauge(),
		PoliciesList: testDefaultPoliciesList,
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
		StagedMetadatas: testDefaultMetadatas,
	}
	testMetricWithForwardMetadata = aggregated.MetricWithForwardMetadata{
		Metric:          testForwarded,
		ForwardMetadata: testForwardMetadata,
	}
	testCmpOpts = []cmp.Option{
		cmpopts.EquateEmpty(),
		cmp.AllowUnexported(policy.StoragePolicy{}),
	}
)

func TestRawTCPServerHandleUnaggregatedMsgpackEncoding(t *testing.T) {
	testRawTCPServerHandleUnaggregated(t, func(int) encodingProtocol { return msgpackEncoding })
}

func TestRawTCPServerHandleUnaggregatedProtobufEncoding(t *testing.T) {
	testRawTCPServerHandleUnaggregated(t, func(int) encodingProtocol { return protobufEncoding })
}

func TestRawTCPServerHandleUnaggregatedMixedEncoding(t *testing.T) {
	testRawTCPServerHandleUnaggregated(t, func(workerID int) encodingProtocol {
		if workerID%2 == 0 {
			return msgpackEncoding
		}
		return protobufEncoding
	})
}

func testRawTCPServerHandleUnaggregated(
	t *testing.T,
	protocolSelector func(int) encodingProtocol,
) {
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
	var expectedTotalMetrics int
	for i := 0; i < numClients; i++ {
		i := i
		wgClient.Add(1)

		// Add test metrics to expected result.
		expectedResult.CountersWithMetadatas = append(expectedResult.CountersWithMetadatas, testCounterWithMetadatas)
		expectedResult.BatchTimersWithMetadatas = append(expectedResult.BatchTimersWithMetadatas, testBatchTimerWithMetadatas)
		expectedResult.GaugesWithMetadatas = append(expectedResult.GaugesWithMetadatas, testGaugeWithMetadatas)

		protocol := protocolSelector(i)
		if protocol == protobufEncoding {
			expectedResult.MetricsWithForwardMetadata = append(expectedResult.MetricsWithForwardMetadata, testMetricWithForwardMetadata)
			expectedTotalMetrics += 4
		} else {
			expectedTotalMetrics += 3
		}

		go func() {
			defer wgClient.Done()

			conn, err := net.Dial("tcp", listener.Addr().String())
			require.NoError(t, err)

			var stream []byte
			switch protocol {
			case msgpackEncoding:
				encoder := msgpack.NewUnaggregatedEncoder(msgpack.NewPooledBufferedEncoder(nil))
				require.NoError(t, encoder.EncodeCounterWithPoliciesList(testCounterWithPoliciesList))
				require.NoError(t, encoder.EncodeBatchTimerWithPoliciesList(testBatchTimerWithPoliciesList))
				require.NoError(t, encoder.EncodeGaugeWithPoliciesList(testGaugeWithPoliciesList))
				stream = encoder.Encoder().Bytes()
			case protobufEncoding:
				encoder := protobuf.NewUnaggregatedEncoder(protobuf.NewUnaggregatedOptions())
				require.NoError(t, encoder.EncodeMessage(encoding.UnaggregatedMessageUnion{
					Type:                 encoding.CounterWithMetadatasType,
					CounterWithMetadatas: testCounterWithMetadatas,
				}))
				require.NoError(t, encoder.EncodeMessage(encoding.UnaggregatedMessageUnion{
					Type: encoding.BatchTimerWithMetadatasType,
					BatchTimerWithMetadatas: testBatchTimerWithMetadatas,
				}))
				require.NoError(t, encoder.EncodeMessage(encoding.UnaggregatedMessageUnion{
					Type:               encoding.GaugeWithMetadatasType,
					GaugeWithMetadatas: testGaugeWithMetadatas,
				}))
				require.NoError(t, encoder.EncodeMessage(encoding.UnaggregatedMessageUnion{
					Type: encoding.TimedMetricWithForwardMetadataType,
					TimedMetricWithForwardMetadata: testMetricWithForwardMetadata,
				}))
				buf := encoder.Relinquish()
				stream = buf.Bytes()
			}
			_, err = conn.Write(stream)
			require.NoError(t, err)
		}()
	}

	// Wait for all metrics to be processed.
	wgClient.Wait()
	for agg.NumMetricsAdded() < expectedTotalMetrics {
		time.Sleep(50 * time.Millisecond)
	}

	// Close the server.
	s.Close()

	// Assert the snapshot match expectations.
	require.True(t, cmp.Equal(expectedResult, agg.Snapshot(), testCmpOpts...))
}

func testServerOptions() Options {
	opts := NewOptions()
	instrumentOpts := opts.InstrumentOptions().SetReportInterval(time.Second)
	serverOpts := xserver.NewOptions().SetRetryOptions(retry.NewOptions().SetMaxRetries(2))
	return opts.SetInstrumentOptions(instrumentOpts).SetServerOptions(serverOpts)
}

type encodingProtocol int

const (
	msgpackEncoding encodingProtocol = iota
	protobufEncoding
)
