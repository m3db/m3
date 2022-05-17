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
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/aggregator/aggregator/capture"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/encoding"
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/retry"
	xserver "github.com/m3db/m3/src/x/server"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
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
	testTimed = aggregated.Metric{
		Type:      metric.CounterType,
		ID:        []byte("testTimed"),
		TimeNanos: 12345,
		Value:     -13,
	}
	testForwarded = aggregated.ForwardedMetric{
		Type:      metric.CounterType,
		ID:        []byte("testForwarded"),
		TimeNanos: 12345,
		Values:    []float64{908, -13},
	}
	testPassthrough = aggregated.Metric{
		Type:      metric.CounterType,
		ID:        []byte("testPassthrough"),
		TimeNanos: 12345,
		Value:     -13,
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
	testTimedMetadata = metadata.TimedMetadata{
		AggregationID: aggregation.DefaultID,
		StoragePolicy: policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour),
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
		SourceID:          1234,
		NumForwardedTimes: 3,
	}
	testPassthroughStoragePolicy   = policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour)
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
	testTimedMetricWithMetadata = aggregated.TimedMetricWithMetadata{
		Metric:        testTimed,
		TimedMetadata: testTimedMetadata,
	}
	testTimedMetricWithMetadatas = aggregated.TimedMetricWithMetadatas{
		Metric:          testTimed,
		StagedMetadatas: testDefaultMetadatas,
	}
	testForwardedMetricWithMetadata = aggregated.ForwardedMetricWithMetadata{
		ForwardedMetric: testForwarded,
		ForwardMetadata: testForwardMetadata,
	}
	testPassthroughMetricWithMetadata = aggregated.PassthroughMetricWithMetadata{
		Metric:        testPassthrough,
		StoragePolicy: testPassthroughStoragePolicy,
	}
	testCmpOpts = []cmp.Option{
		cmpopts.EquateEmpty(),
		cmp.AllowUnexported(policy.StoragePolicy{}),
	}
)

func TestRawTCPServerHandleUnaggregatedProtobufEncoding(t *testing.T) {
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
		wgClient.Add(1)

		// Add test metrics to expected result.
		expectedResult.CountersWithMetadatas = append(expectedResult.CountersWithMetadatas, testCounterWithMetadatas)
		expectedResult.BatchTimersWithMetadatas = append(expectedResult.BatchTimersWithMetadatas, testBatchTimerWithMetadatas)
		expectedResult.GaugesWithMetadatas = append(expectedResult.GaugesWithMetadatas, testGaugeWithMetadatas)
		expectedResult.TimedMetricWithMetadata = append(expectedResult.TimedMetricWithMetadata, testTimedMetricWithMetadata)
		expectedResult.PassthroughMetricWithMetadata = append(expectedResult.PassthroughMetricWithMetadata, testPassthroughMetricWithMetadata)
		expectedResult.ForwardedMetricsWithMetadata = append(expectedResult.ForwardedMetricsWithMetadata, testForwardedMetricWithMetadata)
		expectedTotalMetrics += 5

		go func() {
			defer wgClient.Done()

			conn, err := net.Dial("tcp", listener.Addr().String())
			require.NoError(t, err)

			encoder := protobuf.NewUnaggregatedEncoder(protobuf.NewUnaggregatedOptions())
			require.NoError(t, encoder.EncodeMessage(encoding.UnaggregatedMessageUnion{
				Type:                 encoding.CounterWithMetadatasType,
				CounterWithMetadatas: testCounterWithMetadatas,
			}))
			require.NoError(t, encoder.EncodeMessage(encoding.UnaggregatedMessageUnion{
				Type:                    encoding.BatchTimerWithMetadatasType,
				BatchTimerWithMetadatas: testBatchTimerWithMetadatas,
			}))
			require.NoError(t, encoder.EncodeMessage(encoding.UnaggregatedMessageUnion{
				Type:               encoding.GaugeWithMetadatasType,
				GaugeWithMetadatas: testGaugeWithMetadatas,
			}))
			require.NoError(t, encoder.EncodeMessage(encoding.UnaggregatedMessageUnion{
				Type:                    encoding.TimedMetricWithMetadataType,
				TimedMetricWithMetadata: testTimedMetricWithMetadata,
			}))
			require.NoError(t, encoder.EncodeMessage(encoding.UnaggregatedMessageUnion{
				Type:                          encoding.PassthroughMetricWithMetadataType,
				PassthroughMetricWithMetadata: testPassthroughMetricWithMetadata,
			}))
			require.NoError(t, encoder.EncodeMessage(encoding.UnaggregatedMessageUnion{
				Type:                        encoding.ForwardedMetricWithMetadataType,
				ForwardedMetricWithMetadata: testForwardedMetricWithMetadata,
			}))

			_, err = conn.Write(encoder.Relinquish().Bytes())
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
	snapshot := agg.Snapshot()
	require.True(t, cmp.Equal(expectedResult, snapshot, testCmpOpts...), expectedResult, snapshot)
}

func TestHandle_Errors(t *testing.T) {
	cases := []struct {
		name   string
		msg    encoding.UnaggregatedMessageUnion
		logMsg string
	}{
		{
			name: "timed with staged metadata",
			msg: encoding.UnaggregatedMessageUnion{
				Type:                     encoding.TimedMetricWithMetadatasType,
				TimedMetricWithMetadatas: testTimedMetricWithMetadatas,
			},
			logMsg: "error adding timed metric",
		},
		{
			name: "timed with metadata",
			msg: encoding.UnaggregatedMessageUnion{
				Type:                    encoding.TimedMetricWithMetadataType,
				TimedMetricWithMetadata: testTimedMetricWithMetadata,
			},
			logMsg: "error adding timed metric",
		},
		{
			name: "gauge untimed",
			msg: encoding.UnaggregatedMessageUnion{
				Type:               encoding.GaugeWithMetadatasType,
				GaugeWithMetadatas: testGaugeWithMetadatas,
			},
			logMsg: "error adding untimed metric",
		},
		{
			name: "counter untimed",
			msg: encoding.UnaggregatedMessageUnion{
				Type:                 encoding.CounterWithMetadatasType,
				CounterWithMetadatas: testCounterWithMetadatas,
			},
			logMsg: "error adding untimed metric",
		},
		{
			name: "timer untimed",
			msg: encoding.UnaggregatedMessageUnion{
				Type:                    encoding.BatchTimerWithMetadatasType,
				BatchTimerWithMetadatas: testBatchTimerWithMetadatas,
			},
			logMsg: "error adding untimed metric",
		},
		{
			name: "forward",
			msg: encoding.UnaggregatedMessageUnion{
				Type:                        encoding.ForwardedMetricWithMetadataType,
				ForwardedMetricWithMetadata: testForwardedMetricWithMetadata,
			},
			logMsg: "error adding forwarded metric",
		},
		{
			name: "passthrough",
			msg: encoding.UnaggregatedMessageUnion{
				Type:                          encoding.PassthroughMetricWithMetadataType,
				PassthroughMetricWithMetadata: testPassthroughMetricWithMetadata,
			},
			logMsg: "error adding passthrough metric",
		},
	}

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	agg := aggregator.NewMockAggregator(ctrl)

	aggErr := errors.New("boom")
	agg.EXPECT().AddTimedWithStagedMetadatas(gomock.Any(), gomock.Any()).Return(aggErr).AnyTimes()
	agg.EXPECT().AddUntimed(gomock.Any(), gomock.Any()).Return(aggErr).AnyTimes()
	agg.EXPECT().AddForwarded(gomock.Any(), gomock.Any()).Return(aggErr).AnyTimes()
	agg.EXPECT().AddTimed(gomock.Any(), gomock.Any()).Return(aggErr).AnyTimes()
	agg.EXPECT().AddPassthrough(gomock.Any(), gomock.Any()).Return(aggErr).AnyTimes()

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			core, recorded := observer.New(zapcore.InfoLevel)
			listener, err := net.Listen("tcp", testListenAddress)
			require.NoError(t, err)

			h := NewHandler(agg, testServerOptions().SetInstrumentOptions(instrument.NewOptions().
				SetLogger(zap.New(core))))
			s := xserver.NewServer(testListenAddress, h, xserver.NewOptions())
			// Start server.
			require.NoError(t, s.Serve(listener))

			conn, err := net.Dial("tcp", listener.Addr().String())
			encoder := protobuf.NewUnaggregatedEncoder(protobuf.NewUnaggregatedOptions())
			require.NoError(t, err)

			require.NoError(t, encoder.EncodeMessage(tc.msg))

			_, err = conn.Write(encoder.Relinquish().Bytes())
			require.NoError(t, err)
			res := clock.WaitUntil(func() bool {
				return len(recorded.FilterMessage(tc.logMsg).All()) == 1
			}, time.Second*5)
			if !res {
				require.Fail(t,
					"failed to find expected log message",
					"expected=%v logs=%v", tc.logMsg, recorded.All())
			}
		})
	}
}

func testServerOptions() Options {
	opts := NewOptions()
	instrumentOpts := opts.InstrumentOptions().SetReportInterval(time.Second)
	serverOpts := xserver.NewOptions().SetRetryOptions(retry.NewOptions().SetMaxRetries(2))
	return opts.SetInstrumentOptions(instrumentOpts).SetServerOptions(serverOpts)
}
