package rawtcp

import (
	"context"
	"io/ioutil"
	"net"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/panjf2000/ants/v2"
	"github.com/panjf2000/gnet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap"

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
	xtime "github.com/m3db/m3/src/x/time"
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
	testPassthroughStoragePolicy = policy.NewStoragePolicy(time.Minute, xtime.Minute, 12*time.Hour)
	testCounterWithMetadatas     = unaggregated.CounterWithMetadatas{
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

func getTestUnixSockName(t *testing.T) string {
	t.Helper()
	file, err := ioutil.TempFile(os.TempDir(), "gnet")
	require.NoError(t, err)

	name := file.Name()

	require.NoError(t, file.Close())
	require.NoError(t, os.Remove(file.Name()))

	return name
}

//nolint:lll
func TestServer(t *testing.T) {
	const (
		numClients = 50
		numIters   = 100
	)

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	defer logger.Sync() //nolint:errcheck

	agg := capture.NewAggregator()
	sockName := getTestUnixSockName(t)

	pool, err := ants.NewPool(runtime.GOMAXPROCS(0)*2,
		ants.WithPanicHandler(func(v interface{}) {
			panic(v)
		}),
		ants.WithLogger(poolZapLogger{logger: logger}),
		ants.WithExpiryDuration(1*time.Minute),
		ants.WithPreAlloc(false))
	require.NoError(t, err)

	h := NewConnHandler(agg, pool, logger, tally.NoopScope, 100)

	sockName = getTestUnixSockName(t) //"127.0.0.1:12341"
	errCh := make(chan error, 1)
	go func() {
		errCh <- testServer(h, sockName, logger)
		close(errCh)
	}()

	// wait for server to come up
	for i := 0; i < 100; i++ {
		conn, err := net.Dial("unix", sockName)
		if conn != nil {
			conn.Close() //nolint:errcheck
		}

		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	var (
		wgClient       sync.WaitGroup
		expectedResult capture.SnapshotResult
	)

	// Now establish multiple connections and send data to the server.
	var expectedTotalMetrics int
	for i := 0; i < numClients; i++ {
		wgClient.Add(1)

		// Add test metrics to expected result.
		for i := 0; i < numIters; i++ {
			expectedResult.CountersWithMetadatas = append(expectedResult.CountersWithMetadatas, testCounterWithMetadatas)
			expectedResult.BatchTimersWithMetadatas = append(expectedResult.BatchTimersWithMetadatas, testBatchTimerWithMetadatas)
			expectedResult.GaugesWithMetadatas = append(expectedResult.GaugesWithMetadatas, testGaugeWithMetadatas)
			expectedResult.TimedMetricWithMetadata = append(expectedResult.TimedMetricWithMetadata, testTimedMetricWithMetadata)
			expectedResult.PassthroughMetricWithMetadata = append(expectedResult.PassthroughMetricWithMetadata, testPassthroughMetricWithMetadata)
			expectedResult.ForwardedMetricsWithMetadata = append(expectedResult.ForwardedMetricsWithMetadata, testForwardedMetricWithMetadata)
			expectedTotalMetrics += 6
		}
		go func() {
			defer wgClient.Done()
			conn, err := net.Dial("unix", sockName)
			require.NoError(t, err)

			encoder := protobuf.NewUnaggregatedEncoder(protobuf.NewUnaggregatedOptions())

			for i := 0; i < numIters; i++ {
				assert.NoError(t, encoder.EncodeMessage(encoding.UnaggregatedMessageUnion{
					Type:                 encoding.CounterWithMetadatasType,
					CounterWithMetadatas: testCounterWithMetadatas,
				}))
				assert.NoError(t, encoder.EncodeMessage(encoding.UnaggregatedMessageUnion{
					Type:                    encoding.BatchTimerWithMetadatasType,
					BatchTimerWithMetadatas: testBatchTimerWithMetadatas,
				}))
				assert.NoError(t, encoder.EncodeMessage(encoding.UnaggregatedMessageUnion{
					Type:               encoding.GaugeWithMetadatasType,
					GaugeWithMetadatas: testGaugeWithMetadatas,
				}))
				assert.NoError(t, encoder.EncodeMessage(encoding.UnaggregatedMessageUnion{
					Type:                    encoding.TimedMetricWithMetadataType,
					TimedMetricWithMetadata: testTimedMetricWithMetadata,
				}))
				assert.NoError(t, encoder.EncodeMessage(encoding.UnaggregatedMessageUnion{
					Type:                          encoding.PassthroughMetricWithMetadataType,
					PassthroughMetricWithMetadata: testPassthroughMetricWithMetadata,
				}))
				assert.NoError(t, encoder.EncodeMessage(encoding.UnaggregatedMessageUnion{
					Type:                        encoding.ForwardedMetricWithMetadataType,
					ForwardedMetricWithMetadata: testForwardedMetricWithMetadata,
				}))
				b := encoder.Relinquish().Bytes()
				_, err = conn.Write(b)
				require.NoError(t, err)
			}
			// should read all data off local unix socket in <1 s
			time.Sleep(1 * time.Second)
			require.NoError(t, conn.Close())
		}()
	}

	<-time.After(1 * time.Second)
	// Wait for all metrics to be processed.
	wgClient.Wait()
	for i := 0; i < 20 && agg.NumMetricsAdded() < expectedTotalMetrics; i++ {
		time.Sleep(100 * time.Millisecond)
	}
	h.Close()

	t.Log("stopping server")
	// Close the server.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	if err := gnet.Stop(ctx, "unix://"+sockName); err != nil {
		t.Fail()
		t.Log("got error while shutting down server:", err.Error())
	}
	cancel()

	t.Log("comparing results")
	// Assert the snapshot match expectations.
	snapshot := agg.Snapshot()
	if !cmp.Equal(expectedResult, snapshot, testCmpOpts...) {
		t.Log("expected result to match snapshot")
		t.Log(cmp.Diff(expectedResult, snapshot, testCmpOpts...))
		t.Fail()
	} else {
		t.Log("snapshot matches expected result")
	}

	for err := range errCh {
		require.NoError(t, err)
	}
}

func testServer(handler *connHandler, addr string, logger *zap.Logger) error {
	return gnet.Serve(handler, "unix://"+addr, gnet.WithOptions(
		gnet.Options{
			Codec:         handler,
			Logger:        logger.Sugar(),
			ReadBufferCap: 16384,
			TCPKeepAlive:  10 * time.Second,
			NumEventLoop:  runtime.GOMAXPROCS(0)/4 + 1,
		},
	))
}
