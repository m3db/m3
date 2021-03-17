// +build integration

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

package integration

import (
	"sort"
	"sync"
	"testing"
	"time"

	aggclient "github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/cluster/kv"
	aggserver "github.com/m3db/m3/src/collector/integration/server"
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/matcher"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/policy"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
)

type testReportWithRuleUpdatesOptions struct {
	Description   string
	Store         kv.Store
	MatcherOpts   matcher.Options
	AggClientOpts aggclient.Options
	InputIDGen    idGenerator
	OutputRes     outputResults
	RuleUpdateFn  func()
}

func testReportWithRuleUpdates(
	t *testing.T,
	opts testReportWithRuleUpdatesOptions,
) {
	var (
		resultsLock sync.Mutex
		received    int
		results     []metricWithMetadatas
	)
	handleFn := func(
		metric unaggregated.MetricUnion,
		metadatas metadata.StagedMetadatas,
	) error {
		// The metric ID and metadatas decoded from the iterator are only valid till the
		// next decoding iteration, and as such we make a copy here for validation later.
		clonedMetric := cloneMetric(metric)
		clonedMetadatas := cloneMetadatas(metadatas)

		resultsLock.Lock()
		defer resultsLock.Unlock()
		results = append(results, metricWithMetadatas{
			metric:    clonedMetric,
			metadatas: clonedMetadatas,
		})
		received++
		return nil
	}

	// Set up test.
	bytesPool := defaultBytesPool()
	bytesPool.Init()
	decodingIterOpts := protobuf.NewUnaggregatedOptions().SetBytesPool(bytesPool)
	handlerOpts := aggserver.NewHandlerOptions().
		SetHandleFn(handleFn).
		SetProtobufUnaggregatedIteratorOptions(decodingIterOpts)
	serverOpts := aggserver.NewOptions().SetHandlerOptions(handlerOpts)
	testOpts := newTestOptions().
		SetKVStore(opts.Store).
		SetMatcherOptions(opts.MatcherOpts).
		SetAggregatorClientOptions(opts.AggClientOpts).
		SetServerOptions(serverOpts)
	testSetup := newTestSetup(t, testOpts)
	defer testSetup.close()

	// Start the server.
	log := testOpts.InstrumentOptions().Logger()
	log.Info(opts.Description)
	require.NoError(t, testSetup.startServer())
	log.Info("server is now up")

	// Report metrics.
	var (
		reporter            = testSetup.Reporter()
		reportIter          = 100000
		ruleUpdatesIter     = 10000
		types               = []metric.Type{metric.CounterType, metric.TimerType, metric.GaugeType}
		counterVal          = int64(1234)
		batchTimerVals      = []float64{1.57, 2.38, 99.102}
		gaugeVal            = 9.345
		wg                  sync.WaitGroup
		expectedResults     []metricWithMetadatas
		expectedResultsLock sync.Mutex
	)

	wg.Add(2)

	go func() {
		defer wg.Done()

		var subWg sync.WaitGroup
		subWg.Add(3)
		go func() {
			defer subWg.Done()
			for i := 0; i < reportIter; i++ {
				metricID := opts.InputIDGen(i)

				require.NoError(t, reporter.ReportCounter(metricID, counterVal))
				expectedResultsLock.Lock()
				for _, result := range opts.OutputRes {
					resID := id.RawID(result.idGen(i).Bytes())
					expectedResults = append(expectedResults, metricWithMetadatas{
						metric: unaggregated.Counter{
							ID:    resID,
							Value: counterVal,
						}.ToUnion(),
						metadatas: result.metadatas,
					})
				}
				expectedResultsLock.Unlock()
			}
		}()

		go func() {
			defer subWg.Done()
			for i := reportIter; i < 2*reportIter; i++ {
				metricID := opts.InputIDGen(i)
				require.NoError(t, reporter.ReportBatchTimer(metricID, batchTimerVals))
				expectedResultsLock.Lock()
				for _, result := range opts.OutputRes {
					resID := id.RawID(result.idGen(i).Bytes())
					expectedResults = append(expectedResults, metricWithMetadatas{
						metric: unaggregated.BatchTimer{
							ID:     resID,
							Values: batchTimerVals,
						}.ToUnion(),
						metadatas: result.metadatas,
					})
				}
				expectedResultsLock.Unlock()
			}
		}()

		go func() {
			defer subWg.Done()
			for i := 2 * reportIter; i < 3*reportIter; i++ {
				metricID := opts.InputIDGen(i)
				require.NoError(t, reporter.ReportGauge(metricID, gaugeVal))
				expectedResultsLock.Lock()
				for _, result := range opts.OutputRes {
					resID := id.RawID(result.idGen(i).Bytes())
					expectedResults = append(expectedResults, metricWithMetadatas{
						metric: unaggregated.Gauge{
							ID:    resID,
							Value: gaugeVal,
						}.ToUnion(),
						metadatas: result.metadatas,
					})
				}
				expectedResultsLock.Unlock()
				require.NoError(t, reporter.Flush())
			}
		}()

		// Flush everything to the server.
		subWg.Wait()
		require.NoError(t, reporter.Flush())
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < ruleUpdatesIter; i++ {
			opts.RuleUpdateFn()
		}
	}()
	wg.Wait()

	// Wait until all metrics are processed.
	for {
		resultsLock.Lock()
		currReceived := received
		resultsLock.Unlock()
		if currReceived == reportIter*len(types)*len(opts.OutputRes) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Stop the server
	require.NoError(t, testSetup.stopServer())
	log.Info("server is now down")

	// Validate results.
	testCmpOpts := []cmp.Option{
		cmpopts.EquateEmpty(),
		cmp.AllowUnexported(metricWithMetadatas{}),
		cmp.AllowUnexported(policy.StoragePolicy{}),
	}
	sort.Sort(resultsByTypeAscIDAsc(results))
	sort.Sort(resultsByTypeAscIDAsc(expectedResults))
	require.True(t, cmp.Equal(expectedResults, results, testCmpOpts...))
}

func cloneMetric(metric unaggregated.MetricUnion) unaggregated.MetricUnion {
	clonedMetric := metric
	clonedID := make(id.RawID, len(metric.ID))
	copy(clonedID, metric.ID)
	clonedMetric.ID = clonedID
	clonedBatchTimerVal := make([]float64, len(metric.BatchTimerVal))
	copy(clonedBatchTimerVal, metric.BatchTimerVal)
	clonedMetric.BatchTimerVal = clonedBatchTimerVal
	return clonedMetric
}

func cloneMetadatas(metadatas metadata.StagedMetadatas) metadata.StagedMetadatas {
	clonedMetadatas := make(metadata.StagedMetadatas, 0, len(metadatas))
	for _, stagedMetadata := range metadatas {
		clonedStagedMetadata := stagedMetadata
		pipelines := stagedMetadata.Pipelines
		clonedPipelines := make([]metadata.PipelineMetadata, 0, len(pipelines))
		for _, pipeline := range pipelines {
			clonedPipeline := metadata.PipelineMetadata{
				AggregationID:   pipeline.AggregationID,
				StoragePolicies: pipeline.StoragePolicies.Clone(),
				Pipeline:        pipeline.Pipeline.Clone(),
			}
			clonedPipelines = append(clonedPipelines, clonedPipeline)
		}
		clonedStagedMetadata.Pipelines = clonedPipelines
		clonedMetadatas = append(clonedMetadatas, clonedStagedMetadata)
	}
	return clonedMetadatas
}
