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

	"github.com/m3db/m3cluster/kv"
	msgpackbackend "github.com/m3db/m3collector/backend/msgpack"
	msgpackserver "github.com/m3db/m3collector/integration/msgpack"
	"github.com/m3db/m3metrics/matcher"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"

	"github.com/stretchr/testify/require"
)

type testReportWithRuleUpdatesOptions struct {
	Description  string
	Store        kv.Store
	MatcherOpts  matcher.Options
	BackendOpts  msgpackbackend.ServerOptions
	InputIDGen   idGenerator
	OutputRes    outputResults
	RuleUpdateFn func()
}

func testReportWithRuleUpdates(
	t *testing.T,
	opts testReportWithRuleUpdatesOptions,
) {
	var (
		resultsLock sync.Mutex
		received    int
		results     []metricWithPoliciesList
	)
	handleFn := func(metric unaggregated.MetricUnion, policiesList policy.PoliciesList) error {
		if !metric.OwnsID {
			cloned := make([]byte, len(metric.ID))
			copy(cloned, metric.ID)
			metric.ID = id.RawID(cloned)
			metric.OwnsID = true
		}

		// NB: The policies list decoded from the iterator only valid until the next decoding
		// iteration. Make a copy here for validation later.
		clonedPoliciesList := make(policy.PoliciesList, len(policiesList))
		for i, sp := range policiesList {
			policies, isDefault := sp.Policies()
			if isDefault {
				clonedPoliciesList[i] = policy.DefaultStagedPolicies
			} else {
				clonedPolicies := make([]policy.Policy, len(policies))
				copy(clonedPolicies, policies)
				clonedPoliciesList[i] = policy.NewStagedPolicies(sp.CutoverNanos, sp.Tombstoned, clonedPolicies)
			}
		}

		var value interface{}
		switch metric.Type {
		case unaggregated.CounterType:
			value = metric.Counter()
		case unaggregated.BatchTimerType:
			value = metric.BatchTimer()
		case unaggregated.GaugeType:
			value = metric.Gauge()
		}

		resultsLock.Lock()
		defer resultsLock.Unlock()
		results = append(results, metricWithPoliciesList{
			metric:       value,
			policiesList: clonedPoliciesList,
		})
		received++
		return nil
	}

	// Create matcher.
	testOpts := newTestOptions().
		SetKVStore(opts.Store).
		SetMatcherOptions(opts.MatcherOpts).
		SetBackendOptions(opts.BackendOpts).
		SetServerOptions(msgpackserver.NewOptions().SetHandleFn(handleFn))
	testSetup := newTestSetup(t, testOpts)
	defer testSetup.close()

	// Start the server
	log := testOpts.InstrumentOptions().Logger()
	log.Info(opts.Description)
	require.NoError(t, testSetup.startServer())
	log.Info("server is now up")

	// Report metrics.
	var (
		reporter            = testSetup.Reporter()
		reportIter          = 400000
		ruleUpdatesIter     = 10000
		types               = []unaggregated.Type{unaggregated.CounterType, unaggregated.BatchTimerType, unaggregated.GaugeType}
		counterVal          = int64(1234)
		batchTimerVals      = []float64{1.57, 2.38, 99.102}
		gaugeVal            = 9.345
		wg                  sync.WaitGroup
		expectedResults     []metricWithPoliciesList
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
					expectedResults = append(expectedResults, metricWithPoliciesList{
						metric: unaggregated.Counter{
							ID:    resID,
							Value: counterVal,
						},
						policiesList: result.policiesList,
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
					expectedResults = append(expectedResults, metricWithPoliciesList{
						metric: unaggregated.BatchTimer{
							ID:     resID,
							Values: batchTimerVals,
						},
						policiesList: result.policiesList,
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
					expectedResults = append(expectedResults, metricWithPoliciesList{
						metric: unaggregated.Gauge{
							ID:    resID,
							Value: gaugeVal,
						},
						policiesList: result.policiesList,
					})
				}
				expectedResultsLock.Unlock()
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
	sort.Sort(resultsByTypeAscIDAsc(results))
	sort.Sort(resultsByTypeAscIDAsc(expectedResults))
	require.Equal(t, expectedResults, results)
}
