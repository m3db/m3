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

package agent

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3nsch"
	"github.com/m3db/m3nsch/datums"

	"github.com/m3db/m3db/src/dbnode/client"
	"github.com/m3db/m3x/instrument"
	xtime "github.com/m3db/m3x/time"
	"github.com/stretchr/testify/require"
)

const (
	testNumPointsPerDatum = 40
)

type testWrite struct {
	metricName string
	timestamp  time.Time
	value      float64
}

func newTestOptions() m3nsch.AgentOptions {
	iopts := instrument.NewOptions()
	return NewOptions(iopts).
		SetNewSessionFn(func(_, _ string) (client.Session, error) {
			return nil, nil
		})
}

func TestWorkloadMetricStartIdx(t *testing.T) {
	var (
		reg  = datums.NewDefaultRegistry(testNumPointsPerDatum)
		opts = newTestOptions().
			SetConcurrency(1)
		workload = m3nsch.Workload{
			Cardinality:    10,
			IngressQPS:     100,
			MetricStartIdx: 1000,
		}
		agent  = New(reg, opts).(*m3nschAgent)
		writes []testWrite

		token      = ""
		targetZone = ""
		targetEnv  = ""
	)

	agent.params.fn = func(_ int, _ client.Session, _ string, metric generatedMetric, t time.Time, _ xtime.Unit) error {
		writes = append(writes, testWrite{
			metricName: metric.name,
			timestamp:  t,
			value:      metric.timeseries.Next(),
		})
		return nil
	}

	err := agent.Init(token, workload, false, targetZone, targetEnv)
	require.NoError(t, err)

	err = agent.Start()
	require.NoError(t, err)

	// let worker perform write ops for 1 second
	time.Sleep(1 * time.Second)

	err = agent.Stop()
	require.NoError(t, err)

	// ensure we've seen 90% of the writes we're expecting
	eps := 0.1
	numExpectedWrites := workload.IngressQPS
	require.InEpsilon(t, numExpectedWrites, len(writes), eps)

	// ensure the ordering of metric writes is correct
	for i, wr := range writes {
		metricIdx := workload.MetricStartIdx + i%workload.Cardinality
		require.Equal(t, fmt.Sprintf(".m%d", metricIdx), wr.metricName)
	}
}

func TestNewSingleWriterAgent(t *testing.T) {
	var (
		reg  = datums.NewDefaultRegistry(testNumPointsPerDatum)
		opts = newTestOptions().
			SetConcurrency(1)
		workload = m3nsch.Workload{
			Cardinality: 10,
			IngressQPS:  100,
		}
		agent  = New(reg, opts).(*m3nschAgent)
		writes []testWrite

		token      = ""
		targetZone = ""
		targetEnv  = ""
	)

	agent.params.fn = func(_ int, _ client.Session, _ string, metric generatedMetric, t time.Time, _ xtime.Unit) error {
		writes = append(writes, testWrite{
			metricName: metric.name,
			timestamp:  t,
			value:      metric.timeseries.Next(),
		})
		return nil
	}

	err := agent.Init(token, workload, false, targetZone, targetEnv)
	require.NoError(t, err)

	err = agent.Start()
	require.NoError(t, err)

	// let worker perform write ops for 1 second
	time.Sleep(1 * time.Second)

	err = agent.Stop()
	require.NoError(t, err)

	// ensure we've seen 90% of the writes we're expecting
	eps := 0.1
	numExpectedWrites := workload.IngressQPS
	require.InEpsilon(t, numExpectedWrites, len(writes), eps)

	// ensure the ordering of metric writes is correct
	for i, wr := range writes {
		metricIdx := i % workload.Cardinality
		require.Equal(t, fmt.Sprintf(".m%d", metricIdx), wr.metricName)
	}

	// ensure the values written per metric are accurate
	// first, group all values by metricIdx (which are the same as metricName due to assertion above)
	valuesByMetricIdx := make(map[int][]testWrite)
	for i, wr := range writes {
		metricIdx := i % workload.Cardinality
		current, ok := valuesByMetricIdx[metricIdx]
		if !ok {
			current = []testWrite{}
		}
		current = append(current, wr)
		valuesByMetricIdx[metricIdx] = current
	}

	// finally, go through the values per metric, and ensure
	// they line up with expected values from registry
	for idx, values := range valuesByMetricIdx {
		datum := reg.Get(idx)
		for i, wr := range values {
			require.Equal(t, datum.Get(i), wr.value,
				"metric: %s, idx: %d, i: %d, ts: %s", wr.metricName, idx, i, wr.timestamp.String())
		}
	}
}

func TestWorkerParams(t *testing.T) {
	var (
		reg  = datums.NewDefaultRegistry(testNumPointsPerDatum)
		opts = newTestOptions().
			SetConcurrency(10)
		agent    = New(reg, opts).(*m3nschAgent)
		t0       = time.Now()
		testNs   = "testNs"
		workload = m3nsch.Workload{
			Cardinality: 1000,
			IngressQPS:  100,
			BaseTime:    t0,
			Namespace:   testNs,
		}
	)
	agent.SetWorkload(workload)

	expectedWritesPerWorkerPerSec := workload.IngressQPS / opts.Concurrency()
	expectedTickPeriodPerWorker := time.Duration(1000.0/float64(expectedWritesPerWorkerPerSec)) * time.Millisecond

	_, ns, baseTime, tickPeriod := agent.workerParams()
	require.Equal(t, testNs, ns)
	require.Equal(t, t0, baseTime)
	require.Equal(t, expectedTickPeriodPerWorker, tickPeriod)
}

func TestMultipleWriterAgent(t *testing.T) {
	var (
		reg  = datums.NewDefaultRegistry(testNumPointsPerDatum)
		opts = newTestOptions().
			SetConcurrency(2)
		workload = m3nsch.Workload{
			Cardinality: 4,
			IngressQPS:  100,
		}
		token      = ""
		targetZone = ""
		targetEnv  = ""
		agent      = New(reg, opts).(*m3nschAgent)

		writesLock        sync.Mutex
		writesByWorkerIdx map[int][]testWrite
	)

	// initialize writesByWorkerIdx
	writesByWorkerIdx = make(map[int][]testWrite)
	for i := 0; i < opts.Concurrency(); i++ {
		writesByWorkerIdx[i] = []testWrite{}
	}

	agent.params.fn = func(wIdx int, _ client.Session, _ string, metric generatedMetric, t time.Time, _ xtime.Unit) error {
		writesLock.Lock()
		writesByWorkerIdx[wIdx] = append(writesByWorkerIdx[wIdx], testWrite{
			metricName: metric.name,
			timestamp:  t,
			value:      metric.timeseries.Next(),
		})
		writesLock.Unlock()
		return nil
	}

	err := agent.Init(token, workload, false, targetZone, targetEnv)
	require.NoError(t, err)

	err = agent.Start()
	require.NoError(t, err)

	// let worker perform write ops for 1 second
	time.Sleep(1 * time.Second)

	err = agent.Stop()
	require.NoError(t, err)

	// ensure we've seen at least 10% of the expected writes
	// NB(prateek): ideally, this would require a stricter number of writes
	// but in testing, the mutex overhead induced due to protecting the
	// observed writes map is large. and for the purposes of testing,
	// 10% of the values are sufficient for testing ordering assertions
	eps := 0.10
	numExpectedWritesPerWorker := int(float64(workload.IngressQPS) * eps)
	for i := 0; i < opts.Concurrency(); i++ {
		numSeenWrites := len(writesByWorkerIdx[i])
		require.True(t, numSeenWrites > numExpectedWritesPerWorker,
			"worker: %d, expectedWrites: %d, seenWrites: %d", i)
	}

	// helper to identify which metric we're expecting per worker
	metricIdxFn := func(wIdx int, mIdx int) int {
		numWorkers := opts.Concurrency()
		numMetrics := workload.Cardinality
		numMetricsPerWorker := numMetrics / numWorkers
		idx := numMetricsPerWorker*wIdx + mIdx%numMetricsPerWorker
		return idx
	}

	// ensure the ordering of metric writes is correct
	for workerIdx, writes := range writesByWorkerIdx {
		for i, wr := range writes {
			metricIdx := metricIdxFn(workerIdx, i)
			require.Equal(t, fmt.Sprintf(".m%d", metricIdx), wr.metricName)
		}
	}
}

// test for transition checks
func TestTransitions(t *testing.T) {
	var (
		reg  = datums.NewDefaultRegistry(testNumPointsPerDatum)
		opts = newTestOptions().
			SetConcurrency(10)
		agent    = New(reg, opts).(*m3nschAgent)
		workload = m3nsch.Workload{
			Cardinality: 1000,
			IngressQPS:  100,
		}
	)
	agent.params.fn = func(_ int, _ client.Session, _ string, _ generatedMetric, _ time.Time, _ xtime.Unit) error {
		return nil
	}

	err := agent.Start()
	require.Error(t, err)

	err = agent.Stop()
	require.Error(t, err)

	err = agent.Init("", workload, false, "", "")
	require.NoError(t, err)

	err = agent.Stop()
	require.NoError(t, err)

	err = agent.Init("", workload, false, "", "")
	require.NoError(t, err)

	err = agent.Start()
	require.NoError(t, err)

	err = agent.Stop()
	require.NoError(t, err)
}
