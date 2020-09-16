// Copyright (c) 2019 Uber Technologies, Inc.
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
	"fmt"
	"math"
	"testing"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/cost"
	"github.com/m3db/m3/src/x/close"
	"github.com/m3db/m3/src/x/cost/test"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

type enforcerTestCtx struct {
	Scope          tally.TestScope
	GlobalEnforcer cost.ChainedEnforcer
	Closer         close.SimpleCloser
}

func (c enforcerTestCtx) Close() {
	c.Closer.Close()
}

func TestNewConfiguredChainedEnforcer(t *testing.T) {
	setup := func(t *testing.T, perQueryLimit, globalLimit int) enforcerTestCtx {
		s := tally.NewTestScope("", nil)
		iopts := instrument.NewOptions().SetMetricsScope(s)

		globalEnforcer, closer, err := newConfiguredChainedEnforcer(&config.Configuration{
			Limits: config.LimitsConfiguration{
				PerQuery: config.PerQueryLimitsConfiguration{
					MaxFetchedDatapoints: perQueryLimit,
				},
				Global: config.GlobalLimitsConfiguration{
					MaxFetchedDatapoints: globalLimit,
				},
			},
		}, iopts)

		require.NoError(t, err)

		return enforcerTestCtx{
			Scope:          s,
			GlobalEnforcer: globalEnforcer,
			Closer:         closer,
		}
	}

	t.Run("has 3 valid levels", func(t *testing.T) {
		tctx := setup(t, 6, 10)
		defer tctx.Close()

		assertValid := func(ce cost.ChainedEnforcer) {
			assert.NotEqual(t, ce, cost.NoopChainedEnforcer())
		}

		assertValid(tctx.GlobalEnforcer)

		qe := tctx.GlobalEnforcer.Child(cost.QueryLevel)
		assertValid(qe)

		block := qe.Child(cost.BlockLevel)
		assertValid(block)

		badLevel := block.Child("nonExistent")
		assert.Equal(t, cost.NoopChainedEnforcer(),
			badLevel)
	})

	t.Run("configures reporters", func(t *testing.T) {
		tctx := setup(t, 6, 10)
		defer tctx.Close()

		queryEf := tctx.GlobalEnforcer.Child(cost.QueryLevel)
		blockEf := queryEf.Child(cost.BlockLevel)
		blockEf.Add(7)

		assertHasGauge(t,
			tctx.Scope.Snapshot(),
			tally.KeyForPrefixedStringMap(
				fmt.Sprintf("cost.reporter.%s", datapointsMetric), map[string]string{"limiter": "global"}),
			7,
		)

		blockEf.Close()
		queryEf.Close()

		assertHasHistogram(t,
			tctx.Scope.Snapshot(),
			tally.KeyForPrefixedStringMap(
				fmt.Sprintf("cost.reporter.%s", maxDatapointsHistMetric), map[string]string{"limiter": "query"}),
			map[float64]int64{10: 1},
		)
	})

	t.Run("block level doesn't have a limit", func(t *testing.T) {
		tctx := setup(t, -1, -1)
		defer tctx.Close()

		block := tctx.GlobalEnforcer.Child(cost.QueryLevel).Child(cost.BlockLevel)
		assert.NoError(t, block.Add(math.MaxFloat64-1).Error)
	})

	t.Run("works e2e", func(t *testing.T) {
		tctx := setup(t, 6, 10)
		defer tctx.Close()

		qe1, qe2 := tctx.GlobalEnforcer.Child(cost.QueryLevel), tctx.GlobalEnforcer.Child(cost.QueryLevel)
		r := qe1.Add(6)
		test.AssertLimitErrorWithMsg(
			t,
			r.Error,
			"exceeded query limit: exceeded limits.perQuery.maxFetchedDatapoints",
			6,
			6)

		r = qe2.Add(3)
		require.NoError(t, r.Error)

		r = qe2.Add(2)
		test.AssertLimitErrorWithMsg(
			t,
			r.Error,
			"exceeded global limit: exceeded limits.global.maxFetchedDatapoints",
			11,
			10)

		test.AssertCurrentCost(t, 11, tctx.GlobalEnforcer)

		qe2.Close()
		test.AssertCurrentCost(t, 6, tctx.GlobalEnforcer)

		// check the block level
		blockEf := qe1.Child(cost.BlockLevel)
		blockEf.Add(2)

		test.AssertCurrentCost(t, 2, blockEf)
		test.AssertCurrentCost(t, 8, qe1)
		test.AssertCurrentCost(t, 8, tctx.GlobalEnforcer)
	})
}

func setupGlobalReporter() (tally.TestScope, *globalReporter) {
	s := tally.NewTestScope("", nil)
	gr := newGlobalReporter(s)
	return s, gr
}

func TestGlobalReporter_ReportCurrent(t *testing.T) {
	s, gr := setupGlobalReporter()

	gr.ReportCurrent(5.0)
	assertHasGauge(t, s.Snapshot(), tally.KeyForPrefixedStringMap(datapointsMetric, nil), 5.0)
}

func TestGlobalReporter_ReportCost(t *testing.T) {
	t.Run("reports positive", func(t *testing.T) {
		s, gr := setupGlobalReporter()
		gr.ReportCost(5.0)
		assertHasCounter(t, s.Snapshot(), tally.KeyForPrefixedStringMap(datapointsCounterMetric, nil), 5.0)
	})

	t.Run("skips negative", func(t *testing.T) {
		s, gr := setupGlobalReporter()
		gr.ReportCost(-5.0)

		assertHasCounter(t, s.Snapshot(), tally.KeyForPrefixedStringMap(datapointsCounterMetric, nil), 0.0)
	})
}

func TestGlobalReporter_ReportOverLimit(t *testing.T) {
	s, gr := setupGlobalReporter()
	gr.ReportOverLimit(true)
	assertHasCounter(t, s.Snapshot(), tally.KeyForPrefixedStringMap(queriesOverLimitMetric, map[string]string{
		"enabled": "true",
	}), 1)
}

func setupPerQueryReporter() (tally.TestScope, *perQueryReporter) {
	s := tally.NewTestScope("", nil)
	gr := newPerQueryReporter(s)
	return s, gr
}

func TestPerQueryReporter_ReportOverLimit(t *testing.T) {
	s, pqr := setupPerQueryReporter()
	pqr.ReportOverLimit(true)
	assertHasCounter(t, s.Snapshot(), tally.KeyForPrefixedStringMap(queriesOverLimitMetric, map[string]string{
		"enabled": "true",
	}), 1)
}

func TestPerQueryReporter_OnClose(t *testing.T) {
	s, pqr := setupPerQueryReporter()
	pqr.OnChildClose(5.0)
	pqr.OnChildClose(110.0)

	// ignores current cost
	pqr.OnClose(100.0)
	assertHasHistogram(t, s.Snapshot(),
		tally.KeyForPrefixedStringMap(maxDatapointsHistMetric, nil),
		map[float64]int64{
			1000.0: 1,
		})
}

func TestPerQueryReporter_OnChildClose(t *testing.T) {
	_, pqr := setupPerQueryReporter()
	pqr.OnChildClose(5.0)
	pqr.OnChildClose(110.0)

	assert.InDelta(t, 110.0, float64(pqr.maxDatapoints), 0.0001)
}

func TestOverLimitReporter_ReportOverLimit(t *testing.T) {
	s := tally.NewTestScope("", nil)
	orl := newOverLimitReporter(s)

	orl.ReportOverLimit(true)
	assertHasCounter(t, s.Snapshot(), tally.KeyForPrefixedStringMap(queriesOverLimitMetric, map[string]string{
		"enabled": "true",
	}), 1)

	orl.ReportOverLimit(false)
	assertHasCounter(t, s.Snapshot(), tally.KeyForPrefixedStringMap(queriesOverLimitMetric, map[string]string{
		"enabled": "true",
	}), 1)
}

func assertHasCounter(t *testing.T, snapshot tally.Snapshot, key string, v int) {
	counters := snapshot.Counters()
	if !assert.Contains(t, counters, key, "No such metric: %s", key) {
		return
	}

	counter := counters[key]

	assert.Equal(t, int(counter.Value()), v, "Incorrect value for counter %s", key)
}

func assertHasGauge(t *testing.T, snapshot tally.Snapshot, key string, v int) {
	gauges := snapshot.Gauges()
	if !assert.Contains(t, gauges, key, "No such metric: %s", key) {
		return
	}

	gauge := gauges[key]

	assert.Equal(t, int(gauge.Value()), v, "Incorrect value for gauge %s", key)
}

func assertHasHistogram(t *testing.T, snapshot tally.Snapshot, key string, values map[float64]int64) {
	histograms := snapshot.Histograms()
	if !assert.Contains(t, histograms, key, "No such metric: %s", key) {
		return
	}

	hist := histograms[key]

	actualValues := hist.Values()

	// filter zero values
	for k, v := range actualValues {
		if v == 0 {
			delete(actualValues, k)
		}
	}

	assert.Equal(t, values, actualValues)
}
