package graphite

import (
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/graphite/context"
	xtest "github.com/m3db/m3/src/query/graphite/testing"
	"github.com/m3db/m3/src/query/graphite/ts"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBeginMetricConsolidation(t *testing.T) {
	ctx := context.New()
	defer ctx.Close()

	now := time.Now().Truncate(time.Minute)
	tests := []struct {
		path             string
		startTime        time.Duration
		endTime          time.Duration
		inputOffsetsSec  []int
		inputValues      []float64
		outputOffsetsSec []int
		outputValues     []float64
	}{
		// statsd counters - should be added at 10s intervals
		{"stats.sjc1.counts.monkey.bizness",
			0, time.Second * 40,
			[]int{2, 3, 11, 14, 32},
			[]float64{1, 4, 7, 9, 4},
			[]int{0, 10, 20, 30},
			[]float64{5, 16, math.NaN(), 4},
		},

		// statsd counters at 45 days - should be added at 1min intervals
		{"stats.sjc1.counts.monkey.bizness",
			-45 * 24 * time.Hour, time.Minute * 2,
			[]int{2, 3, 11, 14, 32, 64, 72},
			[]float64{1, 4, 7, 9, 4, 200, 341},
			[]int{0, 60},
			[]float64{25, 541},
		},

		// statsd gauges - should be averaged at 10s intervals
		{"stats.sjc1.gauges.donkey.kong.barrels",
			0, time.Second * 40,
			[]int{2, 3, 11, 14, 32},
			[]float64{1, 4, 7, 9, 4},
			[]int{0, 10, 20, 30},
			[]float64{2.5, 8, math.NaN(), 4},
		},

		// statsd gauges - should be averaged at 1min intervals intervals
		{"stats.sjc1.gauges.donkey.kong.barrels",
			-45 * 24 * time.Hour, time.Minute * 2,
			[]int{2, 3, 11, 14, 32, 64, 72},
			[]float64{1, 4, 7, 9, 4, 200, 341},
			[]int{0, 60},
			[]float64{5, 270.5},
		},

		// server stats - should be averaged at 60s intervals
		{"servers.monkey04-sjc1.cpu.load_0",
			0, time.Minute * 3,
			[]int{2, 6, 45, 55, 65, 72, 91},
			[]float64{10, 20, 30, 40, 50, 60, 70},
			[]int{0, 60, 120},
			[]float64{25, 60, math.NaN()},
		},
	}

	for _, test := range tests {
		startTime := now.Add(test.startTime)
		endTime := startTime.Add(test.endTime)
		consolidation := newMetricConsolidation(ctx, test.path, startTime, endTime)
		require.Equal(t, len(test.inputValues), len(test.inputOffsetsSec), "you screwed up %s", test.path)
		require.Equal(t, len(test.outputValues), len(test.outputOffsetsSec), "you screwed up %s", test.path)

		for i := range test.inputValues {
			timestamp := startTime.Add(time.Second * time.Duration(test.inputOffsetsSec[i]))
			consolidation.AddDatapoint(timestamp, test.inputValues[i])
		}

		output := consolidation.BuildSeries("foo", ts.Finalize)
		require.Equal(t, len(test.outputValues), output.Len())

		for i := range test.outputValues {
			offsetSecs := (output.MillisPerStep() / 1000) * i
			val := output.ValueAt(i)
			assert.Equal(t, test.outputOffsetsSec[i], offsetSecs, "wrong offset for %s %d", test.path, i)
			xtest.EqualWithNaNs(t, test.outputValues[i], val, "wrong value for %s %d", test.path, i)
		}
	}
}

func TestFindConsolidationApproach(t *testing.T) {
	sumIDs := []string{
		"stats.sjc1.counts.donkey.kong.barrels",
		"stats.sjc1.counts.test",
		"stats.sjc1.timers.donkey.kong.count",
		"stats.sjc1.timers.m3+service.fetchbatchraw.latency+dc=sjc1,env=production,host=m3-tsdb112-sjc1,pipe=none,service=statsdex_m3dbnode,servicename=node,type=timer.count",
	}

	for _, id := range sumIDs {
		assert.Equal(t, ts.ConsolidationSum, FindConsolidationApproach(id))
	}

	avgIDs := []string{
		"stats.sjc1.gauges.donkey.kong.barrels",
		"stats.sjc1.timers.test",
		"fake.sjc1.counts.test",
		"servers.testabc-sjc1.counts.test",
		"stats.sjc1.timers.donkey.kong.p95",
		"stats.sjc1.timers.m3+service.fetchbatchraw.latency+dc=sjc1,env=production,host=m3-tsdb112-sjc1,pipe=none,service=statsdex_m3dbnode,servicename=node,type=timer.p95",
	}

	for _, id := range avgIDs {
		assert.Equal(t, ts.ConsolidationAvg, FindConsolidationApproach(id))
	}
}

// newMetricConsolidation creates a new consolidation for rolling up a given
// metric according to its default window and consolidation function
func newMetricConsolidation(ctx context.Context, id string, startTime, endTime time.Time) ts.Consolidation {
	policy := FindRetentionPolicy(id, time.Since(startTime))
	stepInMillis := int(policy.UnitPerStep / time.Millisecond)
	return ts.NewConsolidation(ctx, startTime, endTime, stepInMillis, policy.Consolidation.Func())
}
