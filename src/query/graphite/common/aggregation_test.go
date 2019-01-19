package common

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/query/graphite/ts"

	"github.com/stretchr/testify/require"
)

func TestRangeOfSeries(t *testing.T) {
	ctx, input := NewConsolidationTestSeries(consolidationStartTime, consolidationEndTime, 30*time.Second)
	defer ctx.Close()

	in := ts.SeriesList{Values: input}

	expectedStart := ctx.StartTime.Add(-30 * time.Second)
	expectedStep := 10000
	rangeSeries, err := Range(ctx, in, func(series ts.SeriesList) string {
		return "woot"
	})
	require.Nil(t, err)

	expected := TestSeries{
		Name: "woot",
		Data: []float64{0, 0, 0, 12, 12, 12, 14, 14, 14, 0, 0, 0},
	}

	CompareOutputsAndExpected(t, expectedStep, expectedStart, []TestSeries{expected}, []*ts.Series{rangeSeries})
}
