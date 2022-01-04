package index

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResultsUtilizationStatsConstant(t *testing.T) {
	r := resultsUtilizationStats{}
	for i := 0; i < 2*consecutiveTimesUnderCapacityThreshold; i++ {
		require.True(t, r.updateAndCheck(100))
	}
}

func TestResultsUtilizationStatsIncreasing(t *testing.T) {
	r := resultsUtilizationStats{}
	for i := 0; i < 2*consecutiveTimesUnderCapacityThreshold; i++ {
		require.True(t, r.updateAndCheck(i), strconv.Itoa(i))
	}
}

func TestResultsUtilizationStatsDecreasing(t *testing.T) {
	r := resultsUtilizationStats{}
	for i := consecutiveTimesUnderCapacityThreshold; i > 0; i-- {
		require.True(t, r.updateAndCheck(i), strconv.Itoa(i))
	}
	require.False(t, r.updateAndCheck(0))
}

func TestResultsUtilizationStatsAlternating(t *testing.T) {
	r := resultsUtilizationStats{}
	for i := 0; i < 10*consecutiveTimesUnderCapacityThreshold; i++ {
		require.True(t, r.updateAndCheck(i%5), strconv.Itoa(i))
	}
}
