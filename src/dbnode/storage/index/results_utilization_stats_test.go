// Copyright (c) 2022 Uber Technologies, Inc.
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

package index

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResultsUtilizationStatsConstant(t *testing.T) {
	r := resultsUtilizationStats{}
	for i := 0; i < 2*consecutiveTimesUnderCapacityThreshold; i++ {
		require.True(t, r.updateAndCheck(100), strconv.Itoa(i))
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

func TestResultsUtilizationStatsAllSmallerThanFirst(t *testing.T) {
	r := resultsUtilizationStats{}
	require.True(t, r.updateAndCheck(100))
	for i := 1; i < consecutiveTimesUnderCapacityThreshold; i++ {
		require.True(t, r.updateAndCheck(99), strconv.Itoa(i))
	}
	require.False(t, r.updateAndCheck(99))
}

func TestResultsUtilizationStatsAlternating(t *testing.T) {
	r := resultsUtilizationStats{}
	for i := 0; i < 10*consecutiveTimesUnderCapacityThreshold; i++ {
		require.True(t, r.updateAndCheck(i%5), strconv.Itoa(i))
	}
}
