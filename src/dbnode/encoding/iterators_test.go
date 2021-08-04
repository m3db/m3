// Copyright (c) 2018 Uber Technologies, Inc.
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

package encoding

import (
	"testing"
	"time"

	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

var (
	at               = xtime.Now()
	commonTestValues = [][]testValue{
		[]testValue{
			{t: at, value: 2.0, unit: xtime.Second},
			{t: at.Add(time.Second), value: 6.0, unit: xtime.Second},
			{t: at.Add(2 * time.Second), value: 7.0, unit: xtime.Second},
		},
		[]testValue{
			{t: at, value: 1.0, unit: xtime.Second},
			{t: at.Add(time.Second), value: 5.0, unit: xtime.Second},
			{t: at.Add(2 * time.Second), value: 9.0, unit: xtime.Second},
		},
		[]testValue{
			{t: at, value: 3.0, unit: xtime.Second},
			{t: at.Add(time.Second), value: 4.0, unit: xtime.Second},
			{t: at.Add(2 * time.Second), value: 8.0, unit: xtime.Second},
		},
	}
)

func TestIteratorsIterateEqualTimestampStrategy(t *testing.T) {
	testValues := commonTestValues
	lastTestValues := commonTestValues[len(commonTestValues)-1]

	iters := iterators{equalTimesStrategy: IterateLastPushed}
	iters.reset()

	assertIteratorsValues(t, iters, testValues, lastTestValues)
}

func TestIteratorsIterateHighestValue(t *testing.T) {
	testValues := commonTestValues
	lastTestValues := []testValue{
		testValues[2][0],
		testValues[0][1],
		testValues[1][2],
	}

	iters := iterators{equalTimesStrategy: IterateHighestValue}
	iters.reset()

	assertIteratorsValues(t, iters, testValues, lastTestValues)
}

func TestIteratorsIterateLowestValue(t *testing.T) {
	testValues := commonTestValues
	lastTestValues := []testValue{
		testValues[1][0],
		testValues[2][1],
		testValues[0][2],
	}

	iters := iterators{equalTimesStrategy: IterateLowestValue}
	iters.reset()

	assertIteratorsValues(t, iters, testValues, lastTestValues)
}

func TestIteratorsIterateHighestFrequencyValue(t *testing.T) {
	testValues := [][]testValue{
		[]testValue{
			{t: at, value: 2.0, unit: xtime.Second},
			{t: at.Add(time.Second), value: 6.0, unit: xtime.Second},
			{t: at.Add(2 * time.Second), value: 8.0, unit: xtime.Second},
		},
		[]testValue{
			{t: at, value: 2.0, unit: xtime.Second},
			{t: at.Add(time.Second), value: 6.0, unit: xtime.Second},
			{t: at.Add(2 * time.Second), value: 9.0, unit: xtime.Second},
		},
		[]testValue{
			{t: at, value: 3.0, unit: xtime.Second},
			{t: at.Add(time.Second), value: 5.0, unit: xtime.Second},
			{t: at.Add(2 * time.Second), value: 8.0, unit: xtime.Second},

			// Also test just a single value without any other frequencies
			{t: at.Add(3 * time.Second), value: 10.0, unit: xtime.Second},
		},
	}
	lastTestValues := []testValue{
		testValues[0][0],
		testValues[1][1],
		testValues[2][2],
		testValues[2][3],
	}

	iters := iterators{equalTimesStrategy: IterateHighestFrequencyValue}
	iters.reset()

	assertIteratorsValues(t, iters, testValues, lastTestValues)
}

func assertIteratorsValues(
	t *testing.T,
	iters iterators,
	testValues [][]testValue,
	expectedValues []testValue,
) {
	require.Equal(t, 0, len(iters.values))

	var testIters []Iterator
	for _, values := range testValues {
		testIters = append(testIters, newTestIterator(values))
	}

	for _, iter := range testIters {
		require.True(t, iters.push(iter))
	}

	for _, expectedValue := range expectedValues {
		ok, err := iters.moveToValidNext()
		require.NoError(t, err)
		require.True(t, ok)

		dp, unit, annotation := iters.current()
		require.Equal(t, expectedValue.t, dp.TimestampNanos)
		require.Equal(t, dp.Value, expectedValue.value)
		require.Equal(t, unit, expectedValue.unit)
		require.Equal(t, []byte(annotation), expectedValue.annotation)
	}
	ok, err := iters.moveToValidNext()
	require.NoError(t, err)
	require.False(t, ok)
}
