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

	"github.com/m3db/m3/src/dbnode/ts"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

var (
	at               = xtime.Now().Truncate(time.Hour)
	firstAnnotation  = []byte{1, 2, 3}
	commonTestValues = [][]testValue{
		{
			{t: at, value: 2.0, unit: xtime.Second, annotation: firstAnnotation},
			{t: at.Add(time.Second), value: 6.0, unit: xtime.Second, annotation: []byte{2}},
			{t: at.Add(2 * time.Second), value: 7.0, unit: xtime.Second, annotation: []byte{3}},
		},
		{
			{t: at, value: 1.0, unit: xtime.Second, annotation: firstAnnotation},
			{t: at.Add(time.Second), value: 5.0, unit: xtime.Second},
			{t: at.Add(2 * time.Second), value: 9.0, unit: xtime.Second, annotation: []byte{4}},
		},
		{
			{t: at, value: 3.0, unit: xtime.Second, annotation: firstAnnotation},
			{t: at.Add(time.Second), value: 4.0, unit: xtime.Second},
			{t: at.Add(2 * time.Second), value: 8.0, unit: xtime.Second, annotation: []byte{5}},
		},
	}
)

func TestIteratorsIterateEqualTimestampStrategy(t *testing.T) {
	testValues := commonTestValues
	lastTestValues := commonTestValues[len(commonTestValues)-1]

	iters := &iterators{equalTimesStrategy: IterateLastPushed}
	iters.reset()

	assertIteratorsValues(t, iters, testValues, lastTestValues, firstAnnotation)
}

func TestIteratorsIterateHighestValue(t *testing.T) {
	testValues := commonTestValues
	lastTestValues := []testValue{
		testValues[2][0],
		testValues[0][1],
		testValues[1][2],
	}

	iters := &iterators{equalTimesStrategy: IterateHighestValue}
	iters.reset()

	assertIteratorsValues(t, iters, testValues, lastTestValues, firstAnnotation)
}

func TestIteratorsIterateLowestValue(t *testing.T) {
	testValues := commonTestValues
	lastTestValues := []testValue{
		testValues[1][0],
		testValues[2][1],
		testValues[0][2],
	}

	iters := &iterators{equalTimesStrategy: IterateLowestValue}
	iters.reset()

	assertIteratorsValues(t, iters, testValues, lastTestValues, firstAnnotation)
}

func TestIteratorsIterateHighestFrequencyValue(t *testing.T) {
	testValues := [][]testValue{
		{
			{t: at, value: 2.0, unit: xtime.Second},
			{t: at.Add(time.Second), value: 6.0, unit: xtime.Second},
			{t: at.Add(2 * time.Second), value: 8.0, unit: xtime.Second},
		},
		{
			{t: at, value: 2.0, unit: xtime.Second},
			{t: at.Add(time.Second), value: 6.0, unit: xtime.Second},
			{t: at.Add(2 * time.Second), value: 9.0, unit: xtime.Second},
		},
		{
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

	iters := &iterators{equalTimesStrategy: IterateHighestFrequencyValue}
	iters.reset()

	assertIteratorsValues(t, iters, testValues, lastTestValues, nil)
}

func TestIteratorsReuse(t *testing.T) {
	var (
		testValues1 = [][]testValue{
			{
				{t: at, value: 1.0, unit: xtime.Second, annotation: []byte{1}},
			},
		}
		testValues2 = [][]testValue{
			{
				{t: at.Add(time.Hour), value: 2.0, unit: xtime.Second, annotation: nil},
			},
		}
	)

	iters := &iterators{equalTimesStrategy: IterateLastPushed}
	iters.reset()

	assertIteratorsValues(t, iters, testValues1, testValues1[0], []byte{1})
	iters.reset()
	assertIteratorsValues(t, iters, testValues2, testValues2[0], nil)
}

func assertIteratorsValues(
	t *testing.T,
	iters *iterators,
	testValues [][]testValue,
	expectedValues []testValue,
	expectedFirstAnnotation ts.Annotation,
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
		require.Equal(t, expectedValue.value, dp.Value)
		require.Equal(t, expectedValue.unit, unit)
		require.Equal(t, expectedValue.annotation, annotation)
		require.Equal(t, expectedFirstAnnotation, iters.firstAnnotation())
	}
	ok, err := iters.moveToValidNext()
	require.NoError(t, err)
	require.False(t, ok)
}
