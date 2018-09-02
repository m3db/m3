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

package encoding

import (
	"testing"
	"time"

	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestIteratorsSelectsDuplicateTimestampFromLastIterator(t *testing.T) {
	at := time.Now()

	testValues := [][]testValue{
		[]testValue{
			{t: at, value: 2.0, unit: xtime.Second},
			{t: at.Add(time.Second), value: 5.0, unit: xtime.Second},
			{t: at.Add(2 * time.Second), value: 8.0, unit: xtime.Second},
		},
		[]testValue{
			{t: at, value: 1.0, unit: xtime.Second},
			{t: at.Add(time.Second), value: 4.0, unit: xtime.Second},
			{t: at.Add(2 * time.Second), value: 7.0, unit: xtime.Second},
		},
		[]testValue{
			{t: at, value: 3.0, unit: xtime.Second},
			{t: at.Add(time.Second), value: 6.0, unit: xtime.Second},
			{t: at.Add(2 * time.Second), value: 9.0, unit: xtime.Second},
		},
	}
	lastTestValues := testValues[len(testValues)-1]

	var testIters []Iterator
	for _, values := range testValues {
		testIters = append(testIters, newTestIterator(values))
	}

	var iters iterators
	iters.reset()
	for _, iter := range testIters {
		require.True(t, iters.push(iter))
	}

	for i := 0; i < 3; i++ {
		ok, err := iters.moveToValidNext()
		require.NoError(t, err)
		require.True(t, ok)

		dp, unit, annotation := iters.current()
		require.True(t, lastTestValues[i].t.Equal(dp.Timestamp))
		require.Equal(t, lastTestValues[i].value, dp.Value)
		require.Equal(t, lastTestValues[i].unit, unit)
		require.Equal(t, lastTestValues[i].annotation, []byte(annotation))
	}
	ok, err := iters.moveToValidNext()
	require.NoError(t, err)
	require.False(t, ok)
}
