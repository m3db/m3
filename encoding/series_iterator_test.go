// Copyright (c) 2016 Uber Technologies, Inc.
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

	"github.com/m3db/m3db/interfaces/m3db"
	xtime "github.com/m3db/m3db/x/time"

	"github.com/stretchr/testify/assert"
)

type testSeries struct {
	id     string
	start  time.Time
	end    time.Time
	values []testValue
	err    *testSeriesErr
}

type testSeriesErr struct {
	err   error
	atIdx int
}

func TestSeriesIteratorMatchingReplicas(t *testing.T) {
	start := time.Now().Truncate(time.Hour)
	end := start.Add(time.Hour)

	all := []testSeries{
		{
			id:    "foo",
			start: start,
			end:   end,
			values: []testValue{
				{1.0, start.Add(1 * time.Millisecond), xtime.Millisecond, []byte{1, 2, 3}},
				{2.0, start.Add(1 * time.Second), xtime.Second, nil},
				{3.0, start.Add(2 * time.Second), xtime.Second, nil},
			},
		},
		{
			id:    "bar",
			start: start,
			end:   end,
			values: []testValue{
				{4.0, start.Add(1 * time.Second), xtime.Second, []byte{4, 5, 6}},
				{5.0, start.Add(2 * time.Second), xtime.Second, nil},
				{6.0, start.Add(3 * time.Second), xtime.Second, nil},
			},
		},
		{
			id:    "baz",
			start: start,
			end:   end,
			values: []testValue{
				{7.0, start.Add(1 * time.Minute), xtime.Second, []byte{7, 8, 9}},
				{8.0, start.Add(3 * time.Minute), xtime.Second, nil},
				{9.0, start.Add(2 * time.Minute), xtime.Second, nil},
			},
			err: &testSeriesErr{errOutOfOrderIterator, 2},
		},
	}

	for i := range all {
		series := &all[i]

		var replicas []m3db.Iterator
		for i := 0; i < 3; i++ {
			replicas = append(replicas, newTestIterator(series.values))
		}

		assertTestSeriesIterator(t, series, replicas)
	}
}

func assertTestSeriesIterator(
	t *testing.T,
	series *testSeries,
	iters []m3db.Iterator,
) {
	iter := NewSeriesIterator(series.id, series.start, series.end, iters)
	defer iter.Close()

	assert.Equal(t, series.id, iter.ID())
	assert.Equal(t, series.start, iter.Start())
	assert.Equal(t, series.end, iter.End())
	for i := 0; i < len(series.values); i++ {
		next := iter.Next()
		if series.err != nil && i == series.err.atIdx {
			assert.Equal(t, false, next)
			break
		}
		assert.Equal(t, true, next)
		dp, unit, annotation := iter.Current()
		expected := series.values[i]
		assert.Equal(t, expected.value, dp.Value)
		assert.Equal(t, expected.t, dp.Timestamp)
		assert.Equal(t, expected.unit, unit)
		assert.Equal(t, expected.annotation, []byte(annotation))
	}
	if series.err == nil {
		assert.NoError(t, iter.Err())
	} else {
		assert.Equal(t, series.err.err, iter.Err())
	}
}
