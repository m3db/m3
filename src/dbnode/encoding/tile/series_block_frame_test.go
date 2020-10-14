// Copyright (c) 2020 Uber Technologies, Inc.
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

package tile

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/ts"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSeriesBlockFrame(t *testing.T) {
	seriesBlockFrame := newSeriesBlockFrame(newRecorder())
	start := time.Now().Truncate(time.Hour)
	timeAt := func(i int) time.Time { return start.Add(time.Minute * time.Duration(i)) }

	addPoints := func(size int) {
		for i := 0; i < size; i++ {
			seriesBlockFrame.record(
				ts.Datapoint{
					Value:     float64(i),
					Timestamp: timeAt(i),
				},
				xtime.Microsecond,
				ts.Annotation("foobar"),
			)
		}
	}

	verify := func(frame SeriesBlockFrame, size int) {
		ex := make([]float64, size)
		for i := range ex {
			ex[i] = float64(i)
		}

		vals := frame.Values()
		require.Equal(t, size, len(vals))
		for i := 0; i < size; i++ {
			assert.Equal(t, float64(i), vals[i])
		}

		assert.Equal(t, ex, frame.Values())

		times := frame.Timestamps()
		require.Equal(t, size, len(times))
		for i := 0; i < size; i++ {
			require.Equal(t, timeAt(i), times[i])
		}

		for i := 0; i < size; i++ {
			annotation, err := frame.Annotations().Value(i)
			require.NoError(t, err)
			assert.Equal(t, ts.Annotation("foobar"), annotation)
		}

		for i := 0; i < size; i++ {
			units, err := frame.Units().Value(i)
			require.NoError(t, err)
			assert.Equal(t, xtime.Microsecond, units)
		}
	}

	size := 5
	addPoints(size)

	verify(seriesBlockFrame, size)
	seriesBlockFrame.reset(0, 0)

	size = 15
	addPoints(size)
	verify(seriesBlockFrame, size)
}
