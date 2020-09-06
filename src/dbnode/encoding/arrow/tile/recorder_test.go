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

	"github.com/m3db/m3/src/dbnode/ts"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatapointRecorder(t *testing.T) {
	recorder := newFlatDatapointRecorder()
	addPoints := func(size int) {
		for i := 0; i < size; i++ {
			timestampNanos := xtime.UnixNano(i)
			recorder.record(
				ts.Datapoint{
					Value:          float64(i),
					Timestamp:      timestampNanos.ToTime(),
					TimestampNanos: timestampNanos,
				},
				xtime.Microsecond,
				ts.Annotation("foobar"),
			)
		}
	}

	verify := func(rec *record, size int) {
		ex := float64(size*(size-1)) / 2
		assert.Equal(t, ex, rec.sum())

		vals := rec.values()
		require.Equal(t, size, len(vals))
		for i := 0; i < size; i++ {
			assert.Equal(t, float64(i), vals[i])
		}

		times := rec.timestamps()
		require.Equal(t, size, len(times))
		for i := 0; i < size; i++ {
			assert.Equal(t, int64(i), times[i].UnixNano())
		}

		annotation, isSingle := rec.annotations.SingleValue()
		require.True(t, isSingle)
		assert.Equal(t, ts.Annotation("foobar"), annotation)

		exAnns := make([]ts.Annotation, 0, size)
		for i := 0; i < size; i++ {
			exAnns = append(exAnns, ts.Annotation("foobar"))
		}

		assert.Equal(t, exAnns, rec.annotations.Values())

		unit, isSingle := rec.units.SingleValue()
		require.True(t, isSingle)
		assert.Equal(t, xtime.Microsecond, unit)

		exUnits := make([]xtime.Unit, 0, size)
		for i := 0; i < size; i++ {
			exUnits = append(exUnits, xtime.Microsecond)
		}

		assert.Equal(t, exUnits, rec.units.Values())
	}

	size := 10
	addPoints(size)

	rec := newDatapointRecord()
	recorder.updateRecord(rec)
	verify(rec, size)
	rec.release()

	size = 150000
	addPoints(size)
	recorder.updateRecord(rec)
	verify(rec, size)
	rec.release()
}

func TestDatapointRecorderChangingAnnotationsAndUnits(t *testing.T) {
	recorder := newFlatDatapointRecorder()
	frame := SeriesBlockFrame{record: newDatapointRecord()}

	recorder.record(ts.Datapoint{}, xtime.Day, ts.Annotation("foo"))
	recorder.record(ts.Datapoint{}, xtime.Second, ts.Annotation("foo"))
	recorder.record(ts.Datapoint{}, xtime.Second, ts.Annotation("foo"))
	recorder.updateRecord(frame.record)
	a, single := frame.Annotations().SingleValue()
	require.True(t, single)
	annotationEqual(t, "foo", a)
	_, single = frame.Units().SingleValue()
	require.False(t, single)
	units := frame.Units().Values()
	assert.Equal(t, []xtime.Unit{xtime.Day, xtime.Second, xtime.Second}, units)
	frame.release()

	recorder.record(ts.Datapoint{}, xtime.Day, ts.Annotation("foo"))
	recorder.record(ts.Datapoint{}, xtime.Day, ts.Annotation("foo"))
	recorder.record(ts.Datapoint{}, xtime.Day, ts.Annotation("bar"))
	recorder.updateRecord(frame.record)
	u, single := frame.Units().SingleValue()
	require.True(t, single)
	assert.Equal(t, xtime.Day, u)
	_, single = frame.Annotations().SingleValue()
	require.False(t, single)
	annotations := frame.Annotations().Values()
	annotationsEqual(t, []string{"foo", "foo", "bar"}, annotations)
	frame.release()

	recorder.record(ts.Datapoint{}, xtime.Day, ts.Annotation("foo"))
	recorder.record(ts.Datapoint{}, xtime.Second, ts.Annotation("bar"))
	recorder.updateRecord(frame.record)
	_, single = frame.Units().SingleValue()
	require.False(t, single)
	_, single = frame.Annotations().SingleValue()
	require.False(t, single)
	units = frame.Units().Values()
	assert.Equal(t, []xtime.Unit{xtime.Day, xtime.Second}, units)
	annotations = frame.Annotations().Values()
	annotationsEqual(t, []string{"foo", "bar"}, annotations)
	frame.release()

	annotations = frame.Annotations().Values()
	assert.Equal(t, 0, len(annotations))
	units = frame.Units().Values()
	assert.Equal(t, 0, len(units))
}
