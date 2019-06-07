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

package series

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/x/xio"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
	"github.com/m3db/m3/src/dbnode/namespace"
)

var timeDistantFuture = time.Now().Add(10 * 365 * 24 * time.Hour)

func secs(x float64) time.Duration {
	return time.Duration(x * float64(time.Second))
}

func mins(x float64) time.Duration {
	return time.Duration(x * float64(time.Minute))
}

type blockData struct {
	start     time.Time
	writeType WriteType
	data      [][]value
}

type value struct {
	timestamp  time.Time
	value      float64
	unit       xtime.Unit
	annotation []byte
}

type valuesByTime []value

func (v valuesByTime) Len() int {
	return len(v)
}

func (v valuesByTime) Less(lhs, rhs int) bool {
	return v[lhs].timestamp.Before(v[rhs].timestamp)
}

func (v valuesByTime) Swap(lhs, rhs int) {
	v[lhs], v[rhs] = v[rhs], v[lhs]
}

type setAnnotation func([]value) []value
type requireAnnEqual func(*testing.T, []byte, []byte)

func decodedReaderValues(results [][]xio.BlockReader, opts Options, nsCtx namespace.Context) ([]value, error) {
	slicesIter := xio.NewReaderSliceOfSlicesFromBlockReadersIterator(results)
	iter := opts.MultiReaderIteratorPool().Get()
	iter.ResetSliceOfSlices(slicesIter, nsCtx.Schema)
	defer iter.Close()

	var all []value
	for iter.Next() {
		dp, unit, annotation := iter.Current()
		// Iterator reuse annotation byte slices, so make a copy.
		annotationCopy := append([]byte(nil), annotation...)
		all = append(all, value{dp.Timestamp, dp.Value, unit, annotationCopy})
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	return all, nil
}

func requireReaderValuesEqual(t *testing.T, values []value, results [][]xio.BlockReader, opts Options,
	nsCtx namespace.Context) {
	decodedValues, err := decodedReaderValues(results, opts, nsCtx)
	require.NoError(t, err)
	requireValuesEqual(t, values, decodedValues, nsCtx)
}

func requireValuesEqual(t *testing.T, expected, actual []value, nsCtx namespace.Context) {
	require.Len(t, actual, len(expected))
	for i := 0; i < len(actual); i++ {
		require.True(t, expected[i].timestamp.Equal(actual[i].timestamp))
		require.Equal(t, expected[i].value, actual[i].value)
		require.Equal(t, expected[i].unit, actual[i].unit)
		if nsCtx.Schema == nil {
			require.Equal(t, expected[i].annotation, actual[i].annotation)
		} else {
			testProtoEqual(t, expected[i].annotation, actual[i].annotation)
		}
	}
}

func decodedSegmentValues(results []xio.SegmentReader, opts Options, nsCtx namespace.Context) ([]value, error) {
	iter := opts.MultiReaderIteratorPool().Get()
	iter.Reset(results, time.Time{}, time.Duration(0), nsCtx.Schema)
	defer iter.Close()

	var all []value
	for iter.Next() {
		dp, unit, annotation := iter.Current()
		// Iterator reuse annotation byte slices, so make a copy.
		annotationCopy := append([]byte(nil), annotation...)
		all = append(all, value{dp.Timestamp, dp.Value, unit, annotationCopy})
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	return all, nil
}

func requireSegmentValuesEqual(t *testing.T, values []value, results []xio.SegmentReader, opts Options,
	nsCtx namespace.Context) {
	decodedValues, err := decodedSegmentValues(results, opts, nsCtx)

	require.NoError(t, err)
	requireValuesEqual(t, values, decodedValues, nsCtx)
}
