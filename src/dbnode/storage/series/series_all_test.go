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

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/x/xio"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
)

var timeDistantFuture = xtime.Now().Add(10 * 365 * 24 * time.Hour)

func secs(x float64) time.Duration {
	return time.Duration(x * float64(time.Second))
}

func mins(x float64) time.Duration {
	return time.Duration(x * float64(time.Minute))
}

type blockData struct {
	start     xtime.UnixNano
	writeType WriteType
	data      [][]DecodedTestValue
}

type setAnnotation func([]DecodedTestValue) []DecodedTestValue
type requireAnnEqual func(*testing.T, []byte, []byte)

func decodedReaderValues(results [][]xio.BlockReader,
	opts Options, nsCtx namespace.Context) ([]DecodedTestValue, error) {
	slicesIter := xio.NewReaderSliceOfSlicesFromBlockReadersIterator(results)
	iter := opts.MultiReaderIteratorPool().Get()
	iter.ResetSliceOfSlices(slicesIter, nsCtx.Schema)
	defer iter.Close()

	var all []DecodedTestValue
	for iter.Next() {
		dp, unit, annotation := iter.Current()
		// Iterator reuse annotation byte slices, so make a copy.
		annotationCopy := append([]byte(nil), annotation...)
		all = append(all, DecodedTestValue{dp.TimestampNanos, dp.Value, unit, annotationCopy})
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	return all, nil
}

func requireReaderValuesEqual(t *testing.T, values []DecodedTestValue,
	results [][]xio.BlockReader, opts Options,
	nsCtx namespace.Context) {
	decodedValues, err := decodedReaderValues(results, opts, nsCtx)
	require.NoError(t, err)
	requireValuesEqual(t, values, decodedValues, nsCtx)
}

func requireValuesEqual(
	t *testing.T,
	expected, actual []DecodedTestValue,
	nsCtx namespace.Context,
) {
	debugValues := struct {
		ExpectedValues []DecodedTestValue
		ActualValues   []DecodedTestValue
	}{
		ExpectedValues: expected,
		ActualValues:   actual,
	}
	require.Len(t, actual, len(expected),
		"length mismatch: values=%+v", spew.Sdump(debugValues))
	for i := 0; i < len(actual); i++ {
		debugValue := struct {
			ExpectedValue DecodedTestValue
			ActualValue   DecodedTestValue
		}{
			ExpectedValue: expected[i],
			ActualValue:   actual[i],
		}
		require.True(t, expected[i].Timestamp.Equal(actual[i].Timestamp),
			"timestamp mismatch: mismatch=%+v values=%+v",
			spew.Sdump(debugValue), spew.Sdump(debugValues))
		require.Equal(t, expected[i].Value, actual[i].Value,
			"value mismatch: mismatch=%+v values=%+v",
			spew.Sdump(debugValue), spew.Sdump(debugValues))
		require.Equal(t, expected[i].Unit, actual[i].Unit,
			"unit mismatch: mismatch=%+v values=%+v",
			spew.Sdump(debugValue), spew.Sdump(debugValues))
		if nsCtx.Schema == nil {
			require.Equal(t, expected[i].Annotation, actual[i].Annotation,
				"annotation mismatch: mismatch=%+v values=%+v",
				spew.Sdump(debugValue), spew.Sdump(debugValues))
		} else {
			testProtoEqual(t, expected[i].Annotation, actual[i].Annotation)
		}
	}
}

func decodedSegmentValues(results []xio.SegmentReader, opts Options,
	nsCtx namespace.Context) ([]DecodedTestValue, error) {
	iter := opts.MultiReaderIteratorPool().Get()
	return DecodeSegmentValues(results, iter, nsCtx.Schema)
}

func requireSegmentValuesEqual(t *testing.T, values []DecodedTestValue,
	results []xio.SegmentReader, opts Options,
	nsCtx namespace.Context) {
	decodedValues, err := decodedSegmentValues(results, opts, nsCtx)

	require.NoError(t, err)
	requireValuesEqual(t, values, decodedValues, nsCtx)
}
