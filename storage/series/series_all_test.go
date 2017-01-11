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

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/storage/block"
	xio "github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/checked"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var timeDistantFuture = time.Now().Add(10 * 365 * 24 * time.Hour)

func secs(x float64) time.Duration {
	return time.Duration(x * float64(time.Second))
}

func mins(x float64) time.Duration {
	return time.Duration(x * float64(time.Minute))
}

func hrs(x float64) time.Duration {
	return time.Duration(x * float64(time.Hour))
}

func requireDrainedStream(
	ctx context.Context,
	t *testing.T,
	b block.DatabaseBlock,
) xio.SegmentReader {
	stream, err := b.Stream(ctx)
	require.NoError(t, err)
	return stream
}

func bytesRefd(data []byte) checked.Bytes {
	bytes := checked.NewBytes(data, nil)
	bytes.IncRef()
	return bytes
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

type decodedValue struct {
	timestamp  time.Time
	value      float64
	unit       xtime.Unit
	annotation []byte
}

// Implements sort.Interface
type decodedValuesByTime []decodedValue

func (v decodedValuesByTime) Len() int {
	return len(v)
}

func (v decodedValuesByTime) Less(lhs, rhs int) bool {
	return v[lhs].timestamp.Before(v[rhs].timestamp)
}

func (v decodedValuesByTime) Swap(lhs, rhs int) {
	v[lhs], v[rhs] = v[rhs], v[lhs]
}

func decodedValues(results [][]xio.SegmentReader, opts Options) ([]decodedValue, error) {
	slicesIter := xio.NewReaderSliceOfSlicesFromSegmentReadersIterator(results)
	iter := opts.MultiReaderIteratorPool().Get()
	iter.ResetSliceOfSlices(slicesIter)
	defer iter.Close()

	var all []decodedValue
	for iter.Next() {
		dp, unit, annotation := iter.Current()
		all = append(all, decodedValue{dp.Timestamp, dp.Value, unit, annotation})
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	return all, nil
}

func assertValuesEqual(t *testing.T, values []value, results [][]xio.SegmentReader, opts Options) {
	decodedValues, err := decodedValues(results, opts)

	assert.NoError(t, err)
	assert.Len(t, decodedValues, len(values))
	for i := 0; i < len(decodedValues); i++ {
		assert.True(t, values[i].timestamp.Equal(decodedValues[i].timestamp))
		assert.Equal(t, values[i].value, decodedValues[i].value)
		assert.Equal(t, values[i].unit, decodedValues[i].unit)
		assert.Equal(t, values[i].annotation, decodedValues[i].annotation)
	}
}
