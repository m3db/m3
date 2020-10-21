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

package wide

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCrossBlockSeriesIterator(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	iter := newCrossBlockReaderIterator()
	require.False(t, iter.Next())
	require.NoError(t, iter.Err())

	emptyIter := NewMockQuerySeriesIterator(ctrl)
	emptyIter.EXPECT().Next().Return(false).AnyTimes()
	emptyIter.EXPECT().Err().Return(nil).AnyTimes()

	id := ident.BytesID([]byte("foo"))
	firstIter := NewMockQuerySeriesIterator(ctrl)
	firstIter.EXPECT().SeriesMetadata().Return(SeriesMetadata{ID: id})
	firstIter.EXPECT().Next().Return(true).Times(2)
	firstIter.EXPECT().Current().Return(ts.Datapoint{Value: 0}, xtime.Second, nil)
	firstIter.EXPECT().Current().Return(ts.Datapoint{Value: 1}, xtime.Second, nil)
	firstIter.EXPECT().Next().Return(false)
	firstIter.EXPECT().Err().Return(nil)

	secondIter := NewMockQuerySeriesIterator(ctrl)
	secondIter.EXPECT().Next().Return(true)
	secondIter.EXPECT().Current().Return(ts.Datapoint{Value: 2}, xtime.Second, nil)
	secondIter.EXPECT().Next().Return(false)
	secondIter.EXPECT().Err().Return(nil)

	count := 7
	thirdIter := NewMockQuerySeriesIterator(ctrl)
	thirdIter.EXPECT().Next().Return(true).Times(count)
	for i := 0; i < count; i++ {
		dp := ts.Datapoint{Value: 3 + float64(i)}
		thirdIter.EXPECT().Current().Return(dp, xtime.Second, nil)
	}
	thirdIter.EXPECT().Next().Return(false)
	thirdIter.EXPECT().Err().Return(nil)

	iters := []QuerySeriesIterator{
		firstIter,
		emptyIter,
		emptyIter,
		secondIter,
		thirdIter,
		emptyIter,
	}

	iter.reset(iters)
	assert.Equal(t, id, iter.SeriesMetadata().ID)

	var vals = []float64{}
	for iter.Next() {
		ts, _, _ := iter.Current()
		vals = append(vals, ts.Value)
	}

	require.NoError(t, iter.Err())
	assert.Equal(t, 10, len(vals))
	for i, actual := range vals {
		assert.Equal(t, float64(i), actual)
	}
}

func TestCrossBlockIterator(t *testing.T) {
	tests := []struct {
		name           string
		blockSeriesIDs [][]string
		expectedIDs    []string
	}{
		{
			name:           "no iterators",
			blockSeriesIDs: [][]string{},
			expectedIDs:    []string{},
		},
		{
			name:           "one reader, one series",
			blockSeriesIDs: [][]string{{"id1"}},
			expectedIDs:    []string{"id1"},
		},
		{
			name:           "one reader, many series",
			blockSeriesIDs: [][]string{{"id1", "id2", "id3"}},
			expectedIDs:    []string{"id1", "id2", "id3"},
		},
		{
			name:           "many iterators with same series",
			blockSeriesIDs: [][]string{{"id1"}, {"id1"}, {"id1"}},
			expectedIDs:    []string{"id1"},
		},
		{
			name:           "many iterators with different series",
			blockSeriesIDs: [][]string{{"id1"}, {"id2"}, {"id3"}},
			expectedIDs:    []string{"id1", "id2", "id3"},
		},
		{
			name:           "many iterators with unordered series",
			blockSeriesIDs: [][]string{{"id3"}, {"id1"}, {"id2"}},
			expectedIDs:    []string{"id1", "id2", "id3"},
		},
		{
			name:           "complex case",
			blockSeriesIDs: [][]string{{"id2", "id3", "id5"}, {"id1", "id2", "id4"}, {"id1", "id4"}},
			expectedIDs:    []string{"id1", "id2", "id3", "id4", "id5"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCrossBlockIterator(t, tt.blockSeriesIDs, tt.expectedIDs)
		})
	}
}

func testCrossBlockIterator(t *testing.T, blockSeriesIDs [][]string, expectedIDs []string) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	now := time.Now().Truncate(time.Hour)
	var iters []QueryShardIterator

	for blockIdx, ids := range blockSeriesIDs {
		iters = append(iters, newStrShardIterator(
			now.Add(time.Minute*time.Duration(blockIdx)),
			ids, blockIdx,
		))
	}

	iterator, err := NewCrossBlockIterator(iters)
	require.NoError(t, err)
	defer iterator.Close()

	ids := []string{}
	vals := []float64{}
	for iterator.Next() {
		currIterator := iterator.Current()
		ids = append(ids, string(currIterator.SeriesMetadata().ID))
		require.True(t, currIterator.Next())
		dp, _, _ := currIterator.Current()
		vals = append(vals, dp.Value)
	}

	assert.Equal(t, expectedIDs, ids)
}

type strShardIterator struct {
	idx         int
	blockNumber int
	start       time.Time
	ids         []string
}

func newStrShardIterator(
	start time.Time,
	ids []string,
	blockNumber int,
) QueryShardIterator {
	return &strShardIterator{
		idx:         -1,
		start:       start,
		ids:         ids,
		blockNumber: blockNumber,
	}
}

func (it *strShardIterator) Next() bool {
	it.idx++
	return it.idx < len(it.ids)
}

func (it *strShardIterator) Current() QuerySeriesIterator {
	return &strSeriesIterator{
		id:          it.ids[it.idx],
		shardNumber: it.idx,
		blockNumber: it.blockNumber,
	}
}

func (it *strShardIterator) BlockStart() time.Time                  { return it.start }
func (it *strShardIterator) Shard() uint32                          { return 0 }
func (it *strShardIterator) PushRecord(_ ShardIteratorRecord) error { return nil }
func (it *strShardIterator) Err() error                             { return nil }
func (it *strShardIterator) Close()                                 {}

type strSeriesIterator struct {
	used        bool
	id          string
	shardNumber int
	blockNumber int
}

func (it *strSeriesIterator) Err() error { return nil }
func (it *strSeriesIterator) Close()     {}
func (it *strSeriesIterator) Next() bool {
	if it.used {
		return false
	}

	it.used = true
	return true
}

func (it *strSeriesIterator) Current() (ts.Datapoint, xtime.Unit, ts.Annotation) {
	return ts.Datapoint{
		Value: float64(it.shardNumber + it.blockNumber*10),
	}, xtime.Second, nil
}

func (it *strSeriesIterator) SeriesMetadata() SeriesMetadata {
	return SeriesMetadata{ID: ident.BytesID([]byte(it.id))}
}
