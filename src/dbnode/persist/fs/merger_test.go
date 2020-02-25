// Copyright (c) 2019 Uber Technologies, Inc.
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

package fs

import (
	"io"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	blockSize = time.Hour
)

var (
	srPool        xio.SegmentReaderPool
	multiIterPool encoding.MultiReaderIteratorPool
	identPool     ident.Pool
	encoderPool   encoding.EncoderPool
	contextPool   context.Pool

	startTime = time.Now().Truncate(blockSize)

	id0 = ident.StringID("id0")
	id1 = ident.StringID("id1")
	id2 = ident.StringID("id2")
	id3 = ident.StringID("id3")
	id4 = ident.StringID("id4")
	id5 = ident.StringID("id5")
)

// init resources _except_ the fsReader, which should be configured on a
// per-test basis with NewMockDataFileSetReader.
func init() {
	poolOpts := pool.NewObjectPoolOptions().SetSize(1)
	srPool = xio.NewSegmentReaderPool(poolOpts)
	srPool.Init()
	multiIterPool = encoding.NewMultiReaderIteratorPool(poolOpts)
	multiIterPool.Init(func(r io.Reader, _ namespace.SchemaDescr) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encoding.NewOptions())
	})
	bytesPool := pool.NewCheckedBytesPool(nil, poolOpts, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, poolOpts)
	})
	bytesPool.Init()
	identPool = ident.NewPool(bytesPool, ident.PoolOptions{})
	encoderPool = encoding.NewEncoderPool(poolOpts)
	encoderPool.Init(func() encoding.Encoder {
		return m3tsz.NewEncoder(startTime, nil, true, encoding.NewOptions())
	})
	contextPool = context.NewPool(context.NewOptions().
		SetContextPoolOptions(poolOpts).
		SetFinalizerPoolOptions(poolOpts))
}

func TestMergeWithIntersection(t *testing.T) {
	// This test scenario is when there is an overlap in series data between
	// disk and the merge target.
	// id0-id3 is on disk, while the merge target has id1-id5.
	// Both have id1, but they don't have datapoints with overlapping
	// timestamps.
	// Both have id2, and some datapoints have overlapping timestamps.
	// Both have id3, and all datapoints have overlapping timestamps.
	diskData := newCheckedBytesByIDMap(newCheckedBytesByIDMapOptions{})
	diskData.Set(id0, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(0 * time.Second), Value: 0},
		{Timestamp: startTime.Add(1 * time.Second), Value: 1},
		{Timestamp: startTime.Add(2 * time.Second), Value: 2},
	}))
	diskData.Set(id1, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(2 * time.Second), Value: 2},
		{Timestamp: startTime.Add(3 * time.Second), Value: 3},
		{Timestamp: startTime.Add(6 * time.Second), Value: 4},
		{Timestamp: startTime.Add(7 * time.Second), Value: 5},
		{Timestamp: startTime.Add(9 * time.Second), Value: 6},
	}))
	diskData.Set(id2, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(1 * time.Second), Value: 7},
		{Timestamp: startTime.Add(3 * time.Second), Value: 8},
		{Timestamp: startTime.Add(5 * time.Second), Value: 9},
		{Timestamp: startTime.Add(6 * time.Second), Value: 10},
		{Timestamp: startTime.Add(7 * time.Second), Value: 11},
		{Timestamp: startTime.Add(10 * time.Second), Value: 12},
	}))
	diskData.Set(id3, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(2 * time.Second), Value: 13},
		{Timestamp: startTime.Add(4 * time.Second), Value: 14},
		{Timestamp: startTime.Add(8 * time.Second), Value: 15},
	}))

	mergeTargetData := newCheckedBytesByIDMap(newCheckedBytesByIDMapOptions{})
	mergeTargetData.Set(id1, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(4 * time.Second), Value: 16},
		{Timestamp: startTime.Add(5 * time.Second), Value: 17},
		{Timestamp: startTime.Add(8 * time.Second), Value: 18},
	}))
	mergeTargetData.Set(id2, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(3 * time.Second), Value: 19},
		{Timestamp: startTime.Add(6 * time.Second), Value: 20},
		{Timestamp: startTime.Add(7 * time.Second), Value: 21},
		{Timestamp: startTime.Add(9 * time.Second), Value: 22},
		{Timestamp: startTime.Add(10 * time.Second), Value: 23},
		{Timestamp: startTime.Add(13 * time.Second), Value: 24},
		{Timestamp: startTime.Add(16 * time.Second), Value: 25},
	}))
	mergeTargetData.Set(id3, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(2 * time.Second), Value: 26},
		{Timestamp: startTime.Add(4 * time.Second), Value: 27},
		{Timestamp: startTime.Add(8 * time.Second), Value: 28},
	}))
	mergeTargetData.Set(id4, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(8 * time.Second), Value: 29},
	}))
	mergeTargetData.Set(id5, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(3 * time.Second), Value: 30},
		{Timestamp: startTime.Add(7 * time.Second), Value: 31},
		{Timestamp: startTime.Add(12 * time.Second), Value: 32},
		{Timestamp: startTime.Add(15 * time.Second), Value: 34},
	}))

	expected := newCheckedBytesByIDMap(newCheckedBytesByIDMapOptions{})
	expected.Set(id0, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(0 * time.Second), Value: 0},
		{Timestamp: startTime.Add(1 * time.Second), Value: 1},
		{Timestamp: startTime.Add(2 * time.Second), Value: 2},
	}))
	expected.Set(id1, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(2 * time.Second), Value: 2},
		{Timestamp: startTime.Add(3 * time.Second), Value: 3},
		{Timestamp: startTime.Add(4 * time.Second), Value: 16},
		{Timestamp: startTime.Add(5 * time.Second), Value: 17},
		{Timestamp: startTime.Add(6 * time.Second), Value: 4},
		{Timestamp: startTime.Add(7 * time.Second), Value: 5},
		{Timestamp: startTime.Add(8 * time.Second), Value: 18},
		{Timestamp: startTime.Add(9 * time.Second), Value: 6},
	}))
	expected.Set(id2, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(1 * time.Second), Value: 7},
		{Timestamp: startTime.Add(3 * time.Second), Value: 19},
		{Timestamp: startTime.Add(5 * time.Second), Value: 9},
		{Timestamp: startTime.Add(6 * time.Second), Value: 20},
		{Timestamp: startTime.Add(7 * time.Second), Value: 21},
		{Timestamp: startTime.Add(9 * time.Second), Value: 22},
		{Timestamp: startTime.Add(10 * time.Second), Value: 23},
		{Timestamp: startTime.Add(13 * time.Second), Value: 24},
		{Timestamp: startTime.Add(16 * time.Second), Value: 25},
	}))
	expected.Set(id3, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(2 * time.Second), Value: 26},
		{Timestamp: startTime.Add(4 * time.Second), Value: 27},
		{Timestamp: startTime.Add(8 * time.Second), Value: 28},
	}))
	expected.Set(id4, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(8 * time.Second), Value: 29},
	}))
	expected.Set(id5, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(3 * time.Second), Value: 30},
		{Timestamp: startTime.Add(7 * time.Second), Value: 31},
		{Timestamp: startTime.Add(12 * time.Second), Value: 32},
		{Timestamp: startTime.Add(15 * time.Second), Value: 34},
	}))

	testMergeWith(t, diskData, mergeTargetData, expected)
}

func TestMergeWithFullIntersection(t *testing.T) {
	// This test scenario is when the merge target contains only and all data
	// from disk.
	diskData := newCheckedBytesByIDMap(newCheckedBytesByIDMapOptions{})
	diskData.Set(id0, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(0 * time.Second), Value: 0},
		{Timestamp: startTime.Add(1 * time.Second), Value: 1},
		{Timestamp: startTime.Add(2 * time.Second), Value: 2},
	}))
	diskData.Set(id1, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(2 * time.Second), Value: 2},
		{Timestamp: startTime.Add(3 * time.Second), Value: 3},
		{Timestamp: startTime.Add(6 * time.Second), Value: 4},
		{Timestamp: startTime.Add(7 * time.Second), Value: 5},
		{Timestamp: startTime.Add(9 * time.Second), Value: 6},
	}))

	mergeTargetData := newCheckedBytesByIDMap(newCheckedBytesByIDMapOptions{})
	mergeTargetData.Set(id0, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(0 * time.Second), Value: 7},
		{Timestamp: startTime.Add(1 * time.Second), Value: 8},
		{Timestamp: startTime.Add(2 * time.Second), Value: 9},
	}))
	mergeTargetData.Set(id1, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(2 * time.Second), Value: 10},
		{Timestamp: startTime.Add(3 * time.Second), Value: 11},
		{Timestamp: startTime.Add(6 * time.Second), Value: 12},
		{Timestamp: startTime.Add(7 * time.Second), Value: 13},
		{Timestamp: startTime.Add(9 * time.Second), Value: 14},
	}))

	expected := newCheckedBytesByIDMap(newCheckedBytesByIDMapOptions{})
	expected.Set(id0, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(0 * time.Second), Value: 7},
		{Timestamp: startTime.Add(1 * time.Second), Value: 8},
		{Timestamp: startTime.Add(2 * time.Second), Value: 9},
	}))
	expected.Set(id1, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(2 * time.Second), Value: 10},
		{Timestamp: startTime.Add(3 * time.Second), Value: 11},
		{Timestamp: startTime.Add(6 * time.Second), Value: 12},
		{Timestamp: startTime.Add(7 * time.Second), Value: 13},
		{Timestamp: startTime.Add(9 * time.Second), Value: 14},
	}))

	testMergeWith(t, diskData, mergeTargetData, expected)
}

func TestMergeWithNoIntersection(t *testing.T) {
	// This test scenario is when there is no overlap between disk data and
	// merge target data (series from one source does not exist in the other).
	diskData := newCheckedBytesByIDMap(newCheckedBytesByIDMapOptions{})
	diskData.Set(id0, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(0 * time.Second), Value: 0},
		{Timestamp: startTime.Add(1 * time.Second), Value: 1},
		{Timestamp: startTime.Add(2 * time.Second), Value: 2},
	}))
	diskData.Set(id1, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(2 * time.Second), Value: 2},
		{Timestamp: startTime.Add(3 * time.Second), Value: 3},
		{Timestamp: startTime.Add(6 * time.Second), Value: 4},
		{Timestamp: startTime.Add(7 * time.Second), Value: 5},
		{Timestamp: startTime.Add(9 * time.Second), Value: 6},
	}))
	diskData.Set(id2, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(1 * time.Second), Value: 7},
		{Timestamp: startTime.Add(3 * time.Second), Value: 8},
		{Timestamp: startTime.Add(5 * time.Second), Value: 9},
		{Timestamp: startTime.Add(6 * time.Second), Value: 10},
		{Timestamp: startTime.Add(7 * time.Second), Value: 11},
		{Timestamp: startTime.Add(10 * time.Second), Value: 12},
	}))

	mergeTargetData := newCheckedBytesByIDMap(newCheckedBytesByIDMapOptions{})
	mergeTargetData.Set(id3, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(2 * time.Second), Value: 26},
		{Timestamp: startTime.Add(4 * time.Second), Value: 27},
		{Timestamp: startTime.Add(8 * time.Second), Value: 28},
	}))
	mergeTargetData.Set(id4, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(8 * time.Second), Value: 29},
	}))
	mergeTargetData.Set(id5, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(3 * time.Second), Value: 30},
		{Timestamp: startTime.Add(7 * time.Second), Value: 31},
		{Timestamp: startTime.Add(12 * time.Second), Value: 32},
		{Timestamp: startTime.Add(15 * time.Second), Value: 34},
	}))

	expected := newCheckedBytesByIDMap(newCheckedBytesByIDMapOptions{})
	expected.Set(id0, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(0 * time.Second), Value: 0},
		{Timestamp: startTime.Add(1 * time.Second), Value: 1},
		{Timestamp: startTime.Add(2 * time.Second), Value: 2},
	}))
	expected.Set(id1, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(2 * time.Second), Value: 2},
		{Timestamp: startTime.Add(3 * time.Second), Value: 3},
		{Timestamp: startTime.Add(6 * time.Second), Value: 4},
		{Timestamp: startTime.Add(7 * time.Second), Value: 5},
		{Timestamp: startTime.Add(9 * time.Second), Value: 6},
	}))
	expected.Set(id2, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(1 * time.Second), Value: 7},
		{Timestamp: startTime.Add(3 * time.Second), Value: 8},
		{Timestamp: startTime.Add(5 * time.Second), Value: 9},
		{Timestamp: startTime.Add(6 * time.Second), Value: 10},
		{Timestamp: startTime.Add(7 * time.Second), Value: 11},
		{Timestamp: startTime.Add(10 * time.Second), Value: 12},
	}))
	expected.Set(id3, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(2 * time.Second), Value: 26},
		{Timestamp: startTime.Add(4 * time.Second), Value: 27},
		{Timestamp: startTime.Add(8 * time.Second), Value: 28},
	}))
	expected.Set(id4, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(8 * time.Second), Value: 29},
	}))
	expected.Set(id5, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(3 * time.Second), Value: 30},
		{Timestamp: startTime.Add(7 * time.Second), Value: 31},
		{Timestamp: startTime.Add(12 * time.Second), Value: 32},
		{Timestamp: startTime.Add(15 * time.Second), Value: 34},
	}))

	testMergeWith(t, diskData, mergeTargetData, expected)
}

func TestMergeWithNoMergeTargetData(t *testing.T) {
	// This test scenario is when there is no data in the merge target.
	diskData := newCheckedBytesByIDMap(newCheckedBytesByIDMapOptions{})
	diskData.Set(id0, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(0 * time.Second), Value: 0},
		{Timestamp: startTime.Add(1 * time.Second), Value: 1},
		{Timestamp: startTime.Add(2 * time.Second), Value: 2},
	}))
	diskData.Set(id1, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(2 * time.Second), Value: 2},
		{Timestamp: startTime.Add(3 * time.Second), Value: 3},
		{Timestamp: startTime.Add(6 * time.Second), Value: 4},
		{Timestamp: startTime.Add(7 * time.Second), Value: 5},
		{Timestamp: startTime.Add(9 * time.Second), Value: 6},
	}))
	diskData.Set(id2, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(1 * time.Second), Value: 7},
		{Timestamp: startTime.Add(3 * time.Second), Value: 8},
		{Timestamp: startTime.Add(5 * time.Second), Value: 9},
		{Timestamp: startTime.Add(6 * time.Second), Value: 10},
		{Timestamp: startTime.Add(7 * time.Second), Value: 11},
		{Timestamp: startTime.Add(10 * time.Second), Value: 12},
	}))

	mergeTargetData := newCheckedBytesByIDMap(newCheckedBytesByIDMapOptions{})

	expected := newCheckedBytesByIDMap(newCheckedBytesByIDMapOptions{})
	expected.Set(id0, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(0 * time.Second), Value: 0},
		{Timestamp: startTime.Add(1 * time.Second), Value: 1},
		{Timestamp: startTime.Add(2 * time.Second), Value: 2},
	}))
	expected.Set(id1, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(2 * time.Second), Value: 2},
		{Timestamp: startTime.Add(3 * time.Second), Value: 3},
		{Timestamp: startTime.Add(6 * time.Second), Value: 4},
		{Timestamp: startTime.Add(7 * time.Second), Value: 5},
		{Timestamp: startTime.Add(9 * time.Second), Value: 6},
	}))
	expected.Set(id2, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(1 * time.Second), Value: 7},
		{Timestamp: startTime.Add(3 * time.Second), Value: 8},
		{Timestamp: startTime.Add(5 * time.Second), Value: 9},
		{Timestamp: startTime.Add(6 * time.Second), Value: 10},
		{Timestamp: startTime.Add(7 * time.Second), Value: 11},
		{Timestamp: startTime.Add(10 * time.Second), Value: 12},
	}))

	testMergeWith(t, diskData, mergeTargetData, expected)
}

func TestMergeWithNoDiskData(t *testing.T) {
	// This test scenario is there is no data on disk.
	diskData := newCheckedBytesByIDMap(newCheckedBytesByIDMapOptions{})

	mergeTargetData := newCheckedBytesByIDMap(newCheckedBytesByIDMapOptions{})
	mergeTargetData.Set(id3, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(2 * time.Second), Value: 26},
		{Timestamp: startTime.Add(4 * time.Second), Value: 27},
		{Timestamp: startTime.Add(8 * time.Second), Value: 28},
	}))
	mergeTargetData.Set(id4, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(8 * time.Second), Value: 29},
	}))
	mergeTargetData.Set(id5, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(3 * time.Second), Value: 30},
		{Timestamp: startTime.Add(7 * time.Second), Value: 31},
		{Timestamp: startTime.Add(12 * time.Second), Value: 32},
		{Timestamp: startTime.Add(15 * time.Second), Value: 34},
	}))

	expected := newCheckedBytesByIDMap(newCheckedBytesByIDMapOptions{})
	expected.Set(id3, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(2 * time.Second), Value: 26},
		{Timestamp: startTime.Add(4 * time.Second), Value: 27},
		{Timestamp: startTime.Add(8 * time.Second), Value: 28},
	}))
	expected.Set(id4, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(8 * time.Second), Value: 29},
	}))
	expected.Set(id5, datapointsToCheckedBytes(t, []ts.Datapoint{
		{Timestamp: startTime.Add(3 * time.Second), Value: 30},
		{Timestamp: startTime.Add(7 * time.Second), Value: 31},
		{Timestamp: startTime.Add(12 * time.Second), Value: 32},
		{Timestamp: startTime.Add(15 * time.Second), Value: 34},
	}))

	testMergeWith(t, diskData, mergeTargetData, expected)
}

func TestMergeWithNoData(t *testing.T) {
	// This test scenario is there is no data on disk or the merge target.
	diskData := newCheckedBytesByIDMap(newCheckedBytesByIDMapOptions{})

	mergeTargetData := newCheckedBytesByIDMap(newCheckedBytesByIDMapOptions{})

	expected := newCheckedBytesByIDMap(newCheckedBytesByIDMapOptions{})

	testMergeWith(t, diskData, mergeTargetData, expected)
}

func testMergeWith(
	t *testing.T,
	diskData *checkedBytesMap,
	mergeTargetData *checkedBytesMap,
	expectedData *checkedBytesMap,
) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	reader := mockReaderFromData(ctrl, diskData)

	var persisted []persistedData
	preparer := persist.NewMockFlushPreparer(ctrl)
	preparer.EXPECT().PrepareData(gomock.Any()).Return(
		persist.PreparedDataPersist{
			Persist: func(id ident.ID, tags ident.Tags, segment ts.Segment, checksum uint32) error {
				persisted = append(persisted, persistedData{
					id: id,
					// NB(bodu): Once data is persisted the `ts.Segment` gets finalized
					// so we can't read from it anymore or that violates the read after
					// free invariant. So we `Clone` the segment here.
					segment: segment.Clone(nil),
				})
				return nil
			},
			Close: func() error { return nil },
		}, nil)
	nsCtx := namespace.Context{}

	nsOpts := namespace.NewOptions()
	merger := NewMerger(reader, 0, srPool, multiIterPool,
		identPool, encoderPool, contextPool, nsOpts)
	fsID := FileSetFileIdentifier{
		Namespace:  ident.StringID("test-ns"),
		Shard:      uint32(8),
		BlockStart: startTime,
	}
	mergeWith := mockMergeWithFromData(t, ctrl, diskData, mergeTargetData)
	err := merger.Merge(fsID, mergeWith, 1, preparer, nsCtx)
	require.NoError(t, err)

	assertPersistedAsExpected(t, persisted, expectedData)
}

func assertPersistedAsExpected(
	t *testing.T,
	persisted []persistedData,
	expectedData *checkedBytesMap,
) {
	// Assert same number of expected series IDs.
	require.Equal(t, expectedData.Len(), len(persisted))

	for _, actualData := range persisted {
		id := actualData.id
		data, exists := expectedData.Get(id)
		require.True(t, exists)
		seg := ts.NewSegment(data, nil, 0, ts.FinalizeHead)

		expectedDPs := datapointsFromSegment(t, seg)
		actualDPs := datapointsFromSegment(t, actualData.segment)
		// Assert same number of datapoints for this series.
		require.Equal(t, len(expectedDPs), len(actualDPs))
		for i := range expectedDPs {
			// Check each datapoint matches what's expected.
			assert.Equal(t, expectedDPs[i], actualDPs[i])
		}
	}
}

func datapointsToCheckedBytes(t *testing.T, dps []ts.Datapoint) checked.Bytes {
	encoder := encoderPool.Get()
	defer encoder.Close()
	for _, dp := range dps {
		encoder.Encode(dp, xtime.Second, nil)
	}

	ctx := context.NewContext()
	defer ctx.Close()

	r, ok := encoder.Stream(ctx)
	require.True(t, ok)
	var b [1000]byte
	n, err := r.Read(b[:])
	require.NoError(t, err)

	copied := append([]byte(nil), b[:n]...)
	cb := checked.NewBytes(copied, nil)
	return cb
}

func mockReaderFromData(
	ctrl *gomock.Controller,
	diskData *checkedBytesMap,
) *MockDataFileSetReader {
	reader := NewMockDataFileSetReader(ctrl)
	reader.EXPECT().Open(gomock.Any()).Return(nil)
	reader.EXPECT().Entries().Return(diskData.Len()).Times(2)
	reader.EXPECT().Close().Return(nil)
	tagIter := ident.NewTagsIterator(ident.NewTags(ident.StringTag("tag-key0", "tag-val0")))
	fakeChecksum := uint32(42)

	var inOrderCalls []*gomock.Call
	for _, val := range diskData.Iter() {
		id := val.Key()
		data := val.Value()
		inOrderCalls = append(inOrderCalls,
			reader.EXPECT().Read().Return(id, tagIter, data, fakeChecksum, nil))
	}
	// Make sure to return io.EOF at the end.
	inOrderCalls = append(inOrderCalls,
		reader.EXPECT().Read().Return(nil, nil, nil, uint32(0), io.EOF))
	gomock.InOrder(inOrderCalls...)

	return reader
}

func mockMergeWithFromData(
	t *testing.T,
	ctrl *gomock.Controller,
	diskData *checkedBytesMap,
	mergeTargetData *checkedBytesMap,
) *MockMergeWith {
	mergeWith := NewMockMergeWith(ctrl)

	// Get the series IDs in the merge target that does not exist in disk data.
	// This logic is not tested here because it should be part of tests of the
	// mergeWith implementation.
	var remaining []ident.ID

	// Expect mergeWith.Read for all data points once. Go through all data on
	// disk, then go through remaining items from merge target.
	for _, val := range diskData.Iter() {
		id := val.Key()

		if mergeTargetData.Contains(id) {
			data, ok := mergeTargetData.Get(id)
			require.True(t, ok)
			segReader := srPool.Get()
			br := []xio.BlockReader{blockReaderFromData(data, segReader, startTime, blockSize)}
			mergeWith.EXPECT().Read(gomock.Any(), id, gomock.Any(), gomock.Any()).
				Return(br, true, nil)
		} else {
			mergeWith.EXPECT().Read(gomock.Any(), id, gomock.Any(), gomock.Any()).
				Return(nil, false, nil)
		}
	}
	for _, val := range mergeTargetData.Iter() {
		id := val.Key()
		if !diskData.Contains(id) {
			// Capture remaining items so that we can call the ForEachRemaining
			// fn on them later.
			remaining = append(remaining, id)
		}
	}

	mergeWith.EXPECT().
		ForEachRemaining(gomock.Any(), xtime.ToUnixNano(startTime), gomock.Any(), gomock.Any()).
		Return(nil).
		Do(func(ctx context.Context, blockStart xtime.UnixNano, fn ForEachRemainingFn, nsCtx namespace.Context) {
			for _, id := range remaining {
				data, ok := mergeTargetData.Get(id)
				if ok {
					segReader := srPool.Get()
					br := []xio.BlockReader{blockReaderFromData(data, segReader, startTime, blockSize)}
					fn(id, ident.Tags{}, br)
				}
			}
		})

	return mergeWith
}

type persistedData struct {
	id      ident.ID
	segment ts.Segment
}

func datapointsFromSegment(t *testing.T, seg ts.Segment) []ts.Datapoint {
	segReader := srPool.Get()
	segReader.Reset(seg)
	iter := multiIterPool.Get()
	iter.Reset([]xio.SegmentReader{segReader}, startTime, blockSize, nil)
	defer iter.Close()

	var dps []ts.Datapoint
	for iter.Next() {
		dp, _, _ := iter.Current()
		dps = append(dps, dp)
	}
	require.NoError(t, iter.Err())

	return dps
}

func blockReaderFromData(
	data checked.Bytes,
	segReader xio.SegmentReader,
	startTime time.Time,
	blockSize time.Duration,
) xio.BlockReader {
	seg := ts.NewSegmentWithGeneratedChecksum(data, nil, ts.FinalizeHead)
	segReader.Reset(seg)
	return xio.BlockReader{
		SegmentReader: segReader,
		Start:         startTime,
		BlockSize:     blockSize,
	}
}
