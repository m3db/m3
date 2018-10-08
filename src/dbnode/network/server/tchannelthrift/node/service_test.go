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

package node

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/convert"
	tterrors "github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/errors"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/tchannel-go/thrift"
)

// Create opts once to avoid recreating a lot of default pools, etc
var (
	testStorageOpts = storage.NewOptions()
)

func init() {
	// Set all pool sizes to 1 for tests
	checkedBytesPoolSize = 1
	segmentArrayPoolSize = 1
	writeBatchPooledReqPoolSize = 1
}

func TestServiceHealth(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	service := NewService(mockDB, nil).(*service)

	// Assert bootstrapped false
	mockDB.EXPECT().IsBootstrapped().Return(false)

	tctx, _ := thrift.NewContext(time.Minute)
	result, err := service.Health(tctx)
	require.NoError(t, err)

	assert.Equal(t, true, result.Ok)
	assert.Equal(t, "up", result.Status)
	assert.Equal(t, false, result.Bootstrapped)

	// Assert bootstrapped true
	mockDB.EXPECT().IsBootstrapped().Return(true)

	tctx, _ = thrift.NewContext(time.Minute)
	result, err = service.Health(tctx)
	require.NoError(t, err)

	assert.Equal(t, true, result.Ok)
	assert.Equal(t, "up", result.Status)
	assert.Equal(t, true, result.Bootstrapped)
}

func TestServiceBootstrapped(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	service := NewService(mockDB, nil).(*service)

	// Should return an error when not bootstrapped
	mockDB.EXPECT().IsBootstrapped().Return(false)
	tctx, _ := thrift.NewContext(time.Minute)
	_, err := service.Bootstrapped(tctx)
	require.Error(t, err)

	// Should not return an error when bootstrapped
	mockDB.EXPECT().IsBootstrapped().Return(true)

	tctx, _ = thrift.NewContext(time.Minute)
	_, err = service.Health(tctx)
	require.NoError(t, err)
}

func TestServiceQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour)
	end := start.Add(2 * time.Hour)

	start, end = start.Truncate(time.Second), end.Truncate(time.Second)

	enc := testStorageOpts.EncoderPool().Get()
	enc.Reset(start, 0)

	nsID := "metrics"

	streams := map[string]xio.SegmentReader{}
	series := map[string][]struct {
		t time.Time
		v float64
	}{
		"foo": {
			{start.Add(10 * time.Second), 1.0},
			{start.Add(20 * time.Second), 2.0},
		},
		"bar": {
			{start.Add(20 * time.Second), 3.0},
			{start.Add(30 * time.Second), 4.0},
		},
	}
	tags := map[string][]struct {
		name  string
		value string
	}{
		"foo": {{"foo", "bar"}, {"baz", "dxk"}},
		"bar": {{"foo", "bar"}, {"dzk", "baz"}},
	}
	for id, s := range series {
		enc := testStorageOpts.EncoderPool().Get()
		enc.Reset(start, 0)
		for _, v := range s {
			dp := ts.Datapoint{
				Timestamp: v.t,
				Value:     v.v,
			}
			require.NoError(t, enc.Encode(dp, xtime.Second, nil))
		}

		streams[id] = enc.Stream()
		mockDB.EXPECT().
			ReadEncoded(ctx, ident.NewIDMatcher(nsID), ident.NewIDMatcher(id), start, end).
			Return([][]xio.BlockReader{{
				xio.BlockReader{
					SegmentReader: enc.Stream(),
				},
			}}, nil)
	}

	req, err := idx.NewRegexpQuery([]byte("foo"), []byte("b.*"))
	require.NoError(t, err)
	qry := index.Query{Query: req}

	resMap := index.NewResults(index.NewOptions())
	resMap.Reset(ident.StringID(nsID))
	resMap.Map().Set(ident.StringID("foo"), ident.NewTags(
		ident.StringTag(tags["foo"][0].name, tags["foo"][0].value),
		ident.StringTag(tags["foo"][1].name, tags["foo"][1].value),
	))
	resMap.Map().Set(ident.StringID("bar"), ident.NewTags(
		ident.StringTag(tags["bar"][0].name, tags["bar"][0].value),
		ident.StringTag(tags["bar"][1].name, tags["bar"][1].value),
	))

	mockDB.EXPECT().QueryIDs(
		ctx,
		ident.NewIDMatcher(nsID),
		index.NewQueryMatcher(qry),
		index.QueryOptions{
			StartInclusive: start,
			EndExclusive:   end,
			Limit:          10,
		}).Return(index.QueryResults{Results: resMap, Exhaustive: true}, nil)

	limit := int64(10)
	r, err := service.Query(tctx, &rpc.QueryRequest{
		Query: &rpc.Query{
			Regexp: &rpc.RegexpQuery{
				Field:  "foo",
				Regexp: "b.*",
			},
		},
		RangeStart:     start.Unix(),
		RangeEnd:       end.Unix(),
		RangeType:      rpc.TimeType_UNIX_SECONDS,
		NameSpace:      nsID,
		Limit:          &limit,
		ResultTimeType: rpc.TimeType_UNIX_SECONDS,
	})
	require.NoError(t, err)

	// sort to order results to make test deterministic.
	sort.Slice(r.Results, func(i, j int) bool {
		return r.Results[i].ID < r.Results[j].ID
	})

	ids := []string{"bar", "foo"}
	require.Equal(t, len(ids), len(r.Results))
	for i, id := range ids {
		elem := r.Results[i]
		require.NotNil(t, elem)

		require.Equal(t, elem.ID, id)
		require.Equal(t, len(tags[id]), len(elem.Tags))
		for i, tag := range elem.Tags {
			assert.Equal(t, tags[id][i].name, tag.Name)
			assert.Equal(t, tags[id][i].value, tag.Value)
		}

		require.Equal(t, len(series[id]), len(elem.Datapoints))
		for i, dp := range elem.Datapoints {
			assert.Equal(t, series[id][i].t.Unix(), dp.Timestamp)
			assert.Equal(t, series[id][i].v, dp.Value)
		}
	}
}

func TestServiceQueryOverloaded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(true)

	var (
		service = NewService(mockDB, nil).(*service)
		tctx, _ = tchannelthrift.NewContext(time.Minute)
		ctx     = tchannelthrift.Context(tctx)
		start   = time.Now().Add(-2 * time.Hour)
		end     = start.Add(2 * time.Hour)
		enc     = testStorageOpts.EncoderPool().Get()
		nsID    = "metrics"
		limit   = int64(100)
	)

	defer ctx.Close()
	start, end = start.Truncate(time.Second), end.Truncate(time.Second)
	enc.Reset(start, 0)

	_, err := service.Query(tctx, &rpc.QueryRequest{
		Query: &rpc.Query{
			Regexp: &rpc.RegexpQuery{
				Field:  "foo",
				Regexp: "b.*",
			},
		},
		RangeStart:     start.Unix(),
		RangeEnd:       end.Unix(),
		RangeType:      rpc.TimeType_UNIX_SECONDS,
		NameSpace:      nsID,
		Limit:          &limit,
		ResultTimeType: rpc.TimeType_UNIX_SECONDS,
	})
	require.Equal(t, tterrors.NewInternalError(errServerIsOverloaded), err)
}

func TestServiceFetch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour)
	end := start.Add(2 * time.Hour)

	start, end = start.Truncate(time.Second), end.Truncate(time.Second)

	enc := testStorageOpts.EncoderPool().Get()
	enc.Reset(start, 0)

	nsID := "metrics"

	values := []struct {
		t time.Time
		v float64
	}{
		{start.Add(10 * time.Second), 1.0},
		{start.Add(20 * time.Second), 2.0},
	}
	for _, v := range values {
		dp := ts.Datapoint{
			Timestamp: v.t,
			Value:     v.v,
		}
		require.NoError(t, enc.Encode(dp, xtime.Second, nil))
	}

	mockDB.EXPECT().
		ReadEncoded(ctx, ident.NewIDMatcher(nsID), ident.NewIDMatcher("foo"), start, end).
		Return([][]xio.BlockReader{
			[]xio.BlockReader{
				xio.BlockReader{
					SegmentReader: enc.Stream(),
				},
			},
		}, nil)

	r, err := service.Fetch(tctx, &rpc.FetchRequest{
		RangeStart:     start.Unix(),
		RangeEnd:       end.Unix(),
		RangeType:      rpc.TimeType_UNIX_SECONDS,
		NameSpace:      nsID,
		ID:             "foo",
		ResultTimeType: rpc.TimeType_UNIX_SECONDS,
	})
	require.NoError(t, err)

	require.Equal(t, len(values), len(r.Datapoints))
	for i, v := range values {
		assert.Equal(t, v.t, time.Unix(r.Datapoints[i].Timestamp, 0))
		assert.Equal(t, v.v, r.Datapoints[i].Value)
	}
}

func TestServiceFetchIsOverloaded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(true)

	var (
		service = NewService(mockDB, nil).(*service)
		tctx, _ = tchannelthrift.NewContext(time.Minute)
		ctx     = tchannelthrift.Context(tctx)
		start   = time.Now().Add(-2 * time.Hour)
		end     = start.Add(2 * time.Hour)
		enc     = testStorageOpts.EncoderPool().Get()
		nsID    = "metrics"
	)

	defer ctx.Close()
	start, end = start.Truncate(time.Second), end.Truncate(time.Second)
	enc.Reset(start, 0)

	_, err := service.Fetch(tctx, &rpc.FetchRequest{
		RangeStart:     start.Unix(),
		RangeEnd:       end.Unix(),
		RangeType:      rpc.TimeType_UNIX_SECONDS,
		NameSpace:      nsID,
		ID:             "foo",
		ResultTimeType: rpc.TimeType_UNIX_SECONDS,
	})
	require.Equal(t, tterrors.NewInternalError(errServerIsOverloaded), err)
}

func TestServiceFetchBatchRaw(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour)
	end := start.Add(2 * time.Hour)

	start, end = start.Truncate(time.Second), end.Truncate(time.Second)

	nsID := "metrics"

	streams := map[string]xio.SegmentReader{}
	series := map[string][]struct {
		t time.Time
		v float64
	}{
		"foo": {
			{start.Add(10 * time.Second), 1.0},
			{start.Add(20 * time.Second), 2.0},
		},
		"bar": {
			{start.Add(20 * time.Second), 3.0},
			{start.Add(30 * time.Second), 4.0},
		},
	}
	for id, s := range series {
		enc := testStorageOpts.EncoderPool().Get()
		enc.Reset(start, 0)
		for _, v := range s {
			dp := ts.Datapoint{
				Timestamp: v.t,
				Value:     v.v,
			}
			require.NoError(t, enc.Encode(dp, xtime.Second, nil))
		}

		streams[id] = enc.Stream()

		mockDB.EXPECT().
			ReadEncoded(ctx, ident.NewIDMatcher(nsID), ident.NewIDMatcher(id), start, end).
			Return([][]xio.BlockReader{
				[]xio.BlockReader{
					xio.BlockReader{
						SegmentReader: enc.Stream(),
					},
				},
			}, nil)
	}

	ids := [][]byte{[]byte("foo"), []byte("bar")}
	r, err := service.FetchBatchRaw(tctx, &rpc.FetchBatchRawRequest{
		RangeStart:    start.Unix(),
		RangeEnd:      end.Unix(),
		RangeTimeType: rpc.TimeType_UNIX_SECONDS,
		NameSpace:     []byte(nsID),
		Ids:           ids,
	})
	require.NoError(t, err)

	require.Equal(t, len(ids), len(r.Elements))
	for i, id := range ids {
		elem := r.Elements[i]
		require.NotNil(t, elem)

		assert.Nil(t, elem.Err)
		require.Equal(t, 1, len(elem.Segments))

		seg := elem.Segments[0]
		require.NotNil(t, seg)
		require.NotNil(t, seg.Merged)

		var expectHead, expectTail []byte
		expectSegment, err := streams[string(id)].Segment()
		require.NoError(t, err)

		if expectSegment.Head != nil {
			expectHead = expectSegment.Head.Bytes()
		}
		if expectSegment.Tail != nil {
			expectTail = expectSegment.Tail.Bytes()
		}

		assert.Equal(t, expectHead, seg.Merged.Head)
		assert.Equal(t, expectTail, seg.Merged.Tail)
	}
}

func TestServiceFetchBatchRawIsOverloaded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(true)

	var (
		service = NewService(mockDB, nil).(*service)
		tctx, _ = tchannelthrift.NewContext(time.Minute)
		ctx     = tchannelthrift.Context(tctx)
		start   = time.Now().Add(-2 * time.Hour)
		end     = start.Add(2 * time.Hour)
		enc     = testStorageOpts.EncoderPool().Get()
		nsID    = "metrics"
		ids     = [][]byte{[]byte("foo"), []byte("bar")}
	)

	defer ctx.Close()
	start, end = start.Truncate(time.Second), end.Truncate(time.Second)
	enc.Reset(start, 0)

	_, err := service.FetchBatchRaw(tctx, &rpc.FetchBatchRawRequest{
		RangeStart:    start.Unix(),
		RangeEnd:      end.Unix(),
		RangeTimeType: rpc.TimeType_UNIX_SECONDS,
		NameSpace:     []byte(nsID),
		Ids:           ids,
	})
	require.Equal(t, tterrors.NewInternalError(errServerIsOverloaded), err)
}

func TestServiceFetchBlocksRaw(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	nsID := "metrics"
	mockNs := storage.NewMockNamespace(ctrl)
	mockNs.EXPECT().Options().Return(namespace.NewOptions()).AnyTimes()
	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Namespace(ident.NewIDMatcher(nsID)).Return(mockNs, true).AnyTimes()
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	starts := []time.Time{start}

	streams := map[string]xio.SegmentReader{}
	checksums := map[string]uint32{}
	series := map[string][]struct {
		t time.Time
		v float64
	}{
		"foo": {
			{start.Add(10 * time.Second), 1.0},
			{start.Add(20 * time.Second), 2.0},
		},
		"bar": {
			{start.Add(20 * time.Second), 3.0},
			{start.Add(30 * time.Second), 4.0},
		},
	}
	for id, s := range series {
		enc := testStorageOpts.EncoderPool().Get()
		enc.Reset(start, 0)
		for _, v := range s {
			dp := ts.Datapoint{
				Timestamp: v.t,
				Value:     v.v,
			}
			require.NoError(t, enc.Encode(dp, xtime.Second, nil))
		}

		streams[id] = enc.Stream()

		seg, err := streams[id].Segment()
		require.NoError(t, err)

		checksum := digest.SegmentChecksum(seg)
		checksums[id] = checksum

		expectedBlockReader := []xio.BlockReader{
			xio.BlockReader{
				SegmentReader: enc.Stream(),
				Start:         start,
			},
		}

		mockDB.EXPECT().
			FetchBlocks(ctx, ident.NewIDMatcher(nsID), uint32(0), ident.NewIDMatcher(id), starts).
			Return([]block.FetchBlockResult{
				block.NewFetchBlockResult(start, expectedBlockReader, nil),
			}, nil)
	}

	ids := [][]byte{[]byte("foo"), []byte("bar")}
	r, err := service.FetchBlocksRaw(tctx, &rpc.FetchBlocksRawRequest{
		NameSpace: []byte(nsID),
		Shard:     0,
		Elements: []*rpc.FetchBlocksRawRequestElement{
			&rpc.FetchBlocksRawRequestElement{
				ID:     ids[0],
				Starts: []int64{start.UnixNano()},
			},
			&rpc.FetchBlocksRawRequestElement{
				ID:     ids[1],
				Starts: []int64{start.UnixNano()},
			},
		},
	})
	require.NoError(t, err)

	require.Equal(t, len(ids), len(r.Elements))
	for i, id := range ids {
		elem := r.Elements[i]
		require.NotNil(t, elem)
		assert.Equal(t, id, elem.ID)

		require.Equal(t, 1, len(elem.Blocks))
		require.Nil(t, elem.Blocks[0].Err)
		require.Equal(t, checksums[string(id)], uint32(*(elem.Blocks[0].Checksum)))

		seg := elem.Blocks[0].Segments
		require.NotNil(t, seg)
		require.NotNil(t, seg.Merged)

		var expectHead, expectTail []byte
		expectSegment, err := streams[string(id)].Segment()
		require.NoError(t, err)

		if expectSegment.Head != nil {
			expectHead = expectSegment.Head.Bytes()
		}
		if expectSegment.Tail != nil {
			expectTail = expectSegment.Tail.Bytes()
		}

		assert.Equal(t, expectHead, seg.Merged.Head)
		assert.Equal(t, expectTail, seg.Merged.Tail)
	}
}

func TestServiceFetchBlocksRawIsOverloaded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	nsID := "metrics"
	mockNs := storage.NewMockNamespace(ctrl)
	mockNs.EXPECT().Options().Return(namespace.NewOptions()).AnyTimes()
	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Namespace(ident.NewIDMatcher(nsID)).Return(mockNs, true).AnyTimes()
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(true)

	var (
		service = NewService(mockDB, nil).(*service)
		tctx, _ = tchannelthrift.NewContext(time.Minute)
		ctx     = tchannelthrift.Context(tctx)
		start   = time.Now().Add(-2 * time.Hour)
		end     = start.Add(2 * time.Hour)
		enc     = testStorageOpts.EncoderPool().Get()
		ids     = [][]byte{[]byte("foo"), []byte("bar")}
	)

	defer ctx.Close()
	start, end = start.Truncate(time.Second), end.Truncate(time.Second)
	enc.Reset(start, 0)

	_, err := service.FetchBlocksRaw(tctx, &rpc.FetchBlocksRawRequest{
		NameSpace: []byte(nsID),
		Shard:     0,
		Elements: []*rpc.FetchBlocksRawRequestElement{
			&rpc.FetchBlocksRawRequestElement{
				ID:     ids[0],
				Starts: []int64{start.UnixNano()},
			},
			&rpc.FetchBlocksRawRequestElement{
				ID:     ids[1],
				Starts: []int64{start.UnixNano()},
			},
		},
	})
	require.Equal(t, tterrors.NewInternalError(errServerIsOverloaded), err)
}

func TestServiceFetchBlocksMetadataRaw(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-4 * time.Hour)
	end := start.Add(4 * time.Hour)

	start, end = start.Truncate(time.Hour), end.Truncate(time.Hour)

	limit := int64(2)
	pageToken := int64(0)
	next := pageToken + limit
	nextPageToken := &next
	includeSizes := true
	includeChecksums := true
	includeLastRead := true

	nsID := "metrics"

	series := map[string][]struct {
		start    time.Time
		size     int64
		checksum uint32
		lastRead time.Time
	}{
		"foo": {
			{start.Add(0 * time.Hour), 16, 111, time.Now().Add(-time.Minute)},
			{start.Add(2 * time.Hour), 32, 222, time.Time{}},
		},
		"bar": {
			{start.Add(0 * time.Hour), 32, 222, time.Time{}},
			{start.Add(2 * time.Hour), 64, 333, time.Now().Add(-time.Minute)},
		},
	}
	ids := make([][]byte, 0, len(series))
	mockResult := block.NewFetchBlocksMetadataResults()
	for id, s := range series {
		ids = append(ids, []byte(id))
		blocks := block.NewFetchBlockMetadataResults()
		metadata := block.FetchBlocksMetadataResult{
			ID:     ident.StringID(id),
			Blocks: blocks,
		}
		for _, v := range s {
			entry := v
			blocks.Add(block.FetchBlockMetadataResult{
				Start:    entry.start,
				Size:     entry.size,
				Checksum: &entry.checksum,
				LastRead: entry.lastRead,
				Err:      nil,
			})
		}
		mockResult.Add(metadata)
	}
	opts := block.FetchBlocksMetadataOptions{
		IncludeSizes:     includeSizes,
		IncludeChecksums: includeChecksums,
		IncludeLastRead:  includeLastRead,
	}
	mockDB.EXPECT().
		FetchBlocksMetadata(ctx, ident.NewIDMatcher(nsID), uint32(0), start, end,
			limit, pageToken, opts).
		Return(mockResult, nextPageToken, nil)

	r, err := service.FetchBlocksMetadataRaw(tctx, &rpc.FetchBlocksMetadataRawRequest{
		NameSpace:        []byte(nsID),
		Shard:            0,
		RangeStart:       start.UnixNano(),
		RangeEnd:         end.UnixNano(),
		Limit:            limit,
		PageToken:        &pageToken,
		IncludeSizes:     &includeSizes,
		IncludeChecksums: &includeChecksums,
		IncludeLastRead:  &includeLastRead,
	})
	require.NoError(t, err)

	require.Equal(t, len(ids), len(r.Elements))

	for _, elem := range r.Elements {
		require.NotNil(t, elem)

		expectBlocks := series[string(elem.ID)]
		require.Equal(t, len(expectBlocks), len(elem.Blocks))

		for i, expectBlock := range expectBlocks {
			block := elem.Blocks[i]
			assert.Equal(t, expectBlock.start.UnixNano(), block.Start)
			require.NotNil(t, block.Size)
			require.NotNil(t, block.Checksum)
			require.NotNil(t, block.LastRead)
		}
	}
}

func TestServiceFetchBlocksMetadataRawIsOverloaded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(true)

	var (
		service          = NewService(mockDB, nil).(*service)
		tctx, _          = tchannelthrift.NewContext(time.Minute)
		ctx              = tchannelthrift.Context(tctx)
		start            = time.Now().Add(-4 * time.Hour)
		end              = start.Add(4 * time.Hour)
		limit            = int64(2)
		pageToken        = int64(0)
		includeSizes     = true
		includeChecksums = true
		includeLastRead  = true
		nsID             = "metrics"
	)

	defer ctx.Close()
	start, end = start.Truncate(time.Hour), end.Truncate(time.Hour)

	_, err := service.FetchBlocksMetadataRaw(tctx, &rpc.FetchBlocksMetadataRawRequest{
		NameSpace:        []byte(nsID),
		Shard:            0,
		RangeStart:       start.UnixNano(),
		RangeEnd:         end.UnixNano(),
		Limit:            limit,
		PageToken:        &pageToken,
		IncludeSizes:     &includeSizes,
		IncludeChecksums: &includeChecksums,
		IncludeLastRead:  &includeLastRead,
	})
	require.Equal(t, tterrors.NewInternalError(errServerIsOverloaded), err)
}

func TestServiceFetchBlocksMetadataEndpointV2Raw(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Setup mock db / service / context
	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)
	service := NewService(mockDB, nil).(*service)
	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	// Configure constants / options
	now := time.Now()
	var (
		start              = now.Truncate(time.Hour)
		end                = now.Add(4 * time.Hour).Truncate(time.Hour)
		limit              = int64(2)
		nextPageTokenBytes = []byte("page_next")
		includeSizes       = true
		includeChecksums   = true
		includeLastRead    = true
		nsID               = "metrics"
	)

	// Prepare test data
	type testBlock struct {
		start    time.Time
		size     int64
		checksum uint32
		lastRead time.Time
	}
	series := map[string]struct {
		tags ident.Tags
		data []testBlock
	}{
		"foo": {
			// Check with tags
			tags: ident.NewTags(
				ident.StringTag("aaa", "bbb"),
				ident.StringTag("ccc", "ddd"),
			),
			data: []testBlock{
				{start.Add(0 * time.Hour), 16, 111, time.Now().Add(-time.Minute)},
				{start.Add(2 * time.Hour), 32, 222, time.Time{}},
			},
		},
		"bar": {
			// And without tags
			tags: ident.Tags{},
			data: []testBlock{
				{start.Add(0 * time.Hour), 32, 222, time.Time{}},
				{start.Add(2 * time.Hour), 64, 333, time.Now().Add(-time.Minute)},
			},
		},
	}
	ids := make([][]byte, 0, len(series))
	mockResult := block.NewFetchBlocksMetadataResults()
	numBlocks := 0
	for id, s := range series {
		ids = append(ids, []byte(id))
		blocks := block.NewFetchBlockMetadataResults()
		metadata := block.NewFetchBlocksMetadataResult(ident.StringID(id),
			ident.NewTagsIterator(s.tags), blocks)
		for _, v := range s.data {
			numBlocks++
			entry := v
			blocks.Add(block.FetchBlockMetadataResult{
				Start:    entry.start,
				Size:     entry.size,
				Checksum: &entry.checksum,
				LastRead: entry.lastRead,
				Err:      nil,
			})
		}
		mockResult.Add(metadata)
	}

	// Setup db expectations based on test data
	opts := block.FetchBlocksMetadataOptions{
		IncludeSizes:     includeSizes,
		IncludeChecksums: includeChecksums,
		IncludeLastRead:  includeLastRead,
	}
	mockDB.EXPECT().
		FetchBlocksMetadataV2(ctx, ident.NewIDMatcher(nsID), uint32(0), start, end,
			limit, nil, opts).
		Return(mockResult, nextPageTokenBytes, nil)

	// Run RPC method
	r, err := service.FetchBlocksMetadataRawV2(tctx, &rpc.FetchBlocksMetadataRawV2Request{
		NameSpace:        []byte(nsID),
		Shard:            0,
		RangeStart:       start.UnixNano(),
		RangeEnd:         end.UnixNano(),
		Limit:            limit,
		PageToken:        nil,
		IncludeSizes:     &includeSizes,
		IncludeChecksums: &includeChecksums,
		IncludeLastRead:  &includeLastRead,
	})
	require.NoError(t, err)

	// Assert response looks OK
	require.Equal(t, numBlocks, len(r.Elements))
	require.Equal(t, nextPageTokenBytes, r.NextPageToken)

	// Assert all blocks are present
	for _, block := range r.Elements {
		require.NotNil(t, block)

		expectedBlocks := series[string(block.ID)]

		if len(expectedBlocks.tags.Values()) == 0 {
			require.Equal(t, 0, len(block.EncodedTags))
		} else {
			encodedTags := checked.NewBytes(block.EncodedTags, nil)
			decoder := service.pools.tagDecoder.Get()
			decoder.Reset(encodedTags)

			expectedTags := ident.NewTagsIterator(expectedBlocks.tags)
			require.True(t, ident.NewTagIterMatcher(expectedTags).Matches(decoder))

			decoder.Close()
		}

		foundMatch := false
		for _, expectedBlock := range expectedBlocks.data {
			if expectedBlock.start.UnixNano() != block.Start {
				continue
			}
			foundMatch = true
			require.NotNil(t, block.Size)
			require.NotNil(t, block.Checksum)
			require.NotNil(t, block.LastRead)
		}
		require.True(t, foundMatch)
	}
}

func TestServiceFetchBlocksMetadataEndpointV2RawIsOverloaded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Setup mock db / service / context
	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(true)

	// Configure constants / options
	var (
		service          = NewService(mockDB, nil).(*service)
		tctx, _          = tchannelthrift.NewContext(time.Minute)
		ctx              = tchannelthrift.Context(tctx)
		now              = time.Now()
		start            = now.Truncate(time.Hour)
		end              = now.Add(4 * time.Hour).Truncate(time.Hour)
		limit            = int64(2)
		includeSizes     = true
		includeChecksums = true
		includeLastRead  = true
		nsID             = "metrics"
	)

	defer ctx.Close()

	// Run RPC method
	_, err := service.FetchBlocksMetadataRawV2(tctx, &rpc.FetchBlocksMetadataRawV2Request{
		NameSpace:        []byte(nsID),
		Shard:            0,
		RangeStart:       start.UnixNano(),
		RangeEnd:         end.UnixNano(),
		Limit:            limit,
		PageToken:        nil,
		IncludeSizes:     &includeSizes,
		IncludeChecksums: &includeChecksums,
		IncludeLastRead:  &includeLastRead,
	})
	require.Equal(t, tterrors.NewInternalError(errServerIsOverloaded), err)
}

func TestServiceFetchTagged(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour)
	end := start.Add(2 * time.Hour)

	start, end = start.Truncate(time.Second), end.Truncate(time.Second)

	nsID := "metrics"

	streams := map[string]xio.SegmentReader{}
	series := map[string][]struct {
		t time.Time
		v float64
	}{
		"foo": {
			{start.Add(10 * time.Second), 1.0},
			{start.Add(20 * time.Second), 2.0},
		},
		"bar": {
			{start.Add(20 * time.Second), 3.0},
			{start.Add(30 * time.Second), 4.0},
		},
	}
	for id, s := range series {
		enc := testStorageOpts.EncoderPool().Get()
		enc.Reset(start, 0)
		for _, v := range s {
			dp := ts.Datapoint{
				Timestamp: v.t,
				Value:     v.v,
			}
			require.NoError(t, enc.Encode(dp, xtime.Second, nil))
		}

		streams[id] = enc.Stream()
		mockDB.EXPECT().
			ReadEncoded(ctx, ident.NewIDMatcher(nsID), ident.NewIDMatcher(id), start, end).
			Return([][]xio.BlockReader{{
				xio.BlockReader{
					SegmentReader: enc.Stream(),
				},
			}}, nil)
	}

	req, err := idx.NewRegexpQuery([]byte("foo"), []byte("b.*"))
	require.NoError(t, err)
	qry := index.Query{Query: req}

	resMap := index.NewResults(index.NewOptions())
	resMap.Reset(ident.StringID(nsID))
	resMap.Map().Set(ident.StringID("foo"), ident.NewTags(
		ident.StringTag("foo", "bar"),
		ident.StringTag("baz", "dxk"),
	))
	resMap.Map().Set(ident.StringID("bar"), ident.NewTags(
		ident.StringTag("foo", "bar"),
		ident.StringTag("dzk", "baz"),
	))

	mockDB.EXPECT().QueryIDs(
		ctx,
		ident.NewIDMatcher(nsID),
		index.NewQueryMatcher(qry),
		index.QueryOptions{
			StartInclusive: start,
			EndExclusive:   end,
			Limit:          10,
		}).Return(index.QueryResults{Results: resMap, Exhaustive: true}, nil)

	startNanos, err := convert.ToValue(start, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)
	endNanos, err := convert.ToValue(end, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)
	var limit int64 = 10
	data, err := idx.Marshal(req)
	require.NoError(t, err)
	r, err := service.FetchTagged(tctx, &rpc.FetchTaggedRequest{
		NameSpace:  []byte(nsID),
		Query:      data,
		RangeStart: startNanos,
		RangeEnd:   endNanos,
		FetchData:  true,
		Limit:      &limit,
	})
	require.NoError(t, err)

	// sort to order results to make test deterministic.
	sort.Slice(r.Elements, func(i, j int) bool {
		return bytes.Compare(r.Elements[i].ID, r.Elements[j].ID) < 0
	})
	ids := [][]byte{[]byte("bar"), []byte("foo")}
	require.Equal(t, len(ids), len(r.Elements))
	for i, id := range ids {
		elem := r.Elements[i]
		require.NotNil(t, elem)

		assert.Nil(t, elem.Err)
		require.Equal(t, 1, len(elem.Segments))

		seg := elem.Segments[0]
		require.NotNil(t, seg)
		require.NotNil(t, seg.Merged)

		var expectHead, expectTail []byte
		expectSegment, err := streams[string(id)].Segment()
		require.NoError(t, err)

		if expectSegment.Head != nil {
			expectHead = expectSegment.Head.Bytes()
		}
		if expectSegment.Tail != nil {
			expectTail = expectSegment.Tail.Bytes()
		}

		assert.Equal(t, expectHead, seg.Merged.Head)
		assert.Equal(t, expectTail, seg.Merged.Tail)
	}
}

func TestServiceFetchTaggedIsOverloaded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(true)

	var (
		service = NewService(mockDB, nil).(*service)

		tctx, _ = tchannelthrift.NewContext(time.Minute)
		ctx     = tchannelthrift.Context(tctx)

		start = time.Now().Add(-2 * time.Hour)
		end   = start.Add(2 * time.Hour)

		nsID = "metrics"
	)

	defer ctx.Close()
	start, end = start.Truncate(time.Second), end.Truncate(time.Second)

	req, err := idx.NewRegexpQuery([]byte("foo"), []byte("b.*"))
	require.NoError(t, err)

	resMap := index.NewResults(index.NewOptions())
	resMap.Reset(ident.StringID(nsID))
	resMap.Map().Set(ident.StringID("foo"), ident.NewTags(
		ident.StringTag("foo", "bar"),
		ident.StringTag("baz", "dxk"),
	))
	resMap.Map().Set(ident.StringID("bar"), ident.NewTags(
		ident.StringTag("foo", "bar"),
		ident.StringTag("dzk", "baz"),
	))

	startNanos, err := convert.ToValue(start, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)
	endNanos, err := convert.ToValue(end, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)
	var limit int64 = 10
	data, err := idx.Marshal(req)
	require.NoError(t, err)
	_, err = service.FetchTagged(tctx, &rpc.FetchTaggedRequest{
		NameSpace:  []byte(nsID),
		Query:      data,
		RangeStart: startNanos,
		RangeEnd:   endNanos,
		FetchData:  true,
		Limit:      &limit,
	})
	require.Equal(t, tterrors.NewInternalError(errServerIsOverloaded), err)
}

func TestServiceFetchTaggedNoData(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour)
	end := start.Add(2 * time.Hour)

	start, end = start.Truncate(time.Second), end.Truncate(time.Second)
	nsID := "metrics"

	req, err := idx.NewRegexpQuery([]byte("foo"), []byte("b.*"))
	require.NoError(t, err)
	qry := index.Query{Query: req}

	resMap := index.NewResults(index.NewOptions())
	resMap.Reset(ident.StringID(nsID))
	resMap.Map().Set(ident.StringID("foo"), ident.Tags{})
	resMap.Map().Set(ident.StringID("bar"), ident.Tags{})
	mockDB.EXPECT().QueryIDs(
		ctx,
		ident.NewIDMatcher(nsID),
		index.NewQueryMatcher(qry),
		index.QueryOptions{
			StartInclusive: start,
			EndExclusive:   end,
			Limit:          10,
		}).Return(index.QueryResults{Results: resMap, Exhaustive: true}, nil)

	startNanos, err := convert.ToValue(start, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)
	endNanos, err := convert.ToValue(end, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)
	var limit int64 = 10
	data, err := idx.Marshal(req)
	require.NoError(t, err)
	r, err := service.FetchTagged(tctx, &rpc.FetchTaggedRequest{
		NameSpace:  []byte(nsID),
		Query:      data,
		RangeStart: startNanos,
		RangeEnd:   endNanos,
		FetchData:  false,
		Limit:      &limit,
	})
	require.NoError(t, err)

	// sort to order results to make test deterministic.
	sort.Slice(r.Elements, func(i, j int) bool {
		return bytes.Compare(r.Elements[i].ID, r.Elements[j].ID) < 0
	})
	ids := [][]byte{[]byte("bar"), []byte("foo")}
	require.Equal(t, len(ids), len(r.Elements))
	for i, id := range ids {
		elem := r.Elements[i]
		require.NotNil(t, elem)
		require.Nil(t, elem.Err)
		require.Equal(t, id, elem.ID)
	}
}

func TestServiceFetchTaggedErrs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour)
	end := start.Add(2 * time.Hour)

	start, end = start.Truncate(time.Second), end.Truncate(time.Second)
	nsID := "metrics"

	startNanos, err := convert.ToValue(start, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)
	endNanos, err := convert.ToValue(end, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)
	var limit int64 = 10

	req, err := idx.NewRegexpQuery([]byte("foo"), []byte("b.*"))
	require.NoError(t, err)
	data, err := idx.Marshal(req)
	require.NoError(t, err)
	qry := index.Query{Query: req}

	mockDB.EXPECT().QueryIDs(
		ctx,
		ident.NewIDMatcher(nsID),
		index.NewQueryMatcher(qry),
		index.QueryOptions{
			StartInclusive: start,
			EndExclusive:   end,
			Limit:          10,
		}).Return(index.QueryResults{}, fmt.Errorf("random err"))
	_, err = service.FetchTagged(tctx, &rpc.FetchTaggedRequest{
		NameSpace:  []byte(nsID),
		Query:      data,
		RangeStart: startNanos,
		RangeEnd:   endNanos,
		FetchData:  false,
		Limit:      &limit,
	})
	require.Error(t, err)
}

func TestServiceWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	nsID := "metrics"

	id := "foo"

	at := time.Now().Truncate(time.Second)
	value := 42.42

	mockDB.EXPECT().
		Write(ctx, ident.NewIDMatcher(nsID), ident.NewIDMatcher(id), at, value, xtime.Second, nil).
		Return(nil)

	err := service.Write(tctx, &rpc.WriteRequest{
		NameSpace: nsID,
		ID:        id,
		Datapoint: &rpc.Datapoint{
			Timestamp:         at.Unix(),
			TimestampTimeType: rpc.TimeType_UNIX_SECONDS,
			Value:             value,
		},
	})
	require.NoError(t, err)
}

func TestServiceWriteTagged(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	var (
		nsID      = "metrics"
		id        = "foo"
		tagNames  = []string{"foo", "bar", "baz"}
		tagValues = []string{"cmon", "keep", "going"}
		at        = time.Now().Truncate(time.Second)
		value     = 42.42
	)

	mockDB.EXPECT().WriteTagged(ctx,
		ident.NewIDMatcher(nsID),
		ident.NewIDMatcher(id),
		gomock.Any(),
		at, value, xtime.Second, nil,
	).Return(nil)

	request := &rpc.WriteTaggedRequest{
		NameSpace: nsID,
		ID:        id,
		Datapoint: &rpc.Datapoint{
			Timestamp:         at.Unix(),
			TimestampTimeType: rpc.TimeType_UNIX_SECONDS,
			Value:             value,
		},
		Tags: []*rpc.Tag{},
	}

	for i := range tagNames {
		request.Tags = append(request.Tags, &rpc.Tag{
			Name:  tagNames[i],
			Value: tagValues[i],
		})
	}
	err := service.WriteTagged(tctx, request)
	require.NoError(t, err)
}

func TestServiceWriteBatchRaw(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	nsID := "metrics"

	values := []struct {
		id string
		t  time.Time
		v  float64
	}{
		{"foo", time.Now().Truncate(time.Second), 12.34},
		{"bar", time.Now().Truncate(time.Second), 42.42},
	}
	for _, w := range values {
		mockDB.EXPECT().
			Write(ctx, ident.NewIDMatcher(nsID), ident.NewIDMatcher(w.id), w.t, w.v, xtime.Second, nil).
			Return(nil)
	}

	var elements []*rpc.WriteBatchRawRequestElement
	for _, w := range values {
		elem := &rpc.WriteBatchRawRequestElement{
			ID: []byte(w.id),
			Datapoint: &rpc.Datapoint{
				Timestamp:         w.t.Unix(),
				TimestampTimeType: rpc.TimeType_UNIX_SECONDS,
				Value:             w.v,
			},
		}
		elements = append(elements, elem)
	}

	err := service.WriteBatchRaw(tctx, &rpc.WriteBatchRawRequest{
		NameSpace: []byte(nsID),
		Elements:  elements,
	})
	require.NoError(t, err)
}

func TestServiceWriteTaggedBatchRaw(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	mockDecoder := serialize.NewMockTagDecoder(ctrl)
	mockDecoder.EXPECT().Reset(gomock.Any()).AnyTimes()
	mockDecoder.EXPECT().Err().Return(nil).AnyTimes()
	mockDecoder.EXPECT().Close().AnyTimes()
	mockDecoderPool := serialize.NewMockTagDecoderPool(ctrl)
	mockDecoderPool.EXPECT().Get().Return(mockDecoder).AnyTimes()

	opts := tchannelthrift.NewOptions().
		SetTagDecoderPool(mockDecoderPool)

	service := NewService(mockDB, opts).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	nsID := "metrics"

	values := []struct {
		id        string
		tagEncode string
		t         time.Time
		v         float64
	}{
		{"foo", "a|b", time.Now().Truncate(time.Second), 12.34},
		{"bar", "c|dd", time.Now().Truncate(time.Second), 42.42},
	}
	for _, w := range values {
		mockDB.EXPECT().
			WriteTagged(ctx, ident.NewIDMatcher(nsID), ident.NewIDMatcher(w.id),
				mockDecoder,
				w.t, w.v, xtime.Second, nil).
			Return(nil)
	}

	var elements []*rpc.WriteTaggedBatchRawRequestElement
	for _, w := range values {
		elem := &rpc.WriteTaggedBatchRawRequestElement{
			ID:          []byte(w.id),
			EncodedTags: []byte(w.tagEncode),
			Datapoint: &rpc.Datapoint{
				Timestamp:         w.t.Unix(),
				TimestampTimeType: rpc.TimeType_UNIX_SECONDS,
				Value:             w.v,
			},
		}
		elements = append(elements, elem)
	}

	err := service.WriteTaggedBatchRaw(tctx, &rpc.WriteTaggedBatchRawRequest{
		NameSpace: []byte(nsID),
		Elements:  elements,
	})
	require.NoError(t, err)
}
func TestServiceRepair(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	mockDB.EXPECT().Repair().Return(nil)

	err := service.Repair(tctx)
	require.NoError(t, err)
}

func TestServiceTruncate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	nsID := "metrics"

	truncated := int64(123)

	mockDB.EXPECT().Truncate(ident.NewIDMatcher(nsID)).Return(truncated, nil)

	r, err := service.Truncate(tctx, &rpc.TruncateRequest{NameSpace: []byte(nsID)})
	require.NoError(t, err)
	assert.Equal(t, truncated, r.NumSeries)
}

func TestServiceSetPersistRateLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	runtimeOpts := runtime.NewOptions()
	runtimeOpts = runtimeOpts.SetPersistRateLimitOptions(
		runtimeOpts.PersistRateLimitOptions().
			SetLimitEnabled(false))
	runtimeOptsMgr := runtime.NewOptionsManager()
	require.NoError(t, runtimeOptsMgr.Update(runtimeOpts))
	opts := testStorageOpts.SetRuntimeOptionsManager(runtimeOptsMgr)

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(opts).AnyTimes()

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	getResp, err := service.GetPersistRateLimit(tctx)
	require.NoError(t, err)
	assert.Equal(t, false, getResp.LimitEnabled)

	enable := true
	req := &rpc.NodeSetPersistRateLimitRequest{
		LimitEnabled: &enable,
	}
	setResp, err := service.SetPersistRateLimit(tctx, req)
	require.NoError(t, err)
	assert.Equal(t, true, setResp.LimitEnabled)
}

func TestServiceSetWriteNewSeriesAsync(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	runtimeOpts := runtime.NewOptions().
		SetWriteNewSeriesAsync(false)
	runtimeOptsMgr := runtime.NewOptionsManager()
	require.NoError(t, runtimeOptsMgr.Update(runtimeOpts))
	opts := testStorageOpts.SetRuntimeOptionsManager(runtimeOptsMgr)

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(opts).AnyTimes()

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	getResp, err := service.GetWriteNewSeriesAsync(tctx)
	require.NoError(t, err)
	assert.Equal(t, false, getResp.WriteNewSeriesAsync)

	req := &rpc.NodeSetWriteNewSeriesAsyncRequest{
		WriteNewSeriesAsync: true,
	}
	setResp, err := service.SetWriteNewSeriesAsync(tctx, req)
	require.NoError(t, err)
	assert.Equal(t, true, setResp.WriteNewSeriesAsync)
}

func TestServiceSetWriteNewSeriesBackoffDuration(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	runtimeOpts := runtime.NewOptions().
		SetWriteNewSeriesBackoffDuration(3 * time.Millisecond)
	runtimeOptsMgr := runtime.NewOptionsManager()
	require.NoError(t, runtimeOptsMgr.Update(runtimeOpts))
	opts := testStorageOpts.SetRuntimeOptionsManager(runtimeOptsMgr)

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(opts).AnyTimes()

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	getResp, err := service.GetWriteNewSeriesBackoffDuration(tctx)
	require.NoError(t, err)
	assert.Equal(t, int64(3), getResp.WriteNewSeriesBackoffDuration)
	assert.Equal(t, rpc.TimeType_UNIX_MILLISECONDS, getResp.DurationType)

	req := &rpc.NodeSetWriteNewSeriesBackoffDurationRequest{
		WriteNewSeriesBackoffDuration: 1,
		DurationType:                  rpc.TimeType_UNIX_SECONDS,
	}
	setResp, err := service.SetWriteNewSeriesBackoffDuration(tctx, req)
	require.NoError(t, err)
	assert.Equal(t, int64(1000), setResp.WriteNewSeriesBackoffDuration)
	assert.Equal(t, rpc.TimeType_UNIX_MILLISECONDS, setResp.DurationType)
}

func TestServiceSetWriteNewSeriesLimitPerShardPerSecond(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	runtimeOpts := runtime.NewOptions().
		SetWriteNewSeriesLimitPerShardPerSecond(42)
	runtimeOptsMgr := runtime.NewOptionsManager()
	require.NoError(t, runtimeOptsMgr.Update(runtimeOpts))
	opts := testStorageOpts.SetRuntimeOptionsManager(runtimeOptsMgr)

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(opts).AnyTimes()

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	getResp, err := service.GetWriteNewSeriesLimitPerShardPerSecond(tctx)
	require.NoError(t, err)
	assert.Equal(t, int64(42), getResp.WriteNewSeriesLimitPerShardPerSecond)

	req := &rpc.NodeSetWriteNewSeriesLimitPerShardPerSecondRequest{
		WriteNewSeriesLimitPerShardPerSecond: 84,
	}
	setResp, err := service.SetWriteNewSeriesLimitPerShardPerSecond(tctx, req)
	require.NoError(t, err)
	assert.Equal(t, int64(84), setResp.WriteNewSeriesLimitPerShardPerSecond)
}
