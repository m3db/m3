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
	"testing"
	"time"

	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/network/server/tchannelthrift"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3db/digest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/tchannel-go/thrift"
)

// Create test service opts once to avoid recreating a lot of default pools, etc
var testServiceOpts = storage.NewOptions()

func TestServiceHealth(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testServiceOpts).AnyTimes()

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

func TestServiceFetch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testServiceOpts).AnyTimes()

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour)
	end := start.Add(2 * time.Hour)

	start, end = start.Truncate(time.Second), end.Truncate(time.Second)

	enc := testServiceOpts.EncoderPool().Get()
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
		ReadEncoded(ctx, ts.NewIDMatcher(nsID), ts.NewIDMatcher("foo"), start, end).
		Return([][]xio.SegmentReader{
			[]xio.SegmentReader{enc.Stream()},
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

func TestServiceFetchBatchRaw(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testServiceOpts).AnyTimes()

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
		enc := testServiceOpts.EncoderPool().Get()
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
			ReadEncoded(ctx, ts.NewIDMatcher(nsID), ts.NewIDMatcher(id), start, end).
			Return([][]xio.SegmentReader{
				[]xio.SegmentReader{enc.Stream()},
			}, nil)
	}

	ids := [][]byte{[]byte("foo"), []byte("bar")}
	r, err := service.FetchBatchRaw(tctx, &rpc.FetchBatchRawRequest{
		RangeStart: start.Unix(),
		RangeEnd:   end.Unix(),
		RangeType:  rpc.TimeType_UNIX_SECONDS,
		NameSpace:  []byte(nsID),
		Ids:        ids,
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
			expectHead = expectSegment.Head.Get()
		}
		if expectSegment.Tail != nil {
			expectTail = expectSegment.Tail.Get()
		}

		assert.Equal(t, expectHead, seg.Merged.Head)
		assert.Equal(t, expectTail, seg.Merged.Tail)
	}
}

func TestServiceFetchBlocksRaw(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testServiceOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	starts := []time.Time{start}

	nsID := "metrics"

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
		enc := testServiceOpts.EncoderPool().Get()
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

		mockDB.EXPECT().
			FetchBlocks(ctx, ts.NewIDMatcher(nsID), uint32(0), ts.NewIDMatcher(id), starts).
			Return([]block.FetchBlockResult{
				block.NewFetchBlockResult(start, []xio.SegmentReader{enc.Stream()}, nil, &checksum),
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
			expectHead = expectSegment.Head.Get()
		}
		if expectSegment.Tail != nil {
			expectTail = expectSegment.Tail.Get()
		}

		assert.Equal(t, expectHead, seg.Merged.Head)
		assert.Equal(t, expectTail, seg.Merged.Tail)
	}
}

func TestServiceFetchBlocksMetadataRaw(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testServiceOpts).AnyTimes()
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

	nsID := "metrics"

	series := map[string][]struct {
		start    time.Time
		size     int64
		checksum uint32
	}{
		"foo": {
			{start.Add(0 * time.Hour), 16, 111},
			{start.Add(2 * time.Hour), 32, 222},
		},
		"bar": {
			{start.Add(0 * time.Hour), 32, 222},
			{start.Add(2 * time.Hour), 64, 333},
		},
	}
	ids := make([][]byte, 0, len(series))
	mockResult := block.NewFetchBlocksMetadataResults()
	for id, s := range series {
		ids = append(ids, []byte(id))
		blocks := block.NewFetchBlockMetadataResults()
		metadata := block.FetchBlocksMetadataResult{
			ID:     ts.StringID(id),
			Blocks: blocks,
		}
		for _, v := range s {
			entry := v
			blocks.Add(block.FetchBlockMetadataResult{
				Start:    entry.start,
				Size:     &entry.size,
				Checksum: &entry.checksum,
				Err:      nil,
			})
		}
		mockResult.Add(metadata)
	}
	mockDB.EXPECT().
		FetchBlocksMetadata(ctx, ts.NewIDMatcher(nsID), uint32(0), start, end,
			limit, pageToken, includeSizes, includeChecksums).
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
		}
	}
}

func TestServiceWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testServiceOpts).AnyTimes()

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	nsID := "metrics"

	id := "foo"

	at := time.Now().Truncate(time.Second)
	value := 42.42

	mockDB.EXPECT().
		Write(ctx, ts.NewIDMatcher(nsID), ts.NewIDMatcher(id), at, value, xtime.Second, nil).
		Return(nil)

	err := service.Write(tctx, &rpc.WriteRequest{
		NameSpace: nsID,
		ID:        id,
		Datapoint: &rpc.Datapoint{
			Timestamp:     at.Unix(),
			TimestampType: rpc.TimeType_UNIX_SECONDS,
			Value:         value,
		},
	})
	require.NoError(t, err)
}

func TestServiceWriteBatchRaw(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testServiceOpts).AnyTimes()

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
			Write(ctx, ts.NewIDMatcher(nsID), ts.NewIDMatcher(w.id), w.t, w.v, xtime.Second, nil).
			Return(nil)
	}

	var elements []*rpc.WriteBatchRawRequestElement
	for _, w := range values {
		elem := &rpc.WriteBatchRawRequestElement{
			ID: []byte(w.id),
			Datapoint: &rpc.Datapoint{
				Timestamp:     w.t.Unix(),
				TimestampType: rpc.TimeType_UNIX_SECONDS,
				Value:         w.v,
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

func TestServiceRepair(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testServiceOpts).AnyTimes()

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
	mockDB.EXPECT().Options().Return(testServiceOpts).AnyTimes()

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	nsID := "metrics"

	truncated := int64(123)

	mockDB.EXPECT().Truncate(ts.NewIDMatcher(nsID)).Return(truncated, nil)

	r, err := service.Truncate(tctx, &rpc.TruncateRequest{NameSpace: []byte(nsID)})
	require.NoError(t, err)
	assert.Equal(t, truncated, r.NumSeries)
}
