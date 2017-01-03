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
		expectSegment := streams[string(id)].Segment()
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

	service := NewService(mockDB, nil).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	starts := []time.Time{start}

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
			FetchBlocks(ctx, ts.NewIDMatcher(nsID), uint32(0), ts.NewIDMatcher(id), starts).
			Return([]block.FetchBlockResult{
				block.NewFetchBlockResult(start, []xio.SegmentReader{enc.Stream()}, nil),
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

		seg := elem.Blocks[0].Segments
		require.NotNil(t, seg)
		require.NotNil(t, seg.Merged)

		var expectHead, expectTail []byte
		expectSegment := streams[string(id)].Segment()
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

// TODO: add test TestFetchBlocksMetadataRaw

// TODO: add test TestWrite

// TODO: add test TestWriteBatchRaw

// TODO: add test TestRepair

// TODO: add test TestTruncate
