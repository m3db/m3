// Copyright (c) 2017 Uber Technologies, Inc.
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

package peers

import (
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testNamespace          = ts.StringID("testnamespace")
	testDefaultRunOpts     = bootstrap.NewRunOptions().SetIncremental(false)
	testIncrementalRunOpts = bootstrap.NewRunOptions().SetIncremental(true)
	testBlockOpts          = block.NewOptions()
)

func TestPeersSourceCan(t *testing.T) {
	src := newPeersSource(NewOptions())

	assert.True(t, src.Can(bootstrap.BootstrapSequential))
	assert.False(t, src.Can(bootstrap.BootstrapParallel))
}

func TestPeersSourceEmptyShardTimeRanges(t *testing.T) {
	src := newPeersSource(NewOptions())

	target := result.ShardTimeRanges{}
	available := src.Available(testNamespace, target)
	assert.Equal(t, target, available)

	r, err := src.Read(testNamespace, target, testDefaultRunOpts)
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestPeersSourceReturnsErrorForAdminSession(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := NewOptions()
	ropts := opts.ResultOptions().RetentionOptions()

	expectedErr := fmt.Errorf("an error")

	mockAdminClient := client.NewMockAdminClient(ctrl)
	mockAdminClient.EXPECT().DefaultAdminSession().Return(nil, expectedErr)

	opts = opts.SetAdminClient(mockAdminClient)

	src := newPeersSource(opts)

	start := time.Now().Add(-ropts.RetentionPeriod()).Truncate(ropts.BlockSize())
	end := start.Add(ropts.BlockSize())

	target := result.ShardTimeRanges{
		0: xtime.NewRanges().AddRange(xtime.Range{Start: start, End: end}),
		1: xtime.NewRanges().AddRange(xtime.Range{Start: start, End: end}),
	}

	_, err := src.Read(testNamespace, target, testDefaultRunOpts)
	require.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

func TestPeersSourceReturnsFulfilledAndUnfulfilled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := NewOptions()
	ropts := opts.ResultOptions().RetentionOptions()

	start := time.Now().Add(-ropts.RetentionPeriod()).Truncate(ropts.BlockSize())
	end := start.Add(ropts.BlockSize())

	goodResult := result.NewShardResult(0, opts.ResultOptions())
	fooBlock := block.NewDatabaseBlock(start, ts.Segment{}, testBlockOpts)
	goodResult.AddBlock(ts.StringID("foo"), fooBlock)
	badErr := fmt.Errorf("an error")

	mockAdminSession := client.NewMockAdminSession(ctrl)
	mockAdminSession.EXPECT().
		FetchBootstrapBlocksFromPeers(ts.NewIDMatcher(testNamespace.String()),
			uint32(0), start, end, gomock.Any()).
		Return(goodResult, nil)
	mockAdminSession.EXPECT().
		FetchBootstrapBlocksFromPeers(ts.NewIDMatcher(testNamespace.String()),
			uint32(1), start, end, gomock.Any()).
		Return(nil, badErr)

	mockAdminClient := client.NewMockAdminClient(ctrl)
	mockAdminClient.EXPECT().DefaultAdminSession().Return(mockAdminSession, nil)

	opts = opts.SetAdminClient(mockAdminClient)

	src := newPeersSource(opts)

	target := result.ShardTimeRanges{
		0: xtime.NewRanges().AddRange(xtime.Range{Start: start, End: end}),
		1: xtime.NewRanges().AddRange(xtime.Range{Start: start, End: end}),
	}

	r, err := src.Read(testNamespace, target, testDefaultRunOpts)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(r.ShardResults()))
	require.NotNil(t, r.ShardResults()[0])
	require.Nil(t, r.ShardResults()[1])

	require.Nil(t, r.Unfulfilled()[0])
	require.NotNil(t, r.Unfulfilled()[1])
	require.Equal(t, 1, r.Unfulfilled()[1].Len())

	block, ok := r.ShardResults()[0].BlockAt(ts.StringID("foo"), start)
	require.True(t, ok)
	require.Equal(t, fooBlock, block)

	rangeIter := r.Unfulfilled()[1].Iter()
	require.True(t, rangeIter.Next())
	require.Equal(t, xtime.Range{Start: start, End: end}, rangeIter.Value())
}

func TestPeersSourceIncrementalRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := NewOptions()
	ropts := opts.ResultOptions().RetentionOptions()

	start := time.Now().Add(-ropts.RetentionPeriod()).Truncate(ropts.BlockSize())
	end := start.Add(2 * ropts.BlockSize())

	firstResult := result.NewShardResult(0, opts.ResultOptions())
	fooBlock := block.NewDatabaseBlock(start,
		ts.NewSegment(checked.NewBytes([]byte{1, 2, 3}, nil), nil, ts.FinalizeNone),
		testBlockOpts)
	barBlock := block.NewDatabaseBlock(start.Add(ropts.BlockSize()),
		ts.NewSegment(checked.NewBytes([]byte{4, 5, 6}, nil), nil, ts.FinalizeNone),
		testBlockOpts)
	firstResult.AddBlock(ts.StringID("foo"), fooBlock)
	firstResult.AddBlock(ts.StringID("bar"), barBlock)

	secondResult := result.NewShardResult(0, opts.ResultOptions())
	bazBlock := block.NewDatabaseBlock(start,
		ts.NewSegment(checked.NewBytes([]byte{7, 8, 9}, nil), nil, ts.FinalizeNone),
		testBlockOpts)
	secondResult.AddBlock(ts.StringID("baz"), bazBlock)

	mockAdminSession := client.NewMockAdminSession(ctrl)
	mockAdminSession.EXPECT().
		FetchBootstrapBlocksFromPeers(ts.NewIDMatcher(testNamespace.String()),
			uint32(0), start, end, gomock.Any()).
		Return(firstResult, nil)
	mockAdminSession.EXPECT().
		FetchBootstrapBlocksFromPeers(ts.NewIDMatcher(testNamespace.String()),
			uint32(1), start, end, gomock.Any()).
		Return(secondResult, nil)

	mockAdminClient := client.NewMockAdminClient(ctrl)
	mockAdminClient.EXPECT().DefaultAdminSession().Return(mockAdminSession, nil)

	opts = opts.SetAdminClient(mockAdminClient)

	mockRetriever := block.NewMockDatabaseBlockRetriever(ctrl)
	// The shard indices are computed from iterating over a map, they can
	// come in any order
	mockRetriever.EXPECT().CacheShardIndices([]uint32{0, 1}).AnyTimes()
	mockRetriever.EXPECT().CacheShardIndices([]uint32{1, 0}).AnyTimes()

	mockRetrieverMgr := block.NewMockDatabaseBlockRetrieverManager(ctrl)
	mockRetrieverMgr.EXPECT().
		Retriever(ts.NewIDMatcher(testNamespace.String())).
		Return(mockRetriever, nil)

	opts = opts.SetDatabaseBlockRetrieverManager(mockRetrieverMgr)

	mockFlush := persist.NewMockFlush(ctrl)
	mockFlush.EXPECT().Done()
	persists := make(map[string]int)
	closes := make(map[string]int)
	mockFlush.EXPECT().
		Prepare(ts.NewIDMatcher(testNamespace.String()), uint32(0), start).
		Return(persist.PreparedPersist{
			Persist: func(id ts.ID, segment ts.Segment, checksum uint32) error {
				persists["foo"]++
				assert.Equal(t, "foo", id.String())
				assert.Equal(t, []byte{1, 2, 3}, segment.Head.Get())
				assert.Equal(t, fooBlock.Checksum(), checksum)
				return nil
			},
			Close: func() error {
				closes["foo"]++
				return nil
			},
		}, nil)
	mockFlush.EXPECT().
		Prepare(ts.NewIDMatcher(testNamespace.String()), uint32(0), start.Add(ropts.BlockSize())).
		Return(persist.PreparedPersist{
			Persist: func(id ts.ID, segment ts.Segment, checksum uint32) error {
				persists["bar"]++
				assert.Equal(t, "bar", id.String())
				assert.Equal(t, []byte{4, 5, 6}, segment.Head.Get())
				assert.Equal(t, barBlock.Checksum(), checksum)
				return nil
			},
			Close: func() error {
				closes["bar"]++
				return nil
			},
		}, nil)
	mockFlush.EXPECT().
		Prepare(ts.NewIDMatcher(testNamespace.String()), uint32(1), start).
		Return(persist.PreparedPersist{
			Persist: func(id ts.ID, segment ts.Segment, checksum uint32) error {
				persists["baz"]++
				assert.Equal(t, "baz", id.String())
				assert.Equal(t, []byte{7, 8, 9}, segment.Head.Get())
				assert.Equal(t, bazBlock.Checksum(), checksum)
				return nil
			},
			Close: func() error {
				closes["baz"]++
				return nil
			},
		}, nil)
	mockFlush.EXPECT().
		Prepare(ts.NewIDMatcher(testNamespace.String()), uint32(1), start.Add(ropts.BlockSize())).
		Return(persist.PreparedPersist{
			Persist: func(id ts.ID, segment ts.Segment, checksum uint32) error {
				assert.Fail(t, "no expected shard 1 second block")
				return nil
			},
			Close: func() error {
				closes["empty"]++
				return nil
			},
		}, nil)

	mockPersistManager := persist.NewMockManager(ctrl)
	mockPersistManager.EXPECT().StartFlush().Return(mockFlush, nil)

	opts = opts.SetPersistManager(mockPersistManager)

	src := newPeersSource(opts)

	target := result.ShardTimeRanges{
		0: xtime.NewRanges().AddRange(xtime.Range{Start: start, End: end}),
		1: xtime.NewRanges().AddRange(xtime.Range{Start: start, End: end}),
	}

	r, err := src.Read(testNamespace, target, testIncrementalRunOpts)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(r.ShardResults()))
	require.NotNil(t, r.ShardResults()[0])
	require.NotNil(t, r.ShardResults()[1])

	require.Nil(t, r.Unfulfilled()[0])
	require.Nil(t, r.Unfulfilled()[1])

	block, ok := r.ShardResults()[0].BlockAt(ts.StringID("foo"), start)
	require.True(t, ok)
	assert.Equal(t, fooBlock, block)
	assert.False(t, fooBlock.IsRetrieved())

	block, ok = r.ShardResults()[0].BlockAt(ts.StringID("bar"), start.Add(ropts.BlockSize()))
	require.True(t, ok)
	assert.Equal(t, barBlock, block)
	assert.False(t, barBlock.IsRetrieved())

	block, ok = r.ShardResults()[1].BlockAt(ts.StringID("baz"), start)
	require.True(t, ok)
	assert.Equal(t, bazBlock, block)
	assert.False(t, bazBlock.IsRetrieved())

	assert.Equal(t, map[string]int{
		"foo": 1, "bar": 1, "baz": 1,
	}, persists)

	assert.Equal(t, map[string]int{
		"foo": 1, "bar": 1, "baz": 1, "empty": 1,
	}, closes)
}

func TestPeersSourceContinuesOnIncrementalFlushErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := NewOptions()
	ropts := opts.ResultOptions().RetentionOptions()

	start := time.Now().Add(-ropts.RetentionPeriod()).Truncate(ropts.BlockSize())
	end := start.Add(ropts.BlockSize())

	fooBlock := block.NewMockDatabaseBlock(ctrl)
	fooBlock.EXPECT().StartTime().Return(start).AnyTimes()
	fooBlock.EXPECT().Stream(gomock.Any()).Return(nil, fmt.Errorf("stream err"))

	firstResult := result.NewShardResult(0, opts.ResultOptions())
	firstResult.AddBlock(ts.StringID("foo"), fooBlock)

	mockStream := xio.NewMockSegmentReader(ctrl)
	mockStream.EXPECT().Segment().Return(ts.Segment{}, fmt.Errorf("segment err"))

	barBlock := block.NewMockDatabaseBlock(ctrl)
	barBlock.EXPECT().StartTime().Return(start)
	barBlock.EXPECT().Stream(gomock.Any()).Return(mockStream, nil)

	secondResult := result.NewShardResult(0, opts.ResultOptions())
	secondResult.AddBlock(ts.StringID("bar"), barBlock)

	bazBlock := block.NewDatabaseBlock(start,
		ts.NewSegment(checked.NewBytes([]byte{1, 2, 3}, nil), nil, ts.FinalizeNone),
		testBlockOpts)

	thirdResult := result.NewShardResult(0, opts.ResultOptions())
	thirdResult.AddBlock(ts.StringID("baz"), bazBlock)

	quxBlock := block.NewDatabaseBlock(start,
		ts.NewSegment(checked.NewBytes([]byte{4, 5, 6}, nil), nil, ts.FinalizeNone),
		testBlockOpts)

	fourthResult := result.NewShardResult(0, opts.ResultOptions())
	fourthResult.AddBlock(ts.StringID("qux"), quxBlock)

	mockAdminSession := client.NewMockAdminSession(ctrl)
	mockAdminSession.EXPECT().
		FetchBootstrapBlocksFromPeers(ts.NewIDMatcher(testNamespace.String()),
			uint32(0), start, end, gomock.Any()).
		Return(firstResult, nil)
	mockAdminSession.EXPECT().
		FetchBootstrapBlocksFromPeers(ts.NewIDMatcher(testNamespace.String()),
			uint32(1), start, end, gomock.Any()).
		Return(secondResult, nil)
	mockAdminSession.EXPECT().
		FetchBootstrapBlocksFromPeers(ts.NewIDMatcher(testNamespace.String()),
			uint32(2), start, end, gomock.Any()).
		Return(thirdResult, nil)
	mockAdminSession.EXPECT().
		FetchBootstrapBlocksFromPeers(ts.NewIDMatcher(testNamespace.String()),
			uint32(3), start, end, gomock.Any()).
		Return(fourthResult, nil)

	mockAdminClient := client.NewMockAdminClient(ctrl)
	mockAdminClient.EXPECT().DefaultAdminSession().Return(mockAdminSession, nil)

	opts = opts.SetAdminClient(mockAdminClient)

	mockRetriever := block.NewMockDatabaseBlockRetriever(ctrl)
	mockRetriever.EXPECT().CacheShardIndices(gomock.Any()).AnyTimes()

	mockRetrieverMgr := block.NewMockDatabaseBlockRetrieverManager(ctrl)
	mockRetrieverMgr.EXPECT().
		Retriever(ts.NewIDMatcher(testNamespace.String())).
		Return(mockRetriever, nil)

	opts = opts.SetDatabaseBlockRetrieverManager(mockRetrieverMgr)

	mockFlush := persist.NewMockFlush(ctrl)
	mockFlush.EXPECT().Done()
	persists := make(map[string]int)
	closes := make(map[string]int)
	mockFlush.EXPECT().
		Prepare(ts.NewIDMatcher(testNamespace.String()), uint32(0), start).
		Return(persist.PreparedPersist{
			Persist: func(id ts.ID, segment ts.Segment, checksum uint32) error {
				assert.Fail(t, "not expecting to flush shard 0 at start")
				return nil
			},
			Close: func() error {
				closes["foo"]++
				return nil
			},
		}, nil)
	mockFlush.EXPECT().
		Prepare(ts.NewIDMatcher(testNamespace.String()), uint32(1), start).
		Return(persist.PreparedPersist{
			Persist: func(id ts.ID, segment ts.Segment, checksum uint32) error {
				assert.Fail(t, "not expecting to flush shard 0 at start + block size")
				return nil
			},
			Close: func() error {
				closes["bar"]++
				return nil
			},
		}, nil)
	mockFlush.EXPECT().
		Prepare(ts.NewIDMatcher(testNamespace.String()), uint32(2), start).
		Return(persist.PreparedPersist{
			Persist: func(id ts.ID, segment ts.Segment, checksum uint32) error {
				persists["baz"]++
				return fmt.Errorf("a persist error")
			},
			Close: func() error {
				closes["baz"]++
				return nil
			},
		}, nil)
	mockFlush.EXPECT().
		Prepare(ts.NewIDMatcher(testNamespace.String()), uint32(3), start).
		Return(persist.PreparedPersist{
			Persist: func(id ts.ID, segment ts.Segment, checksum uint32) error {
				persists["qux"]++
				return nil
			},
			Close: func() error {
				closes["qux"]++
				return fmt.Errorf("a persist close error")
			},
		}, nil)

	mockPersistManager := persist.NewMockManager(ctrl)
	mockPersistManager.EXPECT().StartFlush().Return(mockFlush, nil)

	opts = opts.SetPersistManager(mockPersistManager)

	src := newPeersSource(opts)

	target := result.ShardTimeRanges{
		0: xtime.NewRanges().AddRange(xtime.Range{Start: start, End: end}),
		1: xtime.NewRanges().AddRange(xtime.Range{Start: start, End: end}),
		2: xtime.NewRanges().AddRange(xtime.Range{Start: start, End: end}),
		3: xtime.NewRanges().AddRange(xtime.Range{Start: start, End: end}),
	}

	r, err := src.Read(testNamespace, target, testIncrementalRunOpts)
	assert.NoError(t, err)

	assert.Equal(t, 4, len(r.ShardResults()))
	for i := uint32(0); i < uint32(len(target)); i++ {
		require.NotNil(t, r.ShardResults()[i])
	}
	for i := uint32(0); i < uint32(len(target)); i++ {
		require.Nil(t, r.Unfulfilled()[i])
	}

	block, ok := r.ShardResults()[0].BlockAt(ts.StringID("foo"), start)
	require.True(t, ok)
	assert.Equal(t, fooBlock, block)

	block, ok = r.ShardResults()[1].BlockAt(ts.StringID("bar"), start)
	require.True(t, ok)
	assert.Equal(t, barBlock, block)

	block, ok = r.ShardResults()[2].BlockAt(ts.StringID("baz"), start)
	require.True(t, ok)
	assert.Equal(t, bazBlock, block)

	block, ok = r.ShardResults()[3].BlockAt(ts.StringID("qux"), start)
	require.True(t, ok)
	assert.Equal(t, quxBlock, block)

	assert.Equal(t, map[string]int{
		"baz": 1, "qux": 1,
	}, persists)

	assert.Equal(t, map[string]int{
		"foo": 1, "bar": 1, "baz": 1, "qux": 1,
	}, closes)
}
