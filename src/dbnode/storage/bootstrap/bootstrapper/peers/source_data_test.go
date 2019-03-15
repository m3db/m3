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

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/persist"
	m3dbruntime "github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	xtest "github.com/m3db/m3x/test"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testNamespace         = ident.StringID("testnamespace")
	testNamespaceMetadata = func(t *testing.T, opts ...namespaceOption) namespace.Metadata {
		namespaceOpts := namespace.NewOptions()
		for _, opt := range opts {
			namespaceOpts = opt(namespaceOpts)
		}
		ns, err := namespace.NewMetadata(testNamespace, namespaceOpts)
		require.NoError(t, err)
		return ns
	}

	testDefaultRunOpts = bootstrap.NewRunOptions().
				SetPersistConfig(bootstrap.PersistConfig{Enabled: false})
	testRunOptsWithPersist = bootstrap.NewRunOptions().
				SetPersistConfig(bootstrap.PersistConfig{Enabled: true})
	testBlockOpts         = block.NewOptions()
	testDefaultResultOpts = result.NewOptions().SetSeriesCachePolicy(series.CacheAll)
	testDefaultOpts       = NewOptions().
				SetResultOptions(testDefaultResultOpts)
)

func newTestDefaultOpts(t *testing.T, ctrl *gomock.Controller) Options {
	return testDefaultOpts.
		SetAdminClient(newValidMockClient(t, ctrl)).
		SetRuntimeOptionsManager(newValidMockRuntimeOptionsManager(t, ctrl))
}

func newValidMockClient(t *testing.T, ctrl *gomock.Controller) *client.MockAdminClient {
	mockAdminSession := client.NewMockAdminSession(ctrl)
	mockClient := client.NewMockAdminClient(ctrl)
	mockClient.EXPECT().
		DefaultAdminSession().
		Return(mockAdminSession, nil).
		AnyTimes()

	return mockClient
}

func newValidMockRuntimeOptionsManager(t *testing.T, ctrl *gomock.Controller) m3dbruntime.OptionsManager {
	mockRuntimeOpts := m3dbruntime.NewMockOptions(ctrl)
	mockRuntimeOpts.
		EXPECT().
		ClientBootstrapConsistencyLevel().
		Return(topology.ReadConsistencyLevelAll).
		AnyTimes()

	mockRuntimeOptsMgr := m3dbruntime.NewMockOptionsManager(ctrl)
	mockRuntimeOptsMgr.
		EXPECT().
		Get().
		Return(mockRuntimeOpts).
		AnyTimes()

	return mockRuntimeOptsMgr
}

type namespaceOption func(namespace.Options) namespace.Options

func TestPeersSourceCan(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	src, err := newPeersSource(newTestDefaultOpts(t, ctrl))
	require.NoError(t, err)

	assert.True(t, src.Can(bootstrap.BootstrapSequential))
	assert.False(t, src.Can(bootstrap.BootstrapParallel))
}

func TestPeersSourceEmptyShardTimeRanges(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDefaultOpts.
		SetRuntimeOptionsManager(newValidMockRuntimeOptionsManager(t, ctrl))

	src, err := newPeersSource(opts)
	require.NoError(t, err)

	var (
		nsMetdata = testNamespaceMetadata(t)
		target    = result.ShardTimeRanges{}
		runOpts   = testDefaultRunOpts.SetInitialTopologyState(&topology.StateSnapshot{})
	)
	available, err := src.AvailableData(nsMetdata, target, runOpts)
	require.NoError(t, err)
	require.Equal(t, target, available)

	r, err := src.ReadData(nsMetdata, target, testDefaultRunOpts)
	require.NoError(t, err)
	require.Equal(t, 0, len(r.ShardResults()))
	require.True(t, r.Unfulfilled().IsEmpty())
}

func TestPeersSourceReturnsErrorForAdminSession(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	nsMetadata := testNamespaceMetadata(t)
	ropts := nsMetadata.Options().RetentionOptions()

	expectedErr := fmt.Errorf("an error")

	mockAdminClient := client.NewMockAdminClient(ctrl)
	mockAdminClient.EXPECT().DefaultAdminSession().Return(nil, expectedErr)

	opts := testDefaultOpts.SetAdminClient(mockAdminClient)
	src, err := newPeersSource(opts)
	require.NoError(t, err)

	start := time.Now().Add(-ropts.RetentionPeriod()).Truncate(ropts.BlockSize())
	end := start.Add(ropts.BlockSize())

	target := result.ShardTimeRanges{
		0: xtime.NewRanges(xtime.Range{Start: start, End: end}),
		1: xtime.NewRanges(xtime.Range{Start: start, End: end}),
	}

	_, err = src.ReadData(nsMetadata, target, testDefaultRunOpts)
	require.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

func TestPeersSourceReturnsFulfilledAndUnfulfilled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDefaultOpts
	nsMetadata := testNamespaceMetadata(t)
	ropts := nsMetadata.Options().RetentionOptions()

	start := time.Now().Add(-ropts.RetentionPeriod()).Truncate(ropts.BlockSize())
	end := start.Add(ropts.BlockSize())

	goodResult := result.NewShardResult(0, opts.ResultOptions())
	fooBlock := block.NewDatabaseBlock(start, ropts.BlockSize(), ts.Segment{}, testBlockOpts)
	goodResult.AddBlock(ident.StringID("foo"), ident.NewTags(ident.StringTag("foo", "oof")), fooBlock)
	badErr := fmt.Errorf("an error")

	mockAdminSession := client.NewMockAdminSession(ctrl)
	mockAdminSession.EXPECT().
		FetchBootstrapBlocksFromPeers(namespace.NewMetadataMatcher(nsMetadata),
			uint32(0), start, end, gomock.Any()).
		Return(goodResult, nil)
	mockAdminSession.EXPECT().
		FetchBootstrapBlocksFromPeers(namespace.NewMetadataMatcher(nsMetadata),
			uint32(1), start, end, gomock.Any()).
		Return(nil, badErr)

	mockAdminClient := client.NewMockAdminClient(ctrl)
	mockAdminClient.EXPECT().DefaultAdminSession().Return(mockAdminSession, nil)

	opts = opts.SetAdminClient(mockAdminClient)

	src, err := newPeersSource(opts)
	require.NoError(t, err)

	target := result.ShardTimeRanges{
		0: xtime.NewRanges(xtime.Range{Start: start, End: end}),
		1: xtime.NewRanges(xtime.Range{Start: start, End: end}),
	}

	r, err := src.ReadData(nsMetadata, target, testDefaultRunOpts)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(r.ShardResults()))
	require.NotNil(t, r.ShardResults()[0])
	require.Nil(t, r.ShardResults()[1])

	require.True(t, r.Unfulfilled()[0].IsEmpty())
	require.False(t, r.Unfulfilled()[1].IsEmpty())
	require.Equal(t, 1, r.Unfulfilled()[1].Len())

	block, ok := r.ShardResults()[0].BlockAt(ident.StringID("foo"), start)
	require.True(t, ok)
	require.Equal(t, fooBlock, block)

	rangeIter := r.Unfulfilled()[1].Iter()
	require.True(t, rangeIter.Next())
	require.Equal(t, xtime.Range{Start: start, End: end}, rangeIter.Value())
	require.Equal(t, ropts.BlockSize(), block.BlockSize())
}

func TestPeersSourceRunWithPersist(t *testing.T) {
	for _, cachePolicy := range []series.CachePolicy{
		series.CacheRecentlyRead,
		series.CacheLRU,
	} {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		testNsMd := testNamespaceMetadata(t)
		resultOpts := testDefaultResultOpts.SetSeriesCachePolicy(cachePolicy)
		opts := testDefaultOpts.SetResultOptions(resultOpts)
		ropts := testNsMd.Options().RetentionOptions()
		blockSize := ropts.BlockSize()

		start := time.Now().Add(-ropts.RetentionPeriod()).Truncate(ropts.BlockSize())
		end := start.Add(2 * ropts.BlockSize())

		shard0ResultBlock1 := result.NewShardResult(0, opts.ResultOptions())
		shard0ResultBlock2 := result.NewShardResult(0, opts.ResultOptions())
		fooBlock := block.NewDatabaseBlock(start, ropts.BlockSize(),
			ts.NewSegment(checked.NewBytes([]byte{1, 2, 3}, nil), nil, ts.FinalizeNone),
			testBlockOpts)
		barBlock := block.NewDatabaseBlock(start.Add(ropts.BlockSize()), ropts.BlockSize(),
			ts.NewSegment(checked.NewBytes([]byte{4, 5, 6}, nil), nil, ts.FinalizeNone),
			testBlockOpts)
		shard0ResultBlock1.AddBlock(ident.StringID("foo"), ident.NewTags(ident.StringTag("foo", "oof")), fooBlock)
		shard0ResultBlock2.AddBlock(ident.StringID("bar"), ident.NewTags(ident.StringTag("bar", "rab")), barBlock)

		shard1ResultBlock1 := result.NewShardResult(0, opts.ResultOptions())
		shard1ResultBlock2 := result.NewShardResult(0, opts.ResultOptions())
		bazBlock := block.NewDatabaseBlock(start, ropts.BlockSize(),
			ts.NewSegment(checked.NewBytes([]byte{7, 8, 9}, nil), nil, ts.FinalizeNone),
			testBlockOpts)
		shard1ResultBlock1.AddBlock(ident.StringID("baz"), ident.NewTags(ident.StringTag("baz", "zab")), bazBlock)

		mockAdminSession := client.NewMockAdminSession(ctrl)
		mockAdminSession.EXPECT().
			FetchBootstrapBlocksFromPeers(namespace.NewMetadataMatcher(testNsMd),
				uint32(0), start, start.Add(blockSize), gomock.Any()).
			Return(shard0ResultBlock1, nil)
		mockAdminSession.EXPECT().
			FetchBootstrapBlocksFromPeers(namespace.NewMetadataMatcher(testNsMd),
				uint32(0), start.Add(blockSize), start.Add(blockSize*2), gomock.Any()).
			Return(shard0ResultBlock2, nil)
		mockAdminSession.EXPECT().
			FetchBootstrapBlocksFromPeers(namespace.NewMetadataMatcher(testNsMd),
				uint32(1), start, start.Add(blockSize), gomock.Any()).
			Return(shard1ResultBlock1, nil)
		mockAdminSession.EXPECT().
			FetchBootstrapBlocksFromPeers(namespace.NewMetadataMatcher(testNsMd),
				uint32(1), start.Add(blockSize), start.Add(blockSize*2), gomock.Any()).
			Return(shard1ResultBlock2, nil)

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
			Retriever(namespace.NewMetadataMatcher(testNsMd)).
			Return(mockRetriever, nil)

		opts = opts.SetDatabaseBlockRetrieverManager(mockRetrieverMgr)

		flushPreparer := persist.NewMockFlushPreparer(ctrl)
		flushPreparer.EXPECT().DoneFlush()
		persists := make(map[string]int)
		closes := make(map[string]int)
		prepareOpts := xtest.CmpMatcher(persist.DataPrepareOptions{
			NamespaceMetadata: testNsMd,
			Shard:             uint32(0),
			BlockStart:        start,
			DeleteIfExists:    true,
		})
		flushPreparer.EXPECT().
			PrepareData(prepareOpts).
			Return(persist.PreparedDataPersist{
				Persist: func(id ident.ID, _ ident.Tags, segment ts.Segment, checksum uint32) error {
					persists["foo"]++
					assert.Equal(t, "foo", id.String())
					assert.Equal(t, []byte{1, 2, 3}, segment.Head.Bytes())
					assertBlockChecksum(t, checksum, fooBlock)
					return nil
				},
				Close: func() error {
					closes["foo"]++
					return nil
				},
			}, nil)
		prepareOpts = xtest.CmpMatcher(persist.DataPrepareOptions{
			NamespaceMetadata: testNsMd,
			Shard:             uint32(0),
			BlockStart:        start.Add(ropts.BlockSize()),
			DeleteIfExists:    true,
		})
		flushPreparer.EXPECT().
			PrepareData(prepareOpts).
			Return(persist.PreparedDataPersist{
				Persist: func(id ident.ID, _ ident.Tags, segment ts.Segment, checksum uint32) error {
					persists["bar"]++
					assert.Equal(t, "bar", id.String())
					assert.Equal(t, []byte{4, 5, 6}, segment.Head.Bytes())
					assertBlockChecksum(t, checksum, barBlock)
					return nil
				},
				Close: func() error {
					closes["bar"]++
					return nil
				},
			}, nil)
		prepareOpts = xtest.CmpMatcher(persist.DataPrepareOptions{
			NamespaceMetadata: testNsMd,
			Shard:             uint32(1),
			BlockStart:        start,
			DeleteIfExists:    true,
		})
		flushPreparer.EXPECT().
			PrepareData(prepareOpts).
			Return(persist.PreparedDataPersist{
				Persist: func(id ident.ID, _ ident.Tags, segment ts.Segment, checksum uint32) error {
					persists["baz"]++
					assert.Equal(t, "baz", id.String())
					assert.Equal(t, []byte{7, 8, 9}, segment.Head.Bytes())
					assertBlockChecksum(t, checksum, bazBlock)
					return nil
				},
				Close: func() error {
					closes["baz"]++
					return nil
				},
			}, nil)
		prepareOpts = xtest.CmpMatcher(persist.DataPrepareOptions{
			NamespaceMetadata: testNsMd,
			Shard:             uint32(1),
			BlockStart:        start.Add(ropts.BlockSize()),
			DeleteIfExists:    true,
		})
		flushPreparer.EXPECT().
			PrepareData(prepareOpts).
			Return(persist.PreparedDataPersist{
				Persist: func(id ident.ID, _ ident.Tags, segment ts.Segment, checksum uint32) error {
					assert.Fail(t, "no expected shard 1 second block")
					return nil
				},
				Close: func() error {
					closes["empty"]++
					return nil
				},
			}, nil)

		mockPersistManager := persist.NewMockManager(ctrl)
		mockPersistManager.EXPECT().StartFlushPersist().Return(flushPreparer, nil)

		opts = opts.SetPersistManager(mockPersistManager)

		src, err := newPeersSource(opts)
		require.NoError(t, err)

		target := result.ShardTimeRanges{
			0: xtime.NewRanges(xtime.Range{Start: start, End: end}),
			1: xtime.NewRanges(xtime.Range{Start: start, End: end}),
		}

		r, err := src.ReadData(testNsMd, target, testRunOptsWithPersist)
		assert.NoError(t, err)

		require.True(t, r.Unfulfilled()[0].IsEmpty())
		require.True(t, r.Unfulfilled()[1].IsEmpty())

		assert.Equal(t, 0, len(r.ShardResults()))
		require.Nil(t, r.ShardResults()[0])
		require.Nil(t, r.ShardResults()[1])

		assert.Equal(t, map[string]int{
			"foo": 1, "bar": 1, "baz": 1,
		}, persists)

		assert.Equal(t, map[string]int{
			"foo": 1, "bar": 1, "baz": 1, "empty": 1,
		}, closes)
	}
}

func TestPeersSourceMarksUnfulfilledOnPersistenceErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDefaultOpts.
		SetResultOptions(testDefaultOpts.
			ResultOptions().
			SetSeriesCachePolicy(series.CacheRecentlyRead),
		)
	testNsMd := testNamespaceMetadata(t)
	ropts := testNsMd.Options().RetentionOptions()

	start := time.Now().Add(-ropts.RetentionPeriod()).Truncate(ropts.BlockSize())
	midway := start.Add(ropts.BlockSize())
	end := start.Add(2 * ropts.BlockSize())

	type resultsKey struct {
		shard uint32
		start int64
		end   int64
	}

	results := make(map[resultsKey]result.ShardResult)
	addResult := func(shard uint32, id string, b block.DatabaseBlock) {
		r := result.NewShardResult(0, opts.ResultOptions())
		r.AddBlock(ident.StringID(id), ident.NewTags(ident.StringTag(id, id)), b)
		start := b.StartTime()
		end := start.Add(ropts.BlockSize())
		results[resultsKey{shard, start.UnixNano(), end.UnixNano()}] = r
	}

	// foo results
	var fooBlocks [2]block.DatabaseBlock
	fooBlocks[0] = block.NewMockDatabaseBlock(ctrl)
	fooBlocks[0].(*block.MockDatabaseBlock).EXPECT().StartTime().Return(start).AnyTimes()
	fooBlocks[0].(*block.MockDatabaseBlock).EXPECT().Stream(gomock.Any()).Return(xio.EmptyBlockReader, fmt.Errorf("stream err"))
	addResult(0, "foo", fooBlocks[0])

	fooBlocks[1] = block.NewDatabaseBlock(midway, ropts.BlockSize(),
		ts.NewSegment(checked.NewBytes([]byte{1, 2, 3}, nil), nil, ts.FinalizeNone),
		testBlockOpts)
	addResult(0, "foo", fooBlocks[1])

	// bar results
	mockStream := xio.NewMockSegmentReader(ctrl)
	mockStream.EXPECT().Segment().Return(ts.Segment{}, fmt.Errorf("segment err"))

	b := xio.BlockReader{
		SegmentReader: mockStream,
	}

	var barBlocks [2]block.DatabaseBlock
	barBlocks[0] = block.NewMockDatabaseBlock(ctrl)
	barBlocks[0].(*block.MockDatabaseBlock).EXPECT().StartTime().Return(start).AnyTimes()
	barBlocks[0].(*block.MockDatabaseBlock).EXPECT().Stream(gomock.Any()).Return(b, nil)
	addResult(1, "bar", barBlocks[0])

	barBlocks[1] = block.NewDatabaseBlock(midway, ropts.BlockSize(),
		ts.NewSegment(checked.NewBytes([]byte{4, 5, 6}, nil), nil, ts.FinalizeNone),
		testBlockOpts)
	addResult(1, "bar", barBlocks[1])

	// baz results
	var bazBlocks [2]block.DatabaseBlock
	bazBlocks[0] = block.NewDatabaseBlock(start, ropts.BlockSize(),
		ts.NewSegment(checked.NewBytes([]byte{7, 8, 9}, nil), nil, ts.FinalizeNone),
		testBlockOpts)
	addResult(2, "baz", bazBlocks[0])

	bazBlocks[1] = block.NewDatabaseBlock(midway, ropts.BlockSize(),
		ts.NewSegment(checked.NewBytes([]byte{10, 11, 12}, nil), nil, ts.FinalizeNone),
		testBlockOpts)
	addResult(2, "baz", bazBlocks[1])

	// qux results
	var quxBlocks [2]block.DatabaseBlock
	quxBlocks[0] = block.NewDatabaseBlock(start, ropts.BlockSize(),
		ts.NewSegment(checked.NewBytes([]byte{13, 14, 15}, nil), nil, ts.FinalizeNone),
		testBlockOpts)
	addResult(3, "qux", quxBlocks[0])

	quxBlocks[1] = block.NewDatabaseBlock(midway, ropts.BlockSize(),
		ts.NewSegment(checked.NewBytes([]byte{16, 17, 18}, nil), nil, ts.FinalizeNone),
		testBlockOpts)
	addResult(3, "qux", quxBlocks[1])

	mockAdminSession := client.NewMockAdminSession(ctrl)

	for key, result := range results {
		mockAdminSession.EXPECT().
			FetchBootstrapBlocksFromPeers(namespace.NewMetadataMatcher(testNsMd),
				key.shard, time.Unix(0, key.start), time.Unix(0, key.end),
				gomock.Any()).
			Return(result, nil)
	}

	mockAdminClient := client.NewMockAdminClient(ctrl)
	mockAdminClient.EXPECT().DefaultAdminSession().Return(mockAdminSession, nil)

	opts = opts.SetAdminClient(mockAdminClient)

	mockRetriever := block.NewMockDatabaseBlockRetriever(ctrl)
	mockRetriever.EXPECT().CacheShardIndices(gomock.Any()).AnyTimes()

	mockRetrieverMgr := block.NewMockDatabaseBlockRetrieverManager(ctrl)
	mockRetrieverMgr.EXPECT().
		Retriever(namespace.NewMetadataMatcher(testNsMd)).
		Return(mockRetriever, nil)

	opts = opts.SetDatabaseBlockRetrieverManager(mockRetrieverMgr)

	flushPreprarer := persist.NewMockFlushPreparer(ctrl)
	flushPreprarer.EXPECT().DoneFlush()

	persists := make(map[string]int)
	closes := make(map[string]int)

	// expect foo
	prepareOpts := xtest.CmpMatcher(persist.DataPrepareOptions{
		NamespaceMetadata: testNsMd,
		Shard:             uint32(0),
		BlockStart:        start,
		DeleteIfExists:    true,
	})
	flushPreprarer.EXPECT().
		PrepareData(prepareOpts).
		Return(persist.PreparedDataPersist{
			Persist: func(id ident.ID, _ ident.Tags, segment ts.Segment, checksum uint32) error {
				assert.Fail(t, "not expecting to flush shard 0 at start")
				return nil
			},
			Close: func() error {
				closes["foo"]++
				return nil
			},
		}, nil)
	prepareOpts = xtest.CmpMatcher(persist.DataPrepareOptions{
		NamespaceMetadata: testNsMd,
		Shard:             uint32(0),
		BlockStart:        midway,
		DeleteIfExists:    true,
	})
	flushPreprarer.EXPECT().
		PrepareData(prepareOpts).
		Return(persist.PreparedDataPersist{
			Persist: func(id ident.ID, _ ident.Tags, segment ts.Segment, checksum uint32) error {
				persists["foo"]++
				return nil
			},
			Close: func() error {
				closes["foo"]++
				return nil
			},
		}, nil)

	// expect bar
	prepareOpts = xtest.CmpMatcher(persist.DataPrepareOptions{
		NamespaceMetadata: testNsMd,
		Shard:             uint32(1),
		BlockStart:        start,
		DeleteIfExists:    true,
	})
	flushPreprarer.EXPECT().
		PrepareData(prepareOpts).
		Return(persist.PreparedDataPersist{
			Persist: func(id ident.ID, _ ident.Tags, segment ts.Segment, checksum uint32) error {
				assert.Fail(t, "not expecting to flush shard 0 at start + block size")
				return nil
			},
			Close: func() error {
				closes["bar"]++
				return nil
			},
		}, nil)
	prepareOpts = xtest.CmpMatcher(persist.DataPrepareOptions{
		NamespaceMetadata: testNsMd,
		Shard:             uint32(1),
		BlockStart:        midway,
		DeleteIfExists:    true,
	})
	flushPreprarer.EXPECT().
		PrepareData(prepareOpts).
		Return(persist.PreparedDataPersist{
			Persist: func(id ident.ID, _ ident.Tags, segment ts.Segment, checksum uint32) error {
				persists["bar"]++
				return nil
			},
			Close: func() error {
				closes["bar"]++
				return nil
			},
		}, nil)

	// expect baz
	prepareOpts = xtest.CmpMatcher(persist.DataPrepareOptions{
		NamespaceMetadata: testNsMd,
		Shard:             uint32(2),
		BlockStart:        start,
		DeleteIfExists:    true,
	})
	flushPreprarer.EXPECT().
		PrepareData(prepareOpts).
		Return(persist.PreparedDataPersist{
			Persist: func(id ident.ID, _ ident.Tags, segment ts.Segment, checksum uint32) error {
				persists["baz"]++
				return fmt.Errorf("a persist error")
			},
			Close: func() error {
				closes["baz"]++
				return nil
			},
		}, nil)
	prepareOpts = xtest.CmpMatcher(persist.DataPrepareOptions{
		NamespaceMetadata: testNsMd,
		Shard:             uint32(2),
		BlockStart:        midway,
		DeleteIfExists:    true,
	})
	flushPreprarer.EXPECT().
		PrepareData(prepareOpts).
		Return(persist.PreparedDataPersist{
			Persist: func(id ident.ID, _ ident.Tags, segment ts.Segment, checksum uint32) error {
				persists["baz"]++
				return nil
			},
			Close: func() error {
				closes["baz"]++
				return nil
			},
		}, nil)

		// expect qux
	prepareOpts = xtest.CmpMatcher(persist.DataPrepareOptions{
		NamespaceMetadata: testNsMd,
		Shard:             uint32(3),
		BlockStart:        start,
		DeleteIfExists:    true,
	})
	flushPreprarer.EXPECT().
		PrepareData(prepareOpts).
		Return(persist.PreparedDataPersist{
			Persist: func(id ident.ID, _ ident.Tags, segment ts.Segment, checksum uint32) error {
				persists["qux"]++
				return nil
			},
			Close: func() error {
				closes["qux"]++
				return fmt.Errorf("a persist close error")
			},
		}, nil)
	prepareOpts = xtest.CmpMatcher(persist.DataPrepareOptions{
		NamespaceMetadata: testNsMd,
		Shard:             uint32(3),
		BlockStart:        midway,
		DeleteIfExists:    true,
	})
	flushPreprarer.EXPECT().
		PrepareData(prepareOpts).
		Return(persist.PreparedDataPersist{
			Persist: func(id ident.ID, _ ident.Tags, segment ts.Segment, checksum uint32) error {
				persists["qux"]++
				return nil
			},
			Close: func() error {
				closes["qux"]++
				return nil
			},
		}, nil)

	mockPersistManager := persist.NewMockManager(ctrl)
	mockPersistManager.EXPECT().StartFlushPersist().Return(flushPreprarer, nil)

	opts = opts.SetPersistManager(mockPersistManager)

	src, err := newPeersSource(opts)
	require.NoError(t, err)

	target := result.ShardTimeRanges{
		0: xtime.Ranges{}.
			AddRange(xtime.Range{Start: start, End: midway}).
			AddRange(xtime.Range{Start: midway, End: end}),
		1: xtime.Ranges{}.
			AddRange(xtime.Range{Start: start, End: midway}).
			AddRange(xtime.Range{Start: midway, End: end}),
		2: xtime.Ranges{}.
			AddRange(xtime.Range{Start: start, End: midway}).
			AddRange(xtime.Range{Start: midway, End: end}),
		3: xtime.Ranges{}.
			AddRange(xtime.Range{Start: start, End: midway}).
			AddRange(xtime.Range{Start: midway, End: end}),
	}

	r, err := src.ReadData(testNsMd, target, testRunOptsWithPersist)
	assert.NoError(t, err)

	assert.Equal(t, 0, len(r.ShardResults()))
	for i := uint32(0); i < uint32(len(target)); i++ {
		require.False(t, r.Unfulfilled()[i].IsEmpty())
		require.Equal(t, xtime.NewRanges(xtime.Range{
			Start: start,
			End:   midway,
		}).String(), r.Unfulfilled()[i].String())
	}

	assert.Equal(t, map[string]int{
		"foo": 1, "bar": 1, "baz": 2, "qux": 2,
	}, persists)

	assert.Equal(t, map[string]int{
		"foo": 2, "bar": 2, "baz": 2, "qux": 2,
	}, closes)
}

func assertBlockChecksum(t *testing.T, expectedChecksum uint32, block block.DatabaseBlock) {
	checksum, err := block.Checksum()
	require.NoError(t, err)
	require.Equal(t, expectedChecksum, checksum)
}
