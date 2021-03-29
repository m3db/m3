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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	m3dbruntime "github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testNamespace         = ident.StringID("testnamespace")
	testNamespaceMetadata = func(t *testing.T, opts ...namespaceOption) namespace.Metadata {
		namespaceOpts := namespace.NewOptions()
		idxOpts := namespaceOpts.IndexOptions()
		namespaceOpts = namespaceOpts.SetIndexOptions(idxOpts.SetEnabled(true))
		for _, opt := range opts {
			namespaceOpts = opt(namespaceOpts)
		}
		ns, err := namespace.NewMetadata(testNamespace, namespaceOpts)
		require.NoError(t, err)
		return ns
	}
	testNamespaceMetadataNoIndex = func(t *testing.T, opts ...namespaceOption) namespace.Metadata {
		newOpts := append([]namespaceOption(nil), opts...)
		newOpts = append(newOpts, func(namespaceOpts namespace.Options) namespace.Options {
			idxOpts := namespaceOpts.IndexOptions()
			return namespaceOpts.SetIndexOptions(idxOpts.SetEnabled(false))
		})
		return testNamespaceMetadata(t, newOpts...)
	}

	testDefaultRunOpts = bootstrap.NewRunOptions().
				SetPersistConfig(bootstrap.PersistConfig{Enabled: false})
	testRunOptsWithPersist = bootstrap.NewRunOptions().
				SetPersistConfig(bootstrap.PersistConfig{Enabled: true})
	testBlockOpts         = block.NewOptions()
	testDefaultResultOpts = result.NewOptions().SetSeriesCachePolicy(series.CacheAll)
)

type namespaceOption func(namespace.Options) namespace.Options

func newTestDefaultOpts(t *testing.T, ctrl *gomock.Controller) Options {
	idxOpts := index.NewOptions()
	compactor, err := compaction.NewCompactor(idxOpts.MetadataArrayPool(),
		index.MetadataArrayPoolCapacity,
		idxOpts.SegmentBuilderOptions(),
		idxOpts.FSTSegmentOptions(),
		compaction.CompactorOptions{
			FSTWriterOptions: &fst.WriterOptions{
				// DisableRegistry is set to true to trade a larger FST size
				// for a faster FST compaction since we want to reduce the end
				// to end latency for time to first index a metric.
				DisableRegistry: true,
			},
		})
	require.NoError(t, err)
	fsOpts := fs.NewOptions()

	// Allow multiple index claim managers since need to create one
	// for each file path prefix (fs options changes between tests).
	fs.ResetIndexClaimsManagersUnsafe()

	icm, err := fs.NewIndexClaimsManager(fsOpts)
	require.NoError(t, err)
	return NewOptions().
		SetResultOptions(testDefaultResultOpts).
		SetPersistManager(persist.NewMockManager(ctrl)).
		SetIndexClaimsManager(icm).
		SetAdminClient(client.NewMockAdminClient(ctrl)).
		SetFilesystemOptions(fsOpts).
		SetCompactor(compactor).
		SetIndexOptions(idxOpts).
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

func TestPeersSourceEmptyShardTimeRanges(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := newTestDefaultOpts(t, ctrl).
		SetRuntimeOptionsManager(newValidMockRuntimeOptionsManager(t, ctrl))

	src, err := newPeersSource(opts)
	require.NoError(t, err)

	var (
		nsMetadata = testNamespaceMetadataNoIndex(t)
		target     = result.NewShardTimeRanges()
		runOpts    = testDefaultRunOpts.SetInitialTopologyState(&topology.StateSnapshot{})
	)
	cache, err := bootstrap.NewCache(bootstrap.NewCacheOptions().
		SetFilesystemOptions(opts.FilesystemOptions()).
		SetInstrumentOptions(opts.FilesystemOptions().InstrumentOptions()))
	require.NoError(t, err)
	available, err := src.AvailableData(nsMetadata, target, cache, runOpts)
	require.NoError(t, err)
	require.Equal(t, target, available)

	tester := bootstrap.BuildNamespacesTester(t, runOpts, target, nsMetadata)
	defer tester.Finish()
	tester.TestReadWith(src)
	tester.TestUnfulfilledForNamespaceIsEmpty(nsMetadata)
	tester.EnsureNoLoadedBlocks()
	tester.EnsureNoWrites()
}

func TestPeersSourceReturnsErrorForAdminSession(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	nsMetadata := testNamespaceMetadataNoIndex(t)
	ropts := nsMetadata.Options().RetentionOptions()

	expectedErr := errors.New("an error")

	mockAdminClient := client.NewMockAdminClient(ctrl)
	mockAdminClient.EXPECT().DefaultAdminSession().Return(nil, expectedErr)

	opts := newTestDefaultOpts(t, ctrl).SetAdminClient(mockAdminClient)
	src, err := newPeersSource(opts)
	require.NoError(t, err)

	start := time.Now().Add(-ropts.RetentionPeriod()).Truncate(ropts.BlockSize())
	end := start.Add(ropts.BlockSize())

	target := result.NewShardTimeRanges().Set(
		0,
		xtime.NewRanges(xtime.Range{Start: start, End: end}),
	).Set(
		1,
		xtime.NewRanges(xtime.Range{Start: start, End: end}),
	)

	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, target, nsMetadata)
	defer tester.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	_, err = src.Read(ctx, tester.Namespaces, tester.Cache)
	require.Error(t, err)
	assert.Equal(t, expectedErr, err)
	tester.EnsureNoLoadedBlocks()
	tester.EnsureNoWrites()
}

func TestPeersSourceReturnsUnfulfilled(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := newTestDefaultOpts(t, ctrl)
	nsMetadata := testNamespaceMetadataNoIndex(t)
	ropts := nsMetadata.Options().RetentionOptions()

	start := time.Now().Add(-ropts.RetentionPeriod()).Truncate(ropts.BlockSize())
	end := start.Add(ropts.BlockSize())

	goodResult := result.NewShardResult(opts.ResultOptions())
	fooBlock := block.NewDatabaseBlock(start, ropts.BlockSize(), ts.Segment{}, testBlockOpts, namespace.Context{})
	goodID := ident.StringID("foo")
	goodResult.AddBlock(goodID, ident.NewTags(ident.StringTag("foo", "oof")), fooBlock)

	mockAdminSession := client.NewMockAdminSession(ctrl)
	mockAdminSession.EXPECT().
		FetchBootstrapBlocksFromPeers(namespace.NewMetadataMatcher(nsMetadata),
			uint32(0), start, end, gomock.Any()).
		Return(goodResult, nil)

	peerMetaIter := client.NewMockPeerBlockMetadataIter(ctrl)
	peerMetaIter.EXPECT().Next().Return(false).AnyTimes()
	peerMetaIter.EXPECT().Err().Return(nil).AnyTimes()
	mockAdminSession.EXPECT().
		FetchBootstrapBlocksMetadataFromPeers(gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any()).
		Return(peerMetaIter, nil).AnyTimes()

	mockAdminClient := client.NewMockAdminClient(ctrl)
	mockAdminClient.EXPECT().DefaultAdminSession().Return(mockAdminSession, nil).AnyTimes()

	opts = opts.SetAdminClient(mockAdminClient)

	src, err := newPeersSource(opts)
	require.NoError(t, err)

	target := result.NewShardTimeRanges().Set(
		0,
		xtime.NewRanges(xtime.Range{Start: start, End: end}),
	)

	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, target, nsMetadata)
	defer tester.Finish()
	tester.TestReadWith(src)
	tester.TestUnfulfilledForNamespaceIsEmpty(nsMetadata)
	vals := tester.DumpLoadedBlocks()
	assert.Equal(t, 1, len(vals))
	series, found := vals[nsMetadata.ID().String()]
	require.True(t, found)

	assert.Equal(t, 1, len(series))
	points, found := series[goodID.String()]
	require.True(t, found)
	assert.Equal(t, 0, len(points))
	tester.EnsureNoWrites()
}

func TestPeersSourceRunWithPersist(t *testing.T) {
	for _, cachePolicy := range []series.CachePolicy{
		series.CacheNone,
		series.CacheRecentlyRead,
		series.CacheLRU,
	} {
		ctrl := xtest.NewController(t)
		defer ctrl.Finish()

		testNsMd := testNamespaceMetadataNoIndex(t)
		resultOpts := testDefaultResultOpts.SetSeriesCachePolicy(cachePolicy)
		opts := newTestDefaultOpts(t, ctrl).SetResultOptions(resultOpts)
		ropts := testNsMd.Options().RetentionOptions()
		testNsMd.Options()
		blockSize := ropts.BlockSize()

		start := time.Now().Add(-ropts.RetentionPeriod()).Truncate(ropts.BlockSize())
		end := start.Add(2 * ropts.BlockSize())

		shard0ResultBlock1 := result.NewShardResult(opts.ResultOptions())
		shard0ResultBlock2 := result.NewShardResult(opts.ResultOptions())
		fooBlock := block.NewDatabaseBlock(start, ropts.BlockSize(),
			ts.NewSegment(checked.NewBytes([]byte{1, 2, 3}, nil), nil, 1, ts.FinalizeNone),
			testBlockOpts, namespace.Context{})
		barBlock := block.NewDatabaseBlock(start.Add(ropts.BlockSize()), ropts.BlockSize(),
			ts.NewSegment(checked.NewBytes([]byte{4, 5, 6}, nil), nil, 2, ts.FinalizeNone),
			testBlockOpts, namespace.Context{})
		shard0ResultBlock1.AddBlock(ident.StringID("foo"), ident.NewTags(ident.StringTag("foo", "oof")), fooBlock)
		shard0ResultBlock2.AddBlock(ident.StringID("bar"), ident.NewTags(ident.StringTag("bar", "rab")), barBlock)

		shard1ResultBlock1 := result.NewShardResult(opts.ResultOptions())
		shard1ResultBlock2 := result.NewShardResult(opts.ResultOptions())
		bazBlock := block.NewDatabaseBlock(start, ropts.BlockSize(),
			ts.NewSegment(checked.NewBytes([]byte{7, 8, 9}, nil), nil, 3, ts.FinalizeNone),
			testBlockOpts, namespace.Context{})
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

		peerMetaIter := client.NewMockPeerBlockMetadataIter(ctrl)
		peerMetaIter.EXPECT().Next().Return(false).AnyTimes()
		peerMetaIter.EXPECT().Err().Return(nil).AnyTimes()
		mockAdminSession.EXPECT().
			FetchBootstrapBlocksMetadataFromPeers(gomock.Any(), gomock.Any(),
				gomock.Any(), gomock.Any(), gomock.Any()).
			Return(peerMetaIter, nil).AnyTimes()

		mockAdminClient := client.NewMockAdminClient(ctrl)
		mockAdminClient.EXPECT().DefaultAdminSession().Return(mockAdminSession, nil).AnyTimes()

		opts = opts.SetAdminClient(mockAdminClient)

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
				Persist: func(metadata persist.Metadata, segment ts.Segment, checksum uint32) error {
					persists["foo"]++
					assert.Equal(t, "foo", string(metadata.BytesID()))
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
				Persist: func(metadata persist.Metadata, segment ts.Segment, checksum uint32) error {
					persists["bar"]++
					assert.Equal(t, "bar", string(metadata.BytesID()))
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
				Persist: func(metadata persist.Metadata, segment ts.Segment, checksum uint32) error {
					persists["baz"]++
					assert.Equal(t, "baz", string(metadata.BytesID()))
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
				Persist: func(metadata persist.Metadata, segment ts.Segment, checksum uint32) error {
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

		src, err := newPeersSource(opts)
		require.NoError(t, err)

		src.(*peersSource).newPersistManager = func() (persist.Manager, error) {
			return mockPersistManager, nil
		}

		target := result.NewShardTimeRanges().Set(
			0,
			xtime.NewRanges(xtime.Range{Start: start, End: end}),
		).Set(
			1,
			xtime.NewRanges(xtime.Range{Start: start, End: end}),
		)

		tester := bootstrap.BuildNamespacesTester(t, testRunOptsWithPersist, target, testNsMd)
		defer tester.Finish()
		tester.TestReadWith(src)
		tester.TestUnfulfilledForNamespaceIsEmpty(testNsMd)
		assert.Equal(t, map[string]int{
			"foo": 1, "bar": 1, "baz": 1,
		}, persists)

		assert.Equal(t, map[string]int{
			"foo": 1, "bar": 1, "baz": 1, "empty": 1,
		}, closes)

		tester.EnsureNoLoadedBlocks()
		tester.EnsureNoWrites()
	}
}

func TestPeersSourceMarksUnfulfilledOnPersistenceErrors(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := newTestDefaultOpts(t, ctrl).
		SetResultOptions(newTestDefaultOpts(t, ctrl).
			ResultOptions().
			SetSeriesCachePolicy(series.CacheRecentlyRead),
		)
	testNsMd := testNamespaceMetadataNoIndex(t)
	ropts := testNsMd.Options().RetentionOptions()

	start := time.Now().Add(-ropts.RetentionPeriod()).Truncate(ropts.BlockSize())
	midway := start.Add(ropts.BlockSize())
	end := start.Add(2 * ropts.BlockSize())

	type resultsKey struct {
		shard       uint32
		start       int64
		end         int64
		expectedErr bool
	}

	results := make(map[resultsKey]result.ShardResult)
	addResult := func(shard uint32, id string, b block.DatabaseBlock, expectedErr bool) {
		r := result.NewShardResult(opts.ResultOptions())
		r.AddBlock(ident.StringID(id), ident.NewTags(ident.StringTag(id, id)), b)
		start := b.StartTime()
		end := start.Add(ropts.BlockSize())
		results[resultsKey{shard, start.UnixNano(), end.UnixNano(), expectedErr}] = r
	}

	segmentError := errors.New("segment err")

	// foo results
	var fooBlocks [2]block.DatabaseBlock
	fooBlocks[0] = block.NewMockDatabaseBlock(ctrl)
	fooBlocks[0].(*block.MockDatabaseBlock).EXPECT().StartTime().Return(start).AnyTimes()
	fooBlocks[0].(*block.MockDatabaseBlock).EXPECT().Checksum().Return(uint32(0), errors.New("stream err"))
	addResult(0, "foo", fooBlocks[0], true)

	fooBlocks[1] = block.NewDatabaseBlock(midway, ropts.BlockSize(),
		ts.NewSegment(checked.NewBytes([]byte{1, 2, 3}, nil), nil, 1, ts.FinalizeNone),
		testBlockOpts, namespace.Context{})
	addResult(0, "foo", fooBlocks[1], false)

	// bar results
	var barBlocks [2]block.DatabaseBlock
	barBlocks[0] = block.NewMockDatabaseBlock(ctrl)
	barBlocks[0].(*block.MockDatabaseBlock).EXPECT().StartTime().Return(start).AnyTimes()
	barBlocks[0].(*block.MockDatabaseBlock).EXPECT().Checksum().Return(uint32(0), errors.New("stream err"))
	addResult(1, "bar", barBlocks[0], false)

	barBlocks[1] = block.NewDatabaseBlock(midway, ropts.BlockSize(),
		ts.NewSegment(checked.NewBytes([]byte{4, 5, 6}, nil), nil, 2, ts.FinalizeNone),
		testBlockOpts, namespace.Context{})
	addResult(1, "bar", barBlocks[1], false)

	// baz results
	var bazBlocks [2]block.DatabaseBlock
	bazBlocks[0] = block.NewDatabaseBlock(start, ropts.BlockSize(),
		ts.NewSegment(checked.NewBytes([]byte{7, 8, 9}, nil), nil, 3, ts.FinalizeNone),
		testBlockOpts, namespace.Context{})
	addResult(2, "baz", bazBlocks[0], false)

	bazBlocks[1] = block.NewDatabaseBlock(midway, ropts.BlockSize(),
		ts.NewSegment(checked.NewBytes([]byte{10, 11, 12}, nil), nil, 4, ts.FinalizeNone),
		testBlockOpts, namespace.Context{})
	addResult(2, "baz", bazBlocks[1], false)

	// qux results
	var quxBlocks [2]block.DatabaseBlock
	quxBlocks[0] = block.NewDatabaseBlock(start, ropts.BlockSize(),
		ts.NewSegment(checked.NewBytes([]byte{13, 14, 15}, nil), nil, 5, ts.FinalizeNone),
		testBlockOpts, namespace.Context{})
	addResult(3, "qux", quxBlocks[0], false)

	quxBlocks[1] = block.NewDatabaseBlock(midway, ropts.BlockSize(),
		ts.NewSegment(checked.NewBytes([]byte{16, 17, 18}, nil), nil, 6, ts.FinalizeNone),
		testBlockOpts, namespace.Context{})
	addResult(3, "qux", quxBlocks[1], false)

	mockAdminSession := client.NewMockAdminSession(ctrl)

	for key, result := range results {
		mockAdminSession.EXPECT().
			FetchBootstrapBlocksFromPeers(namespace.NewMetadataMatcher(testNsMd),
				key.shard, time.Unix(0, key.start), time.Unix(0, key.end),
				gomock.Any()).
			Return(result, nil)

		peerError := segmentError
		if !key.expectedErr {
			peerError = nil
		}

		peerMetaIter := client.NewMockPeerBlockMetadataIter(ctrl)
		peerMetaIter.EXPECT().Next().Return(false).AnyTimes()
		peerMetaIter.EXPECT().Err().Return(nil).AnyTimes()
		mockAdminSession.EXPECT().
			FetchBootstrapBlocksMetadataFromPeers(testNsMd.ID(),
				key.shard, time.Unix(0, key.start), time.Unix(0, key.end), gomock.Any()).
			Return(peerMetaIter, peerError).AnyTimes()
	}

	mockAdminClient := client.NewMockAdminClient(ctrl)
	mockAdminClient.EXPECT().DefaultAdminSession().
		Return(mockAdminSession, nil).AnyTimes()

	opts = opts.SetAdminClient(mockAdminClient)

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
			Persist: func(metadata persist.Metadata, segment ts.Segment, checksum uint32) error {
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
			Persist: func(metadata persist.Metadata, segment ts.Segment, checksum uint32) error {
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
			Persist: func(metadata persist.Metadata, segment ts.Segment, checksum uint32) error {
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
			Persist: func(metadata persist.Metadata, segment ts.Segment, checksum uint32) error {
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
			Persist: func(metadata persist.Metadata, segment ts.Segment, checksum uint32) error {
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
			Persist: func(metadata persist.Metadata, segment ts.Segment, checksum uint32) error {
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
			Persist: func(metadata persist.Metadata, segment ts.Segment, checksum uint32) error {
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
			Persist: func(metadata persist.Metadata, segment ts.Segment, checksum uint32) error {
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

	src, err := newPeersSource(opts)
	require.NoError(t, err)

	src.(*peersSource).newPersistManager = func() (persist.Manager, error) {
		return mockPersistManager, nil
	}

	target := result.NewShardTimeRanges().Set(
		0,
		xtime.NewRanges(
			xtime.Range{Start: start, End: midway},
			xtime.Range{Start: midway, End: end}),
	).Set(
		1,
		xtime.NewRanges(
			xtime.Range{Start: start, End: midway},
			xtime.Range{Start: midway, End: end}),
	).Set(
		2,
		xtime.NewRanges(
			xtime.Range{Start: start, End: midway},
			xtime.Range{Start: midway, End: end}),
	).Set(
		3,
		xtime.NewRanges(
			xtime.Range{Start: start, End: midway},
			xtime.Range{Start: midway, End: end}),
	)

	tester := bootstrap.BuildNamespacesTester(t, testRunOptsWithPersist, target, testNsMd)
	defer tester.Finish()
	tester.TestReadWith(src)

	expectedRanges := result.NewShardTimeRanges().Set(
		0,
		xtime.NewRanges(xtime.Range{Start: start, End: midway}),
	).Set(
		1,
		xtime.NewRanges(xtime.Range{Start: start, End: midway}),
	).Set(
		2,
		xtime.NewRanges(xtime.Range{Start: start, End: midway}),
	).Set(
		3,
		xtime.NewRanges(xtime.Range{Start: start, End: midway}),
	)

	// NB(bodu): There is no time series data written to disk so all ranges fail to be fulfilled.
	expectedIndexRanges := target

	tester.TestUnfulfilledForNamespace(testNsMd, expectedRanges, expectedIndexRanges)
	assert.Equal(t, map[string]int{
		"foo": 1, "bar": 1, "baz": 2, "qux": 2,
	}, persists)

	assert.Equal(t, map[string]int{
		"foo": 2, "bar": 2, "baz": 2, "qux": 2,
	}, closes)

	tester.EnsureNoLoadedBlocks()
	tester.EnsureNoWrites()
}

func assertBlockChecksum(t *testing.T, expectedChecksum uint32, block block.DatabaseBlock) {
	checksum, err := block.Checksum()
	require.NoError(t, err)
	require.Equal(t, expectedChecksum, checksum)
}
