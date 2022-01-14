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

package storage

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/ts"
	xmetrics "github.com/m3db/m3/src/dbnode/x/metrics"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"
)

type testIncreasingIndex struct {
	created uint64
}

func (i *testIncreasingIndex) nextIndex() uint64 {
	created := atomic.AddUint64(&i.created, 1)
	return created - 1
}

func testDatabaseShard(t *testing.T, opts Options) *dbShard {
	return testDatabaseShardWithIndexFn(t, opts, nil, false)
}

func testDatabaseShardWithIndexFn(
	t *testing.T,
	opts Options,
	idx NamespaceIndex,
	coldWritesEnabled bool,
) *dbShard {
	metadata, err := namespace.NewMetadata(
		defaultTestNs1ID,
		defaultTestNs1Opts.SetColdWritesEnabled(coldWritesEnabled),
	)
	require.NoError(t, err)
	nsReaderMgr := newNamespaceReaderManager(metadata, tally.NoopScope, opts)

	seriesOpts := NewSeriesOptionsFromOptions(opts, defaultTestNs1Opts.RetentionOptions()).
		SetBufferBucketVersionsPool(series.NewBufferBucketVersionsPool(nil)).
		SetBufferBucketPool(series.NewBufferBucketPool(nil)).
		SetColdWritesEnabled(coldWritesEnabled)

	return newDatabaseShard(metadata, 0, nil, nsReaderMgr,
		&testIncreasingIndex{}, idx, true, opts, seriesOpts).(*dbShard)
}

func addMockSeries(ctrl *gomock.Controller, shard *dbShard, id ident.ID, tags ident.Tags, index uint64) *series.MockDatabaseSeries {
	series := series.NewMockDatabaseSeries(ctrl)
	series.EXPECT().ID().Return(id).AnyTimes()
	series.EXPECT().IsEmpty().Return(false).AnyTimes()
	shard.Lock()
	shard.insertNewShardEntryWithLock(NewEntry(NewEntryOptions{
		Series: series,
		Index:  index,
	}))
	shard.Unlock()
	return series
}

func TestShardDontNeedBootstrap(t *testing.T) {
	opts := DefaultTestOptions()
	testNs, closer := newTestNamespace(t)
	defer closer()
	seriesOpts := NewSeriesOptionsFromOptions(opts, testNs.Options().RetentionOptions())
	shard := newDatabaseShard(testNs.metadata, 0, nil, nil,
		&testIncreasingIndex{}, nil, false, opts, seriesOpts).(*dbShard)
	defer shard.Close()

	require.Equal(t, Bootstrapped, shard.bootstrapState)
	require.True(t, shard.IsBootstrapped())
}

func TestShardErrorIfDoubleBootstrap(t *testing.T) {
	opts := DefaultTestOptions()
	testNs, closer := newTestNamespace(t)
	defer closer()
	seriesOpts := NewSeriesOptionsFromOptions(opts, testNs.Options().RetentionOptions())
	shard := newDatabaseShard(testNs.metadata, 0, nil, nil,
		&testIncreasingIndex{}, nil, false, opts, seriesOpts).(*dbShard)
	defer shard.Close()

	require.Equal(t, Bootstrapped, shard.bootstrapState)
	require.True(t, shard.IsBootstrapped())
}

func TestShardBootstrapState(t *testing.T) {
	opts := DefaultTestOptions()
	s := testDatabaseShard(t, opts)
	defer s.Close()

	ctx := context.NewBackground()
	defer ctx.Close()

	nsCtx := namespace.Context{ID: ident.StringID("foo")}
	require.NoError(t, s.Bootstrap(ctx, nsCtx))
	require.Error(t, s.Bootstrap(ctx, nsCtx))
}

func TestShardFlushStateNotStarted(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdir")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	now := xtime.Now()
	nowFn := func() time.Time {
		return now.ToTime()
	}

	opts := DefaultTestOptions()
	fsOpts := opts.CommitLogOptions().FilesystemOptions().
		SetFilePathPrefix(dir)
	opts = opts.
		SetClockOptions(opts.ClockOptions().SetNowFn(nowFn)).
		SetCommitLogOptions(opts.CommitLogOptions().
			SetFilesystemOptions(fsOpts))

	ropts := defaultTestRetentionOpts
	earliest, latest := retention.FlushTimeStart(ropts, now), retention.FlushTimeEnd(ropts, now)

	s := testDatabaseShard(t, opts)
	defer s.Close()

	ctx := context.NewBackground()
	defer ctx.Close()

	nsCtx := namespace.Context{ID: ident.StringID("foo")}
	s.Bootstrap(ctx, nsCtx)

	notStarted := fileOpState{WarmStatus: warmStatus{
		DataFlushed: fileOpNotStarted,
	}}
	for st := earliest; !st.After(latest); st = st.Add(ropts.BlockSize()) {
		flushState, err := s.FlushState(earliest)
		require.NoError(t, err)
		require.Equal(t, notStarted, flushState)
	}
}

// TestShardBootstrapWithFlushVersion ensures that the shard is able to bootstrap
// the cold flush version from the info files.
func TestShardBootstrapWithFlushVersion(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdir")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		opts   = DefaultTestOptions()
		fsOpts = opts.CommitLogOptions().FilesystemOptions().
			SetFilePathPrefix(dir)
		newClOpts = opts.CommitLogOptions().SetFilesystemOptions(fsOpts)
	)
	opts = opts.
		SetCommitLogOptions(newClOpts)

	s := testDatabaseShard(t, opts)
	defer s.Close()

	mockSeriesID := ident.StringID("series-1")
	mockSeries := series.NewMockDatabaseSeries(ctrl)
	mockSeries.EXPECT().ID().Return(mockSeriesID).AnyTimes()
	mockSeries.EXPECT().IsEmpty().Return(false).AnyTimes()
	mockSeries.EXPECT().Bootstrap(gomock.Any())

	// Load the mock into the shard as an expected series so that we can assert
	// on the call to its Bootstrap() method below.
	entry := NewEntry(NewEntryOptions{
		Series: mockSeries,
	})
	s.Lock()
	s.insertNewShardEntryWithLock(entry)
	s.Unlock()

	writer, err := fs.NewWriter(fsOpts)
	require.NoError(t, err)

	var (
		blockSize   = 2 * time.Hour
		start       = xtime.Now().Truncate(blockSize)
		blockStarts = []xtime.UnixNano{start, start.Add(blockSize)}
	)
	for i, blockStart := range blockStarts {
		writer.Open(fs.DataWriterOpenOptions{
			FileSetType: persist.FileSetFlushType,
			Identifier: fs.FileSetFileIdentifier{
				Namespace:   defaultTestNs1ID,
				Shard:       s.ID(),
				BlockStart:  blockStart,
				VolumeIndex: i,
			},
		})
		require.NoError(t, writer.Close())
	}

	ctx := context.NewBackground()
	defer ctx.Close()

	nsCtx := namespace.Context{ID: ident.StringID("foo")}
	err = s.Bootstrap(ctx, nsCtx)
	require.NoError(t, err)

	require.Equal(t, Bootstrapped, s.bootstrapState)

	for i, blockStart := range blockStarts {
		flushState, err := s.FlushState(blockStart)
		require.NoError(t, err)
		require.Equal(t, i, flushState.ColdVersionFlushed)
	}
}

// TestShardBootstrapWithFlushVersionNoCleanUp ensures that the shard is able to
// bootstrap the cold flush version from the info files even if the DB stopped
// before it was able clean up its files. For example, if the DB had volume 0,
// did a cold flush producing volume 1, then terminated before cleaning up the
// files from volume 0, the flush version for that block should be bootstrapped
// to 1.
func TestShardBootstrapWithFlushVersionNoCleanUp(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdir")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		opts      = DefaultTestOptions()
		fsOpts    = opts.CommitLogOptions().FilesystemOptions().SetFilePathPrefix(dir)
		newClOpts = opts.CommitLogOptions().SetFilesystemOptions(fsOpts)
	)
	opts = opts.
		SetCommitLogOptions(newClOpts)

	s := testDatabaseShard(t, opts)
	defer s.Close()

	writer, err := fs.NewWriter(fsOpts)
	require.NoError(t, err)

	var (
		blockSize  = 2 * time.Hour
		start      = xtime.Now().Truncate(blockSize)
		numVolumes = 3
	)
	for i := 0; i < numVolumes; i++ {
		writer.Open(fs.DataWriterOpenOptions{
			FileSetType: persist.FileSetFlushType,
			Identifier: fs.FileSetFileIdentifier{
				Namespace:   defaultTestNs1ID,
				Shard:       s.ID(),
				BlockStart:  start,
				VolumeIndex: i,
			},
		})
		require.NoError(t, writer.Close())
	}

	ctx := context.NewBackground()
	defer ctx.Close()

	nsCtx := namespace.Context{ID: ident.StringID("foo")}
	err = s.Bootstrap(ctx, nsCtx)
	require.NoError(t, err)
	require.Equal(t, Bootstrapped, s.bootstrapState)

	flushState, err := s.FlushState(start)
	require.NoError(t, err)
	require.Equal(t, numVolumes-1, flushState.ColdVersionFlushed)
}

// TestShardBootstrapWithCacheShardIndices ensures that the shard is able to bootstrap
// and call CacheShardIndices if a BlockRetrieverManager is present.
func TestShardBootstrapWithCacheShardIndices(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdir")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		opts          = DefaultTestOptions()
		fsOpts        = opts.CommitLogOptions().FilesystemOptions().SetFilePathPrefix(dir)
		newClOpts     = opts.CommitLogOptions().SetFilesystemOptions(fsOpts)
		mockRetriever = block.NewMockDatabaseBlockRetriever(ctrl)
	)
	opts = opts.SetCommitLogOptions(newClOpts)

	s := testDatabaseShard(t, opts)
	defer s.Close()
	mockRetriever.EXPECT().CacheShardIndices([]uint32{s.ID()}).Return(nil)
	s.setBlockRetriever(mockRetriever)

	ctx := context.NewBackground()
	defer ctx.Close()

	nsCtx := namespace.Context{ID: ident.StringID("foo")}
	err = s.Bootstrap(ctx, nsCtx)
	require.NoError(t, err)
	require.Equal(t, Bootstrapped, s.bootstrapState)
}

func TestShardFlushDuringBootstrap(t *testing.T) {
	s := testDatabaseShard(t, DefaultTestOptions())
	defer s.Close()
	s.bootstrapState = Bootstrapping
	err := s.WarmFlush(xtime.Now(), nil, namespace.Context{})
	require.Equal(t, err, errShardNotBootstrappedToFlush)
}

func TestShardLoadLimitEnforcedIfSet(t *testing.T) {
	testShardLoadLimit(t, 1, true)
}

func TestShardLoadLimitNotEnforcedIfNotSet(t *testing.T) {
	testShardLoadLimit(t, 0, false)
}

func testShardLoadLimit(t *testing.T, limit int64, shouldReturnError bool) {
	var (
		memTrackerOptions = NewMemoryTrackerOptions(limit)
		memTracker        = NewMemoryTracker(memTrackerOptions)
		opts              = DefaultTestOptions().SetMemoryTracker(memTracker)
		s                 = testDatabaseShard(t, opts)
		blOpts            = opts.DatabaseBlockOptions()
		testBlockSize     = 2 * time.Hour
		start             = xtime.Now().Truncate(testBlockSize)
		threeBytes        = checked.NewBytes([]byte("123"), nil)

		sr      = result.NewShardResult(result.NewOptions())
		fooTags = ident.NewTags(ident.StringTag("foo", "foe"))
		barTags = ident.NewTags(ident.StringTag("bar", "baz"))
	)
	defer s.Close()
	threeBytes.IncRef()
	blocks := []block.DatabaseBlock{
		block.NewDatabaseBlock(
			start, testBlockSize, ts.Segment{Head: threeBytes},
			blOpts, namespace.Context{},
		),
		block.NewDatabaseBlock(
			start.Add(1*testBlockSize), testBlockSize, ts.Segment{Tail: threeBytes},
			blOpts, namespace.Context{},
		),
	}

	sr.AddBlock(ident.StringID("foo"), fooTags, blocks[0])
	sr.AddBlock(ident.StringID("bar"), barTags, blocks[1])

	seriesMap := sr.AllSeries()

	ctx := context.NewBackground()
	defer ctx.Close()

	nsCtx := namespace.Context{ID: ident.StringID("foo")}
	require.NoError(t, s.Bootstrap(ctx, nsCtx))

	// First load will never trigger the limit.
	require.NoError(t, s.LoadBlocks(seriesMap))

	if shouldReturnError {
		require.Error(t, s.LoadBlocks(seriesMap))
	} else {
		require.NoError(t, s.LoadBlocks(seriesMap))
	}
}

func TestShardFlushSeriesFlushError(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	blockStart := xtime.FromSeconds(21600)

	s := testDatabaseShard(t, DefaultTestOptions())
	defer s.Close()

	ctx := context.NewBackground()
	defer ctx.Close()

	nsCtx := namespace.Context{ID: ident.StringID("foo")}
	s.Bootstrap(ctx, nsCtx)

	s.flushState.statesByTime[blockStart] = fileOpState{
		WarmStatus: warmStatus{
			DataFlushed: fileOpNotStarted,
		},
		NumFailures: 0,
	}

	var closed bool
	flush := persist.NewMockFlushPreparer(ctrl)
	prepared := persist.PreparedDataPersist{
		Persist: func(persist.Metadata, ts.Segment, uint32) error { return nil },
		Close:   func() error { closed = true; return nil },
	}
	prepareOpts := xtest.CmpMatcher(persist.DataPrepareOptions{
		NamespaceMetadata: s.namespace,
		Shard:             s.shard,
		BlockStart:        blockStart,
	})
	flush.EXPECT().PrepareData(prepareOpts).Return(prepared, nil)

	flushed := make(map[int]struct{})
	for i := 0; i < 2; i++ {
		i := i
		var expectedErr error
		if i == 1 {
			expectedErr = errors.New("error bar")
		}
		curr := series.NewMockDatabaseSeries(ctrl)
		curr.EXPECT().ID().Return(ident.StringID("foo" + strconv.Itoa(i))).AnyTimes()
		curr.EXPECT().IsEmpty().Return(false).AnyTimes()
		curr.EXPECT().
			WarmFlush(gomock.Any(), blockStart, gomock.Any(), gomock.Any()).
			Do(func(context.Context, xtime.UnixNano, persist.DataFn, namespace.Context) {
				flushed[i] = struct{}{}
			}).
			Return(series.FlushOutcomeErr, expectedErr)
		s.list.PushBack(NewEntry(NewEntryOptions{
			Series: curr,
		}))
	}

	flushErr := s.WarmFlush(blockStart, flush, namespace.Context{})

	require.Equal(t, len(flushed), 2)
	for i := 0; i < 2; i++ {
		_, ok := flushed[i]
		require.True(t, ok)
	}

	require.True(t, closed)
	require.NotNil(t, flushErr)
	require.Equal(t, "error bar", flushErr.Error())

	flushState, err := s.FlushState(blockStart)
	require.NoError(t, err)
	require.Equal(t, fileOpState{
		WarmStatus: warmStatus{
			DataFlushed: fileOpFailed,
		},
		NumFailures: 1,
	}, flushState)
}

func TestShardFlushSeriesFlushSuccess(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	blockStart := xtime.FromSeconds(21600)
	now := xtime.Now()
	nowFn := func() time.Time {
		return now.ToTime()
	}
	opts := DefaultTestOptions()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))

	s := testDatabaseShard(t, opts)
	defer s.Close()

	ctx := context.NewBackground()
	defer ctx.Close()

	nsCtx := namespace.Context{ID: ident.StringID("foo")}
	s.Bootstrap(ctx, nsCtx)

	s.flushState.statesByTime[blockStart] = fileOpState{
		WarmStatus: warmStatus{
			DataFlushed: fileOpNotStarted,
		},
		NumFailures: 0,
	}

	var closed bool
	flush := persist.NewMockFlushPreparer(ctrl)
	prepared := persist.PreparedDataPersist{
		Persist: func(persist.Metadata, ts.Segment, uint32) error { return nil },
		Close:   func() error { closed = true; return nil },
	}

	prepareOpts := xtest.CmpMatcher(persist.DataPrepareOptions{
		NamespaceMetadata: s.namespace,
		Shard:             s.shard,
		BlockStart:        blockStart,
	})
	flush.EXPECT().PrepareData(prepareOpts).Return(prepared, nil)

	flushed := make(map[int]struct{})
	for i := 0; i < 2; i++ {
		i := i
		curr := series.NewMockDatabaseSeries(ctrl)
		curr.EXPECT().ID().Return(ident.StringID("foo" + strconv.Itoa(i))).AnyTimes()
		curr.EXPECT().IsEmpty().Return(false).AnyTimes()
		curr.EXPECT().
			WarmFlush(gomock.Any(), blockStart, gomock.Any(), gomock.Any()).
			Do(func(context.Context, xtime.UnixNano, persist.DataFn, namespace.Context) {
				flushed[i] = struct{}{}
			}).
			Return(series.FlushOutcomeFlushedToDisk, nil)
		s.list.PushBack(NewEntry(NewEntryOptions{
			Series: curr,
		}))
	}

	err := s.WarmFlush(blockStart, flush, namespace.Context{})

	require.Equal(t, len(flushed), 2)
	for i := 0; i < 2; i++ {
		_, ok := flushed[i]
		require.True(t, ok)
	}

	require.True(t, closed)
	require.Nil(t, err)

	// State not yet updated since an explicit call to MarkWarmFlushStateSuccessOrError is required.
	flushState, err := s.FlushState(blockStart)
	require.NoError(t, err)
	require.Equal(t, fileOpState{
		WarmStatus: warmStatus{
			DataFlushed: fileOpSuccess,
		},
		ColdVersionRetrievable: 0,
		NumFailures:            0,
	}, flushState)
}

type testDirtySeries struct {
	id         ident.ID
	dirtyTimes []xtime.UnixNano
}

func optimizedTimesFromTimes(times []xtime.UnixNano) series.OptimizedTimes {
	var ret series.OptimizedTimes
	for _, t := range times {
		ret.Add(t)
	}
	return ret
}

func TestShardColdFlush(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdir")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	now := xtime.Now()
	nowFn := func() time.Time {
		return now.ToTime()
	}
	opts := DefaultTestOptions()
	fsOpts := opts.CommitLogOptions().FilesystemOptions().
		SetFilePathPrefix(dir)
	opts = opts.
		SetClockOptions(opts.ClockOptions().SetNowFn(nowFn)).
		SetCommitLogOptions(opts.CommitLogOptions().
			SetFilesystemOptions(fsOpts))

	blockSize := opts.SeriesOptions().RetentionOptions().BlockSize()
	shard := testDatabaseShard(t, opts)

	ctx := context.NewBackground()
	defer ctx.Close()

	nsCtx := namespace.Context{ID: ident.StringID("foo")}
	require.NoError(t, shard.Bootstrap(ctx, nsCtx))
	shard.newMergerFn = newMergerTestFn
	shard.newFSMergeWithMemFn = newFSMergeWithMemTestFn

	t0 := now.Truncate(blockSize).Add(-10 * blockSize)
	t1 := t0.Add(1 * blockSize)
	t2 := t0.Add(2 * blockSize)
	t3 := t0.Add(3 * blockSize)
	t4 := t0.Add(4 * blockSize)
	t5 := t0.Add(5 * blockSize)
	t6 := t0.Add(6 * blockSize)
	t7 := t0.Add(7 * blockSize)
	// Mark t0-t6 (not t7) as having been warm flushed. Cold flushes can only
	// happen after a successful warm flush because warm flushes currently don't
	// have merging logic. This means that all blocks except t7 should
	// successfully cold flush.
	shard.markWarmDataFlushStateSuccess(t0)
	shard.markWarmDataFlushStateSuccess(t1)
	shard.markWarmDataFlushStateSuccess(t2)
	shard.markWarmDataFlushStateSuccess(t3)
	shard.markWarmDataFlushStateSuccess(t4)
	shard.markWarmDataFlushStateSuccess(t5)
	shard.markWarmDataFlushStateSuccess(t6)

	dirtyData := []testDirtySeries{
		{id: ident.StringID("id0"), dirtyTimes: []xtime.UnixNano{t0, t2, t3, t4}},
		{id: ident.StringID("id1"), dirtyTimes: []xtime.UnixNano{t1}},
		{id: ident.StringID("id2"), dirtyTimes: []xtime.UnixNano{t3, t4, t5}},
		{id: ident.StringID("id3"), dirtyTimes: []xtime.UnixNano{t6, t7}},
	}
	for _, ds := range dirtyData {
		curr := series.NewMockDatabaseSeries(ctrl)
		curr.EXPECT().ID().Return(ds.id).AnyTimes()
		curr.EXPECT().Metadata().Return(doc.Metadata{ID: ds.id.Bytes()}).AnyTimes()
		curr.EXPECT().ColdFlushBlockStarts(gomock.Any()).
			Return(optimizedTimesFromTimes(ds.dirtyTimes))
		shard.list.PushBack(NewEntry(NewEntryOptions{
			Series: curr,
		}))
	}

	preparer := persist.NewMockFlushPreparer(ctrl)
	fsReader := fs.NewMockDataFileSetReader(ctrl)
	resources := coldFlushReusableResources{
		dirtySeries:        newDirtySeriesMap(),
		dirtySeriesToWrite: make(map[xtime.UnixNano]*idList),
		idElementPool:      newIDElementPool(nil),
		fsReader:           fsReader,
	}

	// Assert that flush state cold versions all start at 0.
	for i := t0; i.Before(t7.Add(blockSize)); i = i.Add(blockSize) {
		coldVersion, err := shard.RetrievableBlockColdVersion(i)
		require.NoError(t, err)
		require.Equal(t, 0, coldVersion)
	}
	shardColdFlush, err := shard.ColdFlush(preparer, resources, nsCtx, &persist.NoOpColdFlushNamespace{})
	require.NoError(t, err)
	require.NoError(t, shardColdFlush.Done())
	// After a cold flush, t0-t6 previously dirty block starts should be updated
	// to version 1.
	for i := t0; i.Before(t6.Add(blockSize)); i = i.Add(blockSize) {
		coldVersion, err := shard.RetrievableBlockColdVersion(i)
		require.NoError(t, err)
		require.Equal(t, 1, coldVersion)
	}
	// t7 shouldn't be cold flushed because it hasn't been warm flushed.
	coldVersion, err := shard.RetrievableBlockColdVersion(t7)
	require.NoError(t, err)
	require.Equal(t, 0, coldVersion)
}

func TestShardColdFlushNoMergeIfNothingDirty(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	now := xtime.Now()
	nowFn := func() time.Time {
		return now.ToTime()
	}
	opts := DefaultTestOptions()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))
	blockSize := opts.SeriesOptions().RetentionOptions().BlockSize()
	shard := testDatabaseShard(t, opts)

	ctx := context.NewBackground()
	defer ctx.Close()

	nsCtx := namespace.Context{ID: ident.StringID("foo")}
	require.NoError(t, shard.Bootstrap(ctx, nsCtx))

	shard.newMergerFn = newMergerTestFn
	shard.newFSMergeWithMemFn = newFSMergeWithMemTestFn

	t0 := now.Truncate(blockSize).Add(-10 * blockSize)
	t1 := t0.Add(1 * blockSize)
	t2 := t0.Add(2 * blockSize)
	t3 := t0.Add(3 * blockSize)
	shard.markWarmDataFlushStateSuccess(t0)
	shard.markWarmDataFlushStateSuccess(t1)
	shard.markWarmDataFlushStateSuccess(t2)
	shard.markWarmDataFlushStateSuccess(t3)

	preparer := persist.NewMockFlushPreparer(ctrl)
	fsReader := fs.NewMockDataFileSetReader(ctrl)
	idElementPool := newIDElementPool(nil)

	// Pretend that dirtySeriesToWrite has been used previously, leaving
	// behind empty idLists at some block starts. This is desired behavior since
	// we don't want to reallocate new idLists for the same block starts when we
	// process a different shard.
	dirtySeriesToWrite := make(map[xtime.UnixNano]*idList)
	dirtySeriesToWrite[t0] = newIDList(idElementPool)
	dirtySeriesToWrite[t1] = newIDList(idElementPool)
	dirtySeriesToWrite[t2] = newIDList(idElementPool)
	dirtySeriesToWrite[t3] = newIDList(idElementPool)

	resources := coldFlushReusableResources{
		dirtySeries:        newDirtySeriesMap(),
		dirtySeriesToWrite: dirtySeriesToWrite,
		idElementPool:      idElementPool,
		fsReader:           fsReader,
	}

	shardColdFlush, err := shard.ColdFlush(preparer, resources, nsCtx, &persist.NoOpColdFlushNamespace{})
	require.NoError(t, err)
	require.NoError(t, shardColdFlush.Done())
	// After a cold flush, t0-t3 should remain version 0, since nothing should
	// actually be merged.
	for i := t0; i.Before(t3.Add(blockSize)); i = i.Add(blockSize) {
		coldVersion, err := shard.RetrievableBlockColdVersion(i)
		require.NoError(t, err)
		assert.Equal(t, 0, coldVersion)
	}
}

func newMergerTestFn(
	_ fs.DataFileSetReader,
	_ int,
	_ xio.SegmentReaderPool,
	_ encoding.MultiReaderIteratorPool,
	_ ident.Pool,
	_ encoding.EncoderPool,
	_ context.Pool,
	_ string,
	_ namespace.Options,
) fs.Merger {
	return &noopMerger{}
}

type noopMerger struct{}

func (m *noopMerger) Merge(
	_ fs.FileSetFileIdentifier,
	_ fs.MergeWith,
	_ int,
	_ persist.FlushPreparer,
	_ namespace.Context,
	_ persist.OnFlushSeries,
) (persist.DataCloser, error) {
	closer := func() error { return nil }
	return closer, nil
}

func (m *noopMerger) MergeAndCleanup(
	_ fs.FileSetFileIdentifier,
	_ fs.MergeWith,
	_ int,
	_ persist.FlushPreparer,
	_ namespace.Context,
	_ persist.OnFlushSeries,
	_ bool,
) error {
	return nil
}

func newFSMergeWithMemTestFn(
	_ databaseShard,
	_ series.QueryableBlockRetriever,
	_ *dirtySeriesMap,
	_ map[xtime.UnixNano]*idList,
) fs.MergeWith {
	return fs.NewNoopMergeWith()
}

func TestShardSnapshotShardNotBootstrapped(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	blockStart := xtime.FromSeconds(21600)

	s := testDatabaseShard(t, DefaultTestOptions())
	defer s.Close()
	s.bootstrapState = Bootstrapping

	snapshotPreparer := persist.NewMockSnapshotPreparer(ctrl)
	_, err := s.Snapshot(blockStart, blockStart, snapshotPreparer, namespace.Context{})
	require.Equal(t, errShardNotBootstrappedToSnapshot, err)
}

func TestShardSnapshotSeriesSnapshotSuccess(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	blockStart := xtime.FromSeconds(21600)

	s := testDatabaseShard(t, DefaultTestOptions())
	defer s.Close()
	s.bootstrapState = Bootstrapped

	var closed bool
	snapshotPreparer := persist.NewMockSnapshotPreparer(ctrl)
	prepared := persist.PreparedDataPersist{
		Persist: func(persist.Metadata, ts.Segment, uint32) error { return nil },
		Close:   func() error { closed = true; return nil },
	}

	prepareOpts := xtest.CmpMatcher(persist.DataPrepareOptions{
		NamespaceMetadata: s.namespace,
		Shard:             s.shard,
		BlockStart:        blockStart,
		FileSetType:       persist.FileSetSnapshotType,
		Snapshot: persist.DataPrepareSnapshotOptions{
			SnapshotTime: blockStart,
		},
	})
	snapshotPreparer.EXPECT().PrepareData(prepareOpts).Return(prepared, nil)

	snapshotted := make(map[int]struct{})
	for i := 0; i < 2; i++ {
		i := i
		entry := series.NewMockDatabaseSeries(ctrl)
		entry.EXPECT().ID().Return(ident.StringID("foo" + strconv.Itoa(i))).AnyTimes()
		entry.EXPECT().IsEmpty().Return(false).AnyTimes()
		entry.EXPECT().IsBufferEmptyAtBlockStart(blockStart).Return(false).AnyTimes()
		entry.EXPECT().
			Snapshot(gomock.Any(), blockStart, gomock.Any(), gomock.Any()).
			Do(func(context.Context, xtime.UnixNano, persist.DataFn, namespace.Context) {
				snapshotted[i] = struct{}{}
			}).
			Return(series.SnapshotResult{}, nil)
		s.list.PushBack(NewEntry(NewEntryOptions{
			Series: entry,
		}))
	}

	_, err := s.Snapshot(blockStart, blockStart, snapshotPreparer, namespace.Context{})
	require.Equal(t, len(snapshotted), 2)
	for i := 0; i < 2; i++ {
		_, ok := snapshotted[i]
		require.True(t, ok)
	}

	require.True(t, closed)
	require.Nil(t, err)
}

func addMockTestSeries(ctrl *gomock.Controller, shard *dbShard, id ident.ID) *series.MockDatabaseSeries {
	series := series.NewMockDatabaseSeries(ctrl)
	series.EXPECT().ID().AnyTimes().Return(id)
	shard.Lock()
	shard.insertNewShardEntryWithLock(NewEntry(NewEntryOptions{
		Series: series,
	}))
	shard.Unlock()
	return series
}

func addTestSeries(shard *dbShard, id ident.ID) series.DatabaseSeries {
	return addTestSeriesWithCount(shard, id, 0)
}

func addTestSeriesWithCount(shard *dbShard, id ident.ID, count int32) series.DatabaseSeries {
	seriesEntry := series.NewDatabaseSeries(series.DatabaseSeriesOptions{
		ID:          id,
		UniqueIndex: 1,
		Options:     shard.seriesOpts,
	})
	shard.Lock()
	entry := NewEntry(NewEntryOptions{
		Series: seriesEntry,
	})
	for i := int32(0); i < count; i++ {
		entry.IncrementReaderWriterCount()
	}
	shard.insertNewShardEntryWithLock(entry)
	shard.Unlock()
	return seriesEntry
}

func writeShardAndVerify(
	ctx context.Context,
	t *testing.T,
	shard *dbShard,
	id string,
	now xtime.UnixNano,
	value float64,
	expectedShouldWrite bool,
	expectedIdx uint64,
) {
	seriesWrite, err := shard.Write(ctx, ident.StringID(id),
		now, value, xtime.Second, nil, series.WriteOptions{})
	assert.NoError(t, err)
	assert.Equal(t, expectedShouldWrite, seriesWrite.WasWritten)
	assert.Equal(t, id, seriesWrite.Series.ID.String())
	assert.Equal(t, "testns1", seriesWrite.Series.Namespace.String())
	assert.Equal(t, expectedIdx, seriesWrite.Series.UniqueIndex)
}

func TestShardTick(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdir")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	now := xtime.Now()
	nowLock := sync.RWMutex{}
	nowFn := func() xtime.UnixNano {
		nowLock.RLock()
		value := now
		nowLock.RUnlock()
		return value
	}
	clockFn := func() time.Time {
		nowLock.RLock()
		value := now
		nowLock.RUnlock()
		return value.ToTime()
	}
	setNow := func(t xtime.UnixNano) {
		nowLock.Lock()
		now = t
		nowLock.Unlock()
	}

	opts := DefaultTestOptions()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(clockFn))

	fsOpts := opts.CommitLogOptions().FilesystemOptions().
		SetFilePathPrefix(dir)
	opts = opts.
		SetCommitLogOptions(opts.CommitLogOptions().
			SetFilesystemOptions(fsOpts))

	earliestFlush := retention.FlushTimeStart(defaultTestRetentionOpts, now)
	beforeEarliestFlush := earliestFlush.Add(-defaultTestRetentionOpts.BlockSize())

	sleepPerSeries := time.Microsecond

	ctx := context.NewBackground()
	defer ctx.Close()

	shard := testDatabaseShard(t, opts)
	nsCtx := namespace.Context{ID: ident.StringID("foo")}
	shard.Bootstrap(ctx, nsCtx)
	shard.SetRuntimeOptions(runtime.NewOptions().
		SetTickPerSeriesSleepDuration(sleepPerSeries).
		SetTickSeriesBatchSize(1))
	retriever := series.NewMockQueryableBlockRetriever(ctrl)
	retriever.EXPECT().IsBlockRetrievable(gomock.Any()).Return(false, nil).AnyTimes()
	shard.seriesBlockRetriever = retriever
	defer shard.Close()

	// Also check that it expires flush states by time
	shard.flushState.statesByTime[earliestFlush] = fileOpState{
		WarmStatus: warmStatus{
			DataFlushed: fileOpSuccess,
		},
	}
	shard.flushState.statesByTime[beforeEarliestFlush] = fileOpState{
		WarmStatus: warmStatus{
			DataFlushed: fileOpSuccess,
		},
	}
	assert.Equal(t, 2, len(shard.flushState.statesByTime))

	var slept time.Duration
	shard.sleepFn = func(t time.Duration) {
		slept += t
		setNow(nowFn().Add(t))
	}

	writeShardAndVerify(ctx, t, shard, "foo", nowFn(), 1.0, true, 0)
	// same time, different value should write
	writeShardAndVerify(ctx, t, shard, "foo", nowFn(), 2.0, true, 0)

	writeShardAndVerify(ctx, t, shard, "bar", nowFn(), 2.0, true, 1)
	// same tme, same value should not write
	writeShardAndVerify(ctx, t, shard, "bar", nowFn(), 2.0, false, 1)

	writeShardAndVerify(ctx, t, shard, "baz", nowFn(), 3.0, true, 2)
	// different time, same value should write
	writeShardAndVerify(ctx, t, shard, "baz", nowFn().Add(1), 3.0, true, 2)

	// same time, same value should not write, regardless of being out of order
	writeShardAndVerify(ctx, t, shard, "foo", nowFn(), 2.0, false, 0)

	r, err := shard.Tick(context.NewNoOpCanncellable(), nowFn(), namespace.Context{})
	require.NoError(t, err)
	require.Equal(t, 3, r.activeSeries)
	require.Equal(t, 0, r.expiredSeries)
	require.Equal(t, 2*sleepPerSeries, slept) // Never sleeps on the first series

	// Ensure flush states by time was expired correctly
	require.Equal(t, 1, len(shard.flushState.statesByTime))
	_, ok := shard.flushState.statesByTime[earliestFlush]
	require.True(t, ok)
}

type testWrite struct {
	id         string
	value      float64
	unit       xtime.Unit
	annotation []byte
}

func TestShardWriteAsync(t *testing.T) {
	testShardWriteAsync(t, []testWrite{
		{
			id:    "foo",
			value: 1.0,
			unit:  xtime.Second,
		},
		{
			id:    "bar",
			value: 2.0,
			unit:  xtime.Second,
		},
		{
			id:    "baz",
			value: 3.0,
			unit:  xtime.Second,
		},
	})
}

func TestShardWriteAsyncWithAnnotations(t *testing.T) {
	testShardWriteAsync(t, []testWrite{
		{
			id:         "foo",
			value:      1.0,
			unit:       xtime.Second,
			annotation: []byte("annotation1"),
		},
		{
			id:         "bar",
			value:      2.0,
			unit:       xtime.Second,
			annotation: []byte("annotation2"),
		},
		{
			id:         "baz",
			value:      3.0,
			unit:       xtime.Second,
			annotation: []byte("annotation3"),
		},
	})
}

func testShardWriteAsync(t *testing.T, writes []testWrite) {
	dir, err := ioutil.TempDir("", "testdir")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	testReporter := xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())
	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		Reporter: testReporter,
	}, 100*time.Millisecond)
	defer closer.Close()

	now := xtime.Now()
	nowLock := sync.RWMutex{}
	nowFn := func() time.Time {
		nowLock.RLock()
		value := now
		nowLock.RUnlock()
		return value.ToTime()
	}
	setNow := func(t time.Time) {
		nowLock.Lock()
		now = xtime.ToUnixNano(t)
		nowLock.Unlock()
	}

	mockBytesPool := pool.NewMockCheckedBytesPool(ctrl)
	for _, write := range writes {
		if write.annotation != nil {
			mockBytes := checked.NewMockBytes(ctrl)
			mockBytes.EXPECT().IncRef()
			mockBytes.EXPECT().AppendAll(write.annotation)
			mockBytes.EXPECT().Bytes()
			mockBytes.EXPECT().DecRef()
			mockBytes.EXPECT().Finalize()

			mockBytesPool.
				EXPECT().
				Get(gomock.Any()).
				Return(mockBytes)
		}
	}

	opts := DefaultTestOptions().
		SetBytesPool(mockBytesPool)
	fsOpts := opts.CommitLogOptions().FilesystemOptions().
		SetFilePathPrefix(dir)
	opts = opts.
		SetInstrumentOptions(
			opts.InstrumentOptions().
				SetMetricsScope(scope).
				SetReportInterval(100 * time.Millisecond)).
		SetClockOptions(
			opts.ClockOptions().SetNowFn(nowFn)).
		SetCommitLogOptions(opts.CommitLogOptions().
			SetFilesystemOptions(fsOpts))

	earliestFlush := retention.FlushTimeStart(defaultTestRetentionOpts, now)
	beforeEarliestFlush := earliestFlush.Add(-defaultTestRetentionOpts.BlockSize())

	sleepPerSeries := time.Microsecond

	ctx := context.NewBackground()
	defer ctx.Close()

	shard := testDatabaseShard(t, opts)
	nsCtx := namespace.Context{ID: ident.StringID("foo")}
	shard.Bootstrap(ctx, nsCtx)
	shard.SetRuntimeOptions(runtime.NewOptions().
		SetWriteNewSeriesAsync(true).
		SetTickPerSeriesSleepDuration(sleepPerSeries).
		SetTickSeriesBatchSize(1))
	retriever := series.NewMockQueryableBlockRetriever(ctrl)
	retriever.EXPECT().IsBlockRetrievable(gomock.Any()).Return(false, nil).AnyTimes()
	shard.seriesBlockRetriever = retriever
	defer shard.Close()

	// Also check that it expires flush states by time
	shard.flushState.statesByTime[earliestFlush] = fileOpState{
		WarmStatus: warmStatus{
			DataFlushed: fileOpSuccess,
		},
	}
	shard.flushState.statesByTime[beforeEarliestFlush] = fileOpState{
		WarmStatus: warmStatus{
			DataFlushed: fileOpSuccess,
		},
	}
	assert.Equal(t, 2, len(shard.flushState.statesByTime))

	var slept time.Duration
	shard.sleepFn = func(t time.Duration) {
		slept += t
		setNow(nowFn().Add(t))
	}

	for _, write := range writes {
		_, err := shard.Write(ctx, ident.StringID(write.id), xtime.ToUnixNano(nowFn()),
			write.value, write.unit, write.annotation, series.WriteOptions{})
		require.NoError(t, err)
	}

	for {
		counter, ok := testReporter.Counters()["dbshard.insert-queue.inserts"]
		if ok && counter == int64(len(writes)) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	r, err := shard.Tick(context.NewNoOpCanncellable(), xtime.ToUnixNano(nowFn()), namespace.Context{})
	require.NoError(t, err)
	require.Equal(t, len(writes), r.activeSeries)
	require.Equal(t, 0, r.expiredSeries)
	require.Equal(t, 2*sleepPerSeries, slept) // Never sleeps on the first series

	// Ensure flush states by time was expired correctly
	require.Equal(t, 1, len(shard.flushState.statesByTime))
	_, ok := shard.flushState.statesByTime[earliestFlush]
	require.True(t, ok)

	// Verify the documents in the shard's series are present.
	for _, w := range writes {
		doc, exists, err := shard.DocRef(ident.StringID(w.id))
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, w.id, string(doc.ID))
	}
	document, exists, err := shard.DocRef(ident.StringID("NOT_PRESENT_ID"))
	require.NoError(t, err)
	require.False(t, exists)
	require.Equal(t, doc.Metadata{}, document)
}

// This tests a race in shard ticking with an empty series pending expiration.
func TestShardTickRace(t *testing.T) {
	opts := DefaultTestOptions()
	shard := testDatabaseShard(t, opts)
	defer shard.Close()

	ctx := context.NewBackground()
	defer ctx.Close()

	nsCtx := namespace.Context{ID: ident.StringID("foo")}
	shard.Bootstrap(ctx, nsCtx)

	addTestSeries(shard, ident.StringID("foo"))

	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		shard.Tick(context.NewNoOpCanncellable(), xtime.Now(), namespace.Context{}) // nolint
		wg.Done()
	}()

	go func() {
		shard.Tick(context.NewNoOpCanncellable(), xtime.Now(), namespace.Context{}) // nolint
		wg.Done()
	}()

	wg.Wait()

	shard.RLock()
	shardlen := shard.lookup.Len()
	shard.RUnlock()

	require.Equal(t, 0, shardlen)
}

// Catches a logic bug we had trying to purgeSeries and counted the reference
// we had while trying to purge as a concurrent read.
func TestShardTickCleanupSmallBatchSize(t *testing.T) {
	opts := DefaultTestOptions()

	ctx := context.NewBackground()
	defer ctx.Close()

	shard := testDatabaseShard(t, opts)
	nsCtx := namespace.Context{ID: ident.StringID("foo")}
	shard.Bootstrap(ctx, nsCtx)

	addTestSeries(shard, ident.StringID("foo"))
	_, err := shard.Tick(context.NewNoOpCanncellable(), xtime.Now(), namespace.Context{})
	require.NoError(t, err)
	require.Equal(t, 0, shard.lookup.Len())
}

// This tests ensures the shard returns an error if two ticks are triggered concurrently.
func TestShardReturnsErrorForConcurrentTicks(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdir")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := DefaultTestOptions()
	fsOpts := opts.CommitLogOptions().FilesystemOptions().
		SetFilePathPrefix(dir)
	opts = opts.
		SetCommitLogOptions(opts.CommitLogOptions().
			SetFilesystemOptions(fsOpts))

	ctx := context.NewBackground()
	defer ctx.Close()

	shard := testDatabaseShard(t, opts)
	nsCtx := namespace.Context{ID: ident.StringID("foo")}
	shard.Bootstrap(ctx, nsCtx)
	shard.currRuntimeOptions.tickSleepSeriesBatchSize = 1
	shard.currRuntimeOptions.tickSleepPerSeries = time.Millisecond

	var (
		foo     = addMockTestSeries(ctrl, shard, ident.StringID("foo"))
		tick1Wg sync.WaitGroup
		tick2Wg sync.WaitGroup
		closeWg sync.WaitGroup
	)

	tick1Wg.Add(1)
	tick2Wg.Add(1)
	closeWg.Add(2)

	// wait to return the other tick has returned error
	foo.EXPECT().Tick(gomock.Any(), gomock.Any()).Do(func(interface{}, interface{}) {
		tick1Wg.Done()
		tick2Wg.Wait()
	}).Return(series.TickResult{}, nil)

	go func() {
		_, err := shard.Tick(context.NewNoOpCanncellable(), xtime.Now(), namespace.Context{})
		if err != nil {
			panic(err)
		}
		closeWg.Done()
	}()

	go func() {
		tick1Wg.Wait()
		_, err := shard.Tick(context.NewNoOpCanncellable(), xtime.Now(), namespace.Context{})
		require.Error(t, err)
		tick2Wg.Done()
		closeWg.Done()
	}()

	closeWg.Wait()
}

// This tests ensures the resources held in series contained in the shard are released
// when closing the shard.
func TestShardTicksWhenClosed(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := DefaultTestOptions()
	shard := testDatabaseShard(t, opts)
	s := addMockTestSeries(ctrl, shard, ident.StringID("foo"))

	gomock.InOrder(
		s.EXPECT().IsEmpty().Return(true),
		s.EXPECT().Close(),
	)
	require.NoError(t, shard.Close())
}

// This tests ensures the shard terminates Ticks when closing.
func TestShardTicksStopWhenClosing(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := DefaultTestOptions()
	shard := testDatabaseShard(t, opts)
	shard.currRuntimeOptions.tickSleepSeriesBatchSize = 1
	shard.currRuntimeOptions.tickSleepPerSeries = time.Millisecond

	var (
		foo     = addMockTestSeries(ctrl, shard, ident.StringID("foo"))
		bar     = addMockTestSeries(ctrl, shard, ident.StringID("bar"))
		closeWg sync.WaitGroup
		orderWg sync.WaitGroup
	)

	orderWg.Add(1)
	gomock.InOrder(
		// loop until the shard is marked for Closing
		foo.EXPECT().Tick(gomock.Any(), gomock.Any()).Do(func(interface{}, interface{}) {
			orderWg.Done()
			for {
				if shard.isClosing() {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		}).Return(series.TickResult{}, nil),
		// for the shard Close purging
		foo.EXPECT().IsEmpty().Return(true),
		foo.EXPECT().Close(),
		bar.EXPECT().IsEmpty().Return(true),
		bar.EXPECT().Close(),
	)

	closeWg.Add(2)
	go func() {
		shard.Tick(context.NewNoOpCanncellable(), xtime.Now(), namespace.Context{}) // nolint
		closeWg.Done()
	}()

	go func() {
		orderWg.Wait()
		require.NoError(t, shard.Close())
		closeWg.Done()
	}()

	closeWg.Wait()
}

// This tests the scenario where an empty series is expired.
func TestPurgeExpiredSeriesEmptySeries(t *testing.T) {
	opts := DefaultTestOptions()
	shard := testDatabaseShard(t, opts)
	defer shard.Close()

	addTestSeries(shard, ident.StringID("foo"))

	_, err := shard.Tick(context.NewNoOpCanncellable(), xtime.Now(), namespace.Context{})
	require.NoError(t, err)

	shard.RLock()
	require.Equal(t, 0, shard.lookup.Len())
	shard.RUnlock()
}

// This tests the scenario where a non-empty series is not expired.
func TestPurgeExpiredSeriesNonEmptySeries(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := DefaultTestOptions()
	shard := testDatabaseShard(t, opts)
	retriever := series.NewMockQueryableBlockRetriever(ctrl)
	retriever.EXPECT().IsBlockRetrievable(gomock.Any()).Return(false, nil).AnyTimes()
	shard.seriesBlockRetriever = retriever
	defer shard.Close()
	ctx := opts.ContextPool().Get()
	nowFn := opts.ClockOptions().NowFn()
	_, err := shard.Write(ctx, ident.StringID("foo"), xtime.ToUnixNano(nowFn()),
		1.0, xtime.Second, nil, series.WriteOptions{})
	require.NoError(t, err)

	r, err := shard.tickAndExpire(context.NewNoOpCanncellable(), tickPolicyRegular, namespace.Context{})
	require.NoError(t, err)
	require.Equal(t, 1, r.activeSeries)
	require.Equal(t, 0, r.expiredSeries)
}

// This tests the scenario where a series is empty when series.Tick() is called,
// but receives writes after tickForEachSeries finishes but before purgeExpiredSeries
// starts. The expected behavior is not to expire series in this case.
func TestPurgeExpiredSeriesWriteAfterTicking(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := DefaultTestOptions()
	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	id := ident.StringID("foo")
	s := addMockSeries(ctrl, shard, id, ident.Tags{}, 0)
	s.EXPECT().Tick(gomock.Any(), gomock.Any()).Do(func(interface{}, interface{}) {
		// Emulate a write taking place just after tick for this series
		s.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any()).Return(true, series.WarmWrite, nil)

		ctx := opts.ContextPool().Get()
		nowFn := opts.ClockOptions().NowFn()
		_, err := shard.Write(
			ctx, id, xtime.ToUnixNano(nowFn()), 1.0, xtime.Second,
			nil, series.WriteOptions{},
		)
		require.NoError(t, err)
	}).Return(series.TickResult{}, series.ErrSeriesAllDatapointsExpired)

	r, err := shard.tickAndExpire(context.NewNoOpCanncellable(), tickPolicyRegular, namespace.Context{})
	require.NoError(t, err)
	require.Equal(t, 0, r.activeSeries)
	require.Equal(t, 1, r.expiredSeries)
	require.Equal(t, 1, shard.lookup.Len())
}

// This tests the scenario where tickForEachSeries finishes, and before purgeExpiredSeries
// starts, we receive a write for a series, then purgeExpiredSeries runs, then we write to
// the series. The expected behavior is not to expire series in this case.
func TestPurgeExpiredSeriesWriteAfterPurging(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var entry *Entry

	opts := DefaultTestOptions()
	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	id := ident.StringID("foo")
	s := addMockSeries(ctrl, shard, id, ident.Tags{}, 0)
	s.EXPECT().Tick(gomock.Any(), gomock.Any()).Do(func(interface{}, interface{}) {
		// Emulate a write taking place and staying open just after tick for this series
		var err error
		entry, err = shard.writableSeries(id, convert.EmptyTagMetadataResolver)
		require.NoError(t, err)
	}).Return(series.TickResult{}, series.ErrSeriesAllDatapointsExpired)

	r, err := shard.tickAndExpire(context.NewNoOpCanncellable(), tickPolicyRegular, namespace.Context{})
	require.NoError(t, err)
	require.Equal(t, 0, r.activeSeries)
	require.Equal(t, 1, r.expiredSeries)
	require.Equal(t, 1, shard.lookup.Len())

	entry.DecrementReaderWriterCount()
	require.Equal(t, 1, shard.lookup.Len())
}

func TestForEachShardEntry(t *testing.T) {
	opts := DefaultTestOptions()
	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	for i := 0; i < 10; i++ {
		addTestSeries(shard, ident.StringID(fmt.Sprintf("foo.%d", i)))
	}

	count := 0
	entryFn := func(entry *Entry) bool {
		if entry.Series.ID().String() == "foo.8" {
			return false
		}

		// Ensure the readerwriter count is incremented while we operate
		// on this series
		assert.Equal(t, int32(1), entry.ReaderWriterCount())

		count++
		return true
	}

	shard.forEachShardEntry(entryFn)

	assert.Equal(t, 8, count)

	// Ensure that reader writer count gets reset
	shard.RLock()
	for elem := shard.list.Front(); elem != nil; elem = elem.Next() {
		entry := elem.Value.(*Entry)
		assert.Equal(t, int32(0), entry.ReaderWriterCount())
	}
	shard.RUnlock()
}

func TestShardFetchBlocksIDNotExists(t *testing.T) {
	opts := DefaultTestOptions()
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	fetched, err := shard.FetchBlocks(ctx, ident.StringID("foo"), nil, namespace.Context{})
	require.NoError(t, err)
	require.Equal(t, 0, len(fetched))
}

func TestShardFetchBlocksIDExists(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := DefaultTestOptions()
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	id := ident.StringID("foo")
	series := addMockSeries(ctrl, shard, id, ident.Tags{}, 0)
	now := xtime.Now()
	starts := []xtime.UnixNano{now}
	expected := []block.FetchBlockResult{block.NewFetchBlockResult(now, nil, nil)}
	series.EXPECT().FetchBlocks(ctx, starts, gomock.Any()).Return(expected, nil)
	res, err := shard.FetchBlocks(ctx, id, starts, namespace.Context{})
	require.NoError(t, err)
	require.Equal(t, expected, res)
}

func TestShardCleanupExpiredFileSets(t *testing.T) {
	opts := DefaultTestOptions()
	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	shard.filesetPathsBeforeFn = func(
		_ string, namespace ident.ID,
		shardID uint32, _ xtime.UnixNano,
	) ([]string, error) {
		return []string{namespace.String(), strconv.Itoa(int(shardID))}, nil
	}
	var deletedFiles []string
	shard.deleteFilesFn = func(files []string) error {
		deletedFiles = append(deletedFiles, files...)
		return nil
	}
	require.NoError(t, shard.CleanupExpiredFileSets(xtime.Now()))
	require.Equal(t, []string{defaultTestNs1ID.String(), "0"}, deletedFiles)
}

type testCloser struct {
	called int
}

func (c *testCloser) Close() {
	c.called++
}

func TestShardRegisterRuntimeOptionsListeners(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	callRegisterListenerOnShard := 0
	callRegisterListenerOnShardInsertQueue := 0

	closer := &testCloser{}

	runtimeOptsMgr := runtime.NewMockOptionsManager(ctrl)
	runtimeOptsMgr.EXPECT().
		RegisterListener(gomock.Any()).
		Times(2).
		Do(func(l runtime.OptionsListener) {
			if _, ok := l.(*dbShard); ok {
				callRegisterListenerOnShard++
			}
			if _, ok := l.(*dbShardInsertQueue); ok {
				callRegisterListenerOnShardInsertQueue++
			}
		}).
		Return(closer)

	opts := DefaultTestOptions().
		SetRuntimeOptionsManager(runtimeOptsMgr)

	shard := testDatabaseShard(t, opts)

	assert.Equal(t, 1, callRegisterListenerOnShard)
	assert.Equal(t, 1, callRegisterListenerOnShardInsertQueue)

	assert.Equal(t, 0, closer.called)

	shard.Close()

	assert.Equal(t, 2, closer.called)
}

func TestShardReadEncodedCachesSeriesWithRecentlyReadPolicy(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdir")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := DefaultTestOptions().
		SetSeriesCachePolicy(series.CacheRecentlyRead)
	fsOpts := opts.CommitLogOptions().FilesystemOptions().
		SetFilePathPrefix(dir)
	opts = opts.
		SetCommitLogOptions(opts.CommitLogOptions().
			SetFilesystemOptions(fsOpts))

	shard := testDatabaseShard(t, opts)
	defer shard.Close()

	ctx := context.NewBackground()
	defer ctx.Close()

	nsCtx := namespace.Context{ID: ident.StringID("foo")}
	require.NoError(t, shard.Bootstrap(ctx, nsCtx))

	ropts := shard.seriesOpts.RetentionOptions()
	end := xtime.ToUnixNano(opts.ClockOptions().NowFn()()).Truncate(ropts.BlockSize())
	start := end.Add(-2 * ropts.BlockSize())
	shard.markWarmDataFlushStateSuccess(start)
	shard.markWarmDataFlushStateSuccess(start.Add(ropts.BlockSize()))

	retriever := block.NewMockDatabaseBlockRetriever(ctrl)
	shard.setBlockRetriever(retriever)

	segments := []ts.Segment{
		ts.NewSegment(checked.NewBytes([]byte("bar"), nil), nil, 0, ts.FinalizeNone),
		ts.NewSegment(checked.NewBytes([]byte("baz"), nil), nil, 1, ts.FinalizeNone),
	}

	var blockReaders []xio.BlockReader
	for range segments {
		reader := xio.NewMockSegmentReader(ctrl)
		block := xio.BlockReader{
			SegmentReader: reader,
		}

		blockReaders = append(blockReaders, block)
	}

	mid := start.Add(ropts.BlockSize())

	retriever.EXPECT().
		Stream(ctx, shard.shard, ident.NewIDMatcher("foo"),
			start, shard.seriesOnRetrieveBlock, gomock.Any()).
		Do(func(
			ctx context.Context, shard uint32, id ident.ID, at xtime.UnixNano,
			onRetrieve block.OnRetrieveBlock, nsCtx namespace.Context,
		) {
			go onRetrieve.OnRetrieveBlock(id, ident.EmptyTagIterator, at, segments[0], nsCtx)
		}).
		Return(blockReaders[0], nil)
	retriever.EXPECT().
		Stream(ctx, shard.shard, ident.NewIDMatcher("foo"),
			mid, shard.seriesOnRetrieveBlock, gomock.Any()).
		Do(func(ctx context.Context, shard uint32, id ident.ID, at xtime.UnixNano,
			onRetrieve block.OnRetrieveBlock, nsCtx namespace.Context,
		) {
			go onRetrieve.OnRetrieveBlock(id, ident.EmptyTagIterator, at, segments[1], nsCtx)
		}).
		Return(blockReaders[1], nil)

	// Check reads as expected
	iter, err := shard.ReadEncoded(ctx, ident.StringID("foo"), start, end, namespace.Context{})
	require.NoError(t, err)
	count := 0
	for iter.Next(ctx) {
		require.Equal(t, 1, len(iter.Current()))
		assert.Equal(t, blockReaders[count], iter.Current()[0])
		count++
	}
	require.Equal(t, 2, count)
	require.NoError(t, iter.Err())

	// Check that gets cached
	begin := time.Now()
	for time.Since(begin) < 10*time.Second {
		shard.RLock()
		entry, err := shard.lookupEntryWithLock(ident.StringID("foo"))
		shard.RUnlock()
		if err == errShardEntryNotFound {
			time.Sleep(5 * time.Millisecond)
			continue
		}

		if err != nil || entry.Series.NumActiveBlocks() == 2 {
			// Expecting at least 2 active blocks and never an error
			break
		}
	}

	shard.RLock()
	entry, err := shard.lookupEntryWithLock(ident.StringID("foo"))
	shard.RUnlock()
	require.NoError(t, err)
	require.NotNil(t, entry)

	assert.False(t, entry.Series.IsEmpty())
	assert.Equal(t, 2, entry.Series.NumActiveBlocks())
}

func TestShardNewInvalidShardEntry(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	shard := testDatabaseShard(t, DefaultTestOptions())
	defer shard.Close()

	iter := ident.NewMockTagIterator(ctrl)
	gomock.InOrder(
		iter.EXPECT().Duplicate().Return(iter),
		iter.EXPECT().Remaining().Return(8),
		iter.EXPECT().Next().Return(false),
		iter.EXPECT().Err().Return(fmt.Errorf("random err")),
		iter.EXPECT().Close(),
	)

	_, err := shard.newShardEntry(ident.StringID("abc"), convert.NewTagsIterMetadataResolver(iter))
	require.Error(t, err)
}

func TestShardNewValidShardEntry(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	shard := testDatabaseShard(t, DefaultTestOptions())
	defer shard.Close()

	_, err := shard.newShardEntry(
		ident.StringID("abc"),
		convert.NewTagsIterMetadataResolver(ident.EmptyTagIterator),
	)
	require.NoError(t, err)
}

// TestShardNewEntryDoesNotAlterTags tests that the ID and Tags passed
// to newShardEntry is not altered. There are multiple callers that
// reuse the tag iterator passed all the way through to newShardEntry
// either to retry inserting a series or to finalize the tags at the
// end of a request/response cycle or from a disk retrieve cycle.
func TestShardNewEntryDoesNotAlterIDOrTags(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	shard := testDatabaseShard(t, DefaultTestOptions())
	defer shard.Close()

	seriesID := ident.StringID("foo+bar=baz")
	seriesTags := ident.NewTags(ident.Tag{
		Name:  ident.StringID("bar"),
		Value: ident.StringID("baz"),
	})

	// Ensure copied with call to bytes but no close call, etc
	id := ident.NewMockID(ctrl)
	id.EXPECT().Bytes().Times(1).Return(seriesID.Bytes())

	iter := ident.NewMockTagIterator(ctrl)

	// Ensure duplicate called but no close, etc
	iter.EXPECT().
		Duplicate().
		Times(1).
		Return(ident.NewTagsIterator(seriesTags))

	entry, err := shard.newShardEntry(id, convert.NewTagsIterMetadataResolver(iter))
	require.NoError(t, err)

	shard.Lock()
	shard.insertNewShardEntryWithLock(entry)
	shard.Unlock()

	entry, _, err = shard.TryRetrieveSeriesAndIncrementReaderWriterCount(seriesID)
	require.NoError(t, err)

	entryIDBytes := entry.Series.ID().Bytes()
	seriesIDBytes := seriesID.Bytes()

	// Ensure ID equal and not same ref
	assert.True(t, entry.Series.ID().Equal(seriesID))
	// NB(r): Use &slice[0] to get a pointer to the very first byte, i.e. data section
	assert.False(t, unsafe.Pointer(&entryIDBytes[0]) == unsafe.Pointer(&seriesIDBytes[0]))
}

func TestShardIterateBatchSize(t *testing.T) {
	smaller := shardIterateBatchMinSize - 1
	require.Equal(t, shardIterateBatchMinSize, iterateBatchSize(smaller))

	require.Equal(t, shardIterateBatchMinSize, iterateBatchSize(shardIterateBatchMinSize+1))

	require.True(t, shardIterateBatchMinSize < iterateBatchSize(2000))
}

func TestShardAggregateTiles(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewBackground()
	defer ctx.Close()

	var (
		targetBlockSize = 2 * time.Hour
		start           = xtime.Now().Truncate(targetBlockSize)
		opts            = AggregateTilesOptions{
			Start: start, End: start.Add(targetBlockSize), Step: 10 * time.Minute,
		}

		expectedProcessedTileCount = int64(4)

		err error
	)

	aggregator := NewMockTileAggregator(ctrl)
	testOpts := DefaultTestOptions().SetTileAggregator(aggregator)

	targetShard := testDatabaseShardWithIndexFn(t, testOpts, nil, true)
	defer assert.NoError(t, targetShard.Close())

	var (
		noOpColdFlushNs = &persist.NoOpColdFlushNamespace{}
		sourceNs        = NewMockNamespace(ctrl)
		targetNs        = NewMockNamespace(ctrl)
	)

	aggregator.EXPECT().
		AggregateTiles(ctx, sourceNs, targetNs, targetShard.ID(), noOpColdFlushNs, opts).
		Return(expectedProcessedTileCount, 33, nil)

	processedTileCount, err := targetShard.AggregateTiles(
		ctx, sourceNs, targetNs, targetShard.ID(), noOpColdFlushNs, opts)
	require.NoError(t, err)
	assert.Equal(t, expectedProcessedTileCount, processedTileCount)

	flushState, ok := targetShard.flushState.statesByTime[start]
	require.True(t, ok)
	assert.Equal(t, fileOpSuccess, flushState.WarmStatus.DataFlushed)
	assert.Equal(t, fileOpSuccess, flushState.WarmStatus.IndexFlushed)
	assert.Zero(t, flushState.NumFailures)
}

func TestOpenStreamingReader(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		blockStart = xtime.Now().Truncate(time.Hour)
		testOpts   = DefaultTestOptions()
	)

	shard := testDatabaseShard(t, testOpts)
	defer assert.NoError(t, shard.Close())

	latestSourceVolume, err := shard.LatestVolume(blockStart)
	require.NoError(t, err)

	openOpts := fs.DataReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:   shard.namespace.ID(),
			Shard:       shard.ID(),
			BlockStart:  blockStart,
			VolumeIndex: latestSourceVolume,
		},
		FileSetType:      persist.FileSetFlushType,
		StreamingEnabled: true,
	}

	reader := fs.NewMockDataFileSetReader(ctrl)
	reader.EXPECT().Open(openOpts).Return(nil)

	shard.newReaderFn = func(pool.CheckedBytesPool, fs.Options) (fs.DataFileSetReader, error) {
		return reader, nil
	}

	_, err = shard.OpenStreamingReader(blockStart)
	require.NoError(t, err)
}

func TestSeriesRefResolver(t *testing.T) {
	ctrl := xtest.NewController(t)
	shard := testDatabaseShard(t, DefaultTestOptions())
	ctx := context.NewBackground()
	defer func() {
		ctrl.Finish()
		_ = shard.Close()
		ctx.Close()
	}()

	seriesID := ident.StringID("foo+bar=baz")
	seriesTags := ident.NewTags(ident.Tag{
		Name:  ident.StringID("bar"),
		Value: ident.StringID("baz"),
	})

	iter := ident.NewMockTagIterator(ctrl)
	// Ensure duplicate called but no close, etc
	iter.EXPECT().
		Duplicate().
		Return(ident.NewTagsIterator(seriesTags))

	now := xtime.Now()

	resolver, err := shard.SeriesRefResolver(seriesID, iter)
	require.NoError(t, err)
	seriesRef, err := resolver.SeriesRef()
	require.NoError(t, err)
	write, writeType, err := seriesRef.Write(ctx, now, 1.0, xtime.Second,
		[]byte("annotation1"), series.WriteOptions{})
	require.NoError(t, err)
	require.Equal(t, series.WarmWrite, writeType)
	require.True(t, write)

	// should return already inserted entry as series.
	resolverEntry, err := shard.SeriesRefResolver(seriesID, iter)
	require.NoError(t, err)
	require.IsType(t, &Entry{}, resolverEntry)
	refEntry, err := resolverEntry.SeriesRef()
	require.NoError(t, err)
	require.Equal(t, seriesRef, refEntry)

	databaseBlock := block.NewMockDatabaseBlock(ctrl)
	databaseBlock.EXPECT().StartTime().Return(now).AnyTimes()
	err = seriesRef.LoadBlock(databaseBlock, series.ColdWrite)
	require.NoError(t, err)

	resolver.ReleaseRef()
	resolverEntry.ReleaseRef()
	entry := seriesRef.(*Entry)
	require.Zero(t, entry.ReaderWriterCount())
}

// TestSeriesRefResolverAsync tests async resolver creation/closure for the same series
// to validate proper ref counting.
func TestSeriesRefResolverAsync(t *testing.T) {
	ctrl := xtest.NewController(t)
	shard := testDatabaseShard(t, DefaultTestOptions())
	ctx := context.NewBackground()
	defer func() {
		ctrl.Finish()
		_ = shard.Close()
		ctx.Close()
	}()

	seriesID := ident.StringID("foo+bar=baz")
	seriesTags := ident.NewTags(ident.Tag{
		Name:  ident.StringID("bar"),
		Value: ident.StringID("baz"),
	})

	iter := ident.NewTagsIterator(seriesTags)

	// This resolution path is async due to the use of the index insert queue.
	// When many entries for the same series are queued up at once, only one
	// is persisted in the shard map. We induce this by spinning up N goroutines
	// to cause an insert for the same series, and release them all at once.
	// We then verify at the end that the ref counts are correctly freed for the
	// entry ultimately in the shard map (i.e. it should have zero outstanding after
	// every resolver is closed).
	var (
		start  sync.WaitGroup
		finish sync.WaitGroup
	)
	start.Add(1)
	for i := 0; i < 100; i++ {
		i := i
		finish.Add(1)
		go func() {
			start.Wait()

			resolver, err := shard.SeriesRefResolver(seriesID, iter)
			require.NoError(t, err)

			if i%2 == 0 {
				// Half the time exercise the ref retrieval path.
				_, err = resolver.SeriesRef()
				require.NoError(t, err)
			}

			resolver.ReleaseRef()

			finish.Done()
		}()
	}

	start.Done()
	finish.Wait()

	entryInShard, _, err := shard.TryRetrieveSeriesAndIncrementReaderWriterCount(seriesID)
	require.NoError(t, err)
	entryInShard.DecrementReaderWriterCount() // Decrement because the above retrieval increments.
	require.Equal(t, int32(0), entryInShard.ReaderWriterCount())
}

func getMockReader(
	ctrl *gomock.Controller,
	t *testing.T,
	shard *dbShard,
	blockStart xtime.UnixNano,
	openError error,
) (*fs.MockDataFileSetReader, int) {
	latestSourceVolume, err := shard.LatestVolume(blockStart)
	require.NoError(t, err)

	openOpts := fs.DataReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:   shard.namespace.ID(),
			Shard:       shard.ID(),
			BlockStart:  blockStart,
			VolumeIndex: latestSourceVolume,
		},
		FileSetType:      persist.FileSetFlushType,
		StreamingEnabled: true,
	}

	reader := fs.NewMockDataFileSetReader(ctrl)
	if openError == nil {
		reader.EXPECT().Open(openOpts).Return(nil)
		reader.EXPECT().Close()
	} else {
		reader.EXPECT().Open(openOpts).Return(openError)
	}

	return reader, latestSourceVolume
}
