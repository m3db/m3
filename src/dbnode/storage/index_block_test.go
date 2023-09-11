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

package storage

import (
	stdlibctx "context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/limits"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	opentracing "github.com/opentracing/opentracing-go"
	opentracinglog "github.com/opentracing/opentracing-go/log"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/require"
)

var (
	namespaceIndexOptions = namespace.NewIndexOptions()

	defaultQuery = index.Query{
		Query: idx.NewTermQuery([]byte("foo"), []byte("bar")),
	}

	testShardSet sharding.ShardSet
)

func init() {
	shards := sharding.NewShards([]uint32{0, 1, 2, 3}, shard.Available)
	hashFn := sharding.DefaultHashFn(len(shards))
	shardSet, err := sharding.NewShardSet(shards, hashFn)
	if err != nil {
		panic(err)
	}
	testShardSet = shardSet
}

type testWriteBatchOption func(index.WriteBatchOptions) index.WriteBatchOptions

func testWriteBatchBlockSizeOption(blockSize time.Duration) testWriteBatchOption {
	return func(o index.WriteBatchOptions) index.WriteBatchOptions {
		o.IndexBlockSize = blockSize
		return o
	}
}

func testWriteBatch(
	e index.WriteBatchEntry,
	d doc.Metadata,
	opts ...testWriteBatchOption,
) *index.WriteBatch {
	options := index.WriteBatchOptions{}
	for _, opt := range opts {
		options = opt(options)
	}
	b := index.NewWriteBatch(options)
	b.Append(e, d)
	return b
}

func testWriteBatchEntry(
	id ident.ID,
	tags ident.Tags,
	timestamp xtime.UnixNano,
	fns doc.OnIndexSeries,
) (index.WriteBatchEntry, doc.Metadata) {
	d := doc.Metadata{ID: copyBytes(id.Bytes())}
	for _, tag := range tags.Values() {
		d.Fields = append(d.Fields, doc.Field{
			Name:  copyBytes(tag.Name.Bytes()),
			Value: copyBytes(tag.Value.Bytes()),
		})
	}
	return index.WriteBatchEntry{
		Timestamp:     timestamp,
		OnIndexSeries: fns,
	}, d
}

func copyBytes(b []byte) []byte {
	return append([]byte(nil), b...)
}

func testNamespaceMetadata(blockSize, period time.Duration) namespace.Metadata {
	nopts := namespaceOptions.
		SetRetentionOptions(namespaceOptions.RetentionOptions().
			SetRetentionPeriod(period)).
		SetIndexOptions(
			namespaceIndexOptions.
				SetBlockSize(blockSize))
	md, err := namespace.NewMetadata(ident.StringID("testns"), nopts)
	if err != nil {
		panic(err)
	}
	return md
}

func TestNamespaceIndexNewBlockFn(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour
	now := xtime.Now().Truncate(blockSize).Add(2 * time.Minute)
	nowFn := func() time.Time { return now.ToTime() }
	opts := DefaultTestOptions()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))

	mockBlock := index.NewMockBlock(ctrl)
	mockBlock.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	mockBlock.EXPECT().StartTime().Return(now.Truncate(blockSize)).AnyTimes()
	mockBlock.EXPECT().Close().Return(nil).AnyTimes()
	newBlockFn := func(
		ts xtime.UnixNano,
		md namespace.Metadata,
		opts index.BlockOptions,
		_ namespace.RuntimeOptionsManager,
		io index.Options,
	) (index.Block, error) {
		// If active block, the blockStart should be zero.
		// Otherwise, it should match the actual time.
		if opts.ActiveBlock {
			require.Equal(t, xtime.UnixNano(0), ts)
		} else {
			require.Equal(t, now.Truncate(blockSize), ts)
		}
		return mockBlock, nil
	}
	md := testNamespaceMetadata(blockSize, 4*time.Hour)
	index, err := newNamespaceIndexWithNewBlockFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newBlockFn, opts)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, index.Close())
	}()

	blocksSlice := index.(*nsIndex).state.blocksDescOrderImmutable

	require.Equal(t, 1, len(blocksSlice))
	require.Equal(t, now.Truncate(blockSize), blocksSlice[0].blockStart)

	require.Equal(t, mockBlock, index.(*nsIndex).state.latestBlock)

	blocksMap := index.(*nsIndex).state.blocksByTime
	require.Equal(t, 1, len(blocksMap))
	blk, ok := blocksMap[now.Truncate(blockSize)]
	require.True(t, ok)
	require.Equal(t, mockBlock, blk)
}

func TestNamespaceIndexNewBlockFnRandomErr(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour
	now := xtime.Now().Truncate(blockSize).Add(2 * time.Minute)
	nowFn := func() time.Time { return now.ToTime() }
	opts := DefaultTestOptions()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))

	newBlockFn := func(
		ts xtime.UnixNano,
		md namespace.Metadata,
		_ index.BlockOptions,
		_ namespace.RuntimeOptionsManager,
		io index.Options,
	) (index.Block, error) {
		return nil, fmt.Errorf("randomerr")
	}
	defer instrument.SetShouldPanicEnvironmentVariable(true)()
	md := testNamespaceMetadata(blockSize, 4*time.Hour)
	require.Panics(t, func() {
		_, _ = newNamespaceIndexWithNewBlockFn(md,
			namespace.NewRuntimeOptionsManager(md.ID().String()),
			testShardSet, newBlockFn, opts)
	})
}

func TestNamespaceIndexWrite(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour
	now := xtime.Now().Truncate(blockSize).Add(2 * time.Minute)
	nowFn := func() time.Time { return now.ToTime() }
	opts := DefaultTestOptions()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))

	mockBlock := index.NewMockBlock(ctrl)
	mockBlock.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	mockBlock.EXPECT().Close().Return(nil).Times(2) // active and normal
	mockBlock.EXPECT().StartTime().Return(now.Truncate(blockSize)).AnyTimes()
	newBlockFn := func(
		ts xtime.UnixNano,
		md namespace.Metadata,
		opts index.BlockOptions,
		_ namespace.RuntimeOptionsManager,
		io index.Options,
	) (index.Block, error) {
		// If active block, the blockStart should be zero.
		// Otherwise, it should match the actual time.
		if opts.ActiveBlock {
			require.Equal(t, xtime.UnixNano(0), ts)
		} else {
			require.Equal(t, now.Truncate(blockSize), ts)
		}
		return mockBlock, nil
	}
	md := testNamespaceMetadata(blockSize, 4*time.Hour)
	idx, err := newNamespaceIndexWithNewBlockFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newBlockFn, opts)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, idx.Close())
	}()

	id := ident.StringID("foo")
	tag := ident.StringTag("name", "value")
	tags := ident.NewTags(tag)
	lifecycle := doc.NewMockOnIndexSeries(ctrl)
	mockWriteBatch(t, &now, lifecycle, mockBlock, &tag)
	lifecycle.EXPECT().IfAlreadyIndexedMarkIndexSuccessAndFinalize(gomock.Any()).Return(false)
	batch := index.NewWriteBatch(index.WriteBatchOptions{
		IndexBlockSize: blockSize,
	})
	batch.Append(testWriteBatchEntry(id, tags, now, lifecycle))
	require.NoError(t, idx.WriteBatch(batch))
}

func TestNamespaceIndexWriteCreatesBlock(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour
	now := xtime.Now().Truncate(blockSize).Add(2 * time.Minute)
	t0 := now.Truncate(blockSize)
	t1 := t0.Add(blockSize)
	var nowLock sync.Mutex
	nowFn := func() time.Time {
		nowLock.Lock()
		defer nowLock.Unlock()
		return now.ToTime()
	}
	opts := DefaultTestOptions()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))

	bActive := index.NewMockBlock(ctrl)
	bActive.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	bActive.EXPECT().Close().Return(nil)
	bActive.EXPECT().StartTime().Return(t0).AnyTimes()
	b0 := index.NewMockBlock(ctrl)
	b0.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	b0.EXPECT().Close().Return(nil)
	b0.EXPECT().StartTime().Return(t0).AnyTimes()
	b1 := index.NewMockBlock(ctrl)
	b1.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	b1.EXPECT().StartTime().Return(t1).AnyTimes()
	newBlockFn := func(
		ts xtime.UnixNano,
		md namespace.Metadata,
		opts index.BlockOptions,
		_ namespace.RuntimeOptionsManager,
		io index.Options,
	) (index.Block, error) {
		if opts.ActiveBlock {
			return bActive, nil
		}
		if ts.Equal(t0) {
			return b0, nil
		}
		if ts.Equal(t1) {
			return b1, nil
		}
		panic("should never get here")
	}
	md := testNamespaceMetadata(blockSize, 4*time.Hour)
	idx, err := newNamespaceIndexWithNewBlockFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newBlockFn, opts)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, idx.Close())
	}()

	id := ident.StringID("foo")
	tag := ident.StringTag("name", "value")
	tags := ident.NewTags(tag)
	lifecycle := doc.NewMockOnIndexSeries(ctrl)
	mockWriteBatch(t, &now, lifecycle, bActive, &tag)
	lifecycle.EXPECT().IfAlreadyIndexedMarkIndexSuccessAndFinalize(gomock.Any()).
		Return(false).
		AnyTimes()
	nowLock.Lock()
	now = now.Add(blockSize)
	nowLock.Unlock()

	entry, doc := testWriteBatchEntry(id, tags, now, lifecycle)
	batch := testWriteBatch(entry, doc, testWriteBatchBlockSizeOption(blockSize))
	require.NoError(t, idx.WriteBatch(batch))
}

func TestNamespaceIndexBootstrap(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour
	now := xtime.Now().Truncate(blockSize).Add(2 * time.Minute)
	t0 := now.Truncate(blockSize)
	t1 := t0.Add(1 * blockSize)
	t2 := t1.Add(1 * blockSize)
	var nowLock sync.Mutex
	nowFn := func() time.Time {
		nowLock.Lock()
		defer nowLock.Unlock()
		return now.ToTime()
	}
	opts := DefaultTestOptions()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))

	bActive := index.NewMockBlock(ctrl)
	bActive.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	bActive.EXPECT().StartTime().Return(t0).AnyTimes()
	b0 := index.NewMockBlock(ctrl)
	b0.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	b0.EXPECT().StartTime().Return(t0).AnyTimes()
	b1 := index.NewMockBlock(ctrl)
	b1.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	b1.EXPECT().StartTime().Return(t1).AnyTimes()
	newBlockFn := func(
		ts xtime.UnixNano,
		md namespace.Metadata,
		opts index.BlockOptions,
		_ namespace.RuntimeOptionsManager,
		io index.Options,
	) (index.Block, error) {
		if opts.ActiveBlock {
			return bActive, nil
		}
		if ts.Equal(t0) {
			return b0, nil
		}
		if ts.Equal(t1) {
			return b1, nil
		}
		panic("should never get here")
	}
	md := testNamespaceMetadata(blockSize, 4*time.Hour)
	idx, err := newNamespaceIndexWithNewBlockFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newBlockFn, opts)
	require.NoError(t, err)

	seg1 := segment.NewMockSegment(ctrl)
	seg2 := segment.NewMockSegment(ctrl)
	seg3 := segment.NewMockSegment(ctrl)
	t0Results := result.NewIndexBlockByVolumeType(t0)
	t0Results.SetBlock(idxpersist.DefaultIndexVolumeType, result.NewIndexBlock([]result.Segment{result.NewSegment(seg1, false)},
		result.NewShardTimeRangesFromRange(t0, t1, 1, 2, 3)))
	t1Results := result.NewIndexBlockByVolumeType(t1)
	t1Results.SetBlock(idxpersist.DefaultIndexVolumeType, result.NewIndexBlock([]result.Segment{result.NewSegment(seg2, false), result.NewSegment(seg3, false)},
		result.NewShardTimeRangesFromRange(t1, t2, 1, 2, 3)))
	bootstrapResults := result.IndexResults{
		t0: t0Results,
		t1: t1Results,
	}

	b0.EXPECT().AddResults(bootstrapResults[t0]).Return(nil)
	b1.EXPECT().AddResults(bootstrapResults[t1]).Return(nil)
	require.NoError(t, idx.Bootstrap(bootstrapResults))
}

func TestNamespaceIndexTickExpire(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	retentionPeriod := 4 * time.Hour
	blockSize := time.Hour
	now := xtime.Now().Truncate(blockSize).Add(2 * time.Minute)
	t0 := now.Truncate(blockSize)
	var nowLock sync.Mutex
	nowFn := func() time.Time {
		nowLock.Lock()
		defer nowLock.Unlock()
		return now.ToTime()
	}
	opts := DefaultTestOptions()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))

	bActive := index.NewMockBlock(ctrl)
	bActive.EXPECT().StartTime().Return(t0).AnyTimes()
	bActive.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	b0 := index.NewMockBlock(ctrl)
	b0.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	b0.EXPECT().StartTime().Return(t0).AnyTimes()
	newBlockFn := func(
		ts xtime.UnixNano,
		md namespace.Metadata,
		opts index.BlockOptions,
		_ namespace.RuntimeOptionsManager,
		io index.Options,
	) (index.Block, error) {
		if opts.ActiveBlock {
			return bActive, nil
		}
		if ts.Equal(t0) {
			return b0, nil
		}
		panic("should never get here")
	}
	md := testNamespaceMetadata(blockSize, retentionPeriod)
	idx, err := newNamespaceIndexWithNewBlockFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newBlockFn, opts)
	require.NoError(t, err)

	nowLock.Lock()
	now = now.Add(retentionPeriod).Add(blockSize)
	nowLock.Unlock()

	c := context.NewCancellable()

	bActive.EXPECT().Tick(c).Return(index.BlockTickResult{}, nil)

	b0.EXPECT().Close().Return(nil)

	result, err := idx.Tick(c, xtime.ToUnixNano(nowFn()))
	require.NoError(t, err)
	require.Equal(t, namespaceIndexTickResult{
		NumBlocks:        0,
		NumBlocksEvicted: 0,
	}, result)
}

func TestNamespaceIndexTick(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	retentionPeriod := 4 * time.Hour
	blockSize := time.Hour
	now := xtime.Now().Truncate(blockSize).Add(2 * time.Minute)
	t0 := now.Truncate(blockSize)
	var nowLock sync.Mutex
	nowFn := func() time.Time {
		nowLock.Lock()
		defer nowLock.Unlock()
		return now.ToTime()
	}
	opts := DefaultTestOptions()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))

	bActive := index.NewMockBlock(ctrl)
	bActive.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	bActive.EXPECT().Close().Return(nil)
	bActive.EXPECT().StartTime().Return(t0).AnyTimes()
	b0 := index.NewMockBlock(ctrl)
	b0.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	b0.EXPECT().Close().Return(nil)
	b0.EXPECT().StartTime().Return(t0).AnyTimes()
	newBlockFn := func(
		ts xtime.UnixNano,
		md namespace.Metadata,
		opts index.BlockOptions,
		_ namespace.RuntimeOptionsManager,
		io index.Options,
	) (index.Block, error) {
		if opts.ActiveBlock {
			return bActive, nil
		}
		if ts.Equal(t0) {
			return b0, nil
		}
		panic("should never get here")
	}
	md := testNamespaceMetadata(blockSize, retentionPeriod)
	idx, err := newNamespaceIndexWithNewBlockFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newBlockFn, opts)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, idx.Close())
	}()

	c := context.NewCancellable()

	bActive.EXPECT().Tick(c).
		Return(index.BlockTickResult{
			NumDocs:     10,
			NumSegments: 2,
		}, nil).
		AnyTimes()
	bActive.EXPECT().IsSealed().Return(false).AnyTimes()

	b0.EXPECT().Tick(c).
		Return(index.BlockTickResult{
			NumDocs:     10,
			NumSegments: 2,
		}, nil)
	b0.EXPECT().IsSealed().Return(false)

	result, err := idx.Tick(c, xtime.ToUnixNano(nowFn()))
	require.NoError(t, err)
	require.Equal(t, namespaceIndexTickResult{
		NumBlocks:    1,
		NumSegments:  4,
		NumTotalDocs: 20,
	}, result)

	nowLock.Lock()
	now = now.Add(2 * blockSize)
	nowLock.Unlock()

	b0.EXPECT().Tick(c).Return(index.BlockTickResult{
		NumDocs:     10,
		NumSegments: 2,
	}, nil)
	b0.EXPECT().IsSealed().Return(false).Times(1)
	b0.EXPECT().Seal().Return(nil).AnyTimes()
	result, err = idx.Tick(c, xtime.ToUnixNano(nowFn()))
	require.NoError(t, err)
	require.Equal(t, namespaceIndexTickResult{
		NumBlocks:       1,
		NumBlocksSealed: 0,
		NumSegments:     4,
		NumTotalDocs:    20,
	}, result)

	b0.EXPECT().Tick(c).Return(index.BlockTickResult{
		NumDocs:     10,
		NumSegments: 2,
	}, nil)
	result, err = idx.Tick(c, xtime.ToUnixNano(nowFn()))
	require.NoError(t, err)
	require.Equal(t, namespaceIndexTickResult{
		NumBlocks:    1,
		NumSegments:  4,
		NumTotalDocs: 20,
	}, result)
}

func TestNamespaceIndexBlockQuery(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	retention := 2 * time.Hour
	blockSize := time.Hour
	now := xtime.Now().Truncate(blockSize).Add(10 * time.Minute)
	t0 := now.Truncate(blockSize)
	t1 := t0.Add(1 * blockSize)
	t2 := t1.Add(1 * blockSize)
	var nowLock sync.Mutex
	nowFn := func() time.Time {
		nowLock.Lock()
		defer nowLock.Unlock()
		return now.ToTime()
	}
	opts := DefaultTestOptions()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))

	bActive := index.NewMockBlock(ctrl)
	bActive.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	bActive.EXPECT().Close().Return(nil)
	bActive.EXPECT().StartTime().Return(t0).AnyTimes()
	bActive.EXPECT().EndTime().Return(t0.Add(blockSize)).AnyTimes()
	b0 := index.NewMockBlock(ctrl)
	b0.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	b0.EXPECT().Close().Return(nil)
	b0.EXPECT().StartTime().Return(t0).AnyTimes()
	b0.EXPECT().EndTime().Return(t0.Add(blockSize)).AnyTimes()
	b1 := index.NewMockBlock(ctrl)
	b1.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	b1.EXPECT().Close().Return(nil)
	b1.EXPECT().StartTime().Return(t1).AnyTimes()
	b1.EXPECT().EndTime().Return(t1.Add(blockSize)).AnyTimes()
	newBlockFn := func(
		ts xtime.UnixNano,
		md namespace.Metadata,
		opts index.BlockOptions,
		_ namespace.RuntimeOptionsManager,
		io index.Options,
	) (index.Block, error) {
		if opts.ActiveBlock {
			return bActive, nil
		}
		if ts.Equal(t0) {
			return b0, nil
		}
		if ts.Equal(t1) {
			return b1, nil
		}
		panic("should never get here")
	}
	md := testNamespaceMetadata(blockSize, retention)
	idx, err := newNamespaceIndexWithNewBlockFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newBlockFn, opts)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, idx.Close())
	}()

	seg1 := segment.NewMockSegment(ctrl)
	seg2 := segment.NewMockSegment(ctrl)
	seg3 := segment.NewMockSegment(ctrl)
	t0Results := result.NewIndexBlockByVolumeType(t0)
	t0Results.SetBlock(idxpersist.DefaultIndexVolumeType, result.NewIndexBlock([]result.Segment{result.NewSegment(seg1, false)},
		result.NewShardTimeRangesFromRange(t0, t1, 1, 2, 3)))
	t1Results := result.NewIndexBlockByVolumeType(t1)
	t1Results.SetBlock(idxpersist.DefaultIndexVolumeType, result.NewIndexBlock([]result.Segment{result.NewSegment(seg2, false), result.NewSegment(seg3, false)},
		result.NewShardTimeRangesFromRange(t1, t2, 1, 2, 3)))
	bootstrapResults := result.IndexResults{
		t0: t0Results,
		t1: t1Results,
	}

	b0.EXPECT().AddResults(bootstrapResults[t0]).Return(nil)
	b1.EXPECT().AddResults(bootstrapResults[t1]).Return(nil)
	require.NoError(t, idx.Bootstrap(bootstrapResults))

	for _, test := range []struct {
		name              string
		requireExhaustive bool
	}{
		{"allow non-exhaustive", false},
		{"require exhaustive", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			// only queries as much as is needed (wrt to time)
			ctx := context.NewBackground()
			q := defaultQuery
			qOpts := index.QueryOptions{
				StartInclusive: t0,
				EndExclusive:   now.Add(time.Minute),
			}

			// Lock to prevent race given these blocks are processed concurrently.
			var resultLock sync.Mutex

			// create initial span from a mock tracer and get ctx
			mtr := mocktracer.New()
			sp := mtr.StartSpan("root")
			ctx.SetGoContext(opentracing.ContextWithSpan(stdlibctx.Background(), sp))

			mockIterActive := index.NewMockQueryIterator(ctrl)
			mockIter0 := index.NewMockQueryIterator(ctrl)
			bActive.EXPECT().QueryIter(gomock.Any(), q).Return(mockIterActive, nil)
			mockIterActive.EXPECT().Done().Return(true)
			mockIterActive.EXPECT().Close().Return(nil)
			b0.EXPECT().QueryIter(gomock.Any(), q).Return(mockIter0, nil)
			mockIter0.EXPECT().Done().Return(true)
			mockIter0.EXPECT().Close().Return(nil)

			result, err := idx.Query(ctx, q, qOpts)
			require.NoError(t, err)
			require.True(t, result.Exhaustive)

			// queries multiple blocks if needed
			qOpts = index.QueryOptions{
				StartInclusive:    t0,
				EndExclusive:      t2.Add(time.Minute),
				RequireExhaustive: test.requireExhaustive,
			}
			bActive.EXPECT().QueryIter(gomock.Any(), q).Return(mockIterActive, nil)
			mockIterActive.EXPECT().Done().Return(true)
			mockIterActive.EXPECT().Close().Return(nil)
			b0.EXPECT().QueryIter(gomock.Any(), q).Return(mockIter0, nil)
			mockIter0.EXPECT().Done().Return(true)
			mockIter0.EXPECT().Close().Return(nil)

			mockIter1 := index.NewMockQueryIterator(ctrl)
			b1.EXPECT().QueryIter(gomock.Any(), q).Return(mockIter1, nil)
			mockIter1.EXPECT().Done().Return(true)
			mockIter1.EXPECT().Close().Return(nil)

			result, err = idx.Query(ctx, q, qOpts)
			require.NoError(t, err)
			require.True(t, result.Exhaustive)

			// stops querying once a block returns non-exhaustive
			qOpts = index.QueryOptions{
				StartInclusive:    t0,
				EndExclusive:      t0.Add(time.Minute),
				RequireExhaustive: test.requireExhaustive,
				SeriesLimit:       1,
			}

			docs := []doc.Document{
				doc.NewDocumentFromMetadata(doc.Metadata{ID: []byte("A")}),
				doc.NewDocumentFromMetadata(doc.Metadata{ID: []byte("B")}),
			}
			mockQueryWithIter(t, mockIterActive, bActive, q, qOpts, &resultLock, docs)
			mockQueryWithIter(t, mockIter0, b0, q, qOpts, &resultLock, docs)

			result, err = idx.Query(ctx, q, qOpts)
			if test.requireExhaustive {
				require.Error(t, err)
				require.False(t, xerrors.IsRetryableError(err))
			} else {
				require.NoError(t, err)
				require.False(t, result.Exhaustive)
			}

			sp.Finish()
			spans := mtr.FinishedSpans()
			require.Len(t, spans, 9)
		})
	}
}

func TestLimits(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	retention := 2 * time.Hour
	blockSize := time.Hour
	now := xtime.Now().Truncate(blockSize).Add(10 * time.Minute)
	t0 := now.Truncate(blockSize)
	t1 := t0.Add(1 * blockSize)
	var nowLock sync.Mutex
	nowFn := func() time.Time {
		nowLock.Lock()
		defer nowLock.Unlock()
		return now.ToTime()
	}
	opts := DefaultTestOptions()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))

	bActive := index.NewMockBlock(ctrl)
	bActive.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	bActive.EXPECT().Close().Return(nil).AnyTimes()
	bActive.EXPECT().StartTime().Return(t0).AnyTimes()
	bActive.EXPECT().EndTime().Return(t0.Add(blockSize)).AnyTimes()
	b0 := index.NewMockBlock(ctrl)
	b0.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	b0.EXPECT().Close().Return(nil).AnyTimes()
	b0.EXPECT().StartTime().Return(t0).AnyTimes()
	b0.EXPECT().EndTime().Return(t0.Add(blockSize)).AnyTimes()
	newBlockFn := func(
		ts xtime.UnixNano,
		md namespace.Metadata,
		opts index.BlockOptions,
		_ namespace.RuntimeOptionsManager,
		io index.Options,
	) (index.Block, error) {
		if opts.ActiveBlock {
			return bActive, nil
		}
		if ts.Equal(t0) {
			return b0, nil
		}
		panic("should never get here")
	}
	md := testNamespaceMetadata(blockSize, retention)
	idx, err := newNamespaceIndexWithNewBlockFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newBlockFn, opts)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, idx.Close())
	}()

	seg1 := segment.NewMockSegment(ctrl)
	t0Results := result.NewIndexBlockByVolumeType(t0)
	t0Results.SetBlock(idxpersist.DefaultIndexVolumeType, result.NewIndexBlock([]result.Segment{result.NewSegment(seg1, false)},
		result.NewShardTimeRangesFromRange(t0, t1, 1, 2, 3)))
	bootstrapResults := result.IndexResults{
		t0: t0Results,
	}

	b0.EXPECT().AddResults(bootstrapResults[t0]).Return(nil)
	require.NoError(t, idx.Bootstrap(bootstrapResults))

	for _, test := range []struct {
		name                            string
		seriesLimit                     int
		docsLimit                       int
		requireExhaustive               bool
		expectedErr                     string
		expectedQueryLimitExceededError bool
	}{
		{
			name:              "no limits",
			seriesLimit:       0,
			docsLimit:         0,
			requireExhaustive: false,
			expectedErr:       "",
		},
		{
			name:              "series limit only",
			seriesLimit:       1,
			docsLimit:         0,
			requireExhaustive: false,
			expectedErr:       "",
		},
		{
			name:              "docs limit only",
			seriesLimit:       0,
			docsLimit:         1,
			requireExhaustive: false,
			expectedErr:       "",
		},
		{
			name:              "both series and docs limit",
			seriesLimit:       1,
			docsLimit:         1,
			requireExhaustive: false,
			expectedErr:       "",
		},
		{
			name:              "series limit only",
			seriesLimit:       1,
			docsLimit:         0,
			requireExhaustive: true,
			expectedErr: "query exceeded limit: require_exhaustive=true, " +
				"series_limit=1, series_matched=1, docs_limit=0, docs_matched=4",
			expectedQueryLimitExceededError: true,
		},
		{
			name:              "docs limit only",
			seriesLimit:       0,
			docsLimit:         1,
			requireExhaustive: true,
			expectedErr: "query exceeded limit: require_exhaustive=true, " +
				"series_limit=0, series_matched=1, docs_limit=1, docs_matched=4",
			expectedQueryLimitExceededError: true,
		},
		{
			name:              "both series and docs limit",
			seriesLimit:       1,
			docsLimit:         1,
			requireExhaustive: true,
			expectedErr: "query exceeded limit: require_exhaustive=true, " +
				"series_limit=1, series_matched=1, docs_limit=1, docs_matched=4",
			expectedQueryLimitExceededError: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// only queries as much as is needed (wrt to time)
			ctx := context.NewBackground()
			q := defaultQuery
			qOpts := index.QueryOptions{
				StartInclusive:    t0,
				EndExclusive:      t1.Add(time.Minute),
				SeriesLimit:       test.seriesLimit,
				DocsLimit:         test.docsLimit,
				RequireExhaustive: test.requireExhaustive,
			}

			// Lock to prevent race given these blocks are processed concurrently.
			var resultLock sync.Mutex

			// create initial span from a mock tracer and get ctx
			mtr := mocktracer.New()
			sp := mtr.StartSpan("root")
			ctx.SetGoContext(opentracing.ContextWithSpan(stdlibctx.Background(), sp))

			mockIterActive := index.NewMockQueryIterator(ctrl)
			mockIter := index.NewMockQueryIterator(ctrl)

			docs := []doc.Document{
				// Results in size=1 and docs=2.
				// Byte array represents ID encoded as bytes.
				// 1 represents the ID length in bytes, 49 is the ID itself which is
				// the ASCII value for A
				doc.NewDocumentFromMetadata(doc.Metadata{ID: []byte("A")}),
				doc.NewDocumentFromMetadata(doc.Metadata{ID: []byte("A")}),
			}
			mockQueryWithIter(t, mockIterActive, bActive, q, qOpts, &resultLock, docs)
			mockQueryWithIter(t, mockIter, b0, q, qOpts, &resultLock, docs)

			result, err := idx.Query(ctx, q, qOpts)
			if test.seriesLimit == 0 && test.docsLimit == 0 {
				require.True(t, result.Exhaustive)
			} else {
				require.False(t, result.Exhaustive)
			}

			if test.requireExhaustive {
				require.Error(t, err)
				require.Equal(t, test.expectedErr, err.Error())
				require.Equal(t, test.expectedQueryLimitExceededError, limits.IsQueryLimitExceededError(err))
				require.Equal(t, test.expectedQueryLimitExceededError, xerrors.IsInvalidParams(err))
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNamespaceIndexBlockQueryReleasingContext(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	retention := 2 * time.Hour
	blockSize := time.Hour
	now := xtime.Now().Truncate(blockSize).Add(10 * time.Minute)
	t0 := now.Truncate(blockSize)
	t1 := t0.Add(1 * blockSize)
	t2 := t1.Add(1 * blockSize)
	var nowLock sync.Mutex
	nowFn := func() time.Time {
		nowLock.Lock()
		defer nowLock.Unlock()
		return now.ToTime()
	}
	opts := DefaultTestOptions()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))

	bActive := index.NewMockBlock(ctrl)
	bActive.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	bActive.EXPECT().Close().Return(nil)
	bActive.EXPECT().StartTime().Return(t0).AnyTimes()
	bActive.EXPECT().EndTime().Return(t0.Add(blockSize)).AnyTimes()
	b0 := index.NewMockBlock(ctrl)
	b0.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	b0.EXPECT().Close().Return(nil)
	b0.EXPECT().StartTime().Return(t0).AnyTimes()
	b0.EXPECT().EndTime().Return(t0.Add(blockSize)).AnyTimes()
	b1 := index.NewMockBlock(ctrl)
	b1.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	b1.EXPECT().Close().Return(nil)
	b1.EXPECT().StartTime().Return(t1).AnyTimes()
	b1.EXPECT().EndTime().Return(t1.Add(blockSize)).AnyTimes()
	newBlockFn := func(
		ts xtime.UnixNano,
		md namespace.Metadata,
		opts index.BlockOptions,
		_ namespace.RuntimeOptionsManager,
		io index.Options,
	) (index.Block, error) {
		if opts.ActiveBlock {
			return bActive, nil
		}
		if ts.Equal(t0) {
			return b0, nil
		}
		if ts.Equal(t1) {
			return b1, nil
		}
		panic("should never get here")
	}

	iopts := opts.IndexOptions()
	mockPool := index.NewMockQueryResultsPool(ctrl)
	iopts = iopts.SetQueryResultsPool(mockPool)
	stubResult := index.NewQueryResults(ident.StringID("ns"), index.QueryResultsOptions{}, iopts)

	md := testNamespaceMetadata(blockSize, retention)
	idxIface, err := newNamespaceIndexWithNewBlockFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newBlockFn, opts)
	require.NoError(t, err)

	idx, ok := idxIface.(*nsIndex)
	require.True(t, ok)
	idx.resultsPool = mockPool

	defer func() {
		require.NoError(t, idx.Close())
	}()

	seg1 := segment.NewMockSegment(ctrl)
	seg2 := segment.NewMockSegment(ctrl)
	seg3 := segment.NewMockSegment(ctrl)
	t0Results := result.NewIndexBlockByVolumeType(t0)
	t0Results.SetBlock(idxpersist.DefaultIndexVolumeType, result.NewIndexBlock([]result.Segment{result.NewSegment(seg1, false)},
		result.NewShardTimeRangesFromRange(t0, t1, 1, 2, 3)))
	t1Results := result.NewIndexBlockByVolumeType(t1)
	t1Results.SetBlock(idxpersist.DefaultIndexVolumeType, result.NewIndexBlock([]result.Segment{result.NewSegment(seg2, false), result.NewSegment(seg3, false)},
		result.NewShardTimeRangesFromRange(t1, t2, 1, 2, 3)))
	bootstrapResults := result.IndexResults{
		t0: t0Results,
		t1: t1Results,
	}

	b0.EXPECT().AddResults(bootstrapResults[t0]).Return(nil)
	b1.EXPECT().AddResults(bootstrapResults[t1]).Return(nil)
	require.NoError(t, idx.Bootstrap(bootstrapResults))

	ctx := context.NewBackground()
	q := defaultQuery
	qOpts := index.QueryOptions{
		StartInclusive: t0,
		EndExclusive:   now.Add(time.Minute),
	}
	mockIterActive := index.NewMockQueryIterator(ctrl)
	mockIter := index.NewMockQueryIterator(ctrl)
	gomock.InOrder(
		mockPool.EXPECT().Get().Return(stubResult),
		bActive.EXPECT().QueryIter(ctx, q).Return(mockIterActive, nil),
		b0.EXPECT().QueryIter(ctx, q).Return(mockIter, nil),
		mockPool.EXPECT().Put(stubResult),
	)

	mockIter.EXPECT().Done().Return(true)
	mockIterActive.EXPECT().Done().Return(true)
	mockIter.EXPECT().Close().Return(nil)
	mockIterActive.EXPECT().Close().Return(nil)

	_, err = idx.Query(ctx, q, qOpts)
	require.NoError(t, err)
	ctx.BlockingClose()
}

func TestNamespaceIndexBlockAggregateQuery(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	query := idx.NewTermQuery([]byte("a"), []byte("b"))
	retention := 2 * time.Hour
	blockSize := time.Hour
	now := xtime.Now().Truncate(blockSize).Add(10 * time.Minute)
	t0 := now.Truncate(blockSize)
	t1 := t0.Add(1 * blockSize)
	t2 := t1.Add(1 * blockSize)
	var nowLock sync.Mutex
	nowFn := func() time.Time {
		nowLock.Lock()
		defer nowLock.Unlock()
		return now.ToTime()
	}
	opts := DefaultTestOptions()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))

	bActive := index.NewMockBlock(ctrl)
	bActive.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	bActive.EXPECT().Close().Return(nil)
	bActive.EXPECT().StartTime().Return(t0).AnyTimes()
	bActive.EXPECT().EndTime().Return(t0.Add(blockSize)).AnyTimes()
	b0 := index.NewMockBlock(ctrl)
	b0.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	b0.EXPECT().Close().Return(nil)
	b0.EXPECT().StartTime().Return(t0).AnyTimes()
	b0.EXPECT().EndTime().Return(t0.Add(blockSize)).AnyTimes()
	b1 := index.NewMockBlock(ctrl)
	b1.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	b1.EXPECT().Close().Return(nil)
	b1.EXPECT().StartTime().Return(t1).AnyTimes()
	b1.EXPECT().EndTime().Return(t1.Add(blockSize)).AnyTimes()
	newBlockFn := func(
		ts xtime.UnixNano,
		md namespace.Metadata,
		opts index.BlockOptions,
		_ namespace.RuntimeOptionsManager,
		io index.Options,
	) (index.Block, error) {
		if opts.ActiveBlock {
			return bActive, nil
		}
		if ts.Equal(t0) {
			return b0, nil
		}
		if ts.Equal(t1) {
			return b1, nil
		}
		panic("should never get here")
	}
	md := testNamespaceMetadata(blockSize, retention)
	idx, err := newNamespaceIndexWithNewBlockFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newBlockFn, opts)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, idx.Close())
	}()

	seg1 := segment.NewMockSegment(ctrl)
	seg2 := segment.NewMockSegment(ctrl)
	seg3 := segment.NewMockSegment(ctrl)
	t0Results := result.NewIndexBlockByVolumeType(t0)
	t0Results.SetBlock(idxpersist.DefaultIndexVolumeType, result.NewIndexBlock([]result.Segment{result.NewSegment(seg1, false)},
		result.NewShardTimeRangesFromRange(t0, t1, 1, 2, 3)))
	t1Results := result.NewIndexBlockByVolumeType(t1)
	t1Results.SetBlock(idxpersist.DefaultIndexVolumeType, result.NewIndexBlock([]result.Segment{result.NewSegment(seg2, false), result.NewSegment(seg3, false)},
		result.NewShardTimeRangesFromRange(t1, t2, 1, 2, 3)))
	bootstrapResults := result.IndexResults{
		t0: t0Results,
		t1: t1Results,
	}

	b0.EXPECT().AddResults(bootstrapResults[t0]).Return(nil)
	b1.EXPECT().AddResults(bootstrapResults[t1]).Return(nil)
	require.NoError(t, idx.Bootstrap(bootstrapResults))

	for _, test := range []struct {
		name              string
		requireExhaustive bool
	}{
		{"allow non-exhaustive", false},
		{"require exhaustive", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			// only queries as much as is needed (wrt to time)
			ctx := context.NewBackground()

			// create initial span from a mock tracer and get ctx
			mtr := mocktracer.New()
			sp := mtr.StartSpan("root")
			ctx.SetGoContext(opentracing.ContextWithSpan(stdlibctx.Background(), sp))

			q := index.Query{
				Query: query,
			}
			qOpts := index.QueryOptions{
				StartInclusive:    t0,
				EndExclusive:      now.Add(time.Minute),
				RequireExhaustive: test.requireExhaustive,
			}
			aggOpts := index.AggregationOptions{QueryOptions: qOpts}

			mockIterActive := index.NewMockAggregateIterator(ctrl)
			bActive.EXPECT().AggregateIter(gomock.Any(), gomock.Any()).Return(mockIterActive, nil)
			mockIterActive.EXPECT().Done().Return(true)
			mockIterActive.EXPECT().Close().Return(nil)
			mockIter0 := index.NewMockAggregateIterator(ctrl)
			b0.EXPECT().AggregateIter(gomock.Any(), gomock.Any()).Return(mockIter0, nil)
			mockIter0.EXPECT().Done().Return(true)
			mockIter0.EXPECT().Close().Return(nil)
			result, err := idx.AggregateQuery(ctx, q, aggOpts)
			require.NoError(t, err)
			require.True(t, result.Exhaustive)

			// queries multiple blocks if needed
			qOpts = index.QueryOptions{
				StartInclusive:    t0,
				EndExclusive:      t2.Add(time.Minute),
				RequireExhaustive: test.requireExhaustive,
			}
			aggOpts = index.AggregationOptions{QueryOptions: qOpts}
			bActive.EXPECT().AggregateIter(gomock.Any(), gomock.Any()).Return(mockIterActive, nil)
			mockIterActive.EXPECT().Done().Return(true)
			mockIterActive.EXPECT().Close().Return(nil)
			b0.EXPECT().AggregateIter(gomock.Any(), gomock.Any()).Return(mockIter0, nil)
			mockIter0.EXPECT().Done().Return(true)
			mockIter0.EXPECT().Close().Return(nil)

			mockIter1 := index.NewMockAggregateIterator(ctrl)
			b1.EXPECT().AggregateIter(gomock.Any(), gomock.Any()).Return(mockIter1, nil)
			mockIter1.EXPECT().Done().Return(true)
			mockIter1.EXPECT().Close().Return(nil)
			result, err = idx.AggregateQuery(ctx, q, aggOpts)
			require.NoError(t, err)
			require.True(t, result.Exhaustive)

			// stops querying once a block returns non-exhaustive
			qOpts = index.QueryOptions{
				StartInclusive:    t0,
				EndExclusive:      t0.Add(time.Minute),
				RequireExhaustive: test.requireExhaustive,
				DocsLimit:         1,
			}
			bActive.EXPECT().AggregateIter(gomock.Any(), gomock.Any()).Return(mockIterActive, nil)
			//nolint: dupl
			bActive.EXPECT().
				AggregateWithIter(gomock.Any(), mockIter0, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					iter index.AggregateIterator,
					opts index.QueryOptions,
					results index.AggregateResults,
					deadline time.Time,
					logFields []opentracinglog.Field,
				) error {
					_, _ = results.AddFields([]index.AggregateResultsEntry{{
						Field: ident.StringID("A"),
						Terms: []ident.ID{ident.StringID("foo")},
					}, {
						Field: ident.StringID("B"),
						Terms: []ident.ID{ident.StringID("bar")},
					}})
					return nil
				})
			gomock.InOrder(
				mockIterActive.EXPECT().Done().Return(false),
				mockIterActive.EXPECT().Done().Return(true),
				mockIterActive.EXPECT().Close().Return(nil),
			)
			b0.EXPECT().AggregateIter(gomock.Any(), gomock.Any()).Return(mockIter0, nil)
			//nolint: dupl
			b0.EXPECT().
				AggregateWithIter(gomock.Any(), mockIter0, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					iter index.AggregateIterator,
					opts index.QueryOptions,
					results index.AggregateResults,
					deadline time.Time,
					logFields []opentracinglog.Field,
				) error {
					_, _ = results.AddFields([]index.AggregateResultsEntry{{
						Field: ident.StringID("A"),
						Terms: []ident.ID{ident.StringID("foo")},
					}, {
						Field: ident.StringID("B"),
						Terms: []ident.ID{ident.StringID("bar")},
					}})
					return nil
				})
			gomock.InOrder(
				mockIter0.EXPECT().Done().Return(false),
				mockIter0.EXPECT().Done().Return(true),
				mockIter0.EXPECT().Close().Return(nil),
			)
			aggOpts = index.AggregationOptions{QueryOptions: qOpts}
			result, err = idx.AggregateQuery(ctx, q, aggOpts)
			if test.requireExhaustive {
				require.Error(t, err)
				require.False(t, xerrors.IsRetryableError(err))
			} else {
				require.NoError(t, err)
				require.False(t, result.Exhaustive)
			}

			sp.Finish()
			spans := mtr.FinishedSpans()
			require.Len(t, spans, 9)
		})
	}
}

func TestNamespaceIndexBlockAggregateQueryReleasingContext(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	retention := 2 * time.Hour
	blockSize := time.Hour
	now := xtime.Now().Truncate(blockSize).Add(10 * time.Minute)
	t0 := now.Truncate(blockSize)
	t1 := t0.Add(1 * blockSize)
	t2 := t1.Add(1 * blockSize)
	var nowLock sync.Mutex
	nowFn := func() time.Time {
		nowLock.Lock()
		defer nowLock.Unlock()
		return now.ToTime()
	}
	opts := DefaultTestOptions()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))

	query := idx.NewTermQuery([]byte("a"), []byte("b"))
	bActive := index.NewMockBlock(ctrl)
	bActive.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	bActive.EXPECT().Close().Return(nil)
	bActive.EXPECT().StartTime().Return(t0).AnyTimes()
	bActive.EXPECT().EndTime().Return(t0.Add(blockSize)).AnyTimes()
	b0 := index.NewMockBlock(ctrl)
	b0.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	b0.EXPECT().Close().Return(nil)
	b0.EXPECT().StartTime().Return(t0).AnyTimes()
	b0.EXPECT().EndTime().Return(t0.Add(blockSize)).AnyTimes()
	b1 := index.NewMockBlock(ctrl)
	b1.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	b1.EXPECT().Close().Return(nil)
	b1.EXPECT().StartTime().Return(t1).AnyTimes()
	b1.EXPECT().EndTime().Return(t1.Add(blockSize)).AnyTimes()
	newBlockFn := func(
		ts xtime.UnixNano,
		md namespace.Metadata,
		opts index.BlockOptions,
		_ namespace.RuntimeOptionsManager,
		io index.Options,
	) (index.Block, error) {
		if opts.ActiveBlock {
			return bActive, nil
		}
		if ts.Equal(t0) {
			return b0, nil
		}
		if ts.Equal(t1) {
			return b1, nil
		}
		panic("should never get here")
	}

	iopts := opts.IndexOptions()
	mockPool := index.NewMockAggregateResultsPool(ctrl)
	iopts = iopts.SetAggregateResultsPool(mockPool)
	stubResult := index.NewAggregateResults(ident.StringID("ns"),
		index.AggregateResultsOptions{}, iopts)

	md := testNamespaceMetadata(blockSize, retention)
	idxIface, err := newNamespaceIndexWithNewBlockFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newBlockFn, opts)
	require.NoError(t, err)

	idx, ok := idxIface.(*nsIndex)
	require.True(t, ok)
	idx.aggregateResultsPool = mockPool

	defer func() {
		require.NoError(t, idx.Close())
	}()

	seg1 := segment.NewMockSegment(ctrl)
	seg2 := segment.NewMockSegment(ctrl)
	seg3 := segment.NewMockSegment(ctrl)
	t0Results := result.NewIndexBlockByVolumeType(t0)
	t0Results.SetBlock(idxpersist.DefaultIndexVolumeType, result.NewIndexBlock([]result.Segment{result.NewSegment(seg1, false)},
		result.NewShardTimeRangesFromRange(t0, t1, 1, 2, 3)))
	t1Results := result.NewIndexBlockByVolumeType(t1)
	t1Results.SetBlock(idxpersist.DefaultIndexVolumeType, result.NewIndexBlock([]result.Segment{result.NewSegment(seg2, false), result.NewSegment(seg3, false)},
		result.NewShardTimeRangesFromRange(t1, t2, 1, 2, 3)))
	bootstrapResults := result.IndexResults{
		t0: t0Results,
		t1: t1Results,
	}

	b0.EXPECT().AddResults(bootstrapResults[t0]).Return(nil)
	b1.EXPECT().AddResults(bootstrapResults[t1]).Return(nil)
	require.NoError(t, idx.Bootstrap(bootstrapResults))

	// only queries as much as is needed (wrt to time)
	ctx := context.NewBackground()
	q := index.Query{
		Query: query,
	}
	qOpts := index.QueryOptions{
		StartInclusive: t0,
		EndExclusive:   now.Add(time.Minute),
	}
	aggOpts := index.AggregationOptions{QueryOptions: qOpts}

	mockIterActive := index.NewMockAggregateIterator(ctrl)
	mockIter := index.NewMockAggregateIterator(ctrl)
	gomock.InOrder(
		mockPool.EXPECT().Get().Return(stubResult),
		bActive.EXPECT().AggregateIter(ctx, gomock.Any()).Return(mockIterActive, nil),
		b0.EXPECT().AggregateIter(ctx, gomock.Any()).Return(mockIter, nil),
		mockPool.EXPECT().Put(stubResult),
	)
	mockIter.EXPECT().Done().Return(true)
	mockIterActive.EXPECT().Done().Return(true)
	mockIter.EXPECT().Close().Return(nil)
	mockIterActive.EXPECT().Close().Return(nil)

	_, err = idx.AggregateQuery(ctx, q, aggOpts)
	require.NoError(t, err)
	ctx.BlockingClose()
}

func TestNamespaceIndexBlockAggregateQueryAggPath(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	queries := []idx.Query{idx.NewAllQuery(), idx.NewFieldQuery([]byte("field"))}
	retention := 2 * time.Hour
	blockSize := time.Hour
	now := xtime.Now().Truncate(blockSize).Add(10 * time.Minute)
	t0 := now.Truncate(blockSize)
	t1 := t0.Add(1 * blockSize)
	t2 := t1.Add(1 * blockSize)
	var nowLock sync.Mutex
	nowFn := func() time.Time {
		nowLock.Lock()
		defer nowLock.Unlock()
		return now.ToTime()
	}
	opts := DefaultTestOptions()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(nowFn))

	bActive := index.NewMockBlock(ctrl)
	bActive.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	bActive.EXPECT().Close().Return(nil)
	bActive.EXPECT().StartTime().Return(t0).AnyTimes()
	bActive.EXPECT().EndTime().Return(t1).AnyTimes()
	b0 := index.NewMockBlock(ctrl)
	b0.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	b0.EXPECT().Close().Return(nil)
	b0.EXPECT().StartTime().Return(t0).AnyTimes()
	b0.EXPECT().EndTime().Return(t1).AnyTimes()
	b1 := index.NewMockBlock(ctrl)
	b1.EXPECT().Stats(gomock.Any()).Return(nil).AnyTimes()
	b1.EXPECT().Close().Return(nil)
	b1.EXPECT().StartTime().Return(t1).AnyTimes()
	b1.EXPECT().EndTime().Return(t2).AnyTimes()
	newBlockFn := func(
		ts xtime.UnixNano,
		md namespace.Metadata,
		opts index.BlockOptions,
		_ namespace.RuntimeOptionsManager,
		io index.Options,
	) (index.Block, error) {
		if opts.ActiveBlock {
			return bActive, nil
		}
		if ts.Equal(t0) {
			return b0, nil
		}
		if ts.Equal(t1) {
			return b1, nil
		}
		panic("should never get here")
	}
	md := testNamespaceMetadata(blockSize, retention)
	idx, err := newNamespaceIndexWithNewBlockFn(md,
		namespace.NewRuntimeOptionsManager(md.ID().String()),
		testShardSet, newBlockFn, opts)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, idx.Close())
	}()

	seg1 := segment.NewMockSegment(ctrl)
	seg2 := segment.NewMockSegment(ctrl)
	seg3 := segment.NewMockSegment(ctrl)
	t0Results := result.NewIndexBlockByVolumeType(t0)
	t0Results.SetBlock(idxpersist.DefaultIndexVolumeType, result.NewIndexBlock([]result.Segment{result.NewSegment(seg1, false)},
		result.NewShardTimeRangesFromRange(t0, t1, 1, 2, 3)))
	t1Results := result.NewIndexBlockByVolumeType(t1)
	t1Results.SetBlock(idxpersist.DefaultIndexVolumeType, result.NewIndexBlock([]result.Segment{result.NewSegment(seg2, false), result.NewSegment(seg3, false)},
		result.NewShardTimeRangesFromRange(t1, t2, 1, 2, 3)))
	bootstrapResults := result.IndexResults{
		t0: t0Results,
		t1: t1Results,
	}

	b0.EXPECT().AddResults(bootstrapResults[t0]).Return(nil)
	b1.EXPECT().AddResults(bootstrapResults[t1]).Return(nil)
	require.NoError(t, idx.Bootstrap(bootstrapResults))

	// only queries as much as is needed (wrt to time)
	ctx := context.NewBackground()

	qOpts := index.QueryOptions{
		StartInclusive: t0,
		EndExclusive:   now.Add(time.Minute),
	}
	aggOpts := index.AggregationOptions{QueryOptions: qOpts}

	for _, test := range []struct {
		name              string
		requireExhaustive bool
	}{
		{"allow non-exhaustive", false},
		{"require exhaustive", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, query := range queries {
				q := index.Query{
					Query: query,
				}
				mockIterActive := index.NewMockAggregateIterator(ctrl)
				mockIterActive.EXPECT().Done().Return(true)
				mockIterActive.EXPECT().Close().Return(nil)
				bActive.EXPECT().AggregateIter(ctx, gomock.Any()).Return(mockIterActive, nil)
				mockIter0 := index.NewMockAggregateIterator(ctrl)
				mockIter0.EXPECT().Done().Return(true)
				mockIter0.EXPECT().Close().Return(nil)
				b0.EXPECT().AggregateIter(ctx, gomock.Any()).Return(mockIter0, nil)
				result, err := idx.AggregateQuery(ctx, q, aggOpts)
				require.NoError(t, err)
				require.True(t, result.Exhaustive)

				// queries multiple blocks if needed
				qOpts = index.QueryOptions{
					StartInclusive:    t0,
					EndExclusive:      t2.Add(time.Minute),
					RequireExhaustive: test.requireExhaustive,
				}
				aggOpts = index.AggregationOptions{QueryOptions: qOpts}

				mockIterActive.EXPECT().Done().Return(true)
				mockIterActive.EXPECT().Close().Return(nil)
				bActive.EXPECT().AggregateIter(ctx, gomock.Any()).Return(mockIterActive, nil)

				mockIter0.EXPECT().Done().Return(true)
				mockIter0.EXPECT().Close().Return(nil)
				b0.EXPECT().AggregateIter(ctx, gomock.Any()).Return(mockIter0, nil)

				mockIter1 := index.NewMockAggregateIterator(ctrl)
				mockIter1.EXPECT().Done().Return(true)
				mockIter1.EXPECT().Close().Return(nil)
				b1.EXPECT().AggregateIter(ctx, gomock.Any()).Return(mockIter1, nil)
				result, err = idx.AggregateQuery(ctx, q, aggOpts)
				require.NoError(t, err)
				require.True(t, result.Exhaustive)

				// stops querying once a block returns non-exhaustive
				qOpts = index.QueryOptions{
					StartInclusive:    t0,
					EndExclusive:      t0.Add(time.Minute),
					RequireExhaustive: test.requireExhaustive,
					DocsLimit:         1,
				}
				bActive.EXPECT().AggregateIter(gomock.Any(), gomock.Any()).Return(mockIterActive, nil)
				//nolint: dupl
				bActive.EXPECT().
					AggregateWithIter(gomock.Any(), mockIter0, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(
						ctx context.Context,
						iter index.AggregateIterator,
						opts index.QueryOptions,
						results index.AggregateResults,
						deadline time.Time,
						logFields []opentracinglog.Field,
					) error {
						_, _ = results.AddFields([]index.AggregateResultsEntry{{
							Field: ident.StringID("A"),
							Terms: []ident.ID{ident.StringID("foo")},
						}, {
							Field: ident.StringID("B"),
							Terms: []ident.ID{ident.StringID("bar")},
						}})
						return nil
					})
				gomock.InOrder(
					mockIterActive.EXPECT().Done().Return(false),
					mockIterActive.EXPECT().Done().Return(true),
					mockIterActive.EXPECT().Close().Return(nil),
				)
				b0.EXPECT().AggregateIter(gomock.Any(), gomock.Any()).Return(mockIter0, nil)
				//nolint: dupl
				b0.EXPECT().
					AggregateWithIter(gomock.Any(), mockIter0, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					DoAndReturn(func(
						ctx context.Context,
						iter index.AggregateIterator,
						opts index.QueryOptions,
						results index.AggregateResults,
						deadline time.Time,
						logFields []opentracinglog.Field,
					) error {
						_, _ = results.AddFields([]index.AggregateResultsEntry{{
							Field: ident.StringID("A"),
							Terms: []ident.ID{ident.StringID("foo")},
						}, {
							Field: ident.StringID("B"),
							Terms: []ident.ID{ident.StringID("bar")},
						}})
						return nil
					})
				gomock.InOrder(
					mockIter0.EXPECT().Done().Return(false),
					mockIter0.EXPECT().Done().Return(true),
					mockIter0.EXPECT().Close().Return(nil),
				)
				aggOpts = index.AggregationOptions{QueryOptions: qOpts}
				result, err = idx.AggregateQuery(ctx, q, aggOpts)
				if test.requireExhaustive {
					require.Error(t, err)
					require.False(t, xerrors.IsRetryableError(err))
				} else {
					require.NoError(t, err)
					require.False(t, result.Exhaustive)
				}
			}
		})
	}
}

func mockWriteBatch(t *testing.T,
	now *xtime.UnixNano,
	lifecycle *doc.MockOnIndexSeries,
	block *index.MockBlock,
	tag *ident.Tag,
) {
	block.EXPECT().
		WriteBatch(gomock.Any()).
		Return(index.WriteBatchResult{}, nil).
		Do(func(batch *index.WriteBatch) {
			docs := batch.PendingDocs()
			require.Equal(t, 1, len(docs))
			require.Equal(t, doc.Metadata{
				ID:     id.Bytes(),
				Fields: doc.Fields{{Name: tag.Name.Bytes(), Value: tag.Value.Bytes()}},
			}, docs[0])
			entries := batch.PendingEntries()
			require.Equal(t, 1, len(entries))
			require.True(t, entries[0].Timestamp.Equal(*now))
			require.True(t, entries[0].OnIndexSeries == lifecycle) // Just ptr equality
		})
}

func mockQueryWithIter(t *testing.T,
	iter *index.MockQueryIterator,
	block *index.MockBlock,
	q index.Query,
	qOpts index.QueryOptions,
	resultLock *sync.Mutex,
	docsToAdd []doc.Document,
) {
	block.EXPECT().QueryIter(gomock.Any(), q).Return(iter, nil)
	block.EXPECT().QueryWithIter(gomock.Any(), qOpts, iter, gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			ctx context.Context,
			opts index.QueryOptions,
			iter index.QueryIterator,
			r index.QueryResults,
			deadline time.Time,
			logFields []opentracinglog.Field,
		) error {
			resultLock.Lock()
			defer resultLock.Unlock()
			_, _, err := r.AddDocuments(docsToAdd)
			require.NoError(t, err)
			return nil
		})
	gomock.InOrder(
		iter.EXPECT().Done().Return(false),
		iter.EXPECT().Done().Return(true),
		iter.EXPECT().Close().Return(nil),
	)
}
