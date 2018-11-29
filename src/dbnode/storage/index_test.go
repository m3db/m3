// Copyright (c) 2018 Uber Technologies, Inc.
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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	xsync "github.com/m3db/m3x/sync"
	xtest "github.com/m3db/m3x/test"
	xtime "github.com/m3db/m3x/time"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNamespaceIndexCleanupExpiredFilesets(t *testing.T) {
	md := testNamespaceMetadata(time.Hour, time.Hour*8)
	nsIdx, err := newNamespaceIndex(md, testDatabaseOptions())
	require.NoError(t, err)

	now := time.Now().Truncate(time.Hour)
	idx := nsIdx.(*nsIndex)

	oldestTime := now.Add(-time.Hour * 8)
	files := []string{"abc"}

	idx.indexFilesetsBeforeFn = func(dir string, nsID ident.ID, exclusiveTime time.Time) ([]string, error) {
		require.True(t, oldestTime.Equal(exclusiveTime), fmt.Sprintf("%v %v", exclusiveTime, oldestTime))
		return files, nil
	}
	idx.deleteFilesFn = func(s []string) error {
		require.Equal(t, files, s)
		return nil
	}
	require.NoError(t, idx.CleanupExpiredFileSets(now))
}

func TestNamespaceIndexCleanupExpiredFilesetsWithBlocks(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	md := testNamespaceMetadata(time.Hour, time.Hour*8)
	nsIdx, err := newNamespaceIndex(md, testDatabaseOptions())
	require.NoError(t, err)

	now := time.Now().Truncate(time.Hour)
	idx := nsIdx.(*nsIndex)

	mockBlock := index.NewMockBlock(ctrl)
	oldestTime := now.Add(-time.Hour * 9)
	idx.state.blocksByTime[xtime.ToUnixNano(oldestTime)] = mockBlock

	idx.indexFilesetsBeforeFn = func(dir string, nsID ident.ID, exclusiveTime time.Time) ([]string, error) {
		require.True(t, exclusiveTime.Equal(oldestTime))
		return nil, nil
	}
	require.NoError(t, idx.CleanupExpiredFileSets(now))
}

func TestNamespaceIndexFlushSuccess(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	test := newTestIndex(t, ctrl)

	now := time.Now().Truncate(test.indexBlockSize)
	idx := test.index.(*nsIndex)

	mockBlock := index.NewMockBlock(ctrl)
	blockTime := now.Add(-2 * test.indexBlockSize)
	mockBlock.EXPECT().StartTime().Return(blockTime).AnyTimes()
	mockBlock.EXPECT().EndTime().Return(blockTime.Add(test.indexBlockSize)).AnyTimes()
	idx.state.blocksByTime[xtime.ToUnixNano(blockTime)] = mockBlock

	mockBlock.EXPECT().IsSealed().Return(true)
	mockBlock.EXPECT().NeedsMutableSegmentsEvicted().Return(true)

	mockShard := NewMockdatabaseShard(ctrl)
	mockShard.EXPECT().ID().Return(uint32(0)).AnyTimes()
	mockShard.EXPECT().FlushState(blockTime).Return(fileOpState{Status: fileOpSuccess})
	mockShard.EXPECT().FlushState(blockTime.Add(test.blockSize)).Return(fileOpState{Status: fileOpSuccess})
	shards := []databaseShard{mockShard}

	mockFlush := persist.NewMockIndexFlush(ctrl)

	persistClosed := false
	persistCalled := false
	closer := func() ([]segment.Segment, error) {
		persistClosed = true
		return nil, nil
	}
	persistFn := func(segment.MutableSegment) error {
		persistCalled = true
		return nil
	}
	preparedPersist := persist.PreparedIndexPersist{
		Close:   closer,
		Persist: persistFn,
	}
	mockFlush.EXPECT().PrepareIndex(xtest.CmpMatcher(persist.IndexPrepareOptions{
		NamespaceMetadata: test.metadata,
		BlockStart:        blockTime,
		FileSetType:       persist.FileSetFlushType,
		Shards:            map[uint32]struct{}{0: struct{}{}},
	})).Return(preparedPersist, nil)

	results := block.NewMockFetchBlocksMetadataResults(ctrl)
	results.EXPECT().Results().Return(nil)
	results.EXPECT().Close()
	mockShard.EXPECT().FetchBlocksMetadataV2(gomock.Any(), blockTime, blockTime.Add(test.indexBlockSize),
		gomock.Any(), gomock.Any(), block.FetchBlocksMetadataOptions{}).Return(results, nil, nil)

	mockBlock.EXPECT().AddResults(gomock.Any()).Return(nil)
	mockBlock.EXPECT().EvictMutableSegments().Return(index.EvictMutableSegmentResults{}, nil)

	require.NoError(t, idx.Flush(mockFlush, shards))
	require.True(t, persistCalled)
	require.True(t, persistClosed)
}

func TestNamespaceIndexFlushShardStateNotSuccess(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	test := newTestIndex(t, ctrl)

	now := time.Now().Truncate(test.indexBlockSize)
	idx := test.index.(*nsIndex)

	mockBlock := index.NewMockBlock(ctrl)
	blockTime := now.Add(-2 * test.indexBlockSize)
	mockBlock.EXPECT().StartTime().Return(blockTime).AnyTimes()
	mockBlock.EXPECT().EndTime().Return(blockTime.Add(test.indexBlockSize)).AnyTimes()
	idx.state.blocksByTime[xtime.ToUnixNano(blockTime)] = mockBlock

	mockBlock.EXPECT().IsSealed().Return(true)
	mockBlock.EXPECT().NeedsMutableSegmentsEvicted().Return(true)

	mockShard := NewMockdatabaseShard(ctrl)
	mockShard.EXPECT().ID().Return(uint32(0)).AnyTimes()
	mockShard.EXPECT().FlushState(blockTime).Return(fileOpState{Status: fileOpSuccess})
	mockShard.EXPECT().FlushState(blockTime.Add(test.blockSize)).Return(fileOpState{Status: fileOpFailed})
	shards := []databaseShard{mockShard}

	mockFlush := persist.NewMockIndexFlush(ctrl)

	require.NoError(t, idx.Flush(mockFlush, shards))
}

func TestNamespaceIndexFlushSuccessMultipleShards(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	test := newTestIndex(t, ctrl)

	now := time.Now().Truncate(test.indexBlockSize)
	idx := test.index.(*nsIndex)

	mockBlock := index.NewMockBlock(ctrl)
	blockTime := now.Add(-2 * test.indexBlockSize)
	mockBlock.EXPECT().StartTime().Return(blockTime).AnyTimes()
	mockBlock.EXPECT().EndTime().Return(blockTime.Add(test.indexBlockSize)).AnyTimes()
	idx.state.blocksByTime[xtime.ToUnixNano(blockTime)] = mockBlock

	mockBlock.EXPECT().IsSealed().Return(true)
	mockBlock.EXPECT().NeedsMutableSegmentsEvicted().Return(true)

	mockShard1 := NewMockdatabaseShard(ctrl)
	mockShard1.EXPECT().ID().Return(uint32(0)).AnyTimes()
	mockShard1.EXPECT().FlushState(blockTime).Return(fileOpState{Status: fileOpSuccess})
	mockShard1.EXPECT().FlushState(blockTime.Add(test.blockSize)).Return(fileOpState{Status: fileOpSuccess})

	mockShard2 := NewMockdatabaseShard(ctrl)
	mockShard2.EXPECT().ID().Return(uint32(1)).AnyTimes()
	mockShard2.EXPECT().FlushState(blockTime).Return(fileOpState{Status: fileOpSuccess})
	mockShard2.EXPECT().FlushState(blockTime.Add(test.blockSize)).Return(fileOpState{Status: fileOpSuccess})

	shards := []databaseShard{mockShard1, mockShard2}

	mockFlush := persist.NewMockIndexFlush(ctrl)

	persistClosed := false
	numPersistCalls := 0
	closer := func() ([]segment.Segment, error) {
		persistClosed = true
		return nil, nil
	}
	persistFn := func(segment.MutableSegment) error {
		numPersistCalls++
		return nil
	}
	preparedPersist := persist.PreparedIndexPersist{
		Close:   closer,
		Persist: persistFn,
	}
	mockFlush.EXPECT().PrepareIndex(xtest.CmpMatcher(persist.IndexPrepareOptions{
		NamespaceMetadata: test.metadata,
		BlockStart:        blockTime,
		FileSetType:       persist.FileSetFlushType,
		Shards:            map[uint32]struct{}{0: struct{}{}, 1: struct{}{}},
	})).Return(preparedPersist, nil)

	results1 := block.NewMockFetchBlocksMetadataResults(ctrl)
	results1.EXPECT().Results().Return(nil)
	results1.EXPECT().Close()
	mockShard1.EXPECT().FetchBlocksMetadataV2(gomock.Any(), blockTime, blockTime.Add(test.indexBlockSize),
		gomock.Any(), gomock.Any(), block.FetchBlocksMetadataOptions{}).Return(results1, nil, nil)

	results2 := block.NewMockFetchBlocksMetadataResults(ctrl)
	results2.EXPECT().Results().Return(nil)
	results2.EXPECT().Close()
	mockShard2.EXPECT().FetchBlocksMetadataV2(gomock.Any(), blockTime, blockTime.Add(test.indexBlockSize),
		gomock.Any(), gomock.Any(), block.FetchBlocksMetadataOptions{}).Return(results2, nil, nil)

	mockBlock.EXPECT().AddResults(gomock.Any()).Return(nil)
	mockBlock.EXPECT().EvictMutableSegments().Return(index.EvictMutableSegmentResults{}, nil)

	require.NoError(t, idx.Flush(mockFlush, shards))
	require.Equal(t, 2, numPersistCalls)
	require.True(t, persistClosed)
}

func TestNamespaceIndexQueryNoMatchingBlocks(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	test := newTestIndex(t, ctrl)

	now := time.Now().Truncate(test.indexBlockSize)
	query := index.Query{idx.NewTermQuery([]byte("foo"), []byte("bar"))}
	idx := test.index.(*nsIndex)

	mockBlock := index.NewMockBlock(ctrl)
	blockTime := now.Add(-1 * test.indexBlockSize)
	mockBlock.EXPECT().StartTime().Return(blockTime).AnyTimes()
	mockBlock.EXPECT().EndTime().Return(blockTime.Add(test.indexBlockSize)).AnyTimes()
	idx.state.blocksByTime[xtime.ToUnixNano(blockTime)] = mockBlock

	ctx := context.NewContext()
	defer ctx.Close()

	// Query non-overlapping range
	result, err := idx.Query(ctx, query, index.QueryOptions{
		StartInclusive: now.Add(-3 * test.indexBlockSize),
		EndExclusive:   now.Add(-2 * test.indexBlockSize),
	})
	require.NoError(t, err)
	assert.True(t, result.Exhaustive)
	assert.Equal(t, 0, result.Results.Size())
}

func TestNamespaceIndexHighConcurrentQueriesWithoutTimeouts(t *testing.T) {
	testNamespaceIndexHighConcurrentQueries(t,
		testNamespaceIndexHighConcurrentQueriesOptions{
			withTimeouts: false,
		})
}

func TestNamespaceIndexHighConcurrentQueriesWithTimeouts(t *testing.T) {
	testNamespaceIndexHighConcurrentQueries(t,
		testNamespaceIndexHighConcurrentQueriesOptions{
			withTimeouts: true,
		})
}

func TestNamespaceIndexHighConcurrentQueriesWithTimeoutsAndForceTimeout(t *testing.T) {
	testNamespaceIndexHighConcurrentQueries(t,
		testNamespaceIndexHighConcurrentQueriesOptions{
			withTimeouts:  true,
			forceTimeouts: true,
		})
}

type testNamespaceIndexHighConcurrentQueriesOptions struct {
	withTimeouts  bool
	forceTimeouts bool
}

func testNamespaceIndexHighConcurrentQueries(
	t *testing.T,
	opts testNamespaceIndexHighConcurrentQueriesOptions,
) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	defer leaktest.CheckTimeout(t, 2*time.Minute)()

	test := newTestIndex(t, ctrl)
	defer func() {
		err := test.index.Close()
		require.NoError(t, err)
	}()

	now := time.Now().Truncate(test.indexBlockSize)

	min, max := now.Add(-6*test.indexBlockSize), now.Add(-test.indexBlockSize)

	var timeoutValue time.Duration
	if opts.withTimeouts {
		timeoutValue = time.Minute
	}
	if opts.forceTimeouts {
		timeoutValue = time.Second
	}

	nsIdx := test.index.(*nsIndex)
	nsIdx.state.Lock()
	// Make the query pool really high to improve concurrency likelihood
	nsIdx.queryWorkersPool = xsync.NewWorkerPool(1000)
	nsIdx.queryWorkersPool.Init()
	if opts.withTimeouts {
		nsIdx.state.runtimeOpts.defaultQueryTimeout = timeoutValue
	} else {
		nsIdx.state.runtimeOpts.defaultQueryTimeout = 0
	}

	currNow := min
	nowLock := &sync.Mutex{}
	nsIdx.nowFn = func() time.Time {
		nowLock.Lock()
		defer nowLock.Unlock()
		return currNow
	}
	setNow := func(t time.Time) {
		nowLock.Lock()
		defer nowLock.Unlock()
		currNow = t
	}
	nsIdx.state.Unlock()

	restoreNow := func() {
		nsIdx.state.Lock()
		nsIdx.nowFn = time.Now
		nsIdx.state.Unlock()
	}

	var (
		idsPerBlock     = 16
		expectedResults = make(map[string]doc.Document)
		blockStarts     []time.Time
		blockIdx        = -1
	)
	for st := min; !st.After(max); st = st.Add(test.indexBlockSize) {
		blockIdx++
		blockStarts = append(blockStarts, st)

		mutableBlockTime := st.Add(test.indexBlockSize).Add(-1 * (test.blockSize / 2))
		setNow(mutableBlockTime)

		var onIndexWg sync.WaitGroup
		onIndexWg.Add(idsPerBlock)
		onIndexSeries := index.NewMockOnIndexSeries(ctrl)
		onIndexSeries.EXPECT().
			OnIndexSuccess(gomock.Any()).
			Times(idsPerBlock).
			Do(func(arg interface{}) {
				onIndexWg.Done()
			})
		onIndexSeries.EXPECT().
			OnIndexFinalize(gomock.Any()).
			Times(idsPerBlock)

		batch := index.NewWriteBatch(index.WriteBatchOptions{
			InitialCapacity: idsPerBlock,
			IndexBlockSize:  test.indexBlockSize,
		})
		for i := 0; i < idsPerBlock; i++ {
			id := fmt.Sprintf("foo.block_%d.id_%d", blockIdx, i)
			doc := doc.Document{
				ID: []byte(id),
				Fields: []doc.Field{
					{
						Name:  []byte("bar"),
						Value: []byte(fmt.Sprintf("baz.%d", i)),
					},
					{
						Name:  []byte("qux"),
						Value: []byte("qaz"),
					},
				},
			}
			expectedResults[id] = doc
			batch.Append(index.WriteBatchEntry{
				Timestamp:     mutableBlockTime,
				OnIndexSeries: onIndexSeries,
			}, doc)
		}

		err := test.index.WriteBatch(batch)
		require.NoError(t, err)
		onIndexWg.Wait()
	}

	// If force timeout, replace one of the blocks with a mock
	// block that times out.
	var timeoutWg, timedOutQueriesWg sync.WaitGroup
	if opts.forceTimeouts {
		// Need to restore now as timeouts are measured by looking at time.Now
		restoreNow()

		timeoutWg.Add(1)
		nsIdx.state.Lock()
		for start, block := range nsIdx.state.blocksByTime {
			block := block // Capture for lambda
			mockBlock := index.NewMockBlock(ctrl)
			mockBlock.EXPECT().
				StartTime().
				DoAndReturn(func() time.Time { return block.StartTime() }).
				AnyTimes()
			mockBlock.EXPECT().
				EndTime().
				DoAndReturn(func() time.Time { return block.EndTime() }).
				AnyTimes()
			mockBlock.EXPECT().
				Query(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(q index.Query, opts index.QueryOptions, r index.Results) (bool, error) {
					timeoutWg.Wait()
					return block.Query(q, opts, r)
				}).
				AnyTimes()
			mockBlock.EXPECT().
				Close().
				DoAndReturn(func() error {
					return block.Close()
				})
			nsIdx.state.blocksByTime[start] = mockBlock
		}
		nsIdx.state.Unlock()
	}

	var (
		query               = idx.NewTermQuery([]byte("qux"), []byte("qaz"))
		queryConcurrency    = 16
		startWg, readyWg    sync.WaitGroup
		timeoutContextsLock sync.Mutex
		timeoutContexts     []context.Context
	)

	var enqueueWg sync.WaitGroup
	startWg.Add(1)
	for i := 0; i < queryConcurrency; i++ {
		readyWg.Add(1)
		enqueueWg.Add(1)
		go func() {
			defer enqueueWg.Done()
			readyWg.Done()
			startWg.Wait()

			rangeStart := min
			for k := 0; k < len(blockStarts); k++ {
				rangeEnd := blockStarts[k].Add(test.indexBlockSize)

				ctx := context.NewContext()
				if opts.forceTimeouts {
					// For the force timeout tests we just want to spin up the
					// contexts for timeouts.
					timeoutContextsLock.Lock()
					timeoutContexts = append(timeoutContexts, ctx)
					timeoutContextsLock.Unlock()
					timedOutQueriesWg.Add(1)
					go func() {
						_, err := test.index.Query(ctx, index.Query{
							Query: query,
						}, index.QueryOptions{
							StartInclusive: rangeStart,
							EndExclusive:   rangeEnd,
						})
						assert.Error(t, err)
						timedOutQueriesWg.Done()
					}()
					continue
				}

				results, err := test.index.Query(ctx, index.Query{
					Query: query,
				}, index.QueryOptions{
					StartInclusive: rangeStart,
					EndExclusive:   rangeEnd,
				})
				assert.NoError(t, err)

				// Read the results concurrently too
				hits := make(map[string]struct{}, results.Results.Size())
				for _, entry := range results.Results.Map().Iter() {
					id := entry.Key().String()

					doc, err := convert.FromMetricNoClone(entry.Key(), entry.Value())
					assert.NoError(t, err)
					if err != nil {
						continue // this will fail the test anyway, but don't want to panic
					}

					expectedDoc, ok := expectedResults[id]
					assert.True(t, ok)
					if !ok {
						continue // this will fail the test anyway, but don't want to panic
					}

					assert.Equal(t, expectedDoc, doc)
					hits[id] = struct{}{}
				}
				expectedHits := idsPerBlock * (k + 1)
				assert.Equal(t, expectedHits, len(hits))

				// Now safe to close the context after reading results
				ctx.Close()
			}
		}()
	}

	// Wait for all routines to be ready then start
	readyWg.Wait()
	startWg.Done()

	// Wait until done
	enqueueWg.Wait()

	// If forcing timeouts then fire off all the async request to finish
	// while we close the contexts so any races with finalization and
	// potentially aborted requests will race against each other.
	if opts.forceTimeouts {
		// First wait for timeouts
		timedOutQueriesWg.Wait()

		var ctxCloseWg sync.WaitGroup
		ctxCloseWg.Add(len(timeoutContexts))
		go func() {
			// Start allowing timedout queries to complete
			timeoutWg.Done()
			// Race closing all contexts at once
			for _, ctx := range timeoutContexts {
				ctx := ctx
				go func() {
					ctx.BlockingClose()
					ctxCloseWg.Done()
				}()
			}
		}()
		ctxCloseWg.Wait()
	}
}

type testIndex struct {
	index          namespaceIndex
	metadata       namespace.Metadata
	opts           Options
	ropts          retention.Options
	blockSize      time.Duration
	indexBlockSize time.Duration
	retention      time.Duration
}

func newTestIndex(t *testing.T, ctrl *gomock.Controller) testIndex {
	blockSize := time.Hour
	indexBlockSize := 2 * time.Hour
	retentionPeriod := 24 * time.Hour
	ropts := retention.NewOptions().
		SetBlockSize(blockSize).
		SetRetentionPeriod(retentionPeriod).
		SetBufferPast(blockSize / 2)
	nopts := namespace.NewOptions().
		SetRetentionOptions(ropts).
		SetIndexOptions(namespace.NewIndexOptions().SetBlockSize(indexBlockSize))
	md, err := namespace.NewMetadata(ident.StringID("testns"), nopts)
	require.NoError(t, err)
	opts := testDatabaseOptions()
	index, err := newNamespaceIndex(md, opts)
	require.NoError(t, err)

	return testIndex{
		index:          index,
		metadata:       md,
		opts:           opts,
		blockSize:      blockSize,
		indexBlockSize: indexBlockSize,
		retention:      retentionPeriod,
	}
}
