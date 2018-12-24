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

	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3x/context"
	xsync "github.com/m3db/m3x/sync"
	xtest "github.com/m3db/m3x/test"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
				Stats(gomock.Any()).
				Return(nil).
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
