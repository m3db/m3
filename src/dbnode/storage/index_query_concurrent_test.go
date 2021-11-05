// +build big
//
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
	stdctx "context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	opentracinglog "github.com/opentracing/opentracing-go/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/storage/limits/permits"
	testutil "github.com/m3db/m3/src/dbnode/test"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"
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

func TestNamespaceIndexHighConcurrentQueriesWithBlockErrors(t *testing.T) {
	testNamespaceIndexHighConcurrentQueries(t,
		testNamespaceIndexHighConcurrentQueriesOptions{
			withTimeouts:  false,
			forceTimeouts: false,
			blockErrors:   true,
		})
}

type testNamespaceIndexHighConcurrentQueriesOptions struct {
	withTimeouts  bool
	forceTimeouts bool
	blockErrors   bool
}

func testNamespaceIndexHighConcurrentQueries(
	t *testing.T,
	opts testNamespaceIndexHighConcurrentQueriesOptions,
) {
	if opts.forceTimeouts && opts.blockErrors {
		t.Fatalf("force timeout and block errors cannot both be enabled")
	}

	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	defer leaktest.CheckTimeout(t, 2*time.Minute)()

	test := newTestIndex(t, ctrl)
	defer func() {
		err := test.index.Close()
		require.NoError(t, err)
	}()

	logger := test.opts.InstrumentOptions().Logger()
	logger.Info("start high index concurrent index query test",
		zap.Any("opts", opts))

	now := xtime.Now().Truncate(test.indexBlockSize)

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
	nsIdx.permitsManager = permits.NewFixedPermitsManager(1000, int64(time.Millisecond), instrument.NewOptions())

	currNow := min
	nowLock := &sync.Mutex{}
	nsIdx.nowFn = func() time.Time {
		nowLock.Lock()
		defer nowLock.Unlock()
		return currNow.ToTime()
	}
	setNow := func(t xtime.UnixNano) {
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
		expectedResults = make(map[string]doc.Metadata)
		blockStarts     []xtime.UnixNano
		blockIdx        = -1
	)
	for st := min; !st.After(max); st = st.Add(test.indexBlockSize) {
		st := st
		blockIdx++
		blockStarts = append(blockStarts, st)

		mutableBlockTime := st.Add(test.indexBlockSize).Add(-1 * (test.blockSize / 2))
		setNow(mutableBlockTime)

		var onIndexWg sync.WaitGroup
		onIndexWg.Add(idsPerBlock)
		onIndexSeries := doc.NewMockOnIndexSeries(ctrl)
		onIndexSeries.EXPECT().
			OnIndexSuccess(gomock.Any()).
			Times(idsPerBlock).
			Do(func(arg interface{}) {
				onIndexWg.Done()
			})
		onIndexSeries.EXPECT().
			OnIndexFinalize(gomock.Any()).
			Times(idsPerBlock)
		onIndexSeries.EXPECT().
			IfAlreadyIndexedMarkIndexSuccessAndFinalize(gomock.Any()).
			Times(idsPerBlock)
		onIndexSeries.EXPECT().
			IndexedRange().
			Return(min, max).
			AnyTimes()
		onIndexSeries.EXPECT().
			IndexedForBlockStart(gomock.Any()).
			DoAndReturn(func(ts xtime.UnixNano) bool {
				return ts.Equal(st)
			}).
			AnyTimes()
		onIndexSeries.EXPECT().
			ReconciledOnIndexSeries().
			Return(onIndexSeries, func() {}, false).
			AnyTimes()

		batch := index.NewWriteBatch(index.WriteBatchOptions{
			InitialCapacity: idsPerBlock,
			IndexBlockSize:  test.indexBlockSize,
		})
		for i := 0; i < idsPerBlock; i++ {
			id := fmt.Sprintf("foo.block_%d.id_%d", blockIdx, i)
			doc := doc.Metadata{
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

	// If force timeout or block errors are enabled, replace one of the blocks
	// with a mock block that times out or returns an error respectively.
	var timedOutQueriesWg sync.WaitGroup
	if opts.forceTimeouts || opts.blockErrors {
		// Need to restore now as timeouts are measured by looking at time.Now
		restoreNow()

		nsIdx.state.Lock()

		for start, block := range nsIdx.state.blocksByTime {
			nsIdx.state.blocksByTime[start] = newMockBlock(ctrl, opts, timeoutValue, block)
		}
		nsIdx.activeBlock = newMockBlock(ctrl, opts, timeoutValue, nsIdx.activeBlock)

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
			var ctxs []context.Context
			defer func() {
				if !opts.forceTimeouts {
					// Only close if not being closed by the force timeouts code
					// at end of the test.
					for _, ctx := range ctxs {
						ctx.Close()
					}
				}
				enqueueWg.Done()
			}()
			readyWg.Done()
			startWg.Wait()

			rangeStart := min
			for k := 0; k < len(blockStarts); k++ {
				rangeEnd := blockStarts[k].Add(test.indexBlockSize)

				goCtx := stdctx.Background()
				if timeoutValue > 0 {
					goCtx, _ = stdctx.WithTimeout(stdctx.Background(), timeoutValue)
				}

				ctx := context.NewWithGoContext(goCtx)
				ctxs = append(ctxs, ctx)

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
						timedOutQueriesWg.Done()
						require.Error(t, err)
					}()
					continue
				}

				results, err := test.index.Query(ctx, index.Query{
					Query: query,
				}, index.QueryOptions{
					StartInclusive: rangeStart,
					EndExclusive:   rangeEnd,
				})

				if opts.blockErrors {
					require.Error(t, err)
					// Early return because we don't want to check the results.
					return
				} else {
					require.NoError(t, err)
				}

				// Read the results concurrently too
				hits := make(map[string]struct{}, results.Results.Size())
				id := ident.NewReusableBytesID()
				for _, entry := range results.Results.Map().Iter() {
					id.Reset(entry.Key())
					tags := testutil.DocumentToTagIter(t, entry.Value())
					doc, err := convert.FromSeriesIDAndTagIter(id, tags)
					require.NoError(t, err)
					if err != nil {
						continue // this will fail the test anyway, but don't want to panic
					}

					expectedDoc, ok := expectedResults[id.String()]
					require.True(t, ok)
					if !ok {
						continue // this will fail the test anyway, but don't want to panic
					}

					require.Equal(t, expectedDoc, doc, "docs")
					hits[id.String()] = struct{}{}
				}
				expectedHits := idsPerBlock * (k + 1)
				require.Equal(t, expectedHits, len(hits), "hits")
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
		logger.Info("waiting for timeouts")

		// First wait for timeouts
		timedOutQueriesWg.Wait()
		logger.Info("timeouts done")

		var ctxCloseWg sync.WaitGroup
		ctxCloseWg.Add(len(timeoutContexts))
		go func() {
			// Start allowing timedout queries to complete.
			logger.Info("allow block queries to begin returning")

			// Race closing all contexts at once.
			for _, ctx := range timeoutContexts {
				ctx := ctx
				go func() {
					ctx.BlockingClose()
					ctxCloseWg.Done()
				}()
			}
		}()
		logger.Info("waiting for contexts to finish blocking closing")
		ctxCloseWg.Wait()

		logger.Info("finished with timeouts")
	}
}

func newMockBlock(ctrl *gomock.Controller,
	opts testNamespaceIndexHighConcurrentQueriesOptions,
	timeout time.Duration,
	block index.Block,
) *index.MockBlock {
	mockBlock := index.NewMockBlock(ctrl)
	mockBlock.EXPECT().
		StartTime().
		DoAndReturn(func() xtime.UnixNano { return block.StartTime() }).
		AnyTimes()
	mockBlock.EXPECT().
		EndTime().
		DoAndReturn(func() xtime.UnixNano { return block.EndTime() }).
		AnyTimes()
	mockBlock.EXPECT().QueryIter(gomock.Any(), gomock.Any()).DoAndReturn(func(
		ctx context.Context, query index.Query) (index.QueryIterator, error) {
		return block.QueryIter(ctx, query)
	},
	).AnyTimes()

	if opts.blockErrors {
		mockBlock.EXPECT().
			QueryWithIter(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(
				_ context.Context,
				_ index.QueryOptions,
				_ index.QueryIterator,
				_ index.QueryResults,
				_ time.Time,
				_ []opentracinglog.Field,
			) error {
				return errors.New("some-error")
			}).
			AnyTimes()
	} else {
		mockBlock.EXPECT().
			QueryWithIter(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(
				ctx context.Context,
				opts index.QueryOptions,
				iter index.QueryIterator,
				r index.QueryResults,
				deadline time.Time,
				logFields []opentracinglog.Field,
			) error {
				time.Sleep(timeout + time.Second)
				return block.QueryWithIter(ctx, opts, iter, r, deadline, logFields)
			}).
			AnyTimes()
	}

	mockBlock.EXPECT().
		Stats(gomock.Any()).
		Return(nil).
		AnyTimes()
	mockBlock.EXPECT().
		Close().
		DoAndReturn(func() error {
			return block.Close()
		})
	return mockBlock
}
