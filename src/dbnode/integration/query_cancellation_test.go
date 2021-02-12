// +build integration
//
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

package integration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/x/ident"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestQueryCancellation(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	var (
		retentionPeriod = 2 * time.Hour
		dataBlockSize   = time.Hour
		indexBlockSize  = 2 * time.Hour
		bufferFuture    = 20 * time.Minute
		bufferPast      = 10 * time.Minute
	)

	// Test setup
	md, err := namespace.NewMetadata(testNamespaces[0],
		namespace.NewOptions().
			SetRetentionOptions(
				retention.NewOptions().
					SetRetentionPeriod(retentionPeriod).
					SetBufferPast(bufferPast).
					SetBufferFuture(bufferFuture).
					SetBlockSize(dataBlockSize)).
			SetIndexOptions(
				namespace.NewIndexOptions().
					SetBlockSize(indexBlockSize).SetEnabled(true)).
			SetColdWritesEnabled(true))
	require.NoError(t, err)

	var (
		hostQueueWorkerPools     []*mockWorkerPool
		hostQueueWorkerPoolsLock sync.Mutex
	)

	testOpts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{md}).
		SetWriteNewSeriesAsync(true).
		SetCustomClientAdminOptions([]client.CustomAdminOption{
			client.CustomAdminOption(func(opts client.AdminOptions) client.AdminOptions {
				return opts.SetHostQueueNewPooledWorkerFn(func(
					opts xsync.NewPooledWorkerOptions,
				) (xsync.PooledWorkerPool, error) {
					workerPoolOpts := xsync.NewPooledWorkerPoolOptions().
						SetGrowOnDemand(true).
						SetKillWorkerProbability(0.01).
						SetInstrumentOptions(opts.InstrumentOptions)

					workerPool, err := xsync.NewPooledWorkerPool(
						int(workerPoolOpts.NumShards()),
						workerPoolOpts)
					if err != nil {
						return nil, err
					}

					mocked := newMockWorkerPool(workerPool)
					hostQueueWorkerPoolsLock.Lock()
					hostQueueWorkerPools = append(hostQueueWorkerPools, mocked)
					hostQueueWorkerPoolsLock.Unlock()

					return mocked, nil
				}).(client.AdminOptions)
			}),
		})

	testSetup, err := NewTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.Close()

	// Start the server
	log := testSetup.StorageOpts().InstrumentOptions().Logger()
	require.NoError(t, testSetup.StartServer())

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.StopServer())
		log.Debug("server is now down")
	}()

	var (
		nowFn     = testSetup.StorageOpts().ClockOptions().NowFn()
		end       = nowFn().Truncate(time.Hour)
		start     = end.Add(-time.Hour)
		query     = index.Query{Query: idx.NewTermQuery([]byte("shared"), []byte("shared"))}
		queryOpts = index.QueryOptions{StartInclusive: start, EndExclusive: end}
	)
	session, err := testSetup.M3DBClient().DefaultSession()
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		var (
			metricName = fmt.Sprintf("metric_%v", i)
			tags       = ident.StringTag("shared", "shared")
			timestamp  = nowFn().Add(-time.Minute * time.Duration(i+1))
		)
		err := session.WriteTagged(md.ID(), ident.StringID(metricName),
			ident.NewTagsIterator(ident.NewTags(tags)), timestamp, 0.0, xtime.Second, nil)
		require.NoError(t, err)
	}

	// Test query cancellation.
	log.Info("querying results")

	var wg sync.WaitGroup
	wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())

	var (
		once      sync.Once
		intercept = func() {
			defer wg.Done()
			log.Info("host queue worker enqueued, cancelling context")
			cancel()
		}
	)
	hostQueueWorkerPoolsLock.Lock()
	for _, workers := range hostQueueWorkerPools {
		workers.hookSet(func() {
			once.Do(intercept)
		})
	}
	hostQueueWorkerPoolsLock.Unlock()

	_, _, err = session.FetchTagged(ctx, md.ID(), query, queryOpts)
	// Expect error since we cancelled the context.
	require.Error(t, err)
	log.Info("expected error from fetch tagged", zap.Error(err))
	require.True(t, strings.Contains(err.Error(), "ErrCodeCancelled") == strings.Contains(err.Error(), "ErrCodeCancelled"),
		fmt.Sprintf("error: %s\n", err.Error()))
}

var _ xsync.PooledWorkerPool = (*mockWorkerPool)(nil)

type mockWorkerPool struct {
	sync.RWMutex
	hook       func()
	workerPool xsync.PooledWorkerPool
}

func newMockWorkerPool(workerPool xsync.PooledWorkerPool) *mockWorkerPool {
	return &mockWorkerPool{
		workerPool: workerPool,
	}
}

func (p *mockWorkerPool) Init() {
	p.workerPool.Init()
}

func (p *mockWorkerPool) hookSet(hook func()) {
	p.Lock()
	defer p.Unlock()
	p.hook = hook
}

func (p *mockWorkerPool) hookRun() {
	p.RLock()
	defer p.RUnlock()
	if p.hook == nil {
		return
	}
	p.hook()
}

func (p *mockWorkerPool) Go(work xsync.Work) {
	p.hookRun()
	p.workerPool.Go(work)
}

func (p *mockWorkerPool) GoWithTimeout(work xsync.Work, timeout time.Duration) bool {
	p.hookRun()
	return p.workerPool.GoWithTimeout(work, timeout)
}
