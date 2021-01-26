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
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/x/ident"
	xsync "github.com/m3db/m3/src/x/sync"
	"github.com/m3db/m3/src/x/tallytest"
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

	testOpts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{md}).
		SetWriteNewSeriesAsync(true)
	testSetup, err := NewTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.Close()

	storageOpts := testSetup.StorageOpts()
	workers := newMockWorkerPool(storageOpts.QueryIDsWorkerPool().Size())
	workers.Init()
	storageOpts = storageOpts.SetQueryIDsWorkerPool(workers)
	testSetup.SetStorageOpts(storageOpts)

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
	workers.hookSet(func() {
		defer wg.Done()
		log.Info("query IDs worker launched, cancelling context")
		cancel()
		time.Sleep(5 * time.Second)
	})

	_, _, err = session.FetchTagged(ctx, md.ID(), query, queryOpts)

	log.Info("fetch tagged result", zap.Error(err))
	wg.Wait()
	log.Info("counters",
		zap.Any("values", tallytest.CounterMap(testSetup.Scope().Snapshot().Counters())))

	// Expect error since we cancelled the context.
	require.Error(t, err)
}

var _ xsync.WorkerPool = (*mockWorkerPool)(nil)

type mockWorkerPool struct {
	sync.RWMutex
	hook       func()
	workerPool xsync.WorkerPool
}

func newMockWorkerPool(size int) *mockWorkerPool {
	return &mockWorkerPool{
		workerPool: xsync.NewWorkerPool(size),
	}
}

func (p *mockWorkerPool) Init() {
	p.workerPool.Init()
}

func (p *mockWorkerPool) Size() int {
	return p.workerPool.Size()
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

func (p *mockWorkerPool) GoIfAvailable(work xsync.Work) bool {
	p.hookRun()
	return p.workerPool.GoIfAvailable(work)
}

func (p *mockWorkerPool) GoWithTimeout(work xsync.Work, timeout time.Duration) bool {
	p.hookRun()
	return p.workerPool.GoWithTimeout(work, timeout)
}
