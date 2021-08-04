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

	"github.com/stretchr/testify/require"
	"github.com/uber/tchannel-go/thrift"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

func TestQueryCancellationAndDeadlinesClient(t *testing.T) {
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
		intercept       func()
		interceptLock   sync.Mutex
		thriftContextFn = client.ThriftContextFn(func(ctx context.Context) thrift.Context {
			interceptLock.Lock()
			if intercept != nil {
				intercept()
			}
			interceptLock.Unlock()
			return thrift.Wrap(ctx)
		})
	)

	testOpts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{md}).
		SetWriteNewSeriesAsync(true).
		SetCustomClientAdminOptions([]client.CustomAdminOption{
			client.CustomAdminOption(func(opts client.AdminOptions) client.AdminOptions {
				return opts.SetThriftContextFn(thriftContextFn).(client.AdminOptions)
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
		end       = xtime.ToUnixNano(nowFn()).Truncate(time.Hour)
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
			timestamp  = xtime.ToUnixNano(nowFn()).Add(-time.Minute * time.Duration(i+1))
		)
		err := session.WriteTagged(md.ID(), ident.StringID(metricName),
			ident.NewTagsIterator(ident.NewTags(tags)), timestamp, 0.0, xtime.Second, nil)
		require.NoError(t, err)
	}

	log.Info("testing client query semantics")

	// Test query no deadline.
	_, _, err = session.FetchTagged(context.Background(), md.ID(), query, queryOpts)
	// Expect error since we did not set a deadline.
	require.Error(t, err)
	log.Info("expected deadline not set error from fetch tagged", zap.Error(err))

	require.True(t, strings.Contains(err.Error(), client.ErrCallWithoutDeadline.Error()),
		fmt.Sprintf("actual error: %s\n", err.Error()))

	// Test query with cancel.
	var wg sync.WaitGroup
	wg.Add(1)
	ctx, cancel := context.WithCancel(ContextWithDefaultTimeout())

	var (
		once          sync.Once
		interceptExec = func() {
			defer wg.Done()
			log.Info("host queue worker enqueued, cancelling context")
			cancel()
		}
	)
	interceptLock.Lock()
	intercept = func() {
		once.Do(interceptExec)
	}
	interceptLock.Unlock()

	_, _, err = session.FetchTagged(ctx, md.ID(), query, queryOpts)
	// Expect error since we cancelled the context.
	require.Error(t, err)
	log.Info("expected cancelled error from fetch tagged", zap.Error(err))

	require.True(t, strings.Contains(err.Error(), "ErrCodeCancelled"),
		fmt.Sprintf("actual error: %s\n", err.Error()))
}
