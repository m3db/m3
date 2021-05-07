// +build integration

// Copyright (c) 2021 Uber Technologies, Inc.
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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

const (
	numSeries = 10
)

var nsID = ident.StringID("ns0")

func TestWiredListPanic(t *testing.T) {
	// This test is used to repro https://github.com/m3db/m3/issues/2573.
	// Unfortunately, this bug is due to a race condition and this test does not
	// consistently reproduce it reliably in short period of time. As such, the
	// test is configured to run for a very long duration to see if the repro
	// occurs. Comment out the below SkipNow() to actually run this.
	t.SkipNow()

	// Small increment to make race condition more likely.
	tickInterval := 5 * time.Millisecond

	nsOpts := namespace.NewOptions().
		SetRepairEnabled(false).
		SetRetentionOptions(DefaultIntegrationTestRetentionOpts).
		SetCacheBlocksOnRetrieve(true)
	ns, err := namespace.NewMetadata(nsID, nsOpts)
	require.NoError(t, err)
	testOpts := NewTestOptions(t).
		SetTickMinimumInterval(tickInterval).
		SetTickCancellationCheckInterval(tickInterval).
		SetNamespaces([]namespace.Metadata{ns}).
		// Wired list size of one means that if we query for two different IDs
		// alternating between each one, we'll evict from the wired list on
		// every query.
		SetMaxWiredBlocks(1)

	testSetup, err := NewTestSetup(t, testOpts, nil,
		func(opts storage.Options) storage.Options {
			return opts.SetMediatorTickInterval(tickInterval)
		},
		func(opts storage.Options) storage.Options {
			blockRetrieverMgr := block.NewDatabaseBlockRetrieverManager(
				func(
					md namespace.Metadata,
					shardSet sharding.ShardSet,
				) (block.DatabaseBlockRetriever, error) {
					retrieverOpts := fs.NewBlockRetrieverOptions().
						SetBlockLeaseManager(opts.BlockLeaseManager()).
						SetCacheBlocksOnRetrieve(true)
					retriever, err := fs.NewBlockRetriever(retrieverOpts,
						opts.CommitLogOptions().FilesystemOptions())
					if err != nil {
						return nil, err
					}

					if err := retriever.Open(md, shardSet); err != nil {
						return nil, err
					}
					return retriever, nil
				})
			return opts.SetDatabaseBlockRetrieverManager(blockRetrieverMgr)
		},
	)

	require.NoError(t, err)
	defer testSetup.Close()

	// Start the server.
	log := testSetup.StorageOpts().InstrumentOptions().Logger()
	require.NoError(t, testSetup.StartServer())
	log.Info("server is now up")

	// Stop the server.
	defer func() {
		require.NoError(t, testSetup.StopServer())
		log.Info("server is now down")
	}()

	md := testSetup.NamespaceMetadataOrFail(nsID)
	ropts := md.Options().RetentionOptions()
	blockSize := ropts.BlockSize()
	filePathPrefix := testSetup.StorageOpts().CommitLogOptions().FilesystemOptions().FilePathPrefix()

	seriesStrs := make([]string, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		seriesStrs = append(seriesStrs, fmt.Sprintf("series-%d", i))
	}

	start := testSetup.NowFn()()
	go func() {
		for i := 0; true; i++ {
			write(t, testSetup, blockSize, start, filePathPrefix, i, seriesStrs)
			time.Sleep(5 * time.Millisecond)
		}
	}()

	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-doneCh:
				return
			default:
				read(t, testSetup, blockSize, seriesStrs)
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	time.Sleep(time.Hour)
	// Stop reads before tearing down testSetup.
	doneCh <- struct{}{}
}

func write(
	t *testing.T,
	testSetup TestSetup,
	blockSize time.Duration,
	start time.Time,
	filePathPrefix string,
	i int,
	seriesStrs []string,
) {
	blockStart := start.Add(time.Duration(2*i) * blockSize)
	testSetup.SetNowFn(blockStart)

	input := generate.BlockConfig{
		IDs: seriesStrs, NumPoints: 1, Start: blockStart,
	}
	testData := generate.Block(input)
	require.NoError(t, testSetup.WriteBatch(nsID, testData))

	// Progress well past the block boundary so that the series gets flushed to
	// disk. This allows the next tick to purge the series from memory, closing
	// the series and thus making the id nil.
	testSetup.SetNowFn(blockStart.Add(blockSize * 3 / 2))
	require.NoError(t, waitUntilFileSetFilesExist(
		filePathPrefix,
		[]fs.FileSetFileIdentifier{
			{
				Namespace:   nsID,
				Shard:       1,
				BlockStart:  blockStart,
				VolumeIndex: 0,
			},
		},
		time.Second,
	))
}

func read(
	t *testing.T,
	testSetup TestSetup,
	blockSize time.Duration,
	seriesStrs []string,
) {
	// After every write, "now" would be progressed into the future so that the
	// will be flushed to disk. This makes "now" a suitable RangeEnd for the
	// fetch request. The precise range does not matter so long as it overlaps
	// with the current retention.
	now := testSetup.NowFn()()

	req := rpc.NewFetchRequest()
	req.NameSpace = nsID.String()
	req.RangeStart = xtime.ToNormalizedTime(now.Add(-4*blockSize), time.Second)
	req.RangeEnd = xtime.ToNormalizedTime(now, time.Second)
	req.ResultTimeType = rpc.TimeType_UNIX_SECONDS

	// Fetching the series sequentially ensures that the wired list will have
	// evictions assuming that the list is configured with a size of 1.
	for _, seriesStr := range seriesStrs {
		req.ID = seriesStr
		_, err := testSetup.Fetch(req)
		require.NoError(t, err)
	}
}
