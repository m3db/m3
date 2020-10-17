// +build integration

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
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/idx"
	xclock "github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xhash "github.com/m3db/m3/src/x/test/hash"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestWideQueryIterator(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	var (
		batchSize     = 7
		seriesCount   = 100
		blockSize     = time.Hour * 2
		verifyTimeout = time.Minute * 2
	)

	// Test setup
	idxOpts := namespace.NewIndexOptions().
		SetEnabled(true).
		SetBlockSize(time.Hour * 2)
	nsOpts := namespace.NewOptions().
		SetIndexOptions(idxOpts).
		SetRepairEnabled(false).
		SetRetentionOptions(defaultIntegrationTestRetentionOpts)

	nsID := testNamespaces[0]
	nsMetadata, err := namespace.NewMetadata(nsID, nsOpts)
	require.NoError(t, err)

	// Set up file path prefix
	filePathPrefix, err := ioutil.TempDir("", "wide-query-test")
	require.NoError(t, err)

	testOpts := NewTestOptions(t).
		SetTickMinimumInterval(time.Second).
		SetNamespaces([]namespace.Metadata{nsMetadata}).
		SetFilePathPrefix(filePathPrefix)

	require.NoError(t, err)
	fsOpts := fs.NewOptions().SetFilePathPrefix(filePathPrefix)
	decOpts := fsOpts.DecodingOptions().SetIndexEntryHasher(xhash.NewParsedIndexHasher(t))
	fsOpts = fsOpts.SetDecodingOptions(decOpts)

	testSetup, err := NewTestSetup(t, testOpts, fsOpts,
		func(opt storage.Options) storage.Options {
			return opt.SetWideBatchSize(batchSize)
		})
	require.NoError(t, err)
	defer testSetup.Close()

	// Start the server with filesystem bootstrapper
	log := testSetup.StorageOpts().InstrumentOptions().Logger()
	log.Debug("wide fetch test")
	require.NoError(t, testSetup.StartServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.StopServer())
		log.Debug("server is now down")
	}()

	// Setup test data
	now := testSetup.NowFn()()
	indexWrites := make(TestIndexWrites, 0, seriesCount)
	ids := make([]string, 0, seriesCount)
	for i := 0; i < seriesCount; i++ {
		id := fmt.Sprintf("foo-%05d", i)
		ids = append(ids, id)
		indexWrites = append(indexWrites, testIndexWrite{
			id:    ident.StringID(id),
			tags:  ident.MustNewTagStringsIterator("abc", fmt.Sprintf("def%d", i)),
			ts:    now,
			value: float64(i),
		})
	}

	log.Debug("write test data")
	client := testSetup.M3DBClient()
	session, err := client.DefaultSession()
	require.NoError(t, err)

	start := time.Now()
	indexWrites.Write(t, nsID, session)
	log.Info("test data written", zap.Duration("took", time.Since(start)))

	log.Info("waiting until data is indexed")
	indexed := xclock.WaitUntil(func() bool {
		numIndexed := indexWrites.NumIndexed(t, nsID, session)
		return numIndexed == len(indexWrites)
	}, verifyTimeout)
	require.True(t, indexed)
	log.Info("verified data is indexed", zap.Duration("took", time.Since(start)))

	// Advance time to make sure all data are flushed. Because data
	// are flushed to disk asynchronously, need to poll to check
	// when data are written.
	testSetup.SetNowFn(testSetup.NowFn()().Add(blockSize * 2))
	log.Info("waiting until filesets found on disk")
	found := xclock.WaitUntil(func() bool {
		at := now.Truncate(blockSize).Add(-1 * blockSize)
		filesets, err := fs.IndexFileSetsAt(testSetup.FilePathPrefix(), nsID, at)
		require.NoError(t, err)
		return len(filesets) == 1
	}, verifyTimeout)
	require.True(t, found)
	log.Info("filesets found on disk")

	var (
		query    = index.Query{Query: idx.MustCreateRegexpQuery([]byte("abc"), []byte("def.*"))}
		iterOpts = index.IterationOptions{}
	)

	ctx := context.NewContext()
	iter, err := testSetup.DB().WideQuery(ctx, nsMetadata.ID(), query,
		now, testSetup.ShardSet().AllIDs(), iterOpts)
	require.NoError(t, err)

	for iter.Next() {
		shardIter := iter.Current()
		log.Info("shard results", zap.Uint32("shard", shardIter.Shard()))

		for shardIter.Next() {
			seriesIter := shardIter.Current()
			log.Info("series result",
				zap.String("id", seriesIter.ID().String()),
				zap.Int("encodedTagsLen", len(seriesIter.EncodedTags())))

			for seriesIter.Next() {
				// Should get datapoints here when implemented.
			}

			if err := seriesIter.Err(); err != nil {
				require.FailNow(t, "series iter failure", err)
			}
			seriesIter.Close()
		}

		if err := shardIter.Err(); err != nil {
			require.FailNow(t, "shard iter failure", err)
		}
		shardIter.Close()
	}

	if err := iter.Err(); err != nil {
		require.FailNow(t, "iter failure", err)
	}
	iter.Close()

	ctx.Close()
}
