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
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"
	"go.uber.org/zap"

	"github.com/stretchr/testify/require"
)

type shardedIndexChecksum struct {
	shard     uint32
	checksums []ident.IndexChecksum
}

func buildExpectedChecksumsByShard(
	ids []string,
	allowedShards []uint32,
	shardSet sharding.ShardSet,
) []ident.IndexChecksum {
	shardedChecksums := make([]shardedIndexChecksum, 0, len(ids))
	count := 0
	for i, id := range ids {
		checksum := ident.IndexChecksum{ID: []byte(id), Checksum: int64(i)}
		shard := shardSet.Lookup(ident.BytesID([]byte(id)))

		if len(allowedShards) > 0 {
			shardInUse := false
			for _, allowed := range allowedShards {
				if allowed == shard {
					shardInUse = true
					break
				}
			}

			if !shardInUse {
				continue
			}
		}

		found := false
		for idx, sharded := range shardedChecksums {
			if shard != sharded.shard {
				continue
			}

			found = true
			shardedChecksums[idx].checksums = append(sharded.checksums, checksum)
			break
		}

		if found {
			continue
		}

		count++
		shardedChecksums = append(shardedChecksums, shardedIndexChecksum{
			shard:     shard,
			checksums: []ident.IndexChecksum{checksum},
		})
	}

	sort.Slice(shardedChecksums, func(i, j int) bool {
		return shardedChecksums[i].shard < shardedChecksums[j].shard
	})

	checksums := make([]ident.IndexChecksum, 0, count)
	for _, sharded := range shardedChecksums {
		checksums = append(checksums, sharded.checksums...)
	}

	return checksums
}

func writeData(t *testing.T, testSetup TestSetup) {

}

func TestWideFetch(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	var (
		batchSize   = 15
		seriesCount = 15
		blockSize   = time.Hour * 2
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

	testOpts := NewTestOptions(t).
		SetTickMinimumInterval(time.Second).
		SetNamespaces([]namespace.Metadata{nsMetadata})

	filePathPrefix, err := ioutil.TempDir("", fmt.Sprintf("integration-test-11"))
	require.NoError(t, err)

	fsOpts := fs.NewOptions().SetFilePathPrefix(filePathPrefix)
	decOpts := fsOpts.DecodingOptions().SetIndexEntryHasher(xtest.NewParsedIndexHasher(t))
	fsOpts = fsOpts.SetDecodingOptions(decOpts)

	testSetup, err := NewTestSetup(t, testOpts, fsOpts,
		func(opt storage.Options) storage.Options {
			return opt.SetWideBatchSize(batchSize)
		})
	require.NoError(t, err)
	defer testSetup.Close()

	// Write test data
	ids := make([]string, seriesCount)
	for i := range ids {
		// Keep in lex order.
		padCount := i / 10
		pad := ""
		for i := 0; i < padCount; i++ {
			pad = fmt.Sprintf("%so", pad)
		}

		ids[i] = fmt.Sprintf("foo%s-%d", pad, i)
	}

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

	now := testSetup.NowFn()()
	// Write the data.
	tags := ident.NewTags(ident.StringTag("abc", "def"))
	inputData := []generate.BlockConfig{
		{IDs: ids, NumPoints: 1, Start: now, Tags: tags},
	}

	seriesMaps := make(map[xtime.UnixNano]generate.SeriesBlock)
	for _, input := range inputData {
		testSetup.SetNowFn(input.Start)
		testData := generate.Block(input)
		seriesMaps[xtime.ToUnixNano(input.Start)] = testData
		fmt.Println("Writing points at", input.Start)
		require.NoError(t, testSetup.WriteBatch(nsID, testData))
	}

	log.Debug("test data written")
	// Advance time to make sure all data are flushed. Because data
	// are flushed to disk asynchronously, need to poll to check
	// when data are written.
	testSetup.SetNowFn(testSetup.NowFn()().Add(blockSize * 2))
	maxWaitTime := time.Minute
	require.NoError(t, waitUntilDataFilesFlushed(filePathPrefix, testSetup.ShardSet(), nsID, seriesMaps, maxWaitTime))
	// Verify on-disk data match what we expect
	verifyFlushedDataFiles(t, testSetup.ShardSet(), testSetup.StorageOpts(), nsID, seriesMaps)
	log.Debug("test data has been flushed")

	require.True(t, indexed)
	log.Info("verified data is indexed", zap.Duration("took", time.Since(start)))

	var (
		ctx      = context.NewContext()
		query    = index.Query{Query: idx.NewAllQuery()} // idx.NewTermQuery([]byte("abc"), []byte("def"))}
		iterOpts = index.IterationOptions{}
	)

	fmt.Println("Wide querying at", now)
	chk, err := testSetup.DB().WideQuery(ctx, nsMetadata.ID(), query, now, nil, iterOpts)
	// Verify data match what we expect
	require.NoError(t, err)

	fmt.Println("COMPLETED.", len(chk))
	for i, c := range chk {
		fmt.Println(i, c.Checksum, string(c.ID), "Shard:", testSetup.ShardSet().Lookup(ident.BytesID(c.ID)))
	}

	qr, err := testSetup.DB().QueryIDs(ctx, nsMetadata.ID(), query, index.QueryOptions{
		StartInclusive: now.Add(time.Hour * -10),
		EndExclusive:   now.Add(time.Hour * 10),
	})

	require.NoError(t, err)
	fmt.Println("Querying IDs")
	for _, v := range qr.Results.Map().Iter() {
		fmt.Println(v.Key().String())
		for v.Value().Next() {
			c := v.Value().Current()
			fmt.Println(c.Name.String(), ":", c.Value.String())
		}
		fmt.Println(v.Value().Err())
	}
	fmt.Println("QueryIDs done")
}
