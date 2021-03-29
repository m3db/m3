// +build big
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
	"fmt"
	"io/ioutil"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/x/checked"
	xclock "github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	xhash "github.com/m3db/m3/src/x/test/hash"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	wideTagName   = "tag"
	wideTagValFmt = "val-%05d"
)

type shardedWideEntry struct {
	shard   uint32
	entries []schema.WideEntry
}

// buildExpectedChecksumsByShard sorts the given IDs into ascending shard order,
// applying given shard filters.
func buildExpectedChecksumsByShard(
	ids []string,
	allowedShards []uint32,
	shardSet sharding.ShardSet,
	batchSize int,
) []schema.WideEntry {
	shardedEntries := make([]shardedWideEntry, 0, len(ids))
	for i, id := range ids {
		entry := schema.WideEntry{
			IndexEntry: schema.IndexEntry{
				ID: []byte(id),
			},
			MetadataChecksum: int64(i),
		}

		shard := shardSet.Lookup(ident.StringID(id))
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
		for idx, sharded := range shardedEntries {
			if shard != sharded.shard {
				continue
			}

			found = true
			shardedEntries[idx].entries = append(sharded.entries, entry)
			break
		}

		if found {
			continue
		}

		shardedEntries = append(shardedEntries, shardedWideEntry{
			shard:   shard,
			entries: []schema.WideEntry{entry},
		})
	}

	sort.Slice(shardedEntries, func(i, j int) bool {
		return shardedEntries[i].shard < shardedEntries[j].shard
	})

	var entries []schema.WideEntry
	for _, sharded := range shardedEntries {
		entries = append(entries, sharded.entries...)
	}

	return entries
}

func assertTags(
	t *testing.T,
	encodedTags checked.Bytes,
	decoder serialize.TagDecoder,
	expected int64,
) {
	encodedTags.IncRef()
	encoded := encodedTags.Bytes()
	encodedTags.DecRef()

	decoder.Reset(checked.NewBytes(encoded, nil))
	assert.Equal(t, 1, decoder.Len())
	assert.True(t, decoder.Next())
	tag := decoder.Current()
	assert.Equal(t, wideTagName, tag.Name.String())
	assert.Equal(t, fmt.Sprintf(wideTagValFmt, expected), tag.Value.String())
	assert.False(t, decoder.Next())
	require.NoError(t, decoder.Err())
}

func TestWideFetch(t *testing.T) {
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
		SetRetentionOptions(DefaultIntegrationTestRetentionOpts)

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
		id := fmt.Sprintf("id-%05d", i)
		ids = append(ids, id)
		indexWrites = append(indexWrites, TestIndexWrite{
			ID:        ident.StringID(id),
			Tags:      ident.MustNewTagStringsIterator(wideTagName, fmt.Sprintf(wideTagValFmt, i)),
			Timestamp: now,
			Value:     float64(i),
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
		query    = index.Query{Query: idx.MustCreateRegexpQuery([]byte(wideTagName), []byte("val.*"))}
		iterOpts = index.IterationOptions{}
	)

	// Test sharding on query which matches every element.
	shardFilterTests := []struct {
		name   string
		shards []uint32
	}{
		{name: "no shards filter", shards: nil},
		{name: "shards filter", shards: []uint32{1, 3, 5, 7}},
		{name: "all shards filter", shards: testSetup.ShardSet().AllIDs()},
	}

	tagDecoderPool := serialize.NewTagDecoderPool(
		serialize.NewTagDecoderOptions(
			serialize.TagDecoderOptionsConfig{}),
		pool.NewObjectPoolOptions(),
	)

	tagDecoderPool.Init()
	decoder := tagDecoderPool.Get()
	defer decoder.Close()

	for _, tt := range shardFilterTests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.NewContext()
			chk, err := testSetup.DB().WideQuery(ctx, nsMetadata.ID(), query,
				now, tt.shards, iterOpts)
			require.NoError(t, err)

			expected := buildExpectedChecksumsByShard(ids, tt.shards,
				testSetup.ShardSet(), batchSize)
			require.Equal(t, len(expected), len(chk))
			for i, checksum := range chk {
				assert.Equal(t, expected[i].MetadataChecksum, checksum.MetadataChecksum)
				require.Equal(t, string(expected[i].ID), checksum.ID.String())
				assertTags(t, checksum.EncodedTags, decoder, checksum.MetadataChecksum)
			}

			ctx.Close()
		})
	}

	var (
		exactID    = ids[1]
		exactQuery = index.Query{Query: idx.NewTermQuery([]byte(wideTagName), []byte("val-00001"))}
		exactShard = testSetup.ShardSet().Lookup(ident.StringID(exactID))
	)

	// Test sharding on query which matches only a single element.
	exactShardFilterTests := []struct {
		name     string
		expected bool
		shards   []uint32
	}{
		{name: "exact query no shard filter", expected: true,
			shards: nil},
		{name: "exact query at shard filter", expected: true,
			shards: []uint32{exactShard}},
		{name: "exact query within shard filters", expected: true,
			shards: []uint32{exactShard + 1, exactShard - 1, exactShard}},
		{name: "exact query not in shard filter", expected: false,
			shards: []uint32{exactShard + 1}},
	}

	for _, tt := range exactShardFilterTests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.NewContext()
			chk, err := testSetup.DB().WideQuery(ctx, nsMetadata.ID(), exactQuery,
				now, tt.shards, iterOpts)
			require.NoError(t, err)

			if tt.expected {
				require.Equal(t, 1, len(chk))
				checksum := chk[0]
				assert.Equal(t, int64(1), checksum.MetadataChecksum)
				assert.Equal(t, exactID, checksum.ID.String())
				assertTags(t, checksum.EncodedTags, decoder, checksum.MetadataChecksum)
			} else {
				assert.Equal(t, 0, len(chk))
			}

			ctx.Close()
		})
	}

	log.Info("high concurrency filter tests")
	runs := 100
	var wg sync.WaitGroup
	var mu sync.Mutex
	var multiErr xerrors.MultiError
	for i := 0; i < runtime.NumCPU(); i++ {
		q := index.Query{Query: idx.NewAllQuery()}
		expected := buildExpectedChecksumsByShard(
			ids, nil, testSetup.ShardSet(), batchSize)
		wg.Add(1)
		go func() {
			var runError error
			for j := 0; j < runs; j++ {
				ctx := context.NewContext()
				chk, err := testSetup.DB().WideQuery(ctx, nsMetadata.ID(), q,
					now, nil, iterOpts)

				if err != nil {
					runError = fmt.Errorf("query err: %v", err)
					break
				}

				if len(expected) != len(chk) {
					runError = fmt.Errorf("expected %d results, got %d",
						len(expected), len(chk))
					break
				}

				for i, checksum := range chk {
					if expected[i].MetadataChecksum != checksum.MetadataChecksum {
						runError = fmt.Errorf("expected %d checksum, got %d",
							expected[i].MetadataChecksum, checksum.MetadataChecksum)
						break
					}
				}

				ctx.Close()
			}

			if runError != nil {
				mu.Lock()
				multiErr = multiErr.Add(runError)
				mu.Unlock()
			}

			wg.Done()
		}()
	}

	wg.Wait()
	require.NoError(t, multiErr.LastError())
}
