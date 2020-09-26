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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/require"
)

func TestWideFetch(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	var (
		blockSize = 2 * time.Hour
		batchSize = 15
		// seriesCount = 15
	)

	nsOpts := namespace.NewOptions()
	retOpts := nsOpts.RetentionOptions().
		SetRetentionPeriod(2 * time.Hour).SetBlockSize(blockSize)
	idxOpts := namespace.NewIndexOptions().
		SetEnabled(true).
		SetBlockSize(time.Hour * 2)
	nsOpts = nsOpts.
		SetRetentionOptions(retOpts).
		SetIndexOptions(idxOpts)

	ns1, err := namespace.NewMetadata(testNamespaces[0], nsOpts)
	require.NoError(t, err)

	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns1})

	fsOpts := fs.NewOptions()
	decOpts := fsOpts.DecodingOptions().SetIndexEntryHasher(xtest.NewParsedIndexHasher(t))
	fsOpts = fsOpts.SetDecodingOptions(decOpts)

	// Test setup
	setup, err := NewTestSetup(t, opts, fsOpts,
		func(opt storage.Options) storage.Options {
			return opt.SetWideBatchSize(batchSize)
		})
	require.NoError(t, err)
	defer setup.Close()

	require.NoError(t, setup.InitializeBootstrappers(InitializeBootstrappersOptions{
		WithFileSystem: true,
	}))

	// Write test data
	now := setup.NowFn()()
	blockTime := now //.Truncate(time.Hour).Add(-blockSize)
	// ids := make([]string, seriesCount)
	// for i := range ids {
	// 	// Keep in lex order.
	// 	padCount := i / 10
	// 	pad := ""
	// 	for i := 0; i < padCount; i++ {
	// 		pad = fmt.Sprintf("%so", pad)
	// 	}

	// 	ids[i] = fmt.Sprintf("foo%s-%d", pad, i)
	// }
	ids := []string{
		// "a-1", "b-2", "c-3", "d-4", "e-5", "f-6", "g-7", "h-8", "i-9", "j-10", "k-11", "l-12", "m-13", "n-14", "o-15",
		"z-1", "y-2", "x-3", "w-4", "v-5", "u-6", "t-7", "s-8", "r-9", "q-10", "p-11", "o-12", "n-13", "m-14", "l-15",
	}
	tags := ident.NewTags(ident.StringTag("abc", "def"))
	inputData := []generate.BlockConfig{
		{IDs: ids, NumPoints: 1, Start: blockTime, Tags: tags},
	}

	seriesMaps := generate.BlocksByStart(inputData)
	require.NoError(t, writeTestDataToDisk(ns1, setup, seriesMaps, 0))

	// Start the server with filesystem bootstrapper
	log := setup.StorageOpts().InstrumentOptions().Logger()
	log.Debug("filesystem bootstrap test")
	require.NoError(t, setup.StartServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, setup.StopServer())
		log.Debug("server is now down")
	}()

	var (
		ctx      = context.NewContext()
		query    = index.Query{Query: idx.NewTermQuery([]byte("abc"), []byte("def"))}
		iterOpts = index.IterationOptions{}
	)

	// for i := range ids {
	// 	id := ids[len(ids)-i-1]
	// 	require.NoError(t, setup.DB().WriteTagged(context.NewContext(), ns1.ID(),
	// 		ident.StringID(id), ident.MustNewTagStringsIterator("abc", "def"), now, 1,
	// 		xtime.Second, nil))
	// }

	_, err = setup.DB().WideQuery(ctx, ns1.ID(), query, blockTime, nil, iterOpts)
	// Verify in-memory data match what we expect
	require.NoError(t, err)

	// type shardedIndexChecksum struct {
	// 	shard     uint32
	// 	checksums []ident.IndexChecksum
	// }

	// shardedChecksums := make([]shardedIndexChecksum, 0, len(ids))
	// for i, id := range ids {
	// 	checksum := ident.IndexChecksum{ID: []byte(id), Checksum: int64(i)}
	// 	shard := setup.ShardSet().Lookup(ident.BytesID([]byte(id)))
	// 	found := false
	// 	for idx, sharded := range shardedChecksums {
	// 		if shard != sharded.shard {
	// 			continue
	// 		}

	// 		found = true
	// 		shardedChecksums[idx].checksums = append(sharded.checksums, checksum)
	// 		break
	// 	}

	// 	if found {
	// 		continue
	// 	}

	// 	shardedChecksums = append(shardedChecksums, shardedIndexChecksum{
	// 		shard:     shard,
	// 		checksums: []ident.IndexChecksum{checksum},
	// 	})
	// }

	// for _, s := range shardedChecksums {
	// 	fmt.Println(" ", s.shard)
	// 	for _, cs := range s.checksums {
	// 		fmt.Println("   ", cs.Checksum, string(cs.ID))
	// 	}
	// }

	// sort.Slice(shardedChecksums, func(i, j int) bool {
	// 	return shardedChecksums[i].shard < shardedChecksums[j].shard
	// })

	// fmt.Println("===============")

	// for _, s := range shardedChecksums {
	// 	fmt.Println(" ", s.shard)
	// 	for _, cs := range s.checksums {
	// 		fmt.Println("   ", cs.Checksum, string(cs.ID))
	// 	}
	// }

	// for i, c := range chk {
	// 	fmt.Println(i, c.Checksum, string(c.ID), "Shard:", setup.ShardSet().Lookup(ident.BytesID(c.ID)))
	// }
}
