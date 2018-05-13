// Copyright (c) 2016 Uber Technologies, Inc.
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

package fs

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3db/x/xio"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testBlockRetrieverOptions struct {
	retrieverOpts  BlockRetrieverOptions
	fsOpts         Options
	newSeekerMgrFn newSeekerMgrFn
}

type testCleanupFn func()

func newOpenTestBlockRetriever(
	t *testing.T,
	opts testBlockRetrieverOptions,
) (*blockRetriever, testCleanupFn) {
	require.NotNil(t, opts.retrieverOpts)
	require.NotNil(t, opts.fsOpts)

	r := NewBlockRetriever(opts.retrieverOpts, opts.fsOpts)
	retriever := r.(*blockRetriever)
	if opts.newSeekerMgrFn != nil {
		retriever.newSeekerMgrFn = opts.newSeekerMgrFn
	}

	nsPath := NamespaceDataDirPath(opts.fsOpts.FilePathPrefix(), testNs1ID)
	require.NoError(t, os.MkdirAll(nsPath, opts.fsOpts.NewDirectoryMode()))
	require.NoError(t, retriever.Open(testNs1Metadata(t)))

	return retriever, func() {
		assert.NoError(t, retriever.Close())
	}
}

func newOpenTestWriter(
	t *testing.T,
	fsOpts Options,
	shard uint32,
	start time.Time,
) (DataFileSetWriter, testCleanupFn) {
	w := newTestWriter(t, fsOpts.FilePathPrefix())
	writerOpts := DataWriterOpenOptions{
		BlockSize: testBlockSize,
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      shard,
			BlockStart: start,
		},
	}
	require.NoError(t, w.Open(writerOpts))
	return w, func() {
		assert.NoError(t, w.Close())
	}
}

type streamResult struct {
	ctx        context.Context
	shard      uint32
	id         string
	blockStart time.Time
	stream     xio.SegmentReader
}

// TestBlockRetrieverHighConcurrentSeeks tests the retriever with high
// concurrent seeks, but without caching the shard indices. This means that the
// seekers will be opened lazily by calls to ConcurrentIDBloomFilter() in the
// SeekerManager
func TestBlockRetrieverHighConcurrentSeeks(t *testing.T) {
	testBlockRetrieverHighConcurrentSeeks(t, false)
}

// TestBlockRetrieverHighConcurrentSeeksCacheShardIndices tests the retriever
// with high concurrent seekers and calls cache shard indices at the beginning.
// This means that the seekers will be opened all at once in the beginning and
// by the time ConcurrentIDBloomFilter() is called, they seekers will already be
// open.
func TestBlockRetrieverHighConcurrentSeeksCacheShardIndices(t *testing.T) {
	testBlockRetrieverHighConcurrentSeeks(t, true)
}

func testBlockRetrieverHighConcurrentSeeks(t *testing.T, shouldCacheShardIndices bool) {
	defer leaktest.CheckTimeout(t, 2*time.Minute)()

	dir, err := ioutil.TempDir("", "testdb")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	filePathPrefix := filepath.Join(dir, "")
	fsOpts := testDefaultOpts.SetFilePathPrefix(filePathPrefix)

	fetchConcurrency := 4
	seekConcurrency := 4 * fetchConcurrency
	opts := testBlockRetrieverOptions{
		retrieverOpts: NewBlockRetrieverOptions().
			SetFetchConcurrency(fetchConcurrency).
			SetRequestPoolOptions(pool.NewObjectPoolOptions().
				// NB(r): Try to make sure same req structs are reused frequently
				// to surface any race issues that might occur with pooling.
				SetSize(fetchConcurrency / 2)),
		fsOpts: fsOpts,
	}
	retriever, cleanup := newOpenTestBlockRetriever(t, opts)
	defer cleanup()

	ropts := testNs1Metadata(t).Options().RetentionOptions()

	now := time.Now().Truncate(ropts.BlockSize())
	min, max := now.Add(-6*ropts.BlockSize()), now.Add(-ropts.BlockSize())

	var (
		shards         = []uint32{0, 1, 2}
		idsPerShard    = 16
		shardIDs       = make(map[uint32][]ident.ID)
		shardIDStrings = make(map[uint32][]string)
		dataBytesPerID = 32
		shardData      = make(map[uint32]map[string]map[xtime.UnixNano]checked.Bytes)
		blockStarts    []time.Time
	)
	for st := min; !st.After(max); st = st.Add(ropts.BlockSize()) {
		blockStarts = append(blockStarts, st)
	}
	for _, shard := range shards {
		shardIDs[shard] = make([]ident.ID, 0, idsPerShard)
		shardData[shard] = make(map[string]map[xtime.UnixNano]checked.Bytes, idsPerShard)
		for _, blockStart := range blockStarts {
			w, closer := newOpenTestWriter(t, fsOpts, shard, blockStart)
			for i := 0; i < idsPerShard; i++ {
				idString := fmt.Sprintf("foo.%d", i)
				shardIDStrings[shard] = append(shardIDStrings[shard], idString)

				id := ident.StringID(idString)
				shardIDs[shard] = append(shardIDs[shard], id)
				if _, ok := shardData[shard][idString]; !ok {
					shardData[shard][idString] = make(map[xtime.UnixNano]checked.Bytes, len(blockStarts))
				}

				data := checked.NewBytes(nil, nil)
				data.IncRef()
				for j := 0; j < dataBytesPerID; j++ {
					data.Append(byte(rand.Int63n(256)))
				}
				shardData[shard][idString][xtime.ToUnixNano(blockStart)] = data

				err := w.Write(id, ident.Tags{}, data, digest.Checksum(data.Bytes()))
				require.NoError(t, err)
			}
			closer()
		}
	}

	if shouldCacheShardIndices {
		retriever.CacheShardIndices(shards)
	}

	var (
		startWg, readyWg sync.WaitGroup
		seeksPerID       = 24
		seeksEach        = len(shards) * idsPerShard * seeksPerID
	)

	var enqueueWg sync.WaitGroup
	startWg.Add(1)
	for i := 0; i < seekConcurrency; i++ {
		i := i
		readyWg.Add(1)
		enqueueWg.Add(1)
		go func() {
			defer enqueueWg.Done()
			readyWg.Done()
			startWg.Wait()

			shardOffset := i
			idOffset := i % seekConcurrency / 4
			results := make([]streamResult, 0, len(blockStarts))
			compare := ts.Segment{}
			for j := 0; j < seeksEach; j++ {
				shard := uint32((j + shardOffset) % len(shards))
				idIdx := uint32((j + idOffset) % len(shardIDs[shard]))
				id := shardIDs[shard][idIdx]
				idString := shardIDStrings[shard][idIdx]

				for k := 0; k < len(blockStarts); k++ {
					ctx := context.NewContext()
					stream, err := retriever.Stream(ctx, shard, id, blockStarts[k], nil)
					require.NoError(t, err)
					results = append(results, streamResult{
						ctx:        ctx,
						shard:      shard,
						id:         idString,
						blockStart: blockStarts[k],
						stream:     stream,
					})
				}

				for _, r := range results {
					seg, err := r.stream.Segment()
					if err != nil {
						fmt.Printf("\nstream seg err: %v\n", err)
						fmt.Printf("id: %s\n", r.id)
						fmt.Printf("shard: %d\n", r.shard)
						fmt.Printf("start: %v\n", r.blockStart.String())
					}

					require.NoError(t, err)
					compare.Head = shardData[r.shard][r.id][xtime.ToUnixNano(r.blockStart)]
					assert.True(t, seg.Equal(&compare))

					r.ctx.Close()
				}
				results = results[:0]
			}
		}()
	}

	// Wait for all routines to be ready then start
	readyWg.Wait()
	startWg.Done()

	// Wait until done
	enqueueWg.Wait()
}

// TestBlockRetrieverIDDoesNotExist verifies the behavior of the Stream() method
// on the retriever in the case where the requested ID does not exist. In that
// case, Stream() should return an empty segment.
func TestBlockRetrieverIDDoesNotExist(t *testing.T) {
	// Make sure reader/writer are looking at the same test directory
	dir, err := ioutil.TempDir("", "testdb")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	filePathPrefix := filepath.Join(dir, "")

	// Setup constants and config
	fsOpts := testDefaultOpts.SetFilePathPrefix(filePathPrefix)
	rOpts := testNs1Metadata(t).Options().RetentionOptions()
	shard := uint32(0)
	blockStart := time.Now().Truncate(rOpts.BlockSize())

	// Setup the reader
	opts := testBlockRetrieverOptions{
		retrieverOpts: NewBlockRetrieverOptions(),
		fsOpts:        fsOpts,
	}
	retriever, cleanup := newOpenTestBlockRetriever(t, opts)
	defer cleanup()

	// Write out a test file
	w, closer := newOpenTestWriter(t, fsOpts, shard, blockStart)
	data := checked.NewBytes([]byte("Hello world!"), nil)
	data.IncRef()
	defer data.DecRef()
	err = w.Write(ident.StringID("exists"), ident.Tags{}, data, digest.Checksum(data.Bytes()))
	assert.NoError(t, err)
	closer()

	// Make sure we return the correct error if the ID does not exist
	ctx := context.NewContext()
	defer ctx.Close()
	segmentReader, err := retriever.Stream(ctx, shard,
		ident.StringID("not-exists"), blockStart, nil)
	assert.NoError(t, err)

	segment, err := segmentReader.Segment()
	assert.NoError(t, err)
	assert.Equal(t, nil, segment.Head)
	assert.Equal(t, nil, segment.Tail)
}
