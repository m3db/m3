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
	"github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testBlockRetrieverOptions struct {
	retrieverOpts  BlockRetrieverOptions
	fsOpts         Options
	namespace      string
	newSeekerMgrFn newSeekerMgrFn
}

type testCleanupFn func()

func newOpenTestBlockRetriever(
	t *testing.T,
	opts testBlockRetrieverOptions,
) (*blockRetriever, testCleanupFn) {
	dir, err := ioutil.TempDir("", "testdb")
	require.NoError(t, err)

	filePathPrefix := filepath.Join(dir, "")

	if opts.retrieverOpts == nil {
		opts.retrieverOpts = NewBlockRetrieverOptions()
	}
	if opts.fsOpts == nil {
		opts.fsOpts = NewOptions()
	}
	opts.fsOpts = opts.fsOpts.SetFilePathPrefix(filePathPrefix)

	r := NewBlockRetriever(opts.retrieverOpts, opts.fsOpts)
	retriever := r.(*blockRetriever)
	if opts.newSeekerMgrFn != nil {
		retriever.newSeekerMgrFn = opts.newSeekerMgrFn
	}

	namespace := opts.namespace
	if namespace == "" {
		namespace = testNamespaceID.String()
	}

	nsID := ts.StringID(namespace)
	nsPath := NamespaceDirPath(filePathPrefix, nsID)
	require.NoError(t, os.MkdirAll(nsPath, opts.fsOpts.NewDirectoryMode()))
	require.NoError(t, retriever.Open(nsID))

	return retriever, func() {
		assert.NoError(t, retriever.Close())
		os.RemoveAll(dir)
	}
}

func newOpenTestWriter(
	t *testing.T,
	fsOpts Options,
	namespace string,
	shard uint32,
	start time.Time,
) (FileSetWriter, testCleanupFn) {
	w := newTestWriter(fsOpts.FilePathPrefix())
	if namespace == "" {
		namespace = testNamespaceID.String()
	}
	err := w.Open(ts.StringID(namespace), shard, start)
	require.NoError(t, err)
	return w, func() {
		assert.NoError(t, w.Close())
	}
}

type streamResult struct {
	shard      uint32
	id         ts.ID
	blockStart time.Time
	stream     xio.SegmentReader
}

func TestBlockRetrieverHighConcurrentSeeks(t *testing.T) {
	fetchConcurrency := 4
	seekConcurrency := 4 * fetchConcurrency
	opts := testBlockRetrieverOptions{
		retrieverOpts: NewBlockRetrieverOptions().
			SetFetchConcurrency(fetchConcurrency).
			SetRequestPoolOptions(pool.NewObjectPoolOptions().
				// NB(r): Try to make sure same req structs are reused frequently
				// to surface any race issues that might occur with pooling.
				SetSize(fetchConcurrency / 2)),
	}
	retriever, cleanup := newOpenTestBlockRetriever(t, opts)
	defer cleanup()

	fsopts := retriever.fsOpts
	ropts := fsopts.RetentionOptions()

	now := time.Now().Truncate(ropts.BlockSize())
	min, max := now.Add(-6*ropts.BlockSize()), now.Add(-ropts.BlockSize())

	var (
		shards         = 2
		idsPerShard    = 16
		shardIDs       = make(map[uint32][]ts.ID)
		dataBytesPerID = 32
		shardData      = make(map[uint32]map[ts.Hash]map[time.Time]checked.Bytes)
		blockStarts    []time.Time
	)
	for st := min; !st.After(max); st = st.Add(ropts.BlockSize()) {
		blockStarts = append(blockStarts, st)
	}
	for shard := uint32(0); shard < uint32(shards); shard++ {
		shardIDs[shard] = make([]ts.ID, 0, idsPerShard)
		shardData[shard] = make(map[ts.Hash]map[time.Time]checked.Bytes, idsPerShard)
		for _, blockStart := range blockStarts {
			w, closer := newOpenTestWriter(t, fsopts, opts.namespace, shard, blockStart)
			for i := 0; i < idsPerShard; i++ {
				id := ts.StringID(fmt.Sprintf("foo.%d", i))
				shardIDs[shard] = append(shardIDs[shard], id)
				if _, ok := shardData[shard][id.Hash()]; !ok {
					shardData[shard][id.Hash()] = make(map[time.Time]checked.Bytes, len(blockStarts))
				}

				data := checked.NewBytes(nil, nil)
				data.IncRef()
				for j := 0; j < dataBytesPerID; j++ {
					data.Append(byte(rand.Int63n(256)))
				}
				shardData[shard][id.Hash()][blockStart] = data

				err := w.Write(id, data, digest.Checksum(data.Get()))
				require.NoError(t, err)
			}
			closer()
		}
	}

	var (
		startWg, readyWg sync.WaitGroup
		seeksPerID       = 48
		seeksEach        = shards * idsPerShard * seeksPerID
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
				shard := uint32((j + shardOffset) % shards)
				idIdx := uint32((j + idOffset) % len(shardIDs[shard]))
				id := shardIDs[shard][idIdx]

				for k := 0; k < len(blockStarts); k++ {
					stream, err := retriever.Stream(shard, id, blockStarts[k], nil)
					require.NoError(t, err)
					results = append(results, streamResult{
						shard:      shard,
						id:         id,
						blockStart: blockStarts[k],
						stream:     stream,
					})
				}
				for _, r := range results {
					seg, err := r.stream.Segment()
					if err != nil {
						fmt.Printf("\nstream seg err: %v\n", err)
						fmt.Printf("id: %s\n", r.id.String())
						fmt.Printf("shard: %d\n", r.shard)
						fmt.Printf("start: %v\n", r.blockStart.String())
					}

					require.NoError(t, err)
					compare.Head = shardData[r.shard][r.id.Hash()][r.blockStart]
					assert.True(t, seg.Equal(&compare))
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
