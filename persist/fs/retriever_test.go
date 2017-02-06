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
	seekConcurrency := 2 * fetchConcurrency
	opts := testBlockRetrieverOptions{
		retrieverOpts: NewBlockRetrieverOptions().
			SetFetchConcurrency(fetchConcurrency),
	}
	retriever, cleanup := newOpenTestBlockRetriever(t, opts)
	defer cleanup()

	fsopts := retriever.fsOpts
	ropts := fsopts.RetentionOptions()

	now := time.Now().Truncate(ropts.BlockSize())
	min, max := now.Add(-24*ropts.BlockSize()), now.Add(-ropts.BlockSize())

	var (
		shards         = 16
		idsPerShard    = 8
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
		startWg, readyWg, verifyWg sync.WaitGroup
		verifiers                  = 2
		verifierCh                 = make(chan streamResult, 128)
		verifiedLock               sync.Mutex
		verified                   = make(map[uint32]map[ts.Hash]map[time.Time]int)
		seeksPerID                 = 4
		seeksEach                  = shards * idsPerShard * seeksPerID
		seekPauseEvery             = 8
		seekPauseFor               = time.Millisecond
	)
	for i := 0; i < verifiers; i++ {
		verifyWg.Add(1)
		go func() {
			defer verifyWg.Done()
			compare := ts.Segment{}
			for r := range verifierCh {
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

				verifiedLock.Lock()
				if _, ok := verified[r.shard]; !ok {
					verified[r.shard] = make(map[ts.Hash]map[time.Time]int)
				}
				if _, ok := verified[r.shard][r.id.Hash()]; !ok {
					verified[r.shard][r.id.Hash()] = make(map[time.Time]int)
				}
				verified[r.shard][r.id.Hash()][r.blockStart] =
					verified[r.shard][r.id.Hash()][r.blockStart] + 1
				verifiedLock.Unlock()
			}
		}()
	}

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

			for j := 0; j < seeksEach; j++ {
				shard := uint32((i + j) % shards)
				idIdx := uint32((i + j) % len(shardIDs[shard]))
				id := shardIDs[shard][idIdx]
				blockStart := blockStarts[(i+j)%len(blockStarts)]

				stream, err := retriever.Stream(shard, id, blockStart, nil)
				require.NoError(t, err)
				verifierCh <- streamResult{
					shard:      shard,
					id:         id,
					blockStart: blockStart,
					stream:     stream,
				}
				if j+1%seekPauseEvery == 0 {
					time.Sleep(seekPauseFor)
				}
			}
		}()
	}

	// Wait for all routines to be ready then start
	readyWg.Wait()
	startWg.Done()

	// Enqueue then close verifierCh
	enqueueWg.Wait()
	close(verifierCh)

	// Wait for verification
	verifyWg.Wait()

	uniqueVerified := 0
	for shard := range verified {
		for idHash := range verified[shard] {
			uniqueVerified += len(verified[shard][idHash])
		}
	}
	assert.True(t, uniqueVerified > (shards*idsPerShard)/4)
}
