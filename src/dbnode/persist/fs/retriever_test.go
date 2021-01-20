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
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/storage/limits"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testBlockRetrieverOptions struct {
	retrieverOpts  BlockRetrieverOptions
	fsOpts         Options
	newSeekerMgrFn newSeekerMgrFn
	shards         []uint32
}

type testCleanupFn func()

func newOpenTestBlockRetriever(
	t *testing.T,
	md namespace.Metadata,
	opts testBlockRetrieverOptions,
) (*blockRetriever, testCleanupFn) {
	require.NotNil(t, opts.retrieverOpts)
	require.NotNil(t, opts.fsOpts)

	r, err := NewBlockRetriever(opts.retrieverOpts, opts.fsOpts)
	require.NoError(t, err)

	retriever := r.(*blockRetriever)
	if opts.newSeekerMgrFn != nil {
		retriever.newSeekerMgrFn = opts.newSeekerMgrFn
	}

	shardSet, err := sharding.NewShardSet(
		sharding.NewShards(opts.shards, shard.Available),
		sharding.DefaultHashFn(1),
	)
	require.NoError(t, err)

	nsPath := NamespaceDataDirPath(opts.fsOpts.FilePathPrefix(), testNs1ID)
	require.NoError(t, os.MkdirAll(nsPath, opts.fsOpts.NewDirectoryMode()))
	require.NoError(t, retriever.Open(md, shardSet))

	return retriever, func() {
		require.NoError(t, retriever.Close())
	}
}

func newOpenTestWriter(
	t *testing.T,
	fsOpts Options,
	shard uint32,
	start time.Time,
	volume int,
) (DataFileSetWriter, testCleanupFn) {
	w := newTestWriter(t, fsOpts.FilePathPrefix())
	writerOpts := DataWriterOpenOptions{
		BlockSize: testBlockSize,
		Identifier: FileSetFileIdentifier{
			Namespace:   testNs1ID,
			Shard:       shard,
			BlockStart:  start,
			VolumeIndex: volume,
		},
	}
	require.NoError(t, w.Open(writerOpts))
	return w, func() {
		require.NoError(t, w.Close())
	}
}

type streamResult struct {
	ctx        context.Context
	shard      uint32
	id         string
	blockStart time.Time
	stream     xio.BlockReader
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

type seekerTrackCloses struct {
	DataFileSetSeeker

	trackCloseFn func()
}

func (s seekerTrackCloses) Close() error {
	s.trackCloseFn()
	return s.DataFileSetSeeker.Close()
}

func testBlockRetrieverHighConcurrentSeeks(t *testing.T, shouldCacheShardIndices bool) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	defer leaktest.CheckTimeout(t, 2*time.Minute)()

	dir, err := ioutil.TempDir("", "testdb")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Setup data generation.
	var (
		nsMeta   = testNs1Metadata(t)
		ropts    = nsMeta.Options().RetentionOptions()
		nsCtx    = namespace.NewContextFrom(nsMeta)
		now      = time.Now().Truncate(ropts.BlockSize())
		min, max = now.Add(-6 * ropts.BlockSize()), now.Add(-ropts.BlockSize())

		shards         = []uint32{0, 1, 2}
		idsPerShard    = 16
		shardIDs       = make(map[uint32][]ident.ID)
		shardIDStrings = make(map[uint32][]string)
		dataBytesPerID = 32
		// Shard -> ID -> Blockstart -> Data
		shardData   = make(map[uint32]map[string]map[xtime.UnixNano]checked.Bytes)
		blockStarts []time.Time
		volumes     = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	)
	for st := min; !st.After(max); st = st.Add(ropts.BlockSize()) {
		blockStarts = append(blockStarts, st)
	}

	// Setup retriever.
	var (
		filePathPrefix             = filepath.Join(dir, "")
		fsOpts                     = testDefaultOpts.SetFilePathPrefix(filePathPrefix)
		fetchConcurrency           = 4
		seekConcurrency            = 4 * fetchConcurrency
		updateOpenLeaseConcurrency = 4
		// NB(r): Try to make sure same req structs are reused frequently
		// to surface any race issues that might occur with pooling.
		poolOpts = pool.NewObjectPoolOptions().
				SetSize(fetchConcurrency / 2)
	)
	segReaderPool := xio.NewSegmentReaderPool(poolOpts)
	segReaderPool.Init()

	retrieveRequestPool := NewRetrieveRequestPool(segReaderPool, poolOpts)
	retrieveRequestPool.Init()

	opts := testBlockRetrieverOptions{
		retrieverOpts: defaultTestBlockRetrieverOptions.
			SetFetchConcurrency(fetchConcurrency).
			SetRetrieveRequestPool(retrieveRequestPool),
		fsOpts: fsOpts,
		shards: shards,
	}

	retriever, cleanup := newOpenTestBlockRetriever(t, testNs1Metadata(t), opts)
	defer cleanup()

	// Setup the open seeker function to fail sometimes to exercise that code path.
	var (
		seekerMgr                 = retriever.seekerMgr.(*seekerManager)
		existingNewOpenSeekerFn   = seekerMgr.newOpenSeekerFn
		seekerStatsLock           sync.Mutex
		numNonTerminalVolumeOpens int
		numSeekerCloses           int
	)
	newNewOpenSeekerFn := func(shard uint32, blockStart time.Time, volume int) (DataFileSetSeeker, error) {
		// Artificially slow down how long it takes to open a seeker to exercise the logic where
		// multiple goroutines are trying to open seekers for the same shard/blockStart and need
		// to wait for the others to complete.
		time.Sleep(5 * time.Millisecond)
		// 10% chance for this to fail so that error paths get exercised as well.
		if val := rand.Intn(100); val >= 90 {
			return nil, errors.New("some-error")
		}
		seeker, err := existingNewOpenSeekerFn(shard, blockStart, volume)
		if err != nil {
			return nil, err
		}

		if volume != volumes[len(volumes)-1] {
			// Only track the open if its not for the last volume which will help us determine if the correct
			// number of seekers were closed later.
			seekerStatsLock.Lock()
			numNonTerminalVolumeOpens++
			seekerStatsLock.Unlock()
		}
		return &seekerTrackCloses{
			DataFileSetSeeker: seeker,
			trackCloseFn: func() {
				seekerStatsLock.Lock()
				numSeekerCloses++
				seekerStatsLock.Unlock()
			},
		}, nil
	}
	seekerMgr.newOpenSeekerFn = newNewOpenSeekerFn

	// Setup the block lease manager to return errors sometimes to exercise that code path.
	mockBlockLeaseManager := block.NewMockLeaseManager(ctrl)
	mockBlockLeaseManager.EXPECT().RegisterLeaser(gomock.Any()).AnyTimes()
	mockBlockLeaseManager.EXPECT().UnregisterLeaser(gomock.Any()).AnyTimes()
	mockBlockLeaseManager.EXPECT().OpenLatestLease(gomock.Any(), gomock.Any()).DoAndReturn(func(_ block.Leaser, _ block.LeaseDescriptor) (block.LeaseState, error) {
		// 10% chance for this to fail so that error paths get exercised as well.
		if val := rand.Intn(100); val >= 90 {
			return block.LeaseState{}, errors.New("some-error")
		}

		return block.LeaseState{Volume: 0}, nil
	}).AnyTimes()
	seekerMgr.blockRetrieverOpts = seekerMgr.blockRetrieverOpts.
		SetBlockLeaseManager(mockBlockLeaseManager)

	// Generate data.
	for _, shard := range shards {
		shardIDs[shard] = make([]ident.ID, 0, idsPerShard)
		shardData[shard] = make(map[string]map[xtime.UnixNano]checked.Bytes, idsPerShard)
		for _, blockStart := range blockStarts {
			for _, volume := range volumes {
				w, closer := newOpenTestWriter(t, fsOpts, shard, blockStart, volume)
				for i := 0; i < idsPerShard; i++ {
					idString := fmt.Sprintf("foo.%d", i)
					shardIDStrings[shard] = append(shardIDStrings[shard], idString)

					id := ident.StringID(idString)
					shardIDs[shard] = append(shardIDs[shard], id)
					if _, ok := shardData[shard][idString]; !ok {
						shardData[shard][idString] = make(map[xtime.UnixNano]checked.Bytes, len(blockStarts))
					}

					// Always write the same data for each series regardless of volume to make asserting on
					// Stream() responses simpler. Each volume gets a unique tag so we can verify that leases
					// are being upgraded by checking the tags.
					blockStartNanos := xtime.ToUnixNano(blockStart)
					data, ok := shardData[shard][idString][blockStartNanos]
					if !ok {
						data = checked.NewBytes(nil, nil)
						data.IncRef()
						for j := 0; j < dataBytesPerID; j++ {
							data.Append(byte(rand.Int63n(256)))
						}
						shardData[shard][idString][blockStartNanos] = data
					}

					tags := testTagsFromIDAndVolume(id.String(), volume)
					metadata := persist.NewMetadataFromIDAndTags(id, tags,
						persist.MetadataOptions{})
					err := w.Write(metadata, data, digest.Checksum(data.Bytes()))
					require.NoError(t, err)
				}
				closer()
			}
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

	// Write a fake onRetrieve function so we can verify the behavior of the callback.
	var (
		retrievedIDs      = map[string]ident.Tags{}
		retrievedIDsMutex = sync.Mutex{}
		bytesPool         = pool.NewCheckedBytesPool(nil, nil, func(s []pool.Bucket) pool.BytesPool {
			return pool.NewBytesPool(s, nil)
		})
		idPool = ident.NewPool(bytesPool, ident.PoolOptions{})
	)
	bytesPool.Init()

	onRetrieve := block.OnRetrieveBlockFn(func(id ident.ID, tagsIter ident.TagIterator, startTime time.Time, segment ts.Segment, nsCtx namespace.Context) {
		// TagsFromTagsIter requires a series ID to try and share bytes so we just pass
		// an empty string because we don't care about efficiency.
		tags, err := convert.TagsFromTagsIter(ident.StringID(""), tagsIter, idPool)
		require.NoError(t, err)

		retrievedIDsMutex.Lock()
		retrievedIDs[id.String()] = tags
		retrievedIDsMutex.Unlock()
	})

	// Setup concurrent seeks.
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

					var (
						stream xio.BlockReader
						err    error
					)
					for {
						// Run in a loop since the open seeker function is configured to randomly fail
						// sometimes.
						stream, err = retriever.Stream(ctx, shard, id, blockStarts[k], onRetrieve, nsCtx)
						if err == nil {
							break
						}
					}

					results = append(results, streamResult{
						ctx:        ctx,
						shard:      shard,
						id:         idString,
						blockStart: blockStarts[k],
						stream:     stream,
					})
				}

				for _, r := range results {
					compare.Head = shardData[r.shard][r.id][xtime.ToUnixNano(r.blockStart)]

					// If the stream is empty, assert that the expected result is also nil
					if r.stream.IsEmpty() {
						require.Nil(t, compare.Head)
						continue
					}

					seg, err := r.stream.Segment()
					if err != nil {
						fmt.Printf("\nstream seg err: %v\n", err)
						fmt.Printf("id: %s\n", r.id)
						fmt.Printf("shard: %d\n", r.shard)
						fmt.Printf("start: %v\n", r.blockStart.String())
					}

					require.NoError(t, err)
					require.True(
						t,
						seg.Equal(&compare),
						fmt.Sprintf(
							"data mismatch for series %s, returned data: %v, expected: %v",
							r.id,
							string(seg.Head.Bytes()),
							string(compare.Head.Bytes())))

					r.ctx.Close()
				}
				results = results[:0]
			}
		}()
	}

	// Wait for all routines to be ready.
	readyWg.Wait()
	// Allow all the goroutines to begin.
	startWg.Done()

	// Setup concurrent block lease updates.
	workers := xsync.NewWorkerPool(updateOpenLeaseConcurrency)
	workers.Init()
	// Volume -> shard -> blockStart to stripe as many shard/blockStart as quickly as possible to
	// improve the chance of triggering the code path where UpdateOpenLease is the first time a set
	// of seekers are opened for a shard/blocksStart combination.
	for _, volume := range volumes {
		for _, shard := range shards {
			for _, blockStart := range blockStarts {
				enqueueWg.Add(1)
				var (
					// Capture vars for async goroutine.
					volume     = volume
					shard      = shard
					blockStart = blockStart
				)
				workers.Go(func() {
					defer enqueueWg.Done()
					leaser := retriever.seekerMgr.(block.Leaser)

					for {
						// Run in a loop since the open seeker function is configured to randomly fail
						// sometimes.
						_, err := leaser.UpdateOpenLease(block.LeaseDescriptor{
							Namespace:  nsMeta.ID(),
							Shard:      shard,
							BlockStart: blockStart,
						}, block.LeaseState{Volume: volume})
						// Ignore errOutOfOrderUpdateOpenLease because the goroutines in this test are not coordinated
						// and thus may try to call UpdateOpenLease() with out of order volumes. Thats fine for the
						// purposes of this test since the goal here is to make sure there are no race conditions and
						// ensure that the SeekerManager ends up in the correct state when the test is complete.
						if err == nil || err == errOutOfOrderUpdateOpenLease {
							break
						}
					}
				})
			}
		}
	}

	// Wait until done.
	enqueueWg.Wait()

	seekerStatsLock.Lock()
	// Don't multiply by fetchConcurrency because the tracking doesn't take concurrent
	// clones into account.
	require.Equal(t, numNonTerminalVolumeOpens, numSeekerCloses)
	seekerStatsLock.Unlock()

	// Verify the onRetrieve callback was called properly for everything.
	for _, shard := range shardIDStrings {
		for _, id := range shard {
			retrievedIDsMutex.Lock()
			tags, ok := retrievedIDs[id]
			retrievedIDsMutex.Unlock()
			require.True(t, ok, fmt.Sprintf("expected %s to be retrieved, but it was not", id))

			// Strip the volume tag because these reads were performed while concurrent block lease updates
			// were happening so its not deterministic which volume tag they'll have at this point.
			tags = stripVolumeTag(tags)
			expectedTags := stripVolumeTag(testTagsFromIDAndVolume(id, 0))
			require.True(
				t,
				tags.Equal(expectedTags),
				fmt.Sprintf("expectedNumTags=%d, actualNumTags=%d", len(expectedTags.Values()), len(tags.Values())))
		}
	}

	// Now that all the block lease updates have completed, all reads from this point should return tags with the
	// highest volume number.
	ctx := context.NewContext()
	for _, shard := range shards {
		for _, blockStart := range blockStarts {
			for _, idString := range shardIDStrings[shard] {
				id := ident.StringID(idString)
				for {
					// Run in a loop since the open seeker function is configured to randomly fail
					// sometimes.
					ctx.Reset()
					_, err := retriever.Stream(ctx, shard, id, blockStart, onRetrieve, nsCtx)
					ctx.BlockingClose()
					if err == nil {
						break
					}
				}

			}
		}
	}

	for _, shard := range shardIDStrings {
		for _, id := range shard {
			retrievedIDsMutex.Lock()
			tags, ok := retrievedIDs[id]
			retrievedIDsMutex.Unlock()
			require.True(t, ok, fmt.Sprintf("expected %s to be retrieved, but it was not", id))
			tagsSlice := tags.Values()

			// Highest volume is expected.
			expectedVolumeTag := strconv.Itoa(volumes[len(volumes)-1])
			// Volume tag is last.
			volumeTag := tagsSlice[len(tagsSlice)-1].Value.String()
			require.Equal(t, expectedVolumeTag, volumeTag)
		}
	}
}

// TestBlockRetrieverIDDoesNotExist verifies the behavior of the Stream() method
// on the retriever in the case where the requested ID does not exist. In that
// case, Stream() should return an empty segment.
func TestBlockRetrieverIDDoesNotExist(t *testing.T) {
	scope := tally.NewTestScope("test", nil)

	// Make sure reader/writer are looking at the same test directory
	dir, err := ioutil.TempDir("", "testdb")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	filePathPrefix := filepath.Join(dir, "")

	// Setup constants and config
	fsOpts := testDefaultOpts.SetFilePathPrefix(filePathPrefix)
	nsMeta := testNs1Metadata(t)
	rOpts := nsMeta.Options().RetentionOptions()
	nsCtx := namespace.NewContextFrom(nsMeta)
	shard := uint32(0)
	blockStart := time.Now().Truncate(rOpts.BlockSize())

	// Setup the reader
	opts := testBlockRetrieverOptions{
		retrieverOpts: defaultTestBlockRetrieverOptions,
		fsOpts:        fsOpts.SetInstrumentOptions(instrument.NewOptions().SetMetricsScope(scope)),
		shards:        []uint32{shard},
	}
	retriever, cleanup := newOpenTestBlockRetriever(t, testNs1Metadata(t), opts)
	defer cleanup()

	// Write out a test file
	w, closer := newOpenTestWriter(t, fsOpts, shard, blockStart, 0)
	data := checked.NewBytes([]byte("Hello world!"), nil)
	data.IncRef()
	defer data.DecRef()
	metadata := persist.NewMetadataFromIDAndTags(ident.StringID("exists"), ident.Tags{},
		persist.MetadataOptions{})
	err = w.Write(metadata, data, digest.Checksum(data.Bytes()))
	assert.NoError(t, err)
	closer()

	ctx := context.NewContext()
	defer ctx.Close()
	segmentReader, err := retriever.Stream(ctx, shard,
		ident.StringID("not-exists"), blockStart, nil, nsCtx)
	assert.NoError(t, err)

	assert.True(t, segmentReader.IsEmpty())

	// Check that the bloom filter miss metric was incremented
	snapshot := scope.Snapshot()
	seriesRead := snapshot.Counters()["test.retriever.series-bloom-filter-misses+"]
	require.Equal(t, int64(1), seriesRead.Value())
}

// TestBlockRetrieverOnlyCreatesTagItersIfTagsExists verifies that the block retriever
// only creates a tag iterator in the OnRetrieve pathway if the series has tags.
func TestBlockRetrieverOnlyCreatesTagItersIfTagsExists(t *testing.T) {
	// Make sure reader/writer are looking at the same test directory.
	dir, err := ioutil.TempDir("", "testdb")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	filePathPrefix := filepath.Join(dir, "")

	// Setup constants and config.
	fsOpts := testDefaultOpts.SetFilePathPrefix(filePathPrefix)
	rOpts := testNs1Metadata(t).Options().RetentionOptions()
	nsCtx := namespace.NewContextFrom(testNs1Metadata(t))
	shard := uint32(0)
	blockStart := time.Now().Truncate(rOpts.BlockSize())

	// Setup the reader.
	opts := testBlockRetrieverOptions{
		retrieverOpts: defaultTestBlockRetrieverOptions,
		fsOpts:        fsOpts,
		shards:        []uint32{shard},
	}
	retriever, cleanup := newOpenTestBlockRetriever(t, testNs1Metadata(t), opts)
	defer cleanup()

	// Write out a test file.
	var (
		w, closer = newOpenTestWriter(t, fsOpts, shard, blockStart, 0)
		tag       = ident.Tag{
			Name:  ident.StringID("name"),
			Value: ident.StringID("value"),
		}
		tags = ident.NewTags(tag)
	)
	for _, write := range []struct {
		id   string
		tags ident.Tags
	}{
		{
			id:   "no-tags",
			tags: ident.Tags{},
		},
		{
			id:   "tags",
			tags: tags,
		},
	} {
		data := checked.NewBytes([]byte("Hello world!"), nil)
		data.IncRef()
		defer data.DecRef()

		metadata := persist.NewMetadataFromIDAndTags(ident.StringID(write.id), write.tags,
			persist.MetadataOptions{})
		err = w.Write(metadata, data, digest.Checksum(data.Bytes()))
		require.NoError(t, err)
	}
	closer()

	// Make sure we return the correct error if the ID does not exist
	ctx := context.NewContext()
	defer ctx.Close()

	_, err = retriever.Stream(ctx, shard,
		ident.StringID("no-tags"), blockStart, block.OnRetrieveBlockFn(func(
			id ident.ID,
			tagsIter ident.TagIterator,
			startTime time.Time,
			segment ts.Segment,
			nsCtx namespace.Context,
		) {
			require.Equal(t, ident.EmptyTagIterator, tagsIter)
			for tagsIter.Next() {
			}
			require.NoError(t, tagsIter.Err())
		}), nsCtx)

	_, err = retriever.Stream(ctx, shard,
		ident.StringID("tags"), blockStart, block.OnRetrieveBlockFn(func(
			id ident.ID,
			tagsIter ident.TagIterator,
			startTime time.Time,
			segment ts.Segment,
			nsCtx namespace.Context,
		) {
			for tagsIter.Next() {
				currTag := tagsIter.Current()
				require.True(t, tag.Equal(currTag))
			}
			require.NoError(t, tagsIter.Err())
		}), nsCtx)

	require.NoError(t, err)
}

// TestBlockRetrieverDoesNotInvokeOnRetrieveWithGlobalFlag verifies that the block retriever
// does not invoke the OnRetrieve block if the global CacheBlocksOnRetrieve is not enabled.
func TestBlockRetrieverDoesNotInvokeOnRetrieveWithGlobalFlag(t *testing.T) {
	testBlockRetrieverOnRetrieve(t, false, true)
}

// TestBlockRetrieverDoesNotInvokeOnRetrieveWithNamespacesFlag verifies that the block retriever
// does not invoke the OnRetrieve block if the namespace-specific CacheBlocksOnRetrieve is not enabled.
func TestBlockRetrieverDoesNotInvokeOnRetrieveWithNamespaceFlag(t *testing.T) {
	testBlockRetrieverOnRetrieve(t, true, false)
}

func TestBlockRetrieverDoesNotInvokeOnRetrieve(t *testing.T) {
	testBlockRetrieverOnRetrieve(t, false, false)
}

func TestBlockRetrieverDoesInvokeOnRetrieve(t *testing.T) {
	testBlockRetrieverOnRetrieve(t, true, true)
}

func testBlockRetrieverOnRetrieve(t *testing.T, globalFlag bool, nsFlag bool) {
	// Make sure reader/writer are looking at the same test directory.
	dir, err := ioutil.TempDir("", "testdb")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	filePathPrefix := filepath.Join(dir, "")

	// Setup constants and config.
	md, err := namespace.NewMetadata(testNs1ID, namespace.NewOptions().
		SetCacheBlocksOnRetrieve(nsFlag).
		SetRetentionOptions(retention.NewOptions().SetBlockSize(testBlockSize)).
		SetIndexOptions(namespace.NewIndexOptions().SetEnabled(true).SetBlockSize(testBlockSize)))
	require.NoError(t, err)

	fsOpts := testDefaultOpts.SetFilePathPrefix(filePathPrefix)
	rOpts := md.Options().RetentionOptions()
	nsCtx := namespace.NewContextFrom(md)
	shard := uint32(0)
	blockStart := time.Now().Truncate(rOpts.BlockSize())

	// Setup the reader.
	opts := testBlockRetrieverOptions{
		retrieverOpts: defaultTestBlockRetrieverOptions.SetCacheBlocksOnRetrieve(globalFlag),
		fsOpts:        fsOpts,
		shards:        []uint32{shard},
	}
	retriever, cleanup := newOpenTestBlockRetriever(t, md, opts)
	defer cleanup()

	// Write out a test file.
	var (
		w, closer = newOpenTestWriter(t, fsOpts, shard, blockStart, 0)
		tag       = ident.Tag{
			Name:  ident.StringID("name"),
			Value: ident.StringID("value"),
		}
		tags = ident.NewTags(tag)
		id   = "foo"
	)
	data := checked.NewBytes([]byte("Hello world!"), nil)
	data.IncRef()
	defer data.DecRef()

	metadata := persist.NewMetadataFromIDAndTags(ident.StringID(id), tags,
		persist.MetadataOptions{})
	err = w.Write(metadata, data, digest.Checksum(data.Bytes()))
	require.NoError(t, err)
	closer()

	// Make sure we return the correct error if the ID does not exist
	ctx := context.NewContext()
	defer ctx.Close()

	onRetrieveCalled := false
	retrieveFn := block.OnRetrieveBlockFn(func(
		id ident.ID,
		tagsIter ident.TagIterator,
		startTime time.Time,
		segment ts.Segment,
		nsCtx namespace.Context,
	) {
		onRetrieveCalled = true
	})

	segmentReader, err := retriever.Stream(ctx, shard,
		ident.StringID("foo"), blockStart, retrieveFn, nsCtx)

	_, err = segmentReader.Segment()
	require.NoError(t, err)

	if globalFlag && nsFlag {
		require.True(t, onRetrieveCalled)
	} else {
		require.False(t, onRetrieveCalled)
	}
}

// TestBlockRetrieverHandlesErrors verifies the behavior of the Stream() method
// on the retriever in the case where the SeekIndexEntry function returns an
// error.
func TestBlockRetrieverHandlesSeekIndexEntryErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSeeker := NewMockConcurrentDataFileSetSeeker(ctrl)
	mockSeeker.EXPECT().SeekIndexEntry(gomock.Any(), gomock.Any()).Return(IndexEntry{}, errSeekErr)

	testBlockRetrieverHandlesSeekErrors(t, ctrl, mockSeeker)
}

// TestBlockRetrieverHandlesErrors verifies the behavior of the Stream() method
// on the retriever in the case where the SeekByIndexEntry function returns an
// error.
func TestBlockRetrieverHandlesSeekByIndexEntryErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSeeker := NewMockConcurrentDataFileSetSeeker(ctrl)
	mockSeeker.EXPECT().SeekIndexEntry(gomock.Any(), gomock.Any()).Return(IndexEntry{}, nil)
	mockSeeker.EXPECT().SeekByIndexEntry(gomock.Any(), gomock.Any()).Return(nil, errSeekErr)

	testBlockRetrieverHandlesSeekErrors(t, ctrl, mockSeeker)
}

func TestLimitSeriesReadFromDisk(t *testing.T) {
	scope := tally.NewTestScope("test", nil)
	limitOpts := limits.NewOptions().
		SetInstrumentOptions(instrument.NewOptions().SetMetricsScope(scope)).
		SetBytesReadLimitOpts(limits.DefaultLookbackLimitOptions()).
		SetDocsLimitOpts(limits.DefaultLookbackLimitOptions()).
		SetDiskSeriesReadLimitOpts(limits.LookbackLimitOptions{
			Limit:    2,
			Lookback: time.Second * 1,
		})
	queryLimits, err := limits.NewQueryLimits(limitOpts)
	require.NoError(t, err)
	opts := NewBlockRetrieverOptions().
		SetBlockLeaseManager(&block.NoopLeaseManager{}).
		SetQueryLimits(queryLimits)
	publicRetriever, err := NewBlockRetriever(opts, NewOptions().
		SetInstrumentOptions(instrument.NewOptions().SetMetricsScope(scope)))
	require.NoError(t, err)
	req := &retrieveRequest{}
	retriever := publicRetriever.(*blockRetriever)
	_ = retriever.streamRequest(context.NewContext(), req, 0, ident.StringID("id"), time.Now())
	err = retriever.streamRequest(context.NewContext(), req, 0, ident.StringID("id"), time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "query aborted due to limit")

	snapshot := scope.Snapshot()
	seriesLimit := snapshot.Counters()["test.query-limit.exceeded+limit=disk-series-read"]
	require.Equal(t, int64(1), seriesLimit.Value())
}

var errSeekErr = errors.New("some-error")

func testBlockRetrieverHandlesSeekErrors(t *testing.T, ctrl *gomock.Controller, mockSeeker ConcurrentDataFileSetSeeker) {
	// Make sure reader/writer are looking at the same test directory.
	dir, err := ioutil.TempDir("", "testdb")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	filePathPrefix := filepath.Join(dir, "")

	// Setup constants and config.
	var (
		fsOpts     = testDefaultOpts.SetFilePathPrefix(filePathPrefix)
		rOpts      = testNs1Metadata(t).Options().RetentionOptions()
		nsCtx      = namespace.NewContextFrom(testNs1Metadata(t))
		shard      = uint32(0)
		blockStart = time.Now().Truncate(rOpts.BlockSize())
	)

	mockSeekerManager := NewMockDataFileSetSeekerManager(ctrl)
	mockSeekerManager.EXPECT().Open(gomock.Any(), gomock.Any()).Return(nil)
	mockSeekerManager.EXPECT().Test(gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil)
	mockSeekerManager.EXPECT().Borrow(gomock.Any(), gomock.Any()).Return(mockSeeker, nil)
	mockSeekerManager.EXPECT().Return(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	mockSeekerManager.EXPECT().Close().Return(nil)

	newSeekerMgr := func(
		bytesPool pool.CheckedBytesPool,
		opts Options,
		blockRetrieverOpts BlockRetrieverOptions,
	) DataFileSetSeekerManager {

		return mockSeekerManager
	}

	// Setup the reader.
	opts := testBlockRetrieverOptions{
		retrieverOpts:  defaultTestBlockRetrieverOptions,
		fsOpts:         fsOpts,
		newSeekerMgrFn: newSeekerMgr,
		shards:         []uint32{shard},
	}
	retriever, cleanup := newOpenTestBlockRetriever(t, testNs1Metadata(t), opts)
	defer cleanup()

	// Make sure we return the correct error.
	ctx := context.NewContext()
	defer ctx.Close()
	segmentReader, err := retriever.Stream(ctx, shard,
		ident.StringID("not-exists"), blockStart, nil, nsCtx)
	require.NoError(t, err)

	segment, err := segmentReader.Segment()
	assert.Equal(t, errSeekErr, err)
	assert.Equal(t, nil, segment.Head)
	assert.Equal(t, nil, segment.Tail)
}

func testTagsFromIDAndVolume(seriesID string, volume int) ident.Tags {
	tags := []ident.Tag{}
	for j := 0; j < 5; j++ {
		tags = append(tags, ident.StringTag(
			fmt.Sprintf("%s.tag.%d.name", seriesID, j),
			fmt.Sprintf("%s.tag.%d.value", seriesID, j),
		))
	}
	tags = append(tags, ident.StringTag("volume", strconv.Itoa(volume)))
	return ident.NewTags(tags...)
}

func stripVolumeTag(tags ident.Tags) ident.Tags {
	tagsSlice := tags.Values()
	tagsSlice = tagsSlice[:len(tagsSlice)-1]
	return ident.NewTags(tagsSlice...)
}
