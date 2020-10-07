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

package fs

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/m3ninx/index/segment/mem"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

type testTimesOptions struct {
	numBlocks int
}

type testBootstrapIndexTimes struct {
	start           time.Time
	end             time.Time
	shardTimeRanges result.ShardTimeRanges
}

func newTestBootstrapIndexTimes(
	opts testTimesOptions,
) testBootstrapIndexTimes {
	var (
		start, end time.Time
		at         = time.Now()
	)
	switch opts.numBlocks {
	case 2:
		start = at.Truncate(testBlockSize)
		indexStart := start.Truncate(testIndexBlockSize)
		for !start.Equal(indexStart) {
			// make sure data blocks overlap, test block size is 2h
			// and test index block size is 4h
			start = start.Add(testBlockSize)
			indexStart = start.Truncate(testIndexBlockSize)
		}
		end = start.Add(time.Duration(opts.numBlocks) * testIndexBlockSize)
	default:
		panic("unexpected")
	}

	shardTimeRanges := result.NewShardTimeRanges().Set(
		testShard,
		xtime.NewRanges(xtime.Range{
			Start: start,
			End:   end,
		}),
	)

	return testBootstrapIndexTimes{
		start:           start,
		end:             end,
		shardTimeRanges: shardTimeRanges,
	}
}

type testSeriesBlocks [][]testSeries

func testGoodTaggedSeriesDataBlocks(numBlocks int) testSeriesBlocks {
	dataBlocks := make(testSeriesBlocks, 0, numBlocks)
	for i := 0; i < numBlocks; i++ {
		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, uint64(i))
		dataBlocks = append(dataBlocks, []testSeries{
			{fmt.Sprintf("foo-%d", i), map[string]string{"aaa": "bbb", "ccc": "ddd"}, data},
			{fmt.Sprintf("bar-%d", i), map[string]string{"eee": "fff", "ggg": "hhh"}, data},
			{fmt.Sprintf("baz-%d", i), map[string]string{"iii": "jjj", "kkk": "lll"}, data},
		})
	}
	return dataBlocks
}

func writeTSDBGoodTaggedSeriesDataFiles(
	t require.TestingT,
	dir string,
	namespaceID ident.ID,
	start time.Time,
	dataBlocks testSeriesBlocks,
) {
	for i := range dataBlocks {
		writeTSDBFiles(t, dir, namespaceID, testShard,
			start.Add(time.Duration(i)*testBlockSize), dataBlocks[i])
	}
}

func writeTSDBPersistedIndexBlock(
	t *testing.T,
	dir string,
	namespace namespace.Metadata,
	start time.Time,
	shards map[uint32]struct{},
	block []testSeries,
) {
	seg, err := mem.NewSegment(mem.NewOptions())
	require.NoError(t, err)

	for _, series := range block {
		d, err := convert.FromSeriesIDAndTags(series.ID(), series.Tags())
		require.NoError(t, err)
		exists, err := seg.ContainsID(series.ID().Bytes())
		require.NoError(t, err)
		if exists {
			continue // duplicate
		}
		_, err = seg.Insert(d)
		require.NoError(t, err)
	}

	err = seg.Seal()
	require.NoError(t, err)

	pm := newTestOptionsWithPersistManager(t, dir).PersistManager()
	flush, err := pm.StartIndexPersist()
	require.NoError(t, err)

	preparedPersist, err := flush.PrepareIndexFlush(persist.IndexPrepareOptions{
		NamespaceMetadata: namespace,
		BlockStart:        start,
		FileSetType:       persist.FileSetFlushType,
		Shards:            shards,
	})
	require.NoError(t, err)

	err = preparedPersist.Persist(seg)
	require.NoError(t, err)

	result, err := preparedPersist.Close()
	require.NoError(t, err)

	for _, r := range result {
		require.NoError(t, r.Close())
	}

	err = flush.DoneIndex()
	require.NoError(t, err)
}

func TestBootstrapIndexPartiallyFulfilled(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	timesOpts := testTimesOptions{
		numBlocks: 2,
	}
	times := newTestBootstrapIndexTimes(timesOpts)

	testData := testGoodTaggedSeriesDataBlocks(4)
	writeTSDBGoodTaggedSeriesDataFiles(t, dir, testNs1ID, times.start, testData)
	// Index block size is 4h and data block size is 2h.
	// So write the first two data blocks to disk as first index block.
	// We later ensure that second index block is completely unfulfilled.
	shards := map[uint32]struct{}{testShard: struct{}{}}
	writeTSDBPersistedIndexBlock(t, dir, testNsMetadata(t), times.start, shards,
		append(testData[0], testData[1]...))

	opts := newTestOptionsWithPersistManager(t, dir)
	scope := tally.NewTestScope("", nil)
	opts = opts.SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(scope))

	runOpts := testDefaultRunOpts

	fsSrc, err := newFileSystemSource(opts)
	require.NoError(t, err)

	src, ok := fsSrc.(*fileSystemSource)
	require.True(t, ok)

	nsMD := testNsMetadata(t)
	tester := bootstrap.BuildNamespacesTesterWithFilesystemOptions(t, runOpts,
		times.shardTimeRanges, opts.FilesystemOptions(), nsMD)
	defer tester.Finish()

	tester.TestReadWith(src)
	results := tester.ResultForNamespace(nsMD.ID()).IndexResult

	// Check that the ndex segment for the persisted index block is in the results
	// and came from disk.
	blockByVolumeType, ok := results.IndexResults()[xtime.ToUnixNano(times.start)]
	require.True(t, ok)
	block, ok := blockByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
	require.True(t, ok)
	require.Equal(t, 2, len(block.Segments()))
	for _, seg := range block.Segments() {
		require.True(t, seg.IsPersisted())
	}

	// Ensure that the second index block does not exist in the results.
	_, ok = results.IndexResults()[xtime.ToUnixNano(times.start.Add(testIndexBlockSize))]
	require.False(t, ok)

	// Validate that read the block and that the second index block is unfulfilled.
	expectedUnfulfilled := result.NewShardTimeRanges().Set(
		testShard,
		xtime.NewRanges(xtime.Range{
			Start: times.start.Add(testIndexBlockSize),
			End:   times.start.Add(2 * testIndexBlockSize),
		}),
	)
	require.True(t, expectedUnfulfilled.Equal(results.Unfulfilled()))

	counters := scope.Snapshot().Counters()
	require.Equal(t, int64(1), counters["fs-bootstrapper.persist-index-blocks-read+"].Value())
}

func TestBootstrapIndexUnfulfilled(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	timesOpts := testTimesOptions{
		numBlocks: 2,
	}
	times := newTestBootstrapIndexTimes(timesOpts)

	testData := testGoodTaggedSeriesDataBlocks(4)
	writeTSDBGoodTaggedSeriesDataFiles(t, dir, testNs1ID, times.start, testData)
	// No index data persisted to disk, we ensure later that the entire range is unfulfilled.

	opts := newTestOptionsWithPersistManager(t, dir)
	scope := tally.NewTestScope("", nil)
	opts = opts.SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(scope))

	runOpts := testDefaultRunOpts

	fsSrc, err := newFileSystemSource(opts)
	require.NoError(t, err)

	src, ok := fsSrc.(*fileSystemSource)
	require.True(t, ok)

	nsMD := testNsMetadata(t)
	tester := bootstrap.BuildNamespacesTesterWithFilesystemOptions(t, runOpts,
		times.shardTimeRanges, opts.FilesystemOptions(), nsMD)
	defer tester.Finish()

	tester.TestReadWith(src)
	results := tester.ResultForNamespace(nsMD.ID()).IndexResult

	// Ensure that the first index block does not exist in the results.
	_, ok = results.IndexResults()[xtime.ToUnixNano(times.start)]
	require.False(t, ok)

	// Ensure that the second index block does not exist in the results.
	_, ok = results.IndexResults()[xtime.ToUnixNano(times.start.Add(testIndexBlockSize))]
	require.False(t, ok)

	// Validate that read the block and that the second index block is unfulfilled.
	expectedUnfulfilled := times.shardTimeRanges
	require.True(t, expectedUnfulfilled.Equal(results.Unfulfilled()))

	counters := scope.Snapshot().Counters()
	require.Equal(t, int64(0), counters["fs-bootstrapper.persist-index-blocks-read+"].Value())
}
