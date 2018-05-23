// Copyright (c) 2018 Uber Technologies, Inc.
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
	"os"
	"testing"
	"time"

	"github.com/m3db/m3db/src/dbnode/persist"
	"github.com/m3db/m3db/src/dbnode/persist/fs"
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3db/src/dbnode/storage/index/convert"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3ninx/index/segment"
	"github.com/m3db/m3ninx/index/segment/mem"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

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
	at := time.Now()
	start := at.Truncate(testBlockSize)
	indexStart := start.Truncate(testIndexBlockSize)
	for !start.Equal(indexStart) {
		// make sure data blocks overlap, test block size is 2h
		// and test index block size is 4h
		start = start.Add(testBlockSize)
		indexStart = start.Truncate(testIndexBlockSize)
	}

	end := start.Add(time.Duration(opts.numBlocks) * testIndexBlockSize)

	shardTimeRanges := map[uint32]xtime.Ranges{
		testShard: xtime.Ranges{}.AddRange(xtime.Range{
			Start: start,
			End:   end,
		}),
	}

	return testBootstrapIndexTimes{
		start:           start,
		end:             end,
		shardTimeRanges: shardTimeRanges,
	}
}

type testSeriesBlocks [][]testSeries

func testGoodTaggedSeriesDataBlocks() testSeriesBlocks {
	fooSeries := struct {
		id   string
		tags map[string]string
	}{
		"foo",
		map[string]string{"aaa": "bbb", "ccc": "ddd"},
	}
	dataBlocks := testSeriesBlocks{
		[]testSeries{
			{fooSeries.id, fooSeries.tags, []byte{0x1}},
			{"bar", map[string]string{"eee": "fff", "ggg": "hhh"}, []byte{0x1}},
			{"baz", map[string]string{"iii": "jjj", "kkk": "lll"}, []byte{0x1}},
		},
		[]testSeries{
			{fooSeries.id, fooSeries.tags, []byte{0x2}},
			{"qux", map[string]string{"mmm": "nnn", "ooo": "ppp"}, []byte{0x2}},
			{"qaz", map[string]string{"qqq": "rrr", "sss": "ttt"}, []byte{0x2}},
		},
		[]testSeries{
			{fooSeries.id, fooSeries.tags, []byte{0x3}},
			{"qan", map[string]string{"uuu": "vvv", "www": "xxx"}, []byte{0x3}},
			{"qam", map[string]string{"yyy": "zzz", "000": "111"}, []byte{0x3}},
		},
	}
	return dataBlocks
}

func writeTSDBGoodTaggedSeriesDataFiles(
	t *testing.T,
	dir string,
	namespaceID ident.ID,
	start time.Time,
) {
	dataBlocks := testGoodTaggedSeriesDataBlocks()

	writeTSDBFiles(t, dir, namespaceID, testShard,
		start, dataBlocks[0])
	writeTSDBFiles(t, dir, namespaceID, testShard,
		start.Add(testBlockSize), dataBlocks[1])
	writeTSDBFiles(t, dir, namespaceID, testShard,
		start.Add(2*testBlockSize), dataBlocks[2])
}

func writeTSDBPersistedIndexBlock(
	t *testing.T,
	dir string,
	namespace namespace.Metadata,
	start time.Time,
	shards map[uint32]struct{},
	block []testSeries,
) {
	seg, err := mem.NewSegment(0, mem.NewOptions())
	require.NoError(t, err)

	for _, series := range block {
		d, err := convert.FromMetric(series.ID(), series.Tags())
		require.NoError(t, err)
		exists, err := seg.ContainsID(series.ID().Bytes())
		require.NoError(t, err)
		if exists {
			continue // duplicate
		}
		_, err = seg.Insert(d)
		require.NoError(t, err)
	}

	_, err = seg.Seal()
	require.NoError(t, err)

	pm := newTestOptionsWithPersistManager(t, dir).PersistManager()
	flush, err := pm.StartIndexPersist()
	require.NoError(t, err)

	preparedPersist, err := flush.PrepareIndex(persist.IndexPrepareOptions{
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

func validateGoodTaggedSeries(
	t *testing.T,
	start time.Time,
	indexResults result.IndexResults,
) {
	require.Equal(t, 2, len(indexResults))

	dataBlocks := testGoodTaggedSeriesDataBlocks()

	for _, expected := range []struct {
		indexBlockStart time.Time
		series          map[string]testSeries
	}{
		{
			indexBlockStart: start,
			series: map[string]testSeries{
				dataBlocks[0][0].id: dataBlocks[0][0],
				dataBlocks[0][1].id: dataBlocks[0][1],
				dataBlocks[0][2].id: dataBlocks[0][2],
				dataBlocks[1][1].id: dataBlocks[1][1],
				dataBlocks[1][2].id: dataBlocks[1][2],
			},
		},
		{
			indexBlockStart: start.Add(testIndexBlockSize),
			series: map[string]testSeries{
				dataBlocks[2][0].id: dataBlocks[2][0],
				dataBlocks[2][1].id: dataBlocks[2][1],
				dataBlocks[2][2].id: dataBlocks[2][2],
			},
		},
	} {
		expectedAt := xtime.ToUnixNano(expected.indexBlockStart)
		indexBlock, ok := indexResults[expectedAt]
		require.True(t, ok)
		require.Equal(t, 1, len(indexBlock.Segments()))
		for _, seg := range indexBlock.Segments() {
			reader, err := seg.Reader()
			require.NoError(t, err)

			docs, err := reader.AllDocs()
			require.NoError(t, err)

			matches := map[string]struct{}{}
			for docs.Next() {
				curr := docs.Current()

				_, ok := matches[string(curr.ID)]
				require.False(t, ok)
				matches[string(curr.ID)] = struct{}{}

				series, ok := expected.series[string(curr.ID)]
				require.True(t, ok)

				matchingTags := map[string]struct{}{}
				for _, tag := range curr.Fields {
					_, ok := matchingTags[string(tag.Name)]
					require.False(t, ok)
					matchingTags[string(tag.Name)] = struct{}{}

					tagValue, ok := series.tags[string(tag.Name)]
					require.True(t, ok)

					require.Equal(t, tagValue, string(tag.Value))
				}
				require.Equal(t, len(series.tags), len(matchingTags))
			}
			require.NoError(t, docs.Err())
			require.NoError(t, docs.Close())

			require.Equal(t, len(expected.series), len(matches))
		}
	}
}

func TestBootstrapIndex(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	times := newTestBootstrapIndexTimes(testTimesOptions{
		numBlocks: 2,
	})

	writeTSDBGoodTaggedSeriesDataFiles(t, dir, testNs1ID, times.start)

	src := newFileSystemSource(newTestOptions(dir))
	res, err := src.ReadIndex(testNsMetadata(t), times.shardTimeRanges,
		testDefaultRunOpts)
	require.NoError(t, err)

	indexResults := res.IndexResults()
	validateGoodTaggedSeries(t, times.start, indexResults)
}

func TestBootstrapIndexIncremental(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	times := newTestBootstrapIndexTimes(testTimesOptions{
		numBlocks: 2,
	})

	writeTSDBGoodTaggedSeriesDataFiles(t, dir, testNs1ID, times.start)

	opts := newTestOptionsWithPersistManager(t, dir)
	scope := tally.NewTestScope("", nil)
	opts = opts.SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(scope))

	runOpts := testDefaultRunOpts.SetIncremental(true)

	src := newFileSystemSource(opts).(*fileSystemSource)
	res, err := src.ReadIndex(testNsMetadata(t), times.shardTimeRanges,
		runOpts)
	require.NoError(t, err)

	// Check that single persisted segment got written out
	infoFiles := fs.ReadIndexInfoFiles(src.fsopts.FilePathPrefix(), testNs1ID,
		src.fsopts.InfoReaderBufferSize())
	require.Equal(t, 1, len(infoFiles))

	for _, infoFile := range infoFiles {
		require.NoError(t, infoFile.Err.Error())
		require.Equal(t, times.start.UnixNano(), infoFile.Info.BlockStart)
		require.Equal(t, testIndexBlockSize, time.Duration(infoFile.Info.BlockSize))
		require.Equal(t, persist.FileSetFlushType, persist.FileSetType(infoFile.Info.FileType))
		require.Equal(t, 1, len(infoFile.Info.Segments))
		require.Equal(t, 1, len(infoFile.Info.Shards))
		require.Equal(t, testShard, infoFile.Info.Shards[0])
	}

	indexResults := res.IndexResults()

	// Check that the segment is not a mutable segment for this block
	block, ok := indexResults[xtime.ToUnixNano(times.start)]
	require.True(t, ok)
	require.Equal(t, 1, len(block.Segments()))
	_, mutable := block.Segments()[0].(segment.MutableSegment)
	require.False(t, mutable)

	// Check that the second segment is mutable and was not written out
	block, ok = indexResults[xtime.ToUnixNano(times.start.Add(testIndexBlockSize))]
	require.True(t, ok)
	require.Equal(t, 1, len(block.Segments()))
	_, mutable = block.Segments()[0].(segment.MutableSegment)
	require.True(t, mutable)

	// Validate results
	validateGoodTaggedSeries(t, times.start, indexResults)

	// Validate that wrote the block out (and no index blocks
	// were read as existing index blocks on disk)
	counters := scope.Snapshot().Counters()
	require.Equal(t, int64(0), counters["fs-bootstrapper.persist-index-blocks-read+"].Value())
	require.Equal(t, int64(1), counters["fs-bootstrapper.persist-index-blocks-write+"].Value())
}

func TestBootstrapIndexIncrementalPrefersPersistedIndexBlocks(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	times := newTestBootstrapIndexTimes(testTimesOptions{
		numBlocks: 2,
	})

	// Write data files
	writeTSDBGoodTaggedSeriesDataFiles(t, dir, testNs1ID, times.start)

	// Now write index block segment from first two data blocks
	testData := testGoodTaggedSeriesDataBlocks()
	shards := map[uint32]struct{}{testShard: struct{}{}}
	writeTSDBPersistedIndexBlock(t, dir, testNsMetadata(t), times.start, shards,
		append(testData[0], testData[1]...))

	opts := newTestOptionsWithPersistManager(t, dir)
	scope := tally.NewTestScope("", nil)
	opts = opts.SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(scope))

	runOpts := testDefaultRunOpts.SetIncremental(true)

	src := newFileSystemSource(opts).(*fileSystemSource)
	res, err := src.ReadIndex(testNsMetadata(t), times.shardTimeRanges,
		runOpts)
	require.NoError(t, err)

	indexResults := res.IndexResults()

	// Check that the segment is not a mutable segment for this block
	// and came from disk
	block, ok := indexResults[xtime.ToUnixNano(times.start)]
	require.True(t, ok)
	require.Equal(t, 1, len(block.Segments()))
	_, mutable := block.Segments()[0].(segment.MutableSegment)
	require.False(t, mutable)

	// Check that the second segment is mutable
	block, ok = indexResults[xtime.ToUnixNano(times.start.Add(testIndexBlockSize))]
	require.True(t, ok)
	require.Equal(t, 1, len(block.Segments()))
	_, mutable = block.Segments()[0].(segment.MutableSegment)
	require.True(t, mutable)

	// Validate results
	validateGoodTaggedSeries(t, times.start, indexResults)

	// Validate that read the block and no blocks were written
	// (ensure incremental didn't write it back out again)
	counters := scope.Snapshot().Counters()
	require.Equal(t, int64(1), counters["fs-bootstrapper.persist-index-blocks-read+"].Value())
	require.Equal(t, int64(0), counters["fs-bootstrapper.persist-index-blocks-write+"].Value())
}
