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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/m3ninx/index/segment/mem"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
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
	case 3:
		end = at.Truncate(testIndexBlockSize)
		start = end.Add(time.Duration(-1*opts.numBlocks) * testBlockSize)
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
	t require.TestingT,
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

type expectedTaggedSeries struct {
	indexBlockStart time.Time
	series          map[string]testSeries
}

func expectedTaggedSeriesWithOptions(
	t require.TestingT,
	start time.Time,
	opts testTimesOptions,
) []expectedTaggedSeries {
	dataBlocks := testGoodTaggedSeriesDataBlocks()
	switch opts.numBlocks {
	case 2:
		return []expectedTaggedSeries{
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
		}
	case 3:
		return []expectedTaggedSeries{
			{
				indexBlockStart: start,
				series: map[string]testSeries{
					dataBlocks[0][0].id: dataBlocks[0][0],
					dataBlocks[0][1].id: dataBlocks[0][1],
					dataBlocks[0][2].id: dataBlocks[0][2],
				},
			},
			{
				indexBlockStart: start.Add(testIndexBlockSize),
				series: map[string]testSeries{
					dataBlocks[1][1].id: dataBlocks[1][1],
					dataBlocks[1][2].id: dataBlocks[1][2],
					dataBlocks[2][0].id: dataBlocks[2][0],
					dataBlocks[2][1].id: dataBlocks[2][1],
					dataBlocks[2][2].id: dataBlocks[2][2],
				},
			},
		}
	default:
		require.FailNow(t, "unsupported test times options")
	}
	return nil
}

func validateGoodTaggedSeries(
	t require.TestingT,
	start time.Time,
	indexResults result.IndexResults,
	opts testTimesOptions,
) {
	require.Equal(t, 2, len(indexResults))

	expectedSeriesByBlock := expectedTaggedSeriesWithOptions(t, start, opts)
	for _, expected := range expectedSeriesByBlock {
		expectedAt := xtime.ToUnixNano(expected.indexBlockStart)
		indexBlockByVolumeType, ok := indexResults[expectedAt]
		require.True(t, ok)
		for _, indexBlock := range indexBlockByVolumeType.Iter() {
			require.Equal(t, 1, len(indexBlock.Segments()))
			for _, seg := range indexBlock.Segments() {
				reader, err := seg.Segment().Reader()
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
}

func TestBootstrapIndex(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	timesOpts := testTimesOptions{
		numBlocks: 2,
	}
	times := newTestBootstrapIndexTimes(timesOpts)

	writeTSDBGoodTaggedSeriesDataFiles(t, dir, testNs1ID, times.start)

	opts := newTestOptionsWithPersistManager(t, dir)
	scope := tally.NewTestScope("", nil)
	opts = opts.SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(scope))

	// Should always be run with persist enabled.
	runOpts := testDefaultRunOpts.
		SetPersistConfig(bootstrap.PersistConfig{Enabled: true})

	fsSrc, err := newFileSystemSource(opts)
	require.NoError(t, err)

	src, ok := fsSrc.(*fileSystemSource)
	require.True(t, ok)

	nsMD := testNsMetadata(t)
	tester := bootstrap.BuildNamespacesTesterWithFilesystemOptions(t, runOpts,
		times.shardTimeRanges, opts.FilesystemOptions(), nsMD)
	defer tester.Finish()

	tester.TestReadWith(src)
	indexResults := tester.ResultForNamespace(nsMD.ID()).IndexResult.IndexResults()

	// Check that single persisted segment got written out
	infoFiles := fs.ReadIndexInfoFiles(fs.ReadIndexInfoFilesOptions{
		filePathPrefix:   src.fsopts.FilePathPrefix(),
		namespace:        testNs1ID,
		readerBufferSize: src.fsopts.InfoReaderBufferSize(),
	})
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

	// Check that the segment is not a mutable segment for this block
	blockByVolumeType, ok := indexResults[xtime.ToUnixNano(times.start)]
	require.True(t, ok)
	block, ok := blockByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
	require.True(t, ok)
	require.Equal(t, 1, len(block.Segments()))
	segment := block.Segments()[0]
	require.True(t, ok)
	require.True(t, segment.IsPersisted())

	// Check that the second segment is mutable and was not written out
	blockByVolumeType, ok = indexResults[xtime.ToUnixNano(times.start.Add(testIndexBlockSize))]
	require.True(t, ok)
	block, ok = blockByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
	require.True(t, ok)
	require.Equal(t, 1, len(block.Segments()))
	segment = block.Segments()[0]
	require.True(t, ok)
	require.False(t, segment.IsPersisted())

	// Validate results
	validateGoodTaggedSeries(t, times.start, indexResults, timesOpts)

	// Validate that wrote the block out (and no index blocks
	// were read as existing index blocks on disk)
	counters := scope.Snapshot().Counters()
	require.Equal(t, int64(0), counters["fs-bootstrapper.persist-index-blocks-read+"].Value())
	require.Equal(t, int64(1), counters["fs-bootstrapper.persist-index-blocks-write+"].Value())
}

func TestBootstrapIndexIgnoresPersistConfigIfSnapshotType(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	timesOpts := testTimesOptions{
		numBlocks: 2,
	}
	times := newTestBootstrapIndexTimes(timesOpts)

	writeTSDBGoodTaggedSeriesDataFiles(t, dir, testNs1ID, times.start)

	opts := newTestOptionsWithPersistManager(t, dir)
	scope := tally.NewTestScope("", nil)
	opts = opts.SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(scope))

	runOpts := testDefaultRunOpts.
		SetPersistConfig(bootstrap.PersistConfig{
			Enabled:     true,
			FileSetType: persist.FileSetSnapshotType,
		})

	fsSrc, err := newFileSystemSource(opts)
	require.NoError(t, err)

	src, ok := fsSrc.(*fileSystemSource)
	require.True(t, ok)

	nsMD := testNsMetadata(t)
	tester := bootstrap.BuildNamespacesTesterWithFilesystemOptions(t, runOpts,
		times.shardTimeRanges, opts.FilesystemOptions(), nsMD)
	defer tester.Finish()

	tester.TestReadWith(src)
	indexResults := tester.ResultForNamespace(nsMD.ID()).IndexResult.IndexResults()

	// Check that not segments were written out
	infoFiles := fs.ReadIndexInfoFiles(fs.ReadIndexInfoFilesOptions{
		filePathPrefix:   src.fsopts.FilePathPrefix(),
		namespace:        testNs1ID,
		readerBufferSize: src.fsopts.InfoReaderBufferSize(),
	})
	require.Equal(t, 0, len(infoFiles))

	// Check that both segments are mutable
	blockByVolumeType, ok := indexResults[xtime.ToUnixNano(times.start)]
	require.True(t, ok)
	block, ok := blockByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
	require.True(t, ok)
	require.Equal(t, 1, len(block.Segments()))
	segment := block.Segments()[0]
	require.True(t, ok)
	require.False(t, segment.IsPersisted())

	blockByVolumeType, ok = indexResults[xtime.ToUnixNano(times.start.Add(testIndexBlockSize))]
	require.True(t, ok)
	block, ok = blockByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
	require.True(t, ok)
	require.Equal(t, 1, len(block.Segments()))
	segment = block.Segments()[0]
	require.True(t, ok)
	require.False(t, segment.IsPersisted())

	// Validate results
	validateGoodTaggedSeries(t, times.start, indexResults, timesOpts)

	// Validate that no index blocks were read from disk and that no files were written out
	counters := scope.Snapshot().Counters()
	require.Equal(t, int64(0), counters["fs-bootstrapper.persist-index-blocks-read+"].Value())
	require.Equal(t, int64(0), counters["fs-bootstrapper.persist-index-blocks-write+"].Value())
	tester.EnsureNoWrites()
}

func TestBootstrapIndexWithPersistPrefersPersistedIndexBlocks(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	timesOpts := testTimesOptions{
		numBlocks: 2,
	}
	times := newTestBootstrapIndexTimes(timesOpts)

	// Write data files
	writeTSDBGoodTaggedSeriesDataFiles(t, dir, testNs1ID, times.start)

	// Now write index block segment from first two data blocks
	testData := testGoodTaggedSeriesDataBlocks()
	shards := map[uint32]struct{}{testShard: {}}
	writeTSDBPersistedIndexBlock(t, dir, testNsMetadata(t), times.start, shards,
		append(testData[0], testData[1]...))

	opts := newTestOptionsWithPersistManager(t, dir)
	scope := tally.NewTestScope("", nil)
	opts = opts.SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(scope))

	runOpts := testDefaultRunOpts.
		SetPersistConfig(bootstrap.PersistConfig{Enabled: true})

	fsSrc, err := newFileSystemSource(opts)
	require.NoError(t, err)

	src, ok := fsSrc.(*fileSystemSource)
	require.True(t, ok)

	nsMD := testNsMetadata(t)
	tester := bootstrap.BuildNamespacesTesterWithFilesystemOptions(t, runOpts,
		times.shardTimeRanges, opts.FilesystemOptions(), nsMD)
	defer tester.Finish()

	tester.TestReadWith(src)
	indexResults := tester.ResultForNamespace(nsMD.ID()).IndexResult.IndexResults()

	// Check that the segment is not a mutable segment for this block
	// and came from disk
	blockByVolumeType, ok := indexResults[xtime.ToUnixNano(times.start)]
	require.True(t, ok)
	block, ok := blockByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
	require.True(t, ok)
	require.Equal(t, 1, len(block.Segments()))
	segment := block.Segments()[0]
	require.True(t, ok)
	require.True(t, segment.IsPersisted())

	// Check that the second segment is mutable
	blockByVolumeType, ok = indexResults[xtime.ToUnixNano(times.start.Add(testIndexBlockSize))]
	require.True(t, ok)
	block, ok = blockByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
	require.True(t, ok)
	require.Equal(t, 1, len(block.Segments()))
	segment = block.Segments()[0]
	require.True(t, ok)
	require.False(t, segment.IsPersisted())

	// Validate results
	validateGoodTaggedSeries(t, times.start, indexResults, timesOpts)

	// Validate that read the block and no blocks were written
	// (ensure persist config didn't write it back out again)
	counters := scope.Snapshot().Counters()
	require.Equal(t, int64(1), counters["fs-bootstrapper.persist-index-blocks-read+"].Value())
	require.Equal(t, int64(0), counters["fs-bootstrapper.persist-index-blocks-write+"].Value())
	tester.EnsureNoWrites()
}

// TODO: Make this test actually exercise the case at the retention edge,
// right now it only builds a partial segment for the second of three index
// blocks it is trying to build.
func TestBootstrapIndexWithPersistForIndexBlockAtRetentionEdge(t *testing.T) {
	tests := []testOptions{
		{
			name:                         "now",
			now:                          time.Now(),
			expectedInfoFiles:            2,
			expectedSegmentsFirstBlock:   1,
			expectedSegmentsSecondBlock:  1,
			expectedOutOfRetentionBlocks: 0,
		},
		{
			name:                         "now + 4h",
			now:                          time.Now().Add(time.Hour * 4),
			expectedInfoFiles:            1,
			expectedSegmentsFirstBlock:   0,
			expectedSegmentsSecondBlock:  1,
			expectedOutOfRetentionBlocks: 1,
		},
		{
			name:                         "now + 8h",
			now:                          time.Now().Add(time.Hour * 8),
			expectedInfoFiles:            0,
			expectedSegmentsFirstBlock:   0,
			expectedSegmentsSecondBlock:  0,
			expectedOutOfRetentionBlocks: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testBootstrapIndexWithPersistForIndexBlockAtRetentionEdge(t, test)
		})
	}
}

type testOptions struct {
	name                         string
	now                          time.Time
	expectedInfoFiles            int
	expectedSegmentsFirstBlock   int
	expectedSegmentsSecondBlock  int
	expectedOutOfRetentionBlocks int64
}

func testBootstrapIndexWithPersistForIndexBlockAtRetentionEdge(t *testing.T, test testOptions) {
	dir := createTempDir(t)
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()

	timesOpts := testTimesOptions{
		numBlocks: 3,
	}
	times := newTestBootstrapIndexTimes(timesOpts)
	firstIndexBlockStart := times.start.Truncate(testIndexBlockSize)

	writeTSDBGoodTaggedSeriesDataFiles(t, dir, testNs1ID, times.start)

	opts := newTestOptionsWithPersistManager(t, dir)

	scope := tally.NewTestScope("", nil)
	opts = opts.
		SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(scope))

	at := test.now
	resultOpts := opts.ResultOptions()
	clockOpts := resultOpts.ClockOptions().
		SetNowFn(func() time.Time {
			return at
		})
	opts = opts.SetResultOptions(resultOpts.SetClockOptions(clockOpts))

	runOpts := testDefaultRunOpts.
		SetPersistConfig(bootstrap.PersistConfig{Enabled: true})

	fsSrc, err := newFileSystemSource(opts)
	require.NoError(t, err)

	src, ok := fsSrc.(*fileSystemSource)
	require.True(t, ok)

	retentionPeriod := testBlockSize
	for {
		// Make sure that retention is set to end half way through the first block
		flushStart := retention.FlushTimeStartForRetentionPeriod(retentionPeriod, testBlockSize, time.Now())
		if flushStart.Before(firstIndexBlockStart.Add(testIndexBlockSize)) {
			break
		}
		retentionPeriod += testBlockSize
	}
	ropts := testRetentionOptions.
		SetBlockSize(testBlockSize).
		SetRetentionPeriod(retentionPeriod)
	ns, err := namespace.NewMetadata(testNs1ID, testNamespaceOptions.
		SetRetentionOptions(ropts).
		SetIndexOptions(testNamespaceIndexOptions.
			SetEnabled(true).
			SetBlockSize(testIndexBlockSize)))
	require.NoError(t, err)

	// NB(bodu): Simulate requesting bootstrapping of two whole index blocks instead of 3 data blocks (1.5 index blocks).
	times.shardTimeRanges = result.NewShardTimeRanges().Set(
		testShard,
		xtime.NewRanges(xtime.Range{
			Start: firstIndexBlockStart,
			End:   times.end,
		}),
	)
	tester := bootstrap.BuildNamespacesTesterWithFilesystemOptions(t, runOpts,
		times.shardTimeRanges, opts.FilesystemOptions(), ns)
	defer tester.Finish()

	tester.TestReadWith(src)
	indexResults := tester.ResultForNamespace(ns.ID()).IndexResult.IndexResults()

	// Check that single persisted segment got written out
	infoFiles := fs.ReadIndexInfoFiles(fs.ReadIndexInfoFilesOptions{
		filePathPrefix:   src.fsopts.FilePathPrefix(),
		namespace:        testNs1ID,
		readerBufferSize: src.fsopts.InfoReaderBufferSize(),
	})
	require.Equal(t, test.expectedInfoFiles, len(infoFiles), "index info files")

	for _, infoFile := range infoFiles {
		require.NoError(t, infoFile.Err.Error())

		if infoFile.Info.BlockStart == firstIndexBlockStart.UnixNano() {
			expectedStart := times.end.Add(-2 * testIndexBlockSize).UnixNano()
			require.Equal(t, expectedStart, infoFile.Info.BlockStart,
				fmt.Sprintf("expected=%v, actual=%v",
					time.Unix(0, expectedStart).String(),
					time.Unix(0, infoFile.Info.BlockStart)))
		} else {
			expectedStart := times.end.Add(-1 * testIndexBlockSize).UnixNano()
			require.Equal(t, expectedStart, infoFile.Info.BlockStart,
				fmt.Sprintf("expected=%v, actual=%v",
					time.Unix(0, expectedStart).String(),
					time.Unix(0, infoFile.Info.BlockStart)))
		}

		require.Equal(t, testIndexBlockSize, time.Duration(infoFile.Info.BlockSize))
		require.Equal(t, persist.FileSetFlushType, persist.FileSetType(infoFile.Info.FileType))
		require.Equal(t, 1, len(infoFile.Info.Segments))
		require.Equal(t, 1, len(infoFile.Info.Shards))
		require.Equal(t, testShard, infoFile.Info.Shards[0])
	}

	// Check that the segment is not a mutable segment
	blockByVolumeType, ok := indexResults[xtime.ToUnixNano(firstIndexBlockStart)]
	require.True(t, ok)
	block, ok := blockByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
	require.True(t, ok)
	require.Equal(t, test.expectedSegmentsFirstBlock, len(block.Segments()), "first block segment count")
	if len(block.Segments()) > 0 {
		segment := block.Segments()[0]
		require.True(t, segment.IsPersisted(), "should be persisted")
	}

	// Check that the second is not a mutable segment
	blockByVolumeType, ok = indexResults[xtime.ToUnixNano(firstIndexBlockStart.Add(testIndexBlockSize))]
	require.True(t, ok)
	block, ok = blockByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
	require.True(t, ok)
	require.Equal(t, test.expectedSegmentsSecondBlock, len(block.Segments()), "second block segment count")
	if len(block.Segments()) > 0 {
		segment := block.Segments()[0]
		require.True(t, segment.IsPersisted(), "should be persisted")
	}

	// Validate results
	if test.expectedSegmentsFirstBlock > 0 {
		validateGoodTaggedSeries(t, firstIndexBlockStart, indexResults, timesOpts)
	}

	// Validate that wrote the block out (and no index blocks
	// were read as existing index blocks on disk)
	counters := scope.Snapshot().Counters()
	require.Equal(t, int64(0),
		counters["fs-bootstrapper.persist-index-blocks-read+"].Value(), "index blocks read")
	require.Equal(t, int64(test.expectedInfoFiles),
		counters["fs-bootstrapper.persist-index-blocks-write+"].Value(), "index blocks")
	require.Equal(t, test.expectedOutOfRetentionBlocks,
		counters["fs-bootstrapper.persist-index-blocks-out-of-retention+"].Value(), "out of retention blocks")
	tester.EnsureNoWrites()
}
