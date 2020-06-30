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

package peers

import (
	"io/ioutil"
	"log"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/ts"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
	testShard            = uint32(0)
	testFileMode         = os.FileMode(0666)
	testDirMode          = os.ModeDir | os.FileMode(0755)
	testWriterBufferSize = 10
)

func newTestFsOptions(filePathPrefix string) fs.Options {
	return fs.NewOptions().
		SetFilePathPrefix(filePathPrefix).
		SetWriterBufferSize(testWriterBufferSize).
		SetNewFileMode(testFileMode).
		SetNewDirectoryMode(testDirMode)
}

func createTempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "foo")
	require.NoError(t, err)
	return dir
}

type testSeriesMetadata struct {
	id   string
	tags map[string]string
	data []byte
}

func (s testSeriesMetadata) ID() ident.ID {
	return ident.StringID(s.id)
}

func (s testSeriesMetadata) Tags() ident.Tags {
	if s.tags == nil {
		return ident.Tags{}
	}

	// Return in sorted order for deterministic order
	var keys []string
	for key := range s.tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var tags ident.Tags
	for _, key := range keys {
		tags.Append(ident.StringTag(key, s.tags[key]))
	}

	return tags
}

func writeTSDBFiles(
	t *testing.T,
	dir string,
	namespace ident.ID,
	shard uint32,
	start time.Time,
	series []testSeriesMetadata,
	blockSize time.Duration,
	fsOpts fs.Options,
) {
	w, err := fs.NewWriter(fsOpts)
	require.NoError(t, err)
	writerOpts := fs.DataWriterOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:  namespace,
			Shard:      shard,
			BlockStart: start,
		},
		BlockSize: blockSize,
	}
	require.NoError(t, w.Open(writerOpts))

	for _, v := range series {
		bytes := checked.NewBytes(v.data, nil)
		bytes.IncRef()
		metadata := persist.NewMetadataFromIDAndTags(ident.StringID(v.id),
			sortedTagsFromTagsMap(v.tags),
			persist.MetadataOptions{})
		require.NoError(t, w.Write(metadata, bytes,
			digest.Checksum(bytes.Bytes())))
		bytes.DecRef()
	}

	require.NoError(t, w.Close())
}

func sortedTagsFromTagsMap(tags map[string]string) ident.Tags {
	var (
		seriesTags ident.Tags
		tagNames   []string
	)
	for name := range tags {
		tagNames = append(tagNames, name)
	}
	sort.Strings(tagNames)
	for _, name := range tagNames {
		seriesTags.Append(ident.StringTag(name, tags[name]))
	}
	return seriesTags
}

func TestBootstrapIndex(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newTestDefaultOpts(t, ctrl)
	pm, err := fs.NewPersistManager(opts.FilesystemOptions())
	require.NoError(t, err)
	opts = opts.SetPersistManager(pm)

	blockSize := 2 * time.Hour
	indexBlockSize := 2 * blockSize

	ropts := retention.NewOptions().
		SetBlockSize(blockSize).
		SetRetentionPeriod(24 * blockSize)

	nsMetadata := testNamespaceMetadata(t, func(opts namespace.Options) namespace.Options {
		return opts.
			SetRetentionOptions(ropts).
			SetIndexOptions(opts.IndexOptions().
				SetEnabled(true).
				SetBlockSize(indexBlockSize))
	})

	at := time.Now()
	start := at.Add(-ropts.RetentionPeriod()).Truncate(blockSize)
	indexStart := start.Truncate(indexBlockSize)
	for !start.Equal(indexStart) {
		// make sure data blocks overlap, test block size is 2h
		// and test index block size is 4h
		start = start.Add(blockSize)
		indexStart = start.Truncate(indexBlockSize)
	}

	fooSeries := struct {
		id   string
		tags map[string]string
	}{
		"foo",
		map[string]string{"aaa": "bbb", "ccc": "ddd"},
	}
	dataBlocks := []struct {
		blockStart time.Time
		series     []testSeriesMetadata
	}{
		{
			blockStart: start,
			series: []testSeriesMetadata{
				{fooSeries.id, fooSeries.tags, []byte{0x1}},
				{"bar", map[string]string{"eee": "fff", "ggg": "hhh"}, []byte{0x1}},
				{"baz", map[string]string{"iii": "jjj", "kkk": "lll"}, []byte{0x1}},
			},
		},
		{
			blockStart: start.Add(blockSize),
			series: []testSeriesMetadata{
				{fooSeries.id, fooSeries.tags, []byte{0x2}},
				{"qux", map[string]string{"mmm": "nnn", "ooo": "ppp"}, []byte{0x2}},
				{"qaz", map[string]string{"qqq": "rrr", "sss": "ttt"}, []byte{0x2}},
			},
		},
		{
			blockStart: start.Add(2 * blockSize),
			series: []testSeriesMetadata{
				{fooSeries.id, fooSeries.tags, []byte{0x3}},
				{"qan", map[string]string{"uuu": "vvv", "www": "xxx"}, []byte{0x3}},
				{"qam", map[string]string{"yyy": "zzz", "000": "111"}, []byte{0x3}},
			},
		},
	}
	// NB(bodu): Write time series to disk.
	dir := createTempDir(t)
	defer os.RemoveAll(dir)
	opts = opts.SetFilesystemOptions(newTestFsOptions(dir))
	for _, block := range dataBlocks {
		writeTSDBFiles(
			t,
			dir,
			nsMetadata.ID(),
			testShard,
			block.blockStart,
			block.series,
			blockSize,
			opts.FilesystemOptions(),
		)
	}

	end := start.Add(ropts.RetentionPeriod())

	shardTimeRanges := result.NewShardTimeRanges().Set(
		0,
		xtime.NewRanges(xtime.Range{
			Start: start,
			End:   end,
		}),
	)

	mockAdminSession := client.NewMockAdminSession(ctrl)
	mockAdminSession.EXPECT().
		FetchBootstrapBlocksFromPeers(gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ namespace.Metadata,
			_ uint32,
			blockStart time.Time,
			blockEnd time.Time,
			_ result.Options,
		) (result.ShardResult, error) {
			goodID := ident.StringID("foo")
			goodResult := result.NewShardResult(0, opts.ResultOptions())
			for ; blockStart.Before(blockEnd); blockStart = blockStart.Add(blockSize) {
				fooBlock := block.NewDatabaseBlock(blockStart, ropts.BlockSize(),
					ts.Segment{}, testBlockOpts, namespace.Context{})
				goodResult.AddBlock(goodID, ident.NewTags(ident.StringTag("foo", "oof")), fooBlock)
			}
			return goodResult, nil
		}).AnyTimes()

	mockAdminClient := client.NewMockAdminClient(ctrl)
	mockAdminClient.EXPECT().DefaultAdminSession().Return(mockAdminSession, nil).AnyTimes()
	opts = opts.SetAdminClient(mockAdminClient)

	src, err := newPeersSource(opts)
	require.NoError(t, err)
	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, shardTimeRanges, nsMetadata)
	defer tester.Finish()
	tester.TestReadWith(src)

	tester.TestUnfulfilledForNamespaceIsEmpty(nsMetadata)
	results := tester.ResultForNamespace(nsMetadata.ID())
	indexResults := results.IndexResult.IndexResults()
	numBlocksWithData := 0
	for _, indexBlockByVolumeType := range indexResults {
		indexBlock, ok := indexBlockByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
		require.True(t, ok)
		if len(indexBlock.Segments()) != 0 {
			log.Printf("result block start: %s", indexBlockByVolumeType.BlockStart())
			numBlocksWithData++
		}
	}
	require.Equal(t, 2, numBlocksWithData)

	for _, expected := range []struct {
		indexBlockStart time.Time
		series          map[string]testSeriesMetadata
	}{
		{
			indexBlockStart: indexStart,
			series: map[string]testSeriesMetadata{
				dataBlocks[0].series[0].id: dataBlocks[0].series[0],
				dataBlocks[0].series[1].id: dataBlocks[0].series[1],
				dataBlocks[0].series[2].id: dataBlocks[0].series[2],
				dataBlocks[1].series[1].id: dataBlocks[1].series[1],
				dataBlocks[1].series[2].id: dataBlocks[1].series[2],
			},
		},
		{
			indexBlockStart: indexStart.Add(indexBlockSize),
			series: map[string]testSeriesMetadata{
				dataBlocks[2].series[0].id: dataBlocks[2].series[0],
				dataBlocks[2].series[1].id: dataBlocks[2].series[1],
				dataBlocks[2].series[2].id: dataBlocks[2].series[2],
			},
		},
	} {
		expectedAt := xtime.ToUnixNano(expected.indexBlockStart)
		indexBlockByVolumeType, ok := indexResults[expectedAt]
		require.True(t, ok)
		indexBlock, ok := indexBlockByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
		require.True(t, ok)
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

	t1 := indexStart
	t2 := indexStart.Add(indexBlockSize)
	t3 := t2.Add(indexBlockSize)

	indexBlockByVolumeType, ok := indexResults[xtime.ToUnixNano(t1)]
	require.True(t, ok)
	blk1, ok := indexBlockByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
	require.True(t, ok)
	assertShardRangesEqual(t, result.NewShardTimeRangesFromRange(t1, t2, 0), blk1.Fulfilled())

	indexBlockByVolumeType, ok = indexResults[xtime.ToUnixNano(t2)]
	require.True(t, ok)
	blk2, ok := indexBlockByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
	require.True(t, ok)
	assertShardRangesEqual(t, result.NewShardTimeRangesFromRange(t2, t3, 0), blk2.Fulfilled())

	for _, indexBlockByVolumeType := range indexResults {
		if indexBlockByVolumeType.BlockStart().Equal(t1) || indexBlockByVolumeType.BlockStart().Equal(t2) {
			continue // already checked above
		}
		// rest should all be marked fulfilled despite no data, because we didn't see
		// any errors in the response.
		start := indexBlockByVolumeType.BlockStart()
		end := start.Add(indexBlockSize)
		blk, ok := indexBlockByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
		require.True(t, ok)
		assertShardRangesEqual(t, result.NewShardTimeRangesFromRange(start, end, 0), blk.Fulfilled())
	}

	tester.EnsureNoWrites()
}

// TODO(bodu): Add some error case testing.
//func TestBootstrapIndexErr(t *testing.T) {
//	ctrl := gomock.NewController(t)
//	defer ctrl.Finish()
//
//	opts := newTestDefaultOpts(t, ctrl)
//	pm, err := fs.NewPersistManager(opts.FilesystemOptions())
//	require.NoError(t, err)
//	opts = opts.SetPersistManager(pm)
//
//	blockSize := 2 * time.Hour
//	indexBlockSize := 2 * blockSize
//
//	ropts := retention.NewOptions().
//		SetBlockSize(blockSize).
//		SetRetentionPeriod(24 * blockSize)
//
//	nsMetadata := testNamespaceMetadata(t, func(opts namespace.Options) namespace.Options {
//		return opts.
//			SetRetentionOptions(ropts).
//			SetIndexOptions(opts.IndexOptions().
//				SetEnabled(true).
//				SetBlockSize(indexBlockSize))
//	})
//
//	at := time.Now()
//	start := at.Add(-ropts.RetentionPeriod()).Truncate(blockSize)
//	indexStart := start.Truncate(indexBlockSize)
//	for !start.Equal(indexStart) {
//		// make sure data blocks overlap, test block size is 2h
//		// and test index block size is 4h
//		start = start.Add(blockSize)
//		indexStart = start.Truncate(indexBlockSize)
//	}
//
//	fooSeries := struct {
//		id   string
//		tags map[string]string
//	}{
//		"foo",
//		map[string]string{"aaa": "bbb", "ccc": "ddd"},
//	}
//	dataBlocks := []struct {
//		blockStart time.Time
//		series     []testSeriesMetadata
//	}{
//		{
//			blockStart: start,
//			series: []testSeriesMetadata{
//				{fooSeries.id, fooSeries.tags, []byte{0x1}},
//			},
//		},
//		{
//			blockStart: start.Add(blockSize),
//			series: []testSeriesMetadata{
//				{fooSeries.id, fooSeries.tags, []byte{0x2}},
//			},
//		},
//	}
//
//	end := start.Add(ropts.RetentionPeriod())
//
//	shardTimeRanges := map[uint32]xtime.Ranges{
//		0: xtime.NewRanges(xtime.Range{
//			Start: start,
//			End:   end,
//		}),
//	}
//
//	mockAdminSession := client.NewMockAdminSession(ctrl)
//	mockAdminSession.EXPECT().
//		FetchBootstrapBlocksFromPeers(gomock.Any(),
//			gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
//		func(
//			_ namespace.Metadata,
//			_ uint32,
//			blockStart time.Time,
//			blockEnd time.Time,
//			_ result.Options,
//		) (result.ShardResult, error) {
//			goodID := ident.StringID("foo")
//			goodResult := result.NewShardResult(0, opts.ResultOptions())
//			for ; blockStart.Before(blockEnd); blockStart = blockStart.Add(blockSize) {
//				fooBlock := block.NewDatabaseBlock(blockStart, ropts.BlockSize(),
//					ts.Segment{}, testBlockOpts, namespace.Context{})
//				goodResult.AddBlock(goodID, ident.NewTags(ident.StringTag("foo", "oof")), fooBlock)
//			}
//			return goodResult, nil
//		}).AnyTimes()
//
//	mockAdminClient := client.NewMockAdminClient(ctrl)
//	mockAdminClient.EXPECT().DefaultAdminSession().Return(mockAdminSession, nil).AnyTimes()
//	opts = opts.SetAdminClient(mockAdminClient)
//
//	src, err := newPeersSource(opts)
//	require.NoError(t, err)
//
//	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, shardTimeRanges, nsMetadata)
//	defer tester.Finish()
//	tester.TestReadWith(src)
//
//	tester.TestUnfulfilledForNamespaceIsEmpty(nsMetadata)
//	results := tester.ResultForNamespace(nsMetadata.ID())
//	indexResults := results.IndexResult.IndexResults()
//	numBlocksWithData := 0
//	for _, b := range indexResults {
//		if len(b.Segments()) != 0 {
//			numBlocksWithData++
//		}
//	}
//	require.Equal(t, 1, numBlocksWithData)
//
//	t1 := indexStart
//
//	blk1, ok := indexResults[xtime.ToUnixNano(t1)]
//	require.True(t, ok)
//	require.True(t, blk1.Fulfilled().IsEmpty())
//
//	for _, blk := range indexResults {
//		if blk.BlockStart().Equal(t1) {
//			continue // already checked above
//		}
//		// rest should all be marked fulfilled despite no data, because we didn't see
//		// any errors in the response.
//		start := blk.BlockStart()
//		end := start.Add(indexBlockSize)
//		assertShardRangesEqual(t, result.NewShardTimeRanges(start, end, 0), blk.Fulfilled())
//	}
//
//	tester.EnsureNoWrites()
//}

func assertShardRangesEqual(t *testing.T, a, b result.ShardTimeRanges) {
	ac := a.Copy()
	ac.Subtract(b)
	require.True(t, ac.IsEmpty())
	bc := b.Copy()
	bc.Subtract(a)
	require.True(t, bc.IsEmpty())
}
