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
	"os"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/series"
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

type testOptions struct {
	name                string
	indexBlockStart     xtime.UnixNano
	expectedIndexBlocks int
	retentionPeriod     time.Duration
}

func TestBootstrapIndex(t *testing.T) {
	tests := []testOptions{
		{
			name:                "now",
			indexBlockStart:     xtime.Now(),
			expectedIndexBlocks: 12,
			retentionPeriod:     48 * time.Hour,
		},
		{
			name:                "now - 8h (out of retention)",
			indexBlockStart:     xtime.Now().Add(-8 * time.Hour),
			expectedIndexBlocks: 0,
			retentionPeriod:     4 * time.Hour,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testBootstrapIndex(t, test)
		})
	}
}

//nolint
func testBootstrapIndex(t *testing.T, test testOptions) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := newTestDefaultOpts(t, ctrl)
	opts = opts.SetResultOptions(result.NewOptions().
		SetSeriesCachePolicy(series.CacheLRU))
	pm, err := fs.NewPersistManager(opts.FilesystemOptions())
	require.NoError(t, err)
	opts = opts.SetPersistManager(pm)

	blockSize := 2 * time.Hour
	indexBlockSize := 2 * blockSize

	ropts := retention.NewOptions().
		SetBlockSize(blockSize).
		SetRetentionPeriod(test.retentionPeriod)

	nsMetadata := testNamespaceMetadata(t, func(opts namespace.Options) namespace.Options {
		return opts.
			SetRetentionOptions(ropts).
			SetIndexOptions(opts.IndexOptions().
				SetEnabled(true).
				SetBlockSize(indexBlockSize))
	})

	at := test.indexBlockStart
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
		blockStart xtime.UnixNano
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

	dir := createTempDir(t)
	defer os.RemoveAll(dir)
	opts = opts.SetFilesystemOptions(newTestFsOptions(dir))

	end := start.Add(ropts.RetentionPeriod())

	shardTimeRanges := result.NewShardTimeRanges().Set(
		0,
		xtime.NewRanges(xtime.Range{
			Start: start,
			End:   end,
		}),
	)

	// data block start at the edge of retention so return those first.
	var dataBlocksIdx int
	mockAdminSession := client.NewMockAdminSession(ctrl)
	mockAdminSession.EXPECT().
		FetchBootstrapBlocksFromPeers(gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(
			_ namespace.Metadata,
			_ uint32,
			blockStart xtime.UnixNano,
			blockEnd xtime.UnixNano,
			_ result.Options,
		) (result.ShardResult, error) {
			goodID := ident.StringID("foo")
			goodResult := result.NewShardResult(opts.ResultOptions())
			for ; blockStart.Before(blockEnd); blockStart = blockStart.Add(blockSize) {
				if dataBlocksIdx < len(dataBlocks) {
					dataBlock := dataBlocks[dataBlocksIdx]
					for _, s := range dataBlock.series {
						head := checked.NewBytes(s.data, nil)
						head.IncRef()
						block := block.NewDatabaseBlock(blockStart, ropts.BlockSize(),
							ts.Segment{Head: head}, testBlockOpts, namespace.Context{})
						goodResult.AddBlock(s.ID(), s.Tags(), block)
					}
					dataBlocksIdx++
					continue
				}

				head := checked.NewBytes([]byte{0x1}, nil)
				head.IncRef()
				fooBlock := block.NewDatabaseBlock(blockStart, ropts.BlockSize(),
					ts.Segment{Head: head}, testBlockOpts, namespace.Context{})
				goodResult.AddBlock(goodID, ident.NewTags(
					ident.StringTag("aaa", "bbb"),
					ident.StringTag("ccc", "ddd"),
				), fooBlock)
			}
			return goodResult, nil
		}).AnyTimes()

	mockAdminClient := client.NewMockAdminClient(ctrl)
	mockAdminClient.EXPECT().DefaultAdminSession().Return(mockAdminSession, nil).AnyTimes()
	opts = opts.SetAdminClient(mockAdminClient)
	src, err := newPeersSource(opts)
	require.NoError(t, err)
	tester := bootstrap.BuildNamespacesTesterWithFilesystemOptions(t,
		testRunOptsWithPersist, shardTimeRanges,
		opts.FilesystemOptions(), nsMetadata)
	defer tester.Finish()
	tester.TestReadWith(src)

	tester.TestUnfulfilledForNamespaceIsEmpty(nsMetadata)
	results := tester.ResultForNamespace(nsMetadata.ID())
	indexResults := results.IndexResult.IndexResults()
	numIndexBlocks := 0
	for _, indexBlockByVolumeType := range indexResults {
		indexBlock, ok := indexBlockByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
		require.True(t, ok)
		if len(indexBlock.Segments()) != 0 {
			numIndexBlocks++
		}
	}
	require.Equal(t, test.expectedIndexBlocks, numIndexBlocks)

	if numIndexBlocks > 0 {
		for _, expected := range []*struct {
			indexBlockStart xtime.UnixNano
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
			expectedAt := expected.indexBlockStart
			indexBlockByVolumeType, ok := indexResults[expectedAt]
			require.True(t, ok)
			indexBlock, ok := indexBlockByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
			require.True(t, ok)
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

		t1 := indexStart
		t2 := indexStart.Add(indexBlockSize)
		t3 := t2.Add(indexBlockSize)

		indexBlockByVolumeType, ok := indexResults[t1]
		require.True(t, ok)
		blk1, ok := indexBlockByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
		require.True(t, ok)
		assertShardRangesEqual(t, result.NewShardTimeRangesFromRange(t1, t2, 0), blk1.Fulfilled())

		indexBlockByVolumeType, ok = indexResults[t2]
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
	}
	tester.EnsureNoWrites()
}

func assertShardRangesEqual(t *testing.T, a, b result.ShardTimeRanges) {
	ac := a.Copy()
	ac.Subtract(b)
	require.True(t, ac.IsEmpty())
	bc := b.Copy()
	bc.Subtract(a)
	require.True(t, bc.IsEmpty())
}
