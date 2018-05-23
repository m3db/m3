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

	xtime "github.com/m3db/m3x/time"
	"github.com/stretchr/testify/require"
)

func TestBootstrapIndex(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	fooSeries := struct {
		id   string
		tags map[string]string
	}{
		"foo",
		map[string]string{"aaa": "bbb", "ccc": "ddd"},
	}
	dataBlocks := [][]testSeries{
		[]testSeries{
			{fooSeries.id, fooSeries.tags, []byte{0x1}},
			{"bar", map[string]string{"eee": "fff", "ggg": "hhh"}, []byte{0x1}},
			{"baz", map[string]string{"iii": "jjj", "kkk": "lll"}, []byte{0x1}},
		},
		[]testSeries{
			{fooSeries.id, fooSeries.tags, []byte{0x1}},
			{"qux", map[string]string{"mmm": "nnn", "ooo": "ppp"}, []byte{0x1}},
			{"qaz", map[string]string{"qqq": "rrr", "sss": "ttt"}, []byte{0x1}},
		},
		[]testSeries{
			{fooSeries.id, fooSeries.tags, []byte{0x1}},
			{"qan", map[string]string{"uuu": "vvv", "www": "xxx"}, []byte{0x1}},
			{"qam", map[string]string{"yyy": "zzz", "000": "111"}, []byte{0x1}},
		},
	}

	at := time.Now()
	start := at.Truncate(testBlockSize)
	indexStart := start.Truncate(testIndexBlockSize)
	for !start.Equal(indexStart) {
		// make sure data blocks overlap, test block size is 2h
		// and test index block size is 4h
		start = start.Add(testBlockSize)
		indexStart = start.Truncate(testIndexBlockSize)
	}

	writeTSDBFiles(t, dir, testNs1ID, testShard,
		start, dataBlocks[0])
	writeTSDBFiles(t, dir, testNs1ID, testShard,
		start.Add(testBlockSize), dataBlocks[1])
	writeTSDBFiles(t, dir, testNs1ID, testShard,
		start.Add(2*testBlockSize), dataBlocks[2])

	shardTimeRanges := map[uint32]xtime.Ranges{
		testShard: xtime.NewRanges(xtime.Range{
			Start: indexStart,
			End:   indexStart.Add(2 * testIndexBlockSize),
		}),
	}

	src := newFileSystemSource(newTestOptions(dir))
	res, err := src.ReadIndex(testNsMetadata(t), shardTimeRanges,
		testDefaultRunOpts)
	require.NoError(t, err)

	indexResults := res.IndexResults()
	require.Equal(t, 2, len(indexResults))

	for _, expected := range []struct {
		indexBlockStart time.Time
		series          map[string]testSeries
	}{
		{
			indexBlockStart: indexStart,
			series: map[string]testSeries{
				dataBlocks[0][0].id: dataBlocks[0][0],
				dataBlocks[0][1].id: dataBlocks[0][1],
				dataBlocks[0][2].id: dataBlocks[0][2],
				dataBlocks[1][1].id: dataBlocks[1][1],
				dataBlocks[1][2].id: dataBlocks[1][2],
			},
		},
		{
			indexBlockStart: indexStart.Add(testIndexBlockSize),
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
