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
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

type testSeriesMetadata struct {
	id         string
	tags       map[string]string
	blockStart time.Time
}

func (s testSeriesMetadata) ID() ident.ID {
	return ident.StringID(s.id)
}

func (s testSeriesMetadata) Tags() ident.Tags {
	if s.tags == nil {
		return nil
	}

	// Return in sorted order for deterministic order
	var keys []string
	for key := range s.tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var tags ident.Tags
	for _, key := range keys {
		tags = append(tags, ident.StringTag(key, s.tags[key]))
	}

	return tags
}

func TestBootstrapIndex(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDefaultOpts.
		SetFetchBlocksMetadataEndpointVersion(client.FetchBlocksMetadataEndpointV2)

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
	dataBlocks := [][]testSeriesMetadata{
		[]testSeriesMetadata{
			{fooSeries.id, fooSeries.tags, start},
			{"bar", map[string]string{"eee": "fff", "ggg": "hhh"}, start},
			{"baz", map[string]string{"iii": "jjj", "kkk": "lll"}, start},
		},
		[]testSeriesMetadata{
			{fooSeries.id, fooSeries.tags, start.Add(blockSize)},
			{"qux", map[string]string{"mmm": "nnn", "ooo": "ppp"}, start.Add(blockSize)},
			{"qaz", map[string]string{"qqq": "rrr", "sss": "ttt"}, start.Add(blockSize)},
		},
		[]testSeriesMetadata{
			{fooSeries.id, fooSeries.tags, start.Add(2 * blockSize)},
			{"qan", map[string]string{"uuu": "vvv", "www": "xxx"}, start.Add(2 * blockSize)},
			{"qam", map[string]string{"yyy": "zzz", "000": "111"}, start.Add(2 * blockSize)},
		},
	}

	end := start.Add(ropts.RetentionPeriod())

	shardTimeRanges := map[uint32]xtime.Ranges{
		0: xtime.Ranges{}.AddRange(xtime.Range{
			Start: start,
			End:   end,
		}),
	}

	mockIter := client.NewMockPeerBlockMetadataIter(ctrl)
	mockIterCalls := []*gomock.Call{}
	for _, blocks := range dataBlocks {
		for _, elem := range blocks {
			call := mockIter.EXPECT().Next().Return(true)
			mockIterCalls = append(mockIterCalls, call)

			metadata := block.NewMetadata(elem.ID(), elem.Tags(),
				elem.blockStart, 1, nil, time.Time{})
			call = mockIter.EXPECT().Current().Return(nil, metadata)
			mockIterCalls = append(mockIterCalls, call)
		}
	}
	call := mockIter.EXPECT().Next().Return(false)
	mockIterCalls = append(mockIterCalls, call)
	call = mockIter.EXPECT().Err().Return(nil)
	mockIterCalls = append(mockIterCalls, call)
	gomock.InOrder(mockIterCalls...)

	mockAdminSession := client.NewMockAdminSession(ctrl)
	mockAdminSession.EXPECT().
		FetchBootstrapBlocksMetadataFromPeers(ident.NewIDMatcher(nsMetadata.ID().String()),
			uint32(0), start, end, gomock.Any(), opts.FetchBlocksMetadataEndpointVersion()).
		Return(mockIter, nil)

	mockAdminClient := client.NewMockAdminClient(ctrl)
	mockAdminClient.EXPECT().DefaultAdminSession().Return(mockAdminSession, nil)

	opts = opts.SetAdminClient(mockAdminClient)

	src := newPeersSource(opts)
	res, err := src.ReadIndex(nsMetadata, shardTimeRanges,
		testDefaultRunOpts)
	require.NoError(t, err)

	indexResults := res.IndexResults()
	require.Equal(t, 2, len(indexResults))

	for _, expected := range []struct {
		indexBlockStart time.Time
		series          map[string]testSeriesMetadata
	}{
		{
			indexBlockStart: indexStart,
			series: map[string]testSeriesMetadata{
				dataBlocks[0][0].id: dataBlocks[0][0],
				dataBlocks[0][1].id: dataBlocks[0][1],
				dataBlocks[0][2].id: dataBlocks[0][2],
				dataBlocks[1][1].id: dataBlocks[1][1],
				dataBlocks[1][2].id: dataBlocks[1][2],
			},
		},
		{
			indexBlockStart: indexStart.Add(indexBlockSize),
			series: map[string]testSeriesMetadata{
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
