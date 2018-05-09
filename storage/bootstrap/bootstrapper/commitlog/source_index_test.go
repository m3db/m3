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

package commitlog

import (
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

type expectedIndexBlock struct {
	series []seriesIDAndTag
}

type seriesIDAndTag struct {
	ID   ident.ID
	Tags ident.Tags
}

func TestBootstrapIndex(t *testing.T) {
	var (
		opts             = testOptions()
		src              = newCommitLogSource(opts, fs.Inspection{}).(*commitLogSource)
		dataBlockSize    = 2 * time.Hour
		indexBlockSize   = 4 * time.Hour
		namespaceOptions = namespace.NewOptions().
					SetRetentionOptions(
				namespace.NewOptions().
					RetentionOptions().
					SetBlockSize(dataBlockSize),
			).
			SetIndexOptions(
				namespace.NewOptions().
					IndexOptions().
					SetBlockSize(indexBlockSize),
			)
	)
	md, err := namespace.NewMetadata(testNamespaceID, namespaceOptions)
	require.NoError(t, err)

	now := time.Now()
	start := now.Truncate(dataBlockSize).Add(-2 * dataBlockSize)

	fooTags := ident.Tags{ident.StringTag("city", "ny"), ident.StringTag("conference", "monitoroma")}
	barTags := ident.Tags{ident.StringTag("city", "sf")}
	bazTags := ident.Tags{ident.StringTag("city", "oakland")}
	foo := commitlog.Series{Namespace: testNamespaceID, Shard: 0, ID: ident.StringID("foo"), Tags: fooTags}
	bar := commitlog.Series{Namespace: testNamespaceID, Shard: 1, ID: ident.StringID("bar"), Tags: barTags}
	baz := commitlog.Series{Namespace: testNamespaceID, Shard: 2, ID: ident.StringID("baz"), Tags: bazTags}
	// Make sure we can handle series that don't have tags
	unindexed := commitlog.Series{Namespace: testNamespaceID, Shard: 3, ID: ident.StringID("unindexed"), Tags: nil}

	values := []testValue{
		{foo, start, 1.0, xtime.Second, nil},
		{foo, start, 2.0, xtime.Second, nil},
		{bar, start.Add(dataBlockSize), 1.0, xtime.Second, nil},
		{bar, start.Add(dataBlockSize), 2.0, xtime.Second, nil},
		{baz, start.Add(2 * dataBlockSize), 1.0, xtime.Second, nil},
		{baz, start.Add(2 * dataBlockSize), 2.0, xtime.Second, nil},
		{unindexed, start.Add(2 * dataBlockSize), 1.0, xtime.Second, nil},
	}

	src.newIteratorFn = func(_ commitlog.IteratorOpts) (commitlog.Iterator, error) {
		return newTestCommitLogIterator(values, nil), nil
	}

	ranges := xtime.Ranges{}
	ranges = ranges.AddRange(xtime.Range{
		Start: start.Add(-2 * dataBlockSize),
		End:   start.Add(-dataBlockSize),
	})
	ranges = ranges.AddRange(xtime.Range{
		Start: start.Add(-dataBlockSize),
		End:   start,
	})
	ranges = ranges.AddRange(xtime.Range{
		Start: start,
		End:   start.Add(dataBlockSize),
	})

	targetRanges := result.ShardTimeRanges{0: ranges, 1: ranges, 2: ranges, 3: ranges}
	res, err := src.ReadIndex(md, targetRanges, testDefaultRunOpts)
	require.NoError(t, err)

	// Data blockSize is 2 hours and index blockSize is four hours so the data blocks
	// will span two different index blocks.
	indexResults := res.IndexResults()
	require.Equal(t, 2, len(indexResults))
	require.Equal(t, 0, len(res.Unfulfilled()))

	expectedIndexBlocks := map[xtime.UnixNano]map[string]map[string]string{}
	for _, value := range values {
		indexBlockStart := value.t.Truncate(indexBlockSize)
		expectedSeries, ok := expectedIndexBlocks[xtime.ToUnixNano(indexBlockStart)]
		if !ok {
			expectedSeries = map[string]map[string]string{}
			expectedIndexBlocks[xtime.ToUnixNano(indexBlockStart)] = expectedSeries
		}
		seriesID := string(value.s.ID.Bytes())
		existingTags, ok := expectedSeries[seriesID]
		if !ok {
			existingTags = map[string]string{}
			expectedSeries[seriesID] = existingTags
		}
		for _, tag := range value.s.Tags {
			existingTags[tag.Name.String()] = tag.Value.String()
		}
	}

	for indexBlockStart, expected := range expectedIndexBlocks {
		indexBlock, ok := indexResults[indexBlockStart]
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

				tags := expected[string(curr.ID)]

				matchingTags := map[string]struct{}{}
				for _, tag := range curr.Fields {
					_, ok := matchingTags[string(tag.Name)]
					require.False(t, ok)
					matchingTags[string(tag.Name)] = struct{}{}

					tagValue, ok := tags[string(tag.Name)]
					require.True(t, ok)

					require.Equal(t, tagValue, string(tag.Value))
				}
				require.Equal(t, len(tags), len(matchingTags))
			}
			require.NoError(t, docs.Err())
			require.NoError(t, docs.Close())

			fmt.Println(expected)
			fmt.Println(matches)
			require.Equal(t, len(expected), len(matches))
		}
	}
}
