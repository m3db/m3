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

func TestBootstrapIndex(t *testing.T) {
	testBootstrapIndex(t, false)
}

func TestBootstrapIndexAfterBootstrapData(t *testing.T) {
	testBootstrapIndex(t, true)
}

func testBootstrapIndex(t *testing.T, bootstrapDataFirst bool) {
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
	start := now.Truncate(indexBlockSize)

	fooTags := ident.NewTags(ident.StringTag("city", "ny"), ident.StringTag("conference", "monitoroma"))
	barTags := ident.NewTags(ident.StringTag("city", "sf"))
	bazTags := ident.NewTags(ident.StringTag("city", "oakland"))

	shardZero := uint32(0)
	foo := commitlog.Series{Namespace: testNamespaceID, Shard: shardZero, ID: ident.StringID("foo"), Tags: fooTags}
	bar := commitlog.Series{Namespace: testNamespaceID, Shard: shardZero + 1, ID: ident.StringID("bar"), Tags: barTags}
	baz := commitlog.Series{Namespace: testNamespaceID, Shard: shardZero + 5, ID: ident.StringID("baz"), Tags: bazTags}
	// Make sure we can handle series that don't have tags.
	untagged := commitlog.Series{Namespace: testNamespaceID, Shard: shardZero + 5, ID: ident.StringID("untagged"), Tags: ident.Tags{}}
	// Make sure we skip series that are not within the bootstrap range.
	outOfRange := commitlog.Series{Namespace: testNamespaceID, Shard: shardZero + 3, ID: ident.StringID("outOfRange"), Tags: ident.Tags{}}
	// Make sure we skip and dont panic on writes for shards that are higher than the maximum we're trying to bootstrap.
	shardTooHigh := commitlog.Series{Namespace: testNamespaceID, Shard: shardZero + 100, ID: ident.StringID("shardTooHigh"), Tags: ident.Tags{}}
	// Make sure we skip series for shards that have no requested bootstrap ranges. The shard for this write needs
	// to be less than the highest shard we actually plan to bootstrap.
	noShardBootstrapRange := commitlog.Series{Namespace: testNamespaceID, Shard: shardZero + 4, ID: ident.StringID("noShardBootstrapRange"), Tags: ident.Tags{}}

	seriesNotToExpect := map[string]struct{}{
		outOfRange.ID.String():            struct{}{},
		shardTooHigh.ID.String():          struct{}{},
		noShardBootstrapRange.ID.String(): struct{}{},
	}

	values := []testValue{
		{foo, start, 1.0, xtime.Second, nil},
		{foo, start, 2.0, xtime.Second, nil},
		{bar, start.Add(dataBlockSize), 1.0, xtime.Second, nil},
		{bar, start.Add(dataBlockSize), 2.0, xtime.Second, nil},
		{baz, start.Add(2 * dataBlockSize), 1.0, xtime.Second, nil},
		{baz, start.Add(2 * dataBlockSize), 2.0, xtime.Second, nil},
		{untagged, start.Add(2 * dataBlockSize), 1.0, xtime.Second, nil},
		{outOfRange, start.Add(-blockSize), 1.0, xtime.Second, nil},
		{shardTooHigh, start.Add(dataBlockSize), 1.0, xtime.Second, nil},
		{noShardBootstrapRange, start.Add(dataBlockSize), 1.0, xtime.Second, nil},
	}

	src.newIteratorFn = func(_ commitlog.IteratorOpts) (commitlog.Iterator, error) {
		return newTestCommitLogIterator(values, nil), nil
	}

	ranges := xtime.Ranges{}
	ranges = ranges.AddRange(xtime.Range{
		Start: start,
		End:   start.Add(dataBlockSize),
	})
	ranges = ranges.AddRange(xtime.Range{
		Start: start.Add(dataBlockSize),
		End:   start.Add(2 * dataBlockSize),
	})
	ranges = ranges.AddRange(xtime.Range{
		Start: start.Add(2 * dataBlockSize),
		End:   start.Add(3 * dataBlockSize),
	})

	// Don't include ranges for shard 4 as thats how we're testing the noShardBootstrapRange series.
	targetRanges := result.ShardTimeRanges{
		shardZero: ranges, shardZero + 1: ranges, shardZero + 2: ranges, shardZero + 5: ranges}

	if bootstrapDataFirst {
		// Bootstrap the data to exercise the metadata caching path.

		// Bootstrap some arbitrary time range to make sure that it can handle multiple
		// calls to ReadData() for the same shards, but with different ranges (it needs
		// to merge the ranges accross calls.)
		targetRangesCopy := targetRanges.Copy()
		for key := range targetRangesCopy {
			// The time-range here is "arbitrary", but it does need to be one for which
			// there is data so that we can actually exercise the blockStart merging
			// logic.
			targetRangesCopy[key] = xtime.Ranges{}.AddRange(xtime.Range{
				Start: start,
				End:   start.Add(dataBlockSize),
			})
		}
		_, err = src.ReadData(md, targetRangesCopy, testDefaultRunOpts)
		require.NoError(t, err)

		// Bootstrap the actual time ranges to actually cache the metadata.
		_, err = src.ReadData(md, result.ShardTimeRanges{shardZero: ranges}, testDefaultRunOpts)
		require.NoError(t, err)
	}

	res, err := src.ReadIndex(md, targetRanges, testDefaultRunOpts)
	require.NoError(t, err)

	// Data blockSize is 2 hours and index blockSize is four hours so the data blocks
	// will span two different index blocks.
	indexResults := res.IndexResults()
	require.Equal(t, 2, len(indexResults))
	require.Equal(t, 0, len(res.Unfulfilled()))

	err = verifyIndexResultsAreCorrect(values, seriesNotToExpect, indexResults, indexBlockSize)
	require.NoError(t, err)
}

func TestBootstrapIndexEmptyShardTimeRanges(t *testing.T) {
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

	values := []testValue{}
	src.newIteratorFn = func(_ commitlog.IteratorOpts) (commitlog.Iterator, error) {
		return newTestCommitLogIterator(values, nil), nil
	}

	res, err := src.ReadIndex(md, result.ShardTimeRanges{}, testDefaultRunOpts)
	require.NoError(t, err)

	// Data blockSize is 2 hours and index blockSize is four hours so the data blocks
	// will span two different index blocks.
	indexResults := res.IndexResults()
	require.Equal(t, 0, len(indexResults))
	require.Equal(t, 0, len(res.Unfulfilled()))
}

func verifyIndexResultsAreCorrect(
	values []testValue,
	seriesNotToExpect map[string]struct{},
	indexResults result.IndexResults,
	indexBlockSize time.Duration,
) error {
	expectedIndexBlocks := map[xtime.UnixNano]map[string]map[string]string{}
	for _, value := range values {
		if _, shouldNotExpect := seriesNotToExpect[value.s.ID.String()]; shouldNotExpect {
			continue
		}

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
		for _, tag := range value.s.Tags.Values() {
			existingTags[tag.Name.String()] = tag.Value.String()
		}
	}

	for indexBlockStart, expectedSeries := range expectedIndexBlocks {
		indexBlock, ok := indexResults[indexBlockStart]
		if !ok {
			return fmt.Errorf("missing index block: %v", indexBlockStart.ToTime().String())
		}

		for _, seg := range indexBlock.Segments() {
			reader, err := seg.Reader()
			if err != nil {
				return err
			}

			docs, err := reader.AllDocs()
			if err != nil {
				return err
			}

			seenSeries := map[string]struct{}{}
			for docs.Next() {
				curr := docs.Current()

				_, ok := seenSeries[string(curr.ID)]
				if ok {
					return fmt.Errorf(
						"saw duplicate series: %v for block %v",
						string(curr.ID), indexBlockStart.ToTime().String())
				}
				seenSeries[string(curr.ID)] = struct{}{}

				expectedTags := expectedSeries[string(curr.ID)]
				matchingTags := map[string]struct{}{}
				for _, tag := range curr.Fields {
					if _, ok := matchingTags[string(tag.Name)]; ok {
						return fmt.Errorf("saw duplicate tag: %v for id: %v", tag.Name, string(curr.ID))
					}
					matchingTags[string(tag.Name)] = struct{}{}

					tagValue, ok := expectedTags[string(tag.Name)]
					if !ok {
						return fmt.Errorf("saw unexpected tag: %v for id: %v", tag.Name, string(curr.ID))
					}

					if tagValue != string(tag.Value) {
						return fmt.Errorf(
							"tag values for series: %v do not match. Expected: %v but got: %v",
							curr.ID, tagValue, string(tag.Value),
						)
					}
				}

				if len(expectedTags) != len(matchingTags) {
					return fmt.Errorf(
						"number of tags for series: %v do not match. Expected: %v, but got: %v",
						string(curr.ID), len(expectedTags), len(matchingTags),
					)
				}
			}

			if docs.Err() != nil {
				return docs.Err()
			}

			if err := docs.Close(); err != nil {
				return err
			}

			if len(expectedSeries) != len(seenSeries) {
				return fmt.Errorf(
					"expected %v series, but got %v series", len(expectedSeries), len(seenSeries))
			}
		}
	}

	return nil
}
