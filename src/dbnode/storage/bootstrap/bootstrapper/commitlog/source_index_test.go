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

	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
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
					SetBlockSize(indexBlockSize).
					SetEnabled(true),
			)
	)
	md1, err := namespace.NewMetadata(testNamespaceID, namespaceOptions)
	require.NoError(t, err)

	testNamespaceID2 := ident.StringID("someOtherNamespace")
	md2, err := namespace.NewMetadata(testNamespaceID2, namespaceOptions)
	require.NoError(t, err)

	testNamespaceNoData := ident.StringID("noData")
	md3, err := namespace.NewMetadata(testNamespaceNoData, namespaceOptions)
	require.NoError(t, err)

	now := time.Now()
	start := now.Truncate(indexBlockSize)

	fooTags := ident.NewTags(ident.StringTag("city", "ny"), ident.StringTag("conference", "monitoroma"))
	barTags := ident.NewTags(ident.StringTag("city", "sf"))
	bazTags := ident.NewTags(ident.StringTag("city", "oakland"))

	shardn := func(n int) uint32 { return uint32(n) }
	foo := commitlog.Series{UniqueIndex: 0, Namespace: testNamespaceID, Shard: shardn(0), ID: ident.StringID("foo"), Tags: fooTags}
	bar := commitlog.Series{UniqueIndex: 1, Namespace: testNamespaceID, Shard: shardn(0), ID: ident.StringID("bar"), Tags: barTags}
	baz := commitlog.Series{UniqueIndex: 2, Namespace: testNamespaceID, Shard: shardn(5), ID: ident.StringID("baz"), Tags: bazTags}
	// Make sure we can handle series that don't have tags.
	untagged := commitlog.Series{UniqueIndex: 3, Namespace: testNamespaceID, Shard: shardn(5), ID: ident.StringID("untagged"), Tags: ident.Tags{}}
	// Make sure we skip series that are not within the bootstrap range.
	outOfRange := commitlog.Series{UniqueIndex: 4, Namespace: testNamespaceID, Shard: shardn(3), ID: ident.StringID("outOfRange"), Tags: ident.Tags{}}
	// Make sure we skip and dont panic on writes for shards that are higher than the maximum we're trying to bootstrap.
	shardTooHigh := commitlog.Series{UniqueIndex: 5, Namespace: testNamespaceID, Shard: shardn(100), ID: ident.StringID("shardTooHigh"), Tags: ident.Tags{}}
	// Make sure we skip series for shards that have no requested bootstrap ranges. The shard for this write needs
	// to be less than the highest shard we actually plan to bootstrap.
	noShardBootstrapRange := commitlog.Series{UniqueIndex: 6, Namespace: testNamespaceID, Shard: shardn(4), ID: ident.StringID("noShardBootstrapRange"), Tags: ident.Tags{}}
	// Make sure it handles multiple namespaces
	someOtherNamespace := commitlog.Series{UniqueIndex: 7, Namespace: testNamespaceID2, Shard: shardn(0), ID: ident.StringID("someOtherNamespace"), Tags: ident.Tags{}}

	seriesNotToExpect := map[string]struct{}{
		outOfRange.ID.String():            struct{}{},
		shardTooHigh.ID.String():          struct{}{},
		noShardBootstrapRange.ID.String(): struct{}{},
	}

	values := []testValue{
		{foo, start, 1.0, xtime.Second, nil},
		{foo, start, 2.0, xtime.Second, nil},
		{foo, start.Add(dataBlockSize), 3.0, xtime.Second, nil},
		{bar, start.Add(dataBlockSize), 1.0, xtime.Second, nil},
		{bar, start.Add(dataBlockSize), 2.0, xtime.Second, nil},
		{baz, start.Add(2 * dataBlockSize), 1.0, xtime.Second, nil},
		{baz, start.Add(2 * dataBlockSize), 2.0, xtime.Second, nil},
		{untagged, start.Add(2 * dataBlockSize), 1.0, xtime.Second, nil},
		{outOfRange, start.Add(-dataBlockSize), 1.0, xtime.Second, nil},
		{shardTooHigh, start.Add(dataBlockSize), 1.0, xtime.Second, nil},
		{noShardBootstrapRange, start.Add(dataBlockSize), 1.0, xtime.Second, nil},
		{someOtherNamespace, start.Add(dataBlockSize), 1.0, xtime.Second, nil},
	}

	src.newIteratorFn = func(_ commitlog.IteratorOpts) (commitlog.Iterator, []string, error) {
		return newTestCommitLogIterator(values, nil), nil, nil
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
		shardn(0): ranges, shardn(1): ranges, shardn(2): ranges, shardn(5): ranges}

	if bootstrapDataFirst {
		// Bootstrap the data to exercise the metadata caching path.

		// Bootstrap some arbitrary time range to make sure that it can handle multiple
		// calls to ReadData() with different ranges and only a subset of the shards to make
		// sure it can handle multiple calls to ReadData() with different shards / ranges (it needs
		// to merge the ranges / shards across calls.)
		var (
			targetRangesCopy = targetRanges.Copy()
			highestShard     = uint32(0)
		)
		for key := range targetRangesCopy {
			// The time-range here is "arbitrary", but it does need to be one for which
			// there is data so that we can actually exercise the blockStart merging
			// logic.
			targetRangesCopy[key] = xtime.NewRanges(xtime.Range{
				Start: start,
				End:   start.Add(dataBlockSize),
			})

			if key > highestShard {
				highestShard = key
			}
		}

		// Delete the highest hard number to ensure we're bootstrapping a subset, and the subsequent call
		// to ReadData() will exercise the slice extending loggic.
		delete(targetRangesCopy, highestShard)
		_, err = src.ReadData(md1, targetRangesCopy, testDefaultRunOpts)
		require.NoError(t, err)

		// Bootstrap the actual time ranges to actually cache the metadata.
		_, err = src.ReadData(md1, targetRanges, testDefaultRunOpts)
		require.NoError(t, err)
	}

	res, err := src.ReadIndex(md1, targetRanges, testDefaultRunOpts)
	require.NoError(t, err)

	// Data blockSize is 2 hours and index blockSize is four hours so the data blocks
	// will span two different index blocks.
	indexResults := res.IndexResults()
	require.Equal(t, 2, len(indexResults))
	require.Equal(t, 0, len(res.Unfulfilled()))

	err = verifyIndexResultsAreCorrect(values, seriesNotToExpect, indexResults, indexBlockSize)
	require.NoError(t, err)

	// Update the iterator function to only return values for the second namespace because
	// the real commit log reader does this (via the ReadSeries predicate).
	otherNamespaceValues := []testValue{}
	for _, value := range values {
		if value.s.Namespace.Equal(testNamespaceID2) {
			otherNamespaceValues = append(otherNamespaceValues, value)
		}
	}
	src.newIteratorFn = func(_ commitlog.IteratorOpts) (commitlog.Iterator, []string, error) {
		return newTestCommitLogIterator(otherNamespaceValues, nil), nil, nil
	}

	res, err = src.ReadIndex(md2, targetRanges, testDefaultRunOpts)
	require.NoError(t, err)

	// Only one series so there should only be one index result.
	indexResults = res.IndexResults()
	require.Equal(t, 1, len(indexResults))
	require.Equal(t, 0, len(res.Unfulfilled()))

	err = verifyIndexResultsAreCorrect(otherNamespaceValues, seriesNotToExpect, indexResults, indexBlockSize)
	require.NoError(t, err)

	// Update the iterator function to return no values (since this namespace has no data)
	// because the real commit log reader does this (via the ReadSeries predicate).
	src.newIteratorFn = func(_ commitlog.IteratorOpts) (commitlog.Iterator, []string, error) {
		return newTestCommitLogIterator([]testValue{}, nil), nil, nil
	}

	res, err = src.ReadIndex(md3, targetRanges, testDefaultRunOpts)
	require.NoError(t, err)

	// Zero series so there should be no index results.
	indexResults = res.IndexResults()
	require.Equal(t, 0, len(indexResults))
	require.Equal(t, 0, len(res.Unfulfilled()))
	seg, err := indexResults.GetOrAddSegment(start, namespaceOptions.IndexOptions(), result.NewOptions())
	require.NoError(t, err)
	require.Equal(t, int64(0), seg.Size())
	err = verifyIndexResultsAreCorrect([]testValue{}, seriesNotToExpect, indexResults, indexBlockSize)
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
					SetBlockSize(indexBlockSize).
					SetEnabled(true),
			)
	)
	md, err := namespace.NewMetadata(testNamespaceID, namespaceOptions)
	require.NoError(t, err)

	values := []testValue{}
	src.newIteratorFn = func(_ commitlog.IteratorOpts) (commitlog.Iterator, []string, error) {
		return newTestCommitLogIterator(values, nil), nil, nil
	}

	res, err := src.ReadIndex(md, result.ShardTimeRanges{}, testDefaultRunOpts)
	require.NoError(t, err)

	// Data blockSize is 2 hours and index blockSize is four hours so the data blocks
	// will span two different index blocks.
	indexResults := res.IndexResults()
	require.Equal(t, 0, len(indexResults))
	require.Equal(t, 0, len(res.Unfulfilled()))
}

func TestBootstrapIndexNamespaceIndexNotEnabled(t *testing.T) {
	var (
		opts             = testOptions()
		src              = newCommitLogSource(opts, fs.Inspection{}).(*commitLogSource)
		namespaceOptions = namespace.NewOptions().
					SetIndexOptions(
				namespace.NewOptions().
					IndexOptions().
					SetEnabled(false),
			)
	)
	md, err := namespace.NewMetadata(testNamespaceID, namespaceOptions)
	require.NoError(t, err)

	_, err = src.ReadIndex(md, result.ShardTimeRanges{}, testDefaultRunOpts)
	require.Equal(t, err, errIndexingNotEnableForNamespace)
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

		if indexBlock.Fulfilled().IsEmpty() {
			return fmt.Errorf("index-block %v fulfilled is empty", indexBlockStart)
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
