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

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

var namespaceOptions = namespace.NewOptions()

func toEncodedBytes(
	t *testing.T,
	pool serialize.TagEncoderPool,
	tags ...ident.Tag,
) []byte {
	encoder := pool.Get()
	seriesTags := ident.NewTags(tags...)
	err := encoder.Encode(ident.NewTagsIterator(seriesTags))
	require.NoError(t, err)
	data, ok := encoder.Data()
	require.True(t, ok)
	return data.Bytes()
}

func TestBootstrapIndex(t *testing.T) {
	var (
		opts             = testDefaultOpts
		src              = newCommitLogSource(opts, fs.Inspection{}).(*commitLogSource)
		dataBlockSize    = 2 * time.Hour
		indexBlockSize   = 4 * time.Hour
		namespaceOptions = namespaceOptions.
					SetRetentionOptions(
				namespaceOptions.
					RetentionOptions().
					SetBlockSize(dataBlockSize),
			).
			SetIndexOptions(
				namespaceOptions.
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

	testTagEncodingPool := serialize.
		NewTagEncoderPool(serialize.NewTagEncoderOptions(),
			pool.NewObjectPoolOptions().SetSize(1))
	testTagEncodingPool.Init()

	fooTags := toEncodedBytes(t, testTagEncodingPool,
		ident.StringTag("city", "ny"),
		ident.StringTag("conference", "monitoroma"),
	)

	barTags := toEncodedBytes(t, testTagEncodingPool,
		ident.StringTag("city", "sf"))
	bazTags := toEncodedBytes(t, testTagEncodingPool,
		ident.StringTag("city", "oakland"))

	shardn := func(n int) uint32 { return uint32(n) }
	foo := ts.Series{UniqueIndex: 0, Namespace: testNamespaceID, Shard: shardn(0),
		ID: ident.StringID("foo"), EncodedTags: fooTags}
	bar := ts.Series{UniqueIndex: 1, Namespace: testNamespaceID, Shard: shardn(0),
		ID: ident.StringID("bar"), EncodedTags: barTags}
	baz := ts.Series{UniqueIndex: 2, Namespace: testNamespaceID, Shard: shardn(5),
		ID: ident.StringID("baz"), EncodedTags: bazTags}
	// Make sure we can handle series that don't have tags.
	untagged := ts.Series{UniqueIndex: 3, Namespace: testNamespaceID,
		Shard: shardn(5), ID: ident.StringID("untagged"), Tags: ident.Tags{}}
	// Make sure we skip series that are not within the bootstrap range.
	outOfRange := ts.Series{UniqueIndex: 4, Namespace: testNamespaceID,
		Shard: shardn(3), ID: ident.StringID("outOfRange"), Tags: ident.Tags{}}
	// Make sure we skip and dont panic on writes for shards that are higher than the maximum we're trying to bootstrap.
	shardTooHigh := ts.Series{UniqueIndex: 5, Namespace: testNamespaceID,
		Shard: shardn(100), ID: ident.StringID("shardTooHigh"), Tags: ident.Tags{}}
	// Make sure we skip series for shards that have no requested bootstrap ranges. The shard for this write needs
	// to be less than the highest shard we actually plan to bootstrap.
	noShardBootstrapRange := ts.Series{UniqueIndex: 6, Namespace: testNamespaceID,
		Shard: shardn(4), ID: ident.StringID("noShardBootstrapRange"), Tags: ident.Tags{}}
	// Make sure it handles multiple namespaces
	someOtherNamespace := ts.Series{UniqueIndex: 7, Namespace: testNamespaceID2,
		Shard: shardn(0), ID: ident.StringID("series_OtherNamespace"), Tags: ident.Tags{}}

	valuesNs := testValues{
		{foo, start, 1.0, xtime.Second, nil},
		{foo, start, 2.0, xtime.Second, nil},
		{foo, start.Add(dataBlockSize), 3.0, xtime.Second, nil},
		{bar, start.Add(dataBlockSize), 1.0, xtime.Second, nil},
		{bar, start.Add(dataBlockSize), 2.0, xtime.Second, nil},
		{baz, start.Add(2 * dataBlockSize), 1.0, xtime.Second, nil},
		{baz, start.Add(2 * dataBlockSize), 2.0, xtime.Second, nil},
		{untagged, start.Add(2 * dataBlockSize), 1.0, xtime.Second, nil},
	}

	invalidNs := testValues{
		{outOfRange, start.Add(-dataBlockSize), 1.0, xtime.Second, nil},
		{shardTooHigh, start.Add(dataBlockSize), 1.0, xtime.Second, nil},
		{noShardBootstrapRange, start.Add(dataBlockSize), 1.0, xtime.Second, nil},
	}

	valuesOtherNs := testValues{
		{someOtherNamespace, start.Add(dataBlockSize), 1.0, xtime.Second, nil},
	}

	values := append(valuesNs, invalidNs...)
	values = append(values, valuesOtherNs...)
	src.newIteratorFn = func(
		_ commitlog.IteratorOpts,
	) (commitlog.Iterator, []commitlog.ErrorWithPath, error) {
		return newTestCommitLogIterator(values, nil), nil, nil
	}

	ranges := xtime.NewRanges(
		xtime.Range{Start: start, End: start.Add(dataBlockSize)},
		xtime.Range{Start: start.Add(dataBlockSize), End: start.Add(2 * dataBlockSize)},
		xtime.Range{Start: start.Add(2 * dataBlockSize), End: start.Add(3 * dataBlockSize)})

	// Don't include ranges for shard 4 as thats how we're testing the noShardBootstrapRange series.
	targetRanges := result.NewShardTimeRanges().Set(
		shardn(0),
		ranges,
	).Set(
		shardn(1),
		ranges,
	).Set(
		shardn(2),
		ranges,
	).Set(
		shardn(5),
		ranges,
	)

	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, targetRanges, md1, md2, md3)
	defer tester.Finish()

	tester.TestReadWith(src)
	tester.TestUnfulfilledForNamespaceIsEmpty(md1)
	tester.TestUnfulfilledForNamespaceIsEmpty(md2)
	tester.TestUnfulfilledForNamespaceIsEmpty(md3)

	nsWrites := tester.EnsureDumpWritesForNamespace(md1)
	enforceValuesAreCorrect(t, valuesNs, nsWrites)

	otherNamespaceWrites := tester.EnsureDumpWritesForNamespace(md2)
	enforceValuesAreCorrect(t, valuesOtherNs, otherNamespaceWrites)

	noWrites := tester.EnsureDumpWritesForNamespace(md3)
	require.Equal(t, 0, len(noWrites))
	tester.EnsureNoLoadedBlocks()
}

func TestBootstrapIndexEmptyShardTimeRanges(t *testing.T) {
	var (
		opts             = testDefaultOpts
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

	values := testValues{}
	src.newIteratorFn = func(_ commitlog.IteratorOpts) (commitlog.Iterator, []commitlog.ErrorWithPath, error) {
		return newTestCommitLogIterator(values, nil), nil, nil
	}

	target := result.NewShardTimeRanges()
	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, target, md)
	defer tester.Finish()

	tester.TestReadWith(src)
	tester.TestUnfulfilledForNamespaceIsEmpty(md)
	tester.EnsureNoLoadedBlocks()
	tester.EnsureNoWrites()
}

func verifyIndexResultsAreCorrect(
	values testValues,
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

func TestBootstrapIndexFailsForDecodedTags(t *testing.T) {
	var (
		opts             = testDefaultOpts
		src              = newCommitLogSource(opts, fs.Inspection{}).(*commitLogSource)
		dataBlockSize    = 2 * time.Hour
		indexBlockSize   = 4 * time.Hour
		namespaceOptions = namespaceOptions.
					SetRetentionOptions(
				namespaceOptions.
					RetentionOptions().
					SetBlockSize(dataBlockSize),
			).
			SetIndexOptions(
				namespaceOptions.
					IndexOptions().
					SetBlockSize(indexBlockSize).
					SetEnabled(true),
			)
	)
	md1, err := namespace.NewMetadata(testNamespaceID, namespaceOptions)
	require.NoError(t, err)

	now := time.Now()
	start := now.Truncate(indexBlockSize)

	fooTags := ident.NewTags(ident.StringTag("city", "ny"))

	shardn := func(n int) uint32 { return uint32(n) }
	foo := ts.Series{UniqueIndex: 0, Namespace: testNamespaceID, Shard: shardn(0),
		ID: ident.StringID("foo"), Tags: fooTags}

	values := testValues{
		{foo, start, 1.0, xtime.Second, nil},
	}

	src.newIteratorFn = func(
		_ commitlog.IteratorOpts,
	) (commitlog.Iterator, []commitlog.ErrorWithPath, error) {
		return newTestCommitLogIterator(values, nil), nil, nil
	}

	ranges := xtime.NewRanges(
		xtime.Range{Start: start, End: start.Add(dataBlockSize)},
		xtime.Range{Start: start.Add(dataBlockSize), End: start.Add(2 * dataBlockSize)},
		xtime.Range{Start: start.Add(2 * dataBlockSize), End: start.Add(3 * dataBlockSize)})

	// Don't include ranges for shard 4 as thats how we're testing the noShardBootstrapRange series.
	targetRanges := result.NewShardTimeRanges().Set(
		shardn(0),
		ranges,
	).Set(
		shardn(1),
		ranges,
	).Set(
		shardn(2),
		ranges,
	).Set(
		shardn(5),
		ranges,
	)

	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, targetRanges, md1)
	defer tester.Finish()

	_, err = src.Read(tester.Namespaces)
	require.Error(t, err)

	tester.EnsureNoLoadedBlocks()
	tester.EnsureNoWrites()
}
