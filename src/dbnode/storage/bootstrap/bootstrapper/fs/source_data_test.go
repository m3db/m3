// Copyright (c) 2016 Uber Technologies, Inc.
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
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/migration"
	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testShard                 = uint32(0)
	testNs1ID                 = ident.StringID("test_namespace")
	testBlockSize             = 2 * time.Hour
	testIndexBlockSize        = 4 * time.Hour
	testStart                 = time.Now().Truncate(testBlockSize)
	testFileMode              = os.FileMode(0666)
	testDirMode               = os.ModeDir | os.FileMode(0755)
	testWriterBufferSize      = 10
	testNamespaceIndexOptions = namespace.NewIndexOptions()
	testNamespaceOptions      = namespace.NewOptions()
	testRetentionOptions      = retention.NewOptions()
	testDefaultFsOpts         = fs.NewOptions()
	testDefaultRunOpts        = bootstrap.NewRunOptions().
					SetPersistConfig(bootstrap.PersistConfig{Enabled: false})
	testDefaultResultOpts = result.NewOptions().SetSeriesCachePolicy(series.CacheAll)
	testDefaultOpts       = NewOptions().SetResultOptions(testDefaultResultOpts)
	testShardRanges       = testShardTimeRanges()
)

func newTestOptions(t require.TestingT, filePathPrefix string) Options {
	idxOpts := index.NewOptions()
	compactor, err := compaction.NewCompactor(idxOpts.MetadataArrayPool(),
		index.MetadataArrayPoolCapacity,
		idxOpts.SegmentBuilderOptions(),
		idxOpts.FSTSegmentOptions(),
		compaction.CompactorOptions{
			FSTWriterOptions: &fst.WriterOptions{
				// DisableRegistry is set to true to trade a larger FST size
				// for a faster FST compaction since we want to reduce the end
				// to end latency for time to first index a metric.
				DisableRegistry: true,
			},
		})
	require.NoError(t, err)
	fsOpts := newTestFsOptions(filePathPrefix)
	pm, err := fs.NewPersistManager(fsOpts)
	require.NoError(t, err)

	// Allow multiple index claim managers since need to create one
	// for each file path prefix (fs options changes between tests).
	fs.ResetIndexClaimsManagersUnsafe()

	icm, err := fs.NewIndexClaimsManager(fsOpts)
	require.NoError(t, err)
	return testDefaultOpts.
		SetCompactor(compactor).
		SetIndexOptions(idxOpts).
		SetFilesystemOptions(fsOpts).
		SetPersistManager(pm).
		SetIndexClaimsManager(icm)
}

func newTestOptionsWithPersistManager(t require.TestingT, filePathPrefix string) Options {
	opts := newTestOptions(t, filePathPrefix)
	pm, err := fs.NewPersistManager(opts.FilesystemOptions())
	require.NoError(t, err)
	return opts.SetPersistManager(pm)
}

func newTestFsOptions(filePathPrefix string) fs.Options {
	return testDefaultFsOpts.
		SetFilePathPrefix(filePathPrefix).
		SetWriterBufferSize(testWriterBufferSize).
		SetNewFileMode(testFileMode).
		SetNewDirectoryMode(testDirMode)
}

func testNsMetadata(t require.TestingT) namespace.Metadata {
	return testNsMetadataWithIndex(t, true)
}

func testNsMetadataWithIndex(t require.TestingT, indexOn bool) namespace.Metadata {
	ropts := testRetentionOptions.SetBlockSize(testBlockSize)
	md, err := namespace.NewMetadata(testNs1ID, testNamespaceOptions.
		SetRetentionOptions(ropts).
		SetIndexOptions(testNamespaceIndexOptions.
			SetEnabled(indexOn).
			SetBlockSize(testIndexBlockSize)))
	require.NoError(t, err)
	return md
}

func testCache(t *testing.T, md namespace.Metadata, ranges result.ShardTimeRanges, fsOpts fs.Options) bootstrap.Cache {
	var shards []uint32
	for shard := range ranges.Iter() {
		shards = append(shards, shard)
	}
	cache, err := bootstrap.NewCache(bootstrap.NewCacheOptions().
		SetFilesystemOptions(fsOpts).
		SetInstrumentOptions(fsOpts.InstrumentOptions()).
		SetNamespaceDetails([]bootstrap.NamespaceDetails{
			{
				Namespace: md,
				Shards:    shards,
			},
		}))
	require.NoError(t, err)

	return cache
}

func createTempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "foo")
	require.NoError(t, err)
	return dir
}

func writeInfoFile(t *testing.T, prefix string, namespace ident.ID,
	shard uint32, start time.Time, data []byte) {
	shardDir := fs.ShardDataDirPath(prefix, namespace, shard)
	filePath := path.Join(shardDir,
		fmt.Sprintf("fileset-%d-0-info.db", xtime.ToNanoseconds(start)))
	writeFile(t, filePath, data)
}

func writeDataFile(t *testing.T, prefix string, namespace ident.ID,
	shard uint32, start time.Time, data []byte) {
	shardDir := fs.ShardDataDirPath(prefix, namespace, shard)
	filePath := path.Join(shardDir,
		fmt.Sprintf("fileset-%d-0-data.db", xtime.ToNanoseconds(start)))
	writeFile(t, filePath, data)
}

func writeDigestFile(t *testing.T, prefix string, namespace ident.ID,
	shard uint32, start time.Time, data []byte) {
	shardDir := fs.ShardDataDirPath(prefix, namespace, shard)
	filePath := path.Join(shardDir,
		fmt.Sprintf("fileset-%d-0-digest.db", xtime.ToNanoseconds(start)))
	writeFile(t, filePath, data)
}

func writeFile(t *testing.T, filePath string, data []byte) {
	fd, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, testFileMode)
	require.NoError(t, err)
	if data != nil {
		_, err = fd.Write(data)
		require.NoError(t, err)
	}
	require.NoError(t, fd.Close())
}

func testTimeRanges() xtime.Ranges {
	return xtime.NewRanges(xtime.Range{Start: testStart, End: testStart.Add(11 * time.Hour)})
}

func testShardTimeRanges() result.ShardTimeRanges {
	return result.NewShardTimeRanges().Set(testShard, testTimeRanges())
}

func testBootstrappingIndexShardTimeRanges() result.ShardTimeRanges {
	// NB: since index files are not corrupted on this run, it's expected that
	// `testBlockSize` values should be fulfilled in the index block. This is
	// `testBlockSize` rather than `testIndexSize` since the files generated
	// by this test use 2 hour (which is `testBlockSize`) reader blocks.
	return result.NewShardTimeRanges().Set(
		testShard,
		xtime.NewRanges(xtime.Range{
			Start: testStart.Add(testBlockSize),
			End:   testStart.Add(11 * time.Hour),
		}),
	)
}

func writeGoodFiles(t *testing.T, dir string, namespace ident.ID, shard uint32) {
	writeGoodFilesWithFsOpts(t, namespace, shard, newTestFsOptions(dir))
}

func writeGoodFilesWithFsOpts(t *testing.T, namespace ident.ID, shard uint32, fsOpts fs.Options) {
	inputs := []struct {
		start time.Time
		id    string
		tags  map[string]string
		data  []byte
	}{
		{testStart, "foo", map[string]string{"n": "0"}, []byte{1, 2, 3}},
		{testStart.Add(10 * time.Hour), "bar", map[string]string{"n": "1"}, []byte{4, 5, 6}},
		{testStart.Add(20 * time.Hour), "baz", nil, []byte{7, 8, 9}},
	}

	for _, input := range inputs {
		writeTSDBFilesWithFsOpts(t, namespace, shard, input.start,
			[]testSeries{{input.id, input.tags, input.data}}, fsOpts)
	}
}

type testSeries struct {
	id   string
	tags map[string]string
	data []byte
}

func (s testSeries) ID() ident.ID {
	return ident.StringID(s.id)
}

func (s testSeries) Tags() ident.Tags {
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
	t require.TestingT,
	dir string,
	namespace ident.ID,
	shard uint32,
	start time.Time,
	series []testSeries,
) {
	writeTSDBFilesWithFsOpts(t, namespace, shard, start, series, newTestFsOptions(dir))
}

func writeTSDBFilesWithFsOpts(
	t require.TestingT,
	namespace ident.ID,
	shard uint32,
	start time.Time,
	series []testSeries,
	opts fs.Options,
) {
	w, err := fs.NewWriter(opts)
	require.NoError(t, err)
	writerOpts := fs.DataWriterOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:  namespace,
			Shard:      shard,
			BlockStart: start,
		},
		BlockSize: testBlockSize,
	}
	require.NoError(t, w.Open(writerOpts))

	for _, v := range series {
		bytes := checked.NewBytes(v.data, nil)
		bytes.IncRef()
		metadata := persist.NewMetadataFromIDAndTags(ident.StringID(v.id), sortedTagsFromTagsMap(v.tags),
			persist.MetadataOptions{})
		require.NoError(t, w.Write(metadata, bytes, digest.Checksum(bytes.Bytes())))
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

func validateTimeRanges(t *testing.T, tr xtime.Ranges, expected xtime.Ranges) {
	// Make range eclipses expected
	expectedWithRemovedRanges := expected.Clone()
	expectedWithRemovedRanges.RemoveRanges(tr)
	require.True(t, expectedWithRemovedRanges.IsEmpty())

	// Now make sure no ranges outside of expected
	expectedWithAddedRanges := expected.Clone()
	expectedWithAddedRanges.AddRanges(tr)

	require.Equal(t, expected.Len(), expectedWithAddedRanges.Len())
	iter := expected.Iter()
	withAddedRangesIter := expectedWithAddedRanges.Iter()
	for iter.Next() && withAddedRangesIter.Next() {
		require.True(t, iter.Value().Equal(withAddedRangesIter.Value()))
	}
}

func TestAvailableEmptyRangeError(t *testing.T) {
	opts := newTestOptions(t, "foo")
	src, err := newFileSystemSource(opts)
	require.NoError(t, err)
	md := testNsMetadata(t)
	shardRanges := result.NewShardTimeRanges().Set(0, xtime.NewRanges())
	cache := testCache(t, md, shardRanges, opts.FilesystemOptions())
	res, err := src.AvailableData(
		md,
		shardRanges,
		cache,
		testDefaultRunOpts,
	)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.True(t, res.IsEmpty())
}

func TestAvailablePatternError(t *testing.T) {
	opts := newTestOptions(t, "[[")
	src, err := newFileSystemSource(opts)
	require.NoError(t, err)
	md := testNsMetadata(t)
	res, err := src.AvailableData(
		md,
		testShardRanges,
		testCache(t, md, testShardRanges, opts.FilesystemOptions()),
		testDefaultRunOpts,
	)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.True(t, res.IsEmpty())
}

func TestAvailableReadInfoError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeTSDBFiles(t, dir, testNs1ID, shard, testStart, []testSeries{
		{"foo", nil, []byte{0x1}},
	})
	// Intentionally corrupt the info file
	writeInfoFile(t, dir, testNs1ID, shard, testStart, []byte{0x1, 0x2})

	opts := newTestOptions(t, dir)
	src, err := newFileSystemSource(opts)
	require.NoError(t, err)
	md := testNsMetadata(t)
	res, err := src.AvailableData(
		md,
		testShardRanges,
		testCache(t, md, testShardRanges, opts.FilesystemOptions()),
		testDefaultRunOpts,
	)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.True(t, res.IsEmpty())
}

func TestAvailableDigestOfDigestMismatch(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeTSDBFiles(t, dir, testNs1ID, shard, testStart, []testSeries{
		{"foo", nil, []byte{0x1}},
	})
	// Intentionally corrupt the digest file
	writeDigestFile(t, dir, testNs1ID, shard, testStart, nil)

	opts := newTestOptions(t, dir)
	src, err := newFileSystemSource(opts)
	require.NoError(t, err)
	md := testNsMetadata(t)
	res, err := src.AvailableData(
		md,
		testShardRanges,
		testCache(t, md, testShardRanges, opts.FilesystemOptions()),
		testDefaultRunOpts,
	)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.True(t, res.IsEmpty())
}

func TestAvailableTimeRangeFilter(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeGoodFiles(t, dir, testNs1ID, shard)

	opts := newTestOptions(t, dir)
	src, err := newFileSystemSource(opts)
	require.NoError(t, err)
	md := testNsMetadata(t)
	res, err := src.AvailableData(
		md,
		testShardRanges,
		testCache(t, md, testShardRanges, opts.FilesystemOptions()),
		testDefaultRunOpts,
	)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 1, res.Len())
	_, ok := res.Get(testShard)
	require.True(t, ok)

	expected := xtime.NewRanges(
		xtime.Range{Start: testStart, End: testStart.Add(2 * time.Hour)},
		xtime.Range{Start: testStart.Add(10 * time.Hour), End: testStart.Add(12 * time.Hour)})
	tr, ok := res.Get(testShard)
	require.True(t, ok)
	validateTimeRanges(t, tr, expected)
}

func TestAvailableTimeRangePartialError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeGoodFiles(t, dir, testNs1ID, shard)
	// Intentionally write a corrupted info file
	writeInfoFile(t, dir, testNs1ID, shard, testStart.Add(4*time.Hour), []byte{0x1, 0x2})

	opts := newTestOptions(t, dir)
	src, err := newFileSystemSource(opts)
	require.NoError(t, err)
	md := testNsMetadata(t)
	res, err := src.AvailableData(
		md,
		testShardRanges,
		testCache(t, md, testShardRanges, opts.FilesystemOptions()),
		testDefaultRunOpts,
	)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 1, res.Len())
	_, ok := res.Get(testShard)
	require.True(t, ok)

	expected := xtime.NewRanges(
		xtime.Range{Start: testStart, End: testStart.Add(2 * time.Hour)},
		xtime.Range{Start: testStart.Add(10 * time.Hour), End: testStart.Add(12 * time.Hour)})
	tr, ok := res.Get(testShard)
	require.True(t, ok)
	validateTimeRanges(t, tr, expected)
}

// NB: too real :'(
func unfulfilledAndEmpty(t *testing.T, src bootstrap.Source,
	md namespace.Metadata, tester bootstrap.NamespacesTester) {
	tester.TestReadWith(src)
	tester.TestUnfulfilledForNamespaceIsEmpty(md)

	tester.EnsureNoWrites()
	tester.EnsureNoLoadedBlocks()
}

func TestReadEmptyRangeErr(t *testing.T) {
	src, err := newFileSystemSource(newTestOptions(t, "foo"))
	require.NoError(t, err)
	nsMD := testNsMetadata(t)
	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, result.NewShardTimeRanges(), nsMD)
	defer tester.Finish()
	unfulfilledAndEmpty(t, src, nsMD, tester)
}

func TestReadPatternError(t *testing.T) {
	src, err := newFileSystemSource(newTestOptions(t, "[["))
	require.NoError(t, err)
	timeRanges := result.NewShardTimeRanges().Set(testShard, xtime.NewRanges())
	nsMD := testNsMetadata(t)
	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts,
		timeRanges, nsMD)
	defer tester.Finish()
	unfulfilledAndEmpty(t, src, nsMD, tester)
}

func validateReadResults(
	t *testing.T,
	src bootstrap.Source,
	dir string,
	strs result.ShardTimeRanges,
) {
	nsMD := testNsMetadata(t)
	fsOpts := newTestOptions(t, dir).FilesystemOptions()
	tester := bootstrap.BuildNamespacesTesterWithFilesystemOptions(t, testDefaultRunOpts, strs, fsOpts, nsMD)
	defer tester.Finish()

	tester.TestReadWith(src)
	readers := tester.EnsureDumpReadersForNamespace(nsMD)
	require.Equal(t, 2, len(readers))
	ids := []string{"foo", "bar"}
	data := [][]byte{
		{1, 2, 3},
		{4, 5, 6},
	}

	times := []time.Time{testStart, testStart.Add(10 * time.Hour)}
	for i, id := range ids {
		seriesReaders, ok := readers[id]
		require.True(t, ok)
		require.Equal(t, 1, len(seriesReaders))
		readerAtTime := seriesReaders[0]
		assert.Equal(t, times[i], readerAtTime.Start)
		ctx := context.NewContext()
		var b [100]byte
		n, err := readerAtTime.Reader.Read(b[:])
		ctx.Close()
		require.NoError(t, err)
		require.Equal(t, data[i], b[:n])
	}

	tester.EnsureNoWrites()
}

func TestReadNilTimeRanges(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeGoodFiles(t, dir, testNs1ID, shard)

	src, err := newFileSystemSource(newTestOptions(t, dir))
	require.NoError(t, err)
	timeRanges := result.NewShardTimeRanges().Set(
		testShard,
		testTimeRanges(),
	).Set(
		555,
		xtime.NewRanges(),
	)

	validateReadResults(t, src, dir, timeRanges)
}

func TestReadOpenFileError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeTSDBFiles(t, dir, testNs1ID, shard, testStart, []testSeries{
		{"foo", nil, []byte{0x1}},
	})

	// Intentionally truncate the info file
	writeInfoFile(t, dir, testNs1ID, shard, testStart, nil)

	src, err := newFileSystemSource(newTestOptions(t, dir))
	require.NoError(t, err)
	nsMD := testNsMetadata(t)
	ranges := testShardTimeRanges()
	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts,
		ranges, nsMD)
	defer tester.Finish()

	tester.TestReadWith(src)
	tester.TestUnfulfilledForNamespace(nsMD, ranges, ranges)

	tester.EnsureNoLoadedBlocks()
	tester.EnsureNoWrites()
}

func TestReadDataCorruptionErrorNoIndex(t *testing.T) {
	testReadDataCorruptionErrorWithIndexEnabled(t, false, testShardTimeRanges())
}

func TestReadDataCorruptionErrorWithIndex(t *testing.T) {
	expectedIndex := testBootstrappingIndexShardTimeRanges()
	testReadDataCorruptionErrorWithIndexEnabled(t, true, expectedIndex)
}

func testReadDataCorruptionErrorWithIndexEnabled(
	t *testing.T,
	withIndex bool,
	expectedIndexUnfulfilled result.ShardTimeRanges,
) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeTSDBFiles(t, dir, testNs1ID, shard, testStart, []testSeries{
		{"foo", nil, []byte{0x1}},
	})
	// Intentionally corrupt the data file
	writeDataFile(t, dir, testNs1ID, shard, testStart, []byte{0x2})

	testOpts := newTestOptions(t, dir)
	src, err := newFileSystemSource(testOpts)
	require.NoError(t, err)

	strs := testShardTimeRanges()

	nsMD := testNsMetadataWithIndex(t, withIndex)
	tester := bootstrap.BuildNamespacesTesterWithFilesystemOptions(t, testDefaultRunOpts, strs, testOpts.FilesystemOptions(), nsMD)
	defer tester.Finish()

	tester.TestReadWith(src)
	tester.TestUnfulfilledForNamespace(nsMD, strs, expectedIndexUnfulfilled)
	tester.EnsureNoWrites()
}

func TestReadTimeFilter(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	writeGoodFiles(t, dir, testNs1ID, testShard)

	src, err := newFileSystemSource(newTestOptions(t, dir))
	require.NoError(t, err)

	validateReadResults(t, src, dir, testShardTimeRanges())
}

func TestReadPartialError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	writeGoodFiles(t, dir, testNs1ID, testShard)
	// Intentionally corrupt the data file
	writeDataFile(t, dir, testNs1ID, testShard, testStart.Add(4*time.Hour), []byte{0x1})

	src, err := newFileSystemSource(newTestOptions(t, dir))
	require.NoError(t, err)

	validateReadResults(t, src, dir, testShardTimeRanges())
}

func TestReadValidateErrorNoIndex(t *testing.T) {
	testReadValidateErrorWithIndexEnabled(t, false, testShardTimeRanges())
}

func TestReadValidateErrorWithIndex(t *testing.T) {
	expectedIndex := testBootstrappingIndexShardTimeRanges()
	testReadValidateErrorWithIndexEnabled(t, true, expectedIndex)
}

func testReadValidateErrorWithIndexEnabled(
	t *testing.T,
	enabled bool,
	expectedIndexUnfulfilled result.ShardTimeRanges,
) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	reader := fs.NewMockDataFileSetReader(ctrl)

	testOpts := newTestOptions(t, dir)
	fsSrc, err := newFileSystemSource(testOpts)
	require.NoError(t, err)

	src, ok := fsSrc.(*fileSystemSource)
	require.True(t, ok)

	first := true
	src.newReaderFn = func(
		b pool.CheckedBytesPool,
		opts fs.Options,
	) (fs.DataFileSetReader, error) {
		if first {
			first = false
			return reader, nil
		}
		return fs.NewReader(b, opts)
	}
	src.newReaderPoolOpts.DisableReuse = true

	shard := uint32(0)
	writeTSDBFiles(t, dir, testNs1ID, shard, testStart, []testSeries{
		{"foo", nil, []byte{0x1}},
	})
	rOpenOpts := fs.ReaderOpenOptionsMatcher{
		ID: fs.FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      shard,
			BlockStart: testStart,
		},
	}
	reader.EXPECT().
		Open(rOpenOpts).
		Return(nil)
	reader.EXPECT().
		Range().
		Return(xtime.Range{
			Start: testStart,
			End:   testStart.Add(2 * time.Hour),
		})
	reader.EXPECT().Entries().Return(0).AnyTimes()
	reader.EXPECT().Validate().Return(errors.New("foo"))
	reader.EXPECT().Close().Return(nil)

	nsMD := testNsMetadataWithIndex(t, enabled)
	ranges := testShardTimeRanges()
	tester := bootstrap.BuildNamespacesTesterWithFilesystemOptions(t, testDefaultRunOpts,
		ranges, testOpts.FilesystemOptions(), nsMD)
	defer tester.Finish()

	tester.TestReadWith(src)
	tester.TestUnfulfilledForNamespace(nsMD, ranges, expectedIndexUnfulfilled)
	tester.EnsureNoLoadedBlocks()
	tester.EnsureNoWrites()
}

func TestReadOpenErrorNoIndex(t *testing.T) {
	testReadOpenError(t, false, testShardTimeRanges())
}

func TestReadOpenErrorWithIndex(t *testing.T) {
	expectedIndex := testBootstrappingIndexShardTimeRanges()
	testReadOpenError(t, true, expectedIndex)
}

func testReadOpenError(
	t *testing.T,
	enabled bool,
	expectedIndexUnfulfilled result.ShardTimeRanges,
) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	reader := fs.NewMockDataFileSetReader(ctrl)

	testOpts := newTestOptions(t, dir)
	fsSrc, err := newFileSystemSource(testOpts)
	require.NoError(t, err)

	src, ok := fsSrc.(*fileSystemSource)
	require.True(t, ok)

	first := true
	src.newReaderFn = func(
		b pool.CheckedBytesPool,
		opts fs.Options,
	) (fs.DataFileSetReader, error) {
		if first {
			first = false
			return reader, nil
		}
		return fs.NewReader(b, opts)
	}

	shard := uint32(0)
	writeTSDBFiles(t, dir, testNs1ID, shard, testStart, []testSeries{
		{"foo", nil, []byte{0x1}},
	})
	rOpts := fs.ReaderOpenOptionsMatcher{
		ID: fs.FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      shard,
			BlockStart: testStart,
		},
	}
	reader.EXPECT().
		Open(rOpts).
		Return(errors.New("error"))

	nsMD := testNsMetadataWithIndex(t, enabled)
	ranges := testShardTimeRanges()
	tester := bootstrap.BuildNamespacesTesterWithFilesystemOptions(t, testDefaultRunOpts,
		ranges, testOpts.FilesystemOptions(), nsMD)
	defer tester.Finish()

	tester.TestReadWith(src)
	tester.TestUnfulfilledForNamespace(nsMD, ranges, expectedIndexUnfulfilled)
	tester.EnsureNoLoadedBlocks()
	tester.EnsureNoWrites()
}

func TestReadDeleteOnError(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	reader := fs.NewMockDataFileSetReader(ctrl)

	testOpts := newTestOptions(t, dir)
	fsSrc, err := newFileSystemSource(testOpts)
	require.NoError(t, err)

	src, ok := fsSrc.(*fileSystemSource)
	require.True(t, ok)

	src.newReaderFn = func(
		b pool.CheckedBytesPool,
		opts fs.Options,
	) (fs.DataFileSetReader, error) {
		return reader, nil
	}

	shard := uint32(0)
	writeTSDBFiles(t, dir, testNs1ID, shard, testStart, []testSeries{
		{"foo", nil, []byte{0x1}},
	})

	rOpts := fs.ReaderOpenOptionsMatcher{
		ID: fs.FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      shard,
			BlockStart: testStart,
		},
	}

	reader.EXPECT().Open(rOpts).Return(nil).AnyTimes()
	reader.EXPECT().ReadMetadata().Return(ident.StringID("foo"),
		ident.NewTagsIterator(ident.Tags{}), 0, uint32(0), nil)
	reader.EXPECT().ReadMetadata().Return(ident.StringID("bar"),
		ident.NewTagsIterator(ident.Tags{}), 0, uint32(0), errors.New("foo"))

	reader.EXPECT().
		Range().
		Return(xtime.Range{
			Start: testStart,
			End:   testStart.Add(2 * time.Hour),
		}).AnyTimes()
	reader.EXPECT().Entries().Return(2).AnyTimes()
	reader.EXPECT().
		Read().
		Return(ident.StringID("foo"), ident.EmptyTagIterator,
			nil, digest.Checksum(nil), nil)

	reader.EXPECT().
		Read().
		Return(ident.StringID("bar"), ident.EmptyTagIterator,
			nil, uint32(0), errors.New("foo"))
	reader.EXPECT().Close().Return(nil).AnyTimes()

	nsMD := testNsMetadata(t)
	ranges := testShardTimeRanges()
	tester := bootstrap.BuildNamespacesTesterWithFilesystemOptions(t, testDefaultRunOpts,
		ranges, testOpts.FilesystemOptions(), nsMD)
	defer tester.Finish()

	tester.TestReadWith(src)
	tester.TestUnfulfilledForNamespace(nsMD, ranges, ranges)
	tester.EnsureNoWrites()
}

func TestReadTags(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	id := "foo"
	tags := map[string]string{
		"bar": "baz",
		"qux": "qaz",
	}
	data := []byte{0x1}

	writeTSDBFiles(t, dir, testNs1ID, testShard, testStart, []testSeries{
		{id, tags, data},
	})

	testOpts := newTestOptions(t, dir)
	src, err := newFileSystemSource(testOpts)
	require.NoError(t, err)

	nsMD := testNsMetadata(t)
	ranges := testShardTimeRanges()
	tester := bootstrap.BuildNamespacesTesterWithFilesystemOptions(t, testDefaultRunOpts,
		ranges, testOpts.FilesystemOptions(), nsMD)
	defer tester.Finish()

	tester.TestReadWith(src)
	readers := tester.EnsureDumpReadersForNamespace(nsMD)
	require.Equal(t, 1, len(readers))
	readersForTime, found := readers[id]
	require.True(t, found)
	require.Equal(t, 1, len(readersForTime))
	reader := readersForTime[0]
	require.Equal(t, tags, reader.Tags)
	tester.EnsureNoWrites()
}

func TestReadRunMigrations(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	// Write existing data filesets with legacy encoding
	eOpts := msgpack.LegacyEncodingOptions{
		EncodeLegacyIndexInfoVersion:  msgpack.LegacyEncodingIndexVersionV4,      // MinorVersion 0
		EncodeLegacyIndexEntryVersion: msgpack.LegacyEncodingIndexEntryVersionV2, // No checksum data
	}
	writeGoodFilesWithFsOpts(t, testNs1ID, testShard, newTestFsOptions(dir).SetEncodingOptions(eOpts))

	opts := newTestOptions(t, dir)
	sOpts, closer := newTestStorageOptions(t, opts.PersistManager(), opts.IndexClaimsManager())
	defer closer()

	src, err := newFileSystemSource(opts.
		SetMigrationOptions(migration.NewOptions().
			SetTargetMigrationVersion(migration.MigrationVersion_1_1).
			SetConcurrency(2)). // Lower concurrency to ensure workers process more than 1 migration.
		SetStorageOptions(sOpts))
	require.NoError(t, err)

	validateReadResults(t, src, dir, testShardTimeRanges())
}

func newTestStorageOptions(
	t *testing.T,
	pm persist.Manager,
	icm fs.IndexClaimsManager,
) (storage.Options, index.Closer) {
	plCache, closer, err := index.NewPostingsListCache(1, index.PostingsListCacheOptions{
		InstrumentOptions: instrument.NewOptions(),
	})
	require.NoError(t, err)

	md, err := namespace.NewMetadata(testNs1ID, testNamespaceOptions)
	require.NoError(t, err)

	return storage.NewOptions().
		SetPersistManager(pm).
		SetIndexClaimsManager(icm).
		SetNamespaceInitializer(namespace.NewStaticInitializer([]namespace.Metadata{md})).
		SetRepairEnabled(false).
		SetIndexOptions(index.NewOptions().
			SetPostingsListCache(plCache)).
		SetBlockLeaseManager(block.NewLeaseManager(nil)), closer
}
