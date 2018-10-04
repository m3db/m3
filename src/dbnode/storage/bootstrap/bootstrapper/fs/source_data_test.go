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
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
	testShard            = uint32(0)
	testNs1ID            = ident.StringID("testNs")
	testBlockSize        = 2 * time.Hour
	testIndexBlockSize   = 4 * time.Hour
	testStart            = time.Now().Truncate(testBlockSize)
	testFileMode         = os.FileMode(0666)
	testDirMode          = os.ModeDir | os.FileMode(0755)
	testWriterBufferSize = 10
	testDefaultRunOpts   = bootstrap.NewRunOptions().
				SetPersistConfig(bootstrap.PersistConfig{Enabled: false})
	testDefaultResultOpts = result.NewOptions().SetSeriesCachePolicy(series.CacheAll)
	testDefaultOpts       = NewOptions().SetResultOptions(testDefaultResultOpts)
)

func newTestOptions(filePathPrefix string) Options {
	return testDefaultOpts.
		SetFilesystemOptions(newTestFsOptions(filePathPrefix))
}

func newTestOptionsWithPersistManager(t *testing.T, filePathPrefix string) Options {
	opts := newTestOptions(filePathPrefix)
	pm, err := fs.NewPersistManager(opts.FilesystemOptions())
	require.NoError(t, err)
	return opts.SetPersistManager(pm)
}

func newTestFsOptions(filePathPrefix string) fs.Options {
	return fs.NewOptions().
		SetFilePathPrefix(filePathPrefix).
		SetWriterBufferSize(testWriterBufferSize).
		SetNewFileMode(testFileMode).
		SetNewDirectoryMode(testDirMode)
}

func testNsMetadata(t *testing.T) namespace.Metadata {
	ropts := retention.NewOptions().SetBlockSize(testBlockSize)
	md, err := namespace.NewMetadata(testNs1ID, namespace.NewOptions().
		SetRetentionOptions(ropts).
		SetIndexOptions(namespace.NewIndexOptions().
			SetEnabled(true).
			SetBlockSize(testIndexBlockSize)))
	require.NoError(t, err)
	return md
}

func createTempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "foo")
	require.NoError(t, err)
	return dir
}

func writeInfoFile(t *testing.T, prefix string, namespace ident.ID, shard uint32, start time.Time, data []byte) {
	shardDir := fs.ShardDataDirPath(prefix, namespace, shard)
	filePath := path.Join(shardDir, fmt.Sprintf("fileset-%d-info.db", xtime.ToNanoseconds(start)))
	writeFile(t, filePath, data)
}

func writeDataFile(t *testing.T, prefix string, namespace ident.ID, shard uint32, start time.Time, data []byte) {
	shardDir := fs.ShardDataDirPath(prefix, namespace, shard)
	filePath := path.Join(shardDir, fmt.Sprintf("fileset-%d-data.db", xtime.ToNanoseconds(start)))
	writeFile(t, filePath, data)
}

func writeDigestFile(t *testing.T, prefix string, namespace ident.ID, shard uint32, start time.Time, data []byte) {
	shardDir := fs.ShardDataDirPath(prefix, namespace, shard)
	filePath := path.Join(shardDir, fmt.Sprintf("fileset-%d-digest.db", xtime.ToNanoseconds(start)))
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
	return map[uint32]xtime.Ranges{testShard: testTimeRanges()}
}

func writeGoodFiles(t *testing.T, dir string, namespace ident.ID, shard uint32) {
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
		writeTSDBFiles(t, dir, namespace, shard, input.start,
			[]testSeries{{input.id, input.tags, input.data}})
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
	t *testing.T,
	dir string,
	namespace ident.ID,
	shard uint32,
	start time.Time,
	series []testSeries,
) {
	w, err := fs.NewWriter(newTestFsOptions(dir))
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
		require.NoError(t, w.Write(ident.StringID(v.id),
			sortedTagsFromTagsMap(v.tags), bytes, digest.Checksum(bytes.Bytes())))
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
	require.True(t, expected.RemoveRanges(tr).IsEmpty())

	// Now make sure no ranges outside of expected
	expectedWithAddedRanges := expected.AddRanges(tr)

	require.Equal(t, expected.Len(), expectedWithAddedRanges.Len())
	iter := expected.Iter()
	withAddedRangesIter := expectedWithAddedRanges.Iter()
	for iter.Next() && withAddedRangesIter.Next() {
		require.True(t, iter.Value().Equal(withAddedRangesIter.Value()))
	}
}

func TestAvailableEmptyRangeError(t *testing.T) {
	src := newFileSystemSource(newTestOptions("foo"))
	res, err := src.AvailableData(
		testNsMetadata(t),
		map[uint32]xtime.Ranges{0: xtime.Ranges{}},
		testDefaultRunOpts,
	)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.True(t, res.IsEmpty())
}

func TestAvailablePatternError(t *testing.T) {
	src := newFileSystemSource(newTestOptions("[["))
	res, err := src.AvailableData(
		testNsMetadata(t),
		testShardTimeRanges(),
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

	src := newFileSystemSource(newTestOptions(dir))
	res, err := src.AvailableData(
		testNsMetadata(t),
		testShardTimeRanges(),
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

	src := newFileSystemSource(newTestOptions(dir))
	res, err := src.AvailableData(
		testNsMetadata(t),
		testShardTimeRanges(),
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

	src := newFileSystemSource(newTestOptions(dir))
	res, err := src.AvailableData(
		testNsMetadata(t),
		testShardTimeRanges(),
		testDefaultRunOpts,
	)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 1, len(res))
	require.NotNil(t, res[testShard])

	expected := xtime.Ranges{}.
		AddRange(xtime.Range{Start: testStart, End: testStart.Add(2 * time.Hour)}).
		AddRange(xtime.Range{Start: testStart.Add(10 * time.Hour), End: testStart.Add(12 * time.Hour)})
	validateTimeRanges(t, res[testShard], expected)
}

func TestAvailableTimeRangePartialError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeGoodFiles(t, dir, testNs1ID, shard)
	// Intentionally write a corrupted info file
	writeInfoFile(t, dir, testNs1ID, shard, testStart.Add(4*time.Hour), []byte{0x1, 0x2})

	src := newFileSystemSource(newTestOptions(dir))
	res, err := src.AvailableData(
		testNsMetadata(t),
		testShardTimeRanges(),
		testDefaultRunOpts,
	)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 1, len(res))
	require.NotNil(t, res[testShard])

	expected := xtime.Ranges{}.
		AddRange(xtime.Range{Start: testStart, End: testStart.Add(2 * time.Hour)}).
		AddRange(xtime.Range{Start: testStart.Add(10 * time.Hour), End: testStart.Add(12 * time.Hour)})
	validateTimeRanges(t, res[testShard], expected)
}

func TestReadEmptyRangeErr(t *testing.T) {
	src := newFileSystemSource(newTestOptions("foo"))
	res, err := src.ReadData(testNsMetadata(t), nil, testDefaultRunOpts)
	require.NoError(t, err)
	require.Equal(t, 0, len(res.ShardResults()))
	require.True(t, res.Unfulfilled().IsEmpty())
}

func TestReadPatternError(t *testing.T) {
	src := newFileSystemSource(newTestOptions("[["))
	res, err := src.ReadData(testNsMetadata(t),
		map[uint32]xtime.Ranges{testShard: xtime.Ranges{}},
		testDefaultRunOpts)
	require.NoError(t, err)
	require.Equal(t, 0, len(res.ShardResults()))
	require.True(t, res.Unfulfilled().IsEmpty())
}

func TestReadNilTimeRanges(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeGoodFiles(t, dir, testNs1ID, shard)

	src := newFileSystemSource(newTestOptions(dir))

	validateReadResults(t, src, dir, map[uint32]xtime.Ranges{
		testShard: testTimeRanges(),
		555:       xtime.Ranges{},
	})
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

	src := newFileSystemSource(newTestOptions(dir))
	res, err := src.ReadData(testNsMetadata(t), testShardTimeRanges(),
		testDefaultRunOpts)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Unfulfilled())
	require.NotNil(t, res.Unfulfilled()[testShard])

	expected := xtime.Ranges{}.
		AddRange(xtime.Range{Start: testStart, End: testStart.Add(11 * time.Hour)})
	validateTimeRanges(t, res.Unfulfilled()[testShard], expected)
}

func TestReadDataCorruptionError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeTSDBFiles(t, dir, testNs1ID, shard, testStart, []testSeries{
		{"foo", nil, []byte{0x1}},
	})
	// Intentionally corrupt the data file
	writeDataFile(t, dir, testNs1ID, shard, testStart, []byte{0x2})

	src := newFileSystemSource(newTestOptions(dir))
	strs := testShardTimeRanges()
	res, err := src.ReadData(testNsMetadata(t), strs, testDefaultRunOpts)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 0, len(res.ShardResults()))
	require.Equal(t, 1, len(res.Unfulfilled()))
	validateTimeRanges(t, res.Unfulfilled()[testShard], strs[testShard])
}

func validateReadResults(
	t *testing.T,
	src bootstrap.Source,
	dir string,
	strs result.ShardTimeRanges,
) {
	expected := xtime.Ranges{}.
		AddRange(xtime.Range{
			Start: testStart.Add(2 * time.Hour),
			End:   testStart.Add(10 * time.Hour),
		})

	res, err := src.ReadData(testNsMetadata(t), strs, testDefaultRunOpts)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.ShardResults())
	require.NotNil(t, res.ShardResults()[testShard])
	allSeries := res.ShardResults()[testShard].AllSeries()
	require.Equal(t, 2, allSeries.Len())
	require.NotNil(t, res.Unfulfilled())
	require.NotNil(t, res.Unfulfilled()[testShard])
	validateTimeRanges(t, res.Unfulfilled()[testShard], expected)

	require.Equal(t, 2, allSeries.Len())

	ids := []ident.ID{
		ident.StringID("foo"), ident.StringID("bar")}
	data := [][]byte{
		{1, 2, 3},
		{4, 5, 6},
	}
	times := []time.Time{testStart, testStart.Add(10 * time.Hour)}
	for i, id := range ids {
		series, ok := allSeries.Get(id)
		require.True(t, ok)
		allBlocks := series.Blocks.AllBlocks()
		require.Equal(t, 1, len(allBlocks))
		block := allBlocks[xtime.ToUnixNano(times[i])]
		ctx := context.NewContext()
		stream, err := block.Stream(ctx)
		require.NoError(t, err)
		var b [100]byte
		n, err := stream.Read(b[:])
		ctx.Close()
		require.NoError(t, err)
		require.Equal(t, data[i], b[:n])
	}
}

func TestReadTimeFilter(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	writeGoodFiles(t, dir, testNs1ID, testShard)

	src := newFileSystemSource(newTestOptions(dir))
	validateReadResults(t, src, dir, testShardTimeRanges())
}

func TestReadPartialError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	writeGoodFiles(t, dir, testNs1ID, testShard)
	// Intentionally corrupt the data file
	writeDataFile(t, dir, testNs1ID, testShard, testStart.Add(4*time.Hour), []byte{0x1})

	src := newFileSystemSource(newTestOptions(dir))
	validateReadResults(t, src, dir, testShardTimeRanges())
}

func TestReadValidateError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	reader := fs.NewMockDataFileSetReader(ctrl)
	src := newFileSystemSource(newTestOptions(dir)).(*fileSystemSource)
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
	src.newReaderPoolOpts.disableReuse = true

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
	reader.EXPECT().Entries().Return(0).Times(2)
	reader.EXPECT().Validate().Return(errors.New("foo"))
	reader.EXPECT().Close().Return(nil)

	res, err := src.ReadData(testNsMetadata(t), testShardTimeRanges(),
		testDefaultRunOpts)

	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 0, int(res.ShardResults().NumSeries()))
	require.NotNil(t, res.Unfulfilled())
	require.NotNil(t, res.Unfulfilled()[testShard])

	expected := xtime.Ranges{}.
		AddRange(xtime.Range{Start: testStart, End: testStart.Add(11 * time.Hour)})
	validateTimeRanges(t, res.Unfulfilled()[testShard], expected)
}

func TestReadOpenError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	reader := fs.NewMockDataFileSetReader(ctrl)
	src := newFileSystemSource(newTestOptions(dir)).(*fileSystemSource)
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

	res, err := src.ReadData(testNsMetadata(t), testShardTimeRanges(),
		testDefaultRunOpts)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 0, len(res.ShardResults()))
	require.NotNil(t, res.Unfulfilled())
	require.NotNil(t, res.Unfulfilled()[testShard])

	expected := xtime.Ranges{}.
		AddRange(xtime.Range{Start: testStart, End: testStart.Add(11 * time.Hour)})
	validateTimeRanges(t, res.Unfulfilled()[testShard], expected)
}

func TestReadDeleteOnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	reader := fs.NewMockDataFileSetReader(ctrl)
	src := newFileSystemSource(newTestOptions(dir)).(*fileSystemSource)
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
	gomock.InOrder(
		reader.EXPECT().Open(rOpts).Return(nil),
		reader.EXPECT().
			Range().
			Return(xtime.Range{
				Start: testStart,
				End:   testStart.Add(2 * time.Hour),
			}).AnyTimes(),
		reader.EXPECT().Entries().Return(2).AnyTimes(),
		reader.EXPECT().
			Range().
			Return(xtime.Range{
				Start: testStart,
				End:   testStart.Add(2 * time.Hour),
			}).AnyTimes(),
		reader.EXPECT().Entries().Return(2).AnyTimes(),
		reader.EXPECT().
			Read().
			Return(ident.StringID("foo"), ident.EmptyTagIterator, nil, digest.Checksum(nil), nil),
		reader.EXPECT().
			Read().
			Return(ident.StringID("bar"), ident.EmptyTagIterator, nil, uint32(0), errors.New("foo")),
		reader.EXPECT().Close().Return(nil),
	)

	res, err := src.ReadData(testNsMetadata(t), testShardTimeRanges(),
		testDefaultRunOpts)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 0, len(res.ShardResults()))
	require.NotNil(t, res.Unfulfilled())
	require.NotNil(t, res.Unfulfilled()[testShard])

	expected := xtime.Ranges{}.
		AddRange(xtime.Range{Start: testStart, End: testStart.Add(11 * time.Hour)})
	validateTimeRanges(t, res.Unfulfilled()[testShard], expected)
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

	src := newFileSystemSource(newTestOptions(dir))
	res, err := src.ReadData(testNsMetadata(t), testShardTimeRanges(),
		testDefaultRunOpts)
	require.NoError(t, err)

	require.Equal(t, 1, len(res.ShardResults()))
	require.NotNil(t, res.ShardResults()[testShard])

	series := res.ShardResults()[testShard]
	require.Equal(t, int64(1), series.NumSeries())

	fooSeries, ok := series.AllSeries().Get(ident.StringID(id))
	require.True(t, ok)
	require.True(t, fooSeries.ID.Equal(ident.StringID(id)))
	require.True(t, fooSeries.Tags.Equal(sortedTagsFromTagsMap(tags)))
}
