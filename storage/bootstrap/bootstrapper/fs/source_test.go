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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/persist/encoding/msgpack"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testShard            = uint32(0)
	testNamespaceID      = ts.StringID("testNs")
	testStart            = time.Now()
	testBlockSize        = 2 * time.Hour
	testFileMode         = os.FileMode(0666)
	testDirMode          = os.ModeDir | os.FileMode(0755)
	testWriterBufferSize = 10
	testDefaultRunOpts   = bootstrap.NewRunOptions().SetIncremental(false)
)

func createTempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "foo")
	require.NoError(t, err)
	return dir
}

func writeInfoFile(t *testing.T, prefix string, namespace ts.ID, shard uint32, start time.Time, data []byte) {
	shardDir := fs.ShardDirPath(prefix, namespace, shard)
	filePath := path.Join(shardDir, fmt.Sprintf("fileset-%d-info.db", xtime.ToNanoseconds(start)))
	writeFile(t, filePath, data)
}

func writeDataFile(t *testing.T, prefix string, namespace ts.ID, shard uint32, start time.Time, data []byte) {
	shardDir := fs.ShardDirPath(prefix, namespace, shard)
	filePath := path.Join(shardDir, fmt.Sprintf("fileset-%d-data.db", xtime.ToNanoseconds(start)))
	writeFile(t, filePath, data)
}

func writeDigestFile(t *testing.T, prefix string, namespace ts.ID, shard uint32, start time.Time, data []byte) {
	shardDir := fs.ShardDirPath(prefix, namespace, shard)
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
	return xtime.NewRanges().AddRange(xtime.Range{Start: testStart, End: testStart.Add(11 * time.Hour)})
}

func testShardTimeRanges() result.ShardTimeRanges {
	return map[uint32]xtime.Ranges{testShard: testTimeRanges()}
}

func writeGoodFiles(t *testing.T, dir string, namespace ts.ID, shard uint32) {
	inputs := []struct {
		start time.Time
		id    string
		data  []byte
	}{
		{testStart, "foo", []byte{1, 2, 3}},
		{testStart.Add(10 * time.Hour), "bar", []byte{4, 5, 6}},
		{testStart.Add(20 * time.Hour), "baz", []byte{7, 8, 9}},
	}

	for _, input := range inputs {
		writeTSDBFiles(t, dir, namespace, shard, input.start, input.id, input.data)
	}
}

func writeTSDBFiles(t *testing.T, dir string, namespace ts.ID, shard uint32, start time.Time, id string, data []byte) {
	w := fs.NewWriter(testBlockSize, dir, testWriterBufferSize, testFileMode, testDirMode)
	require.NoError(t, w.Open(namespace, shard, start))

	bytes := checked.NewBytes(data, nil)
	bytes.IncRef()
	require.NoError(t, w.Write(ts.StringID(id), bytes, digest.Checksum(bytes.Get())))
	require.NoError(t, w.Close())
}

func rangesArray(ranges xtime.Ranges) []xtime.Range {
	var array []xtime.Range
	iter := ranges.Iter()
	for iter.Next() {
		array = append(array, iter.Value())
	}
	return array
}

func validateTimeRanges(t *testing.T, tr xtime.Ranges, expected []xtime.Range) {
	require.Equal(t, len(expected), tr.Len())
	it := tr.Iter()
	idx := 0
	for it.Next() {
		require.Equal(t, expected[idx], it.Value())
		idx++
	}
}

func TestAvailableEmptyRangeError(t *testing.T) {
	src := newFileSystemSource("foo", NewOptions())
	res := src.Available(testNamespaceID, map[uint32]xtime.Ranges{0: nil})
	require.NotNil(t, res)
	require.True(t, res.IsEmpty())
}

func TestAvailablePatternError(t *testing.T) {
	src := newFileSystemSource("[[", NewOptions())
	res := src.Available(testNamespaceID, testShardTimeRanges())
	require.NotNil(t, res)
	require.True(t, res.IsEmpty())
}

func TestAvailableReadInfoError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeTSDBFiles(t, dir, testNamespaceID, shard, testStart, "foo", []byte{0x1})
	// Intentionally corrupt the info file
	writeInfoFile(t, dir, testNamespaceID, shard, testStart, []byte{0x1, 0x2})

	src := newFileSystemSource(dir, NewOptions())
	res := src.Available(testNamespaceID, testShardTimeRanges())
	require.NotNil(t, res)
	require.True(t, res.IsEmpty())
}

func TestAvailableDigestOfDigestMismatch(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeTSDBFiles(t, dir, testNamespaceID, shard, testStart, "foo", []byte{0x1})
	// Intentionally corrupt the digest file
	writeDigestFile(t, dir, testNamespaceID, shard, testStart, nil)

	src := newFileSystemSource(dir, NewOptions())
	res := src.Available(testNamespaceID, testShardTimeRanges())
	require.NotNil(t, res)
	require.True(t, res.IsEmpty())
}

func TestAvailableTimeRangeFilter(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeGoodFiles(t, dir, testNamespaceID, shard)

	src := newFileSystemSource(dir, NewOptions())
	res := src.Available(testNamespaceID, testShardTimeRanges())
	require.NotNil(t, res)
	require.Equal(t, 1, len(res))
	require.NotNil(t, res[testShard])

	expected := []xtime.Range{
		{Start: testStart, End: testStart.Add(2 * time.Hour)},
		{Start: testStart.Add(10 * time.Hour), End: testStart.Add(12 * time.Hour)},
	}
	validateTimeRanges(t, res[testShard], expected)
}

func TestAvailableTimeRangePartialError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeGoodFiles(t, dir, testNamespaceID, shard)
	// Intentionally write a corrupted info file
	writeInfoFile(t, dir, testNamespaceID, shard, testStart.Add(4*time.Hour), []byte{0x1, 0x2})

	src := newFileSystemSource(dir, NewOptions())
	res := src.Available(testNamespaceID, testShardTimeRanges())
	require.NotNil(t, res)
	require.Equal(t, 1, len(res))
	require.NotNil(t, res[testShard])

	expected := []xtime.Range{
		{Start: testStart, End: testStart.Add(2 * time.Hour)},
		{Start: testStart.Add(10 * time.Hour), End: testStart.Add(12 * time.Hour)},
	}
	validateTimeRanges(t, res[testShard], expected)
}

func TestReadEmptyRangeErr(t *testing.T) {
	src := newFileSystemSource("foo", NewOptions())
	res, err := src.Read(testNamespaceID, nil, testDefaultRunOpts)
	require.NoError(t, err)
	require.Nil(t, res)
}

func TestReadPatternError(t *testing.T) {
	src := newFileSystemSource("[[", NewOptions())
	res, err := src.Read(testNamespaceID,
		map[uint32]xtime.Ranges{testShard: xtime.NewRanges()},
		testDefaultRunOpts)
	require.NoError(t, err)
	require.Nil(t, res)
}

func TestReadNilTimeRanges(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeGoodFiles(t, dir, testNamespaceID, shard)

	src := newFileSystemSource(dir, NewOptions())

	validateReadResults(t, src, dir, map[uint32]xtime.Ranges{
		testShard: testTimeRanges(),
		555:       nil,
	})
}

func TestReadOpenFileError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeTSDBFiles(t, dir, testNamespaceID, shard, testStart, "foo", []byte{0x1})
	// Intentionally truncate the info file
	writeInfoFile(t, dir, testNamespaceID, shard, testStart, nil)

	src := newFileSystemSource(dir, NewOptions())
	res, err := src.Read(testNamespaceID, testShardTimeRanges(),
		testDefaultRunOpts)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Unfulfilled())
	require.NotNil(t, res.Unfulfilled()[testShard])

	expected := []xtime.Range{
		{Start: testStart, End: testStart.Add(11 * time.Hour)},
	}
	validateTimeRanges(t, res.Unfulfilled()[testShard], expected)
}

func TestReadDataCorruptionError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeTSDBFiles(t, dir, testNamespaceID, shard, testStart, "foo", []byte{0x1})
	// Intentionally corrupt the data file
	writeDataFile(t, dir, testNamespaceID, shard, testStart, []byte{0x1})

	src := newFileSystemSource(dir, NewOptions())
	strs := testShardTimeRanges()
	res, err := src.Read(testNamespaceID, strs, testDefaultRunOpts)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 0, len(res.ShardResults()))
	require.Equal(t, 1, len(res.Unfulfilled()))
	validateTimeRanges(t, res.Unfulfilled()[testShard], rangesArray(strs[testShard]))
}

func validateReadResults(
	t *testing.T,
	src bootstrap.Source,
	dir string,
	strs result.ShardTimeRanges,
) {
	expected := []xtime.Range{
		{
			Start: testStart.Add(2 * time.Hour),
			End:   testStart.Add(10 * time.Hour),
		},
	}

	res, err := src.Read(testNamespaceID, strs, testDefaultRunOpts)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.ShardResults())
	require.NotNil(t, res.ShardResults()[testShard])
	allSeries := res.ShardResults()[testShard].AllSeries()
	require.Equal(t, 2, len(allSeries))
	require.NotNil(t, res.Unfulfilled())
	require.NotNil(t, res.Unfulfilled()[testShard])
	validateTimeRanges(t, res.Unfulfilled()[testShard], expected)

	require.Equal(t, 2, len(allSeries))

	ids := []ts.Hash{
		ts.StringID("foo").Hash(), ts.StringID("bar").Hash()}
	data := [][]byte{
		{1, 2, 3},
		{4, 5, 6},
	}
	times := []time.Time{testStart, testStart.Add(10 * time.Hour)}
	for i, id := range ids {
		allBlocks := allSeries[id].Blocks.AllBlocks()
		require.Equal(t, 1, len(allBlocks))
		block := allBlocks[times[i]]
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

	writeGoodFiles(t, dir, testNamespaceID, testShard)

	src := newFileSystemSource(dir, NewOptions())
	validateReadResults(t, src, dir, testShardTimeRanges())
}

func TestReadPartialError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	writeGoodFiles(t, dir, testNamespaceID, testShard)
	// Intentionally corrupt the data file
	writeDataFile(t, dir, testNamespaceID, testShard, testStart.Add(4*time.Hour), []byte{0x1})

	src := newFileSystemSource(dir, NewOptions())
	validateReadResults(t, src, dir, testShardTimeRanges())
}

func TestReadValidateError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	reader := fs.NewMockFileSetReader(ctrl)
	src := newFileSystemSource(dir, NewOptions()).(*fileSystemSource)
	src.newReaderFn = func(
		filePathPrefix string,
		readerBufferSize int,
		b pool.CheckedBytesPool,
		decodingOpts msgpack.DecodingOptions,
	) fs.FileSetReader {
		return reader
	}

	shard := uint32(0)
	writeTSDBFiles(t, dir, testNamespaceID, shard, testStart,
		"foo", []byte{0x1})
	reader.EXPECT().
		Open(testNamespaceID, shard, testStart).
		Return(nil)
	reader.EXPECT().
		Range().
		Return(xtime.Range{
			Start: testStart,
			End:   testStart.Add(2 * time.Hour),
		}).
		Times(2)
	reader.EXPECT().Entries().Return(0).Times(2)
	reader.EXPECT().Validate().Return(errors.New("foo"))
	reader.EXPECT().Close().Return(nil)

	res, err := src.Read(testNamespaceID, testShardTimeRanges(),
		testDefaultRunOpts)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 0, len(res.ShardResults()))
	require.NotNil(t, res.Unfulfilled())
	require.NotNil(t, res.Unfulfilled()[testShard])

	expected := []xtime.Range{
		{Start: testStart, End: testStart.Add(11 * time.Hour)},
	}
	validateTimeRanges(t, res.Unfulfilled()[testShard], expected)
}

func TestReadDeleteOnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	reader := fs.NewMockFileSetReader(ctrl)
	src := newFileSystemSource(dir, NewOptions()).(*fileSystemSource)
	src.newReaderFn = func(
		filePathPrefix string,
		readerBufferSize int,
		b pool.CheckedBytesPool,
		decodingOpts msgpack.DecodingOptions,
	) fs.FileSetReader {
		return reader
	}

	shard := uint32(0)
	writeTSDBFiles(t, dir, testNamespaceID, shard, testStart,
		"foo", []byte{0x1})

	gomock.InOrder(
		reader.EXPECT().Open(testNamespaceID, shard, testStart).Return(nil),
		reader.EXPECT().
			Range().
			Return(xtime.Range{
				Start: testStart,
				End:   testStart.Add(2 * time.Hour),
			}),
		reader.EXPECT().Entries().Return(2),
		reader.EXPECT().
			Range().
			Return(xtime.Range{
				Start: testStart,
				End:   testStart.Add(2 * time.Hour),
			}),
		reader.EXPECT().Entries().Return(2),
		reader.EXPECT().
			Read().
			Return(ts.StringID("foo"), nil, digest.Checksum(nil), nil),
		reader.EXPECT().
			Read().
			Return(ts.StringID("bar"), nil, uint32(0), errors.New("foo")),
		reader.EXPECT().Close().Return(nil),
	)

	res, err := src.Read(testNamespaceID, testShardTimeRanges(),
		testDefaultRunOpts)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 0, len(res.ShardResults()))
	require.NotNil(t, res.Unfulfilled())
	require.NotNil(t, res.Unfulfilled()[testShard])

	expected := []xtime.Range{
		{Start: testStart, End: testStart.Add(11 * time.Hour)},
	}
	validateTimeRanges(t, res.Unfulfilled()[testShard], expected)
}
