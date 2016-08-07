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
	"strconv"
	"testing"
	"time"

	"github.com/m3db/m3db/generated/mocks/mocks"
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
	testStart            = time.Now()
	testBlockSize        = 2 * time.Hour
	testFileMode         = os.FileMode(0666)
	testWriterBufferSize = 10
)

func createTempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "foo")
	require.NoError(t, err)
	return dir
}

func getShardDirPath(prefix string, shard uint32) string {
	return path.Join(prefix, strconv.Itoa(int(shard)))
}

func writeInfoFile(t *testing.T, prefix string, shard uint32, start time.Time, data []byte) {
	filePath := path.Join(getShardDirPath(prefix, shard), fmt.Sprintf("%d-info.db", xtime.ToNanoseconds(start)))
	writeFile(t, filePath, data)
}

func writeDataFile(t *testing.T, prefix string, shard uint32, start time.Time, data []byte) {
	filePath := path.Join(getShardDirPath(prefix, shard), fmt.Sprintf("%d-data.db", xtime.ToNanoseconds(start)))
	writeFile(t, filePath, data)
}

func writeDigestFile(t *testing.T, prefix string, shard uint32, start time.Time, data []byte) {
	filePath := path.Join(getShardDirPath(prefix, shard), fmt.Sprintf("%d-digest.db", xtime.ToNanoseconds(start)))
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

func createTimeRanges() xtime.Ranges {
	return xtime.NewRanges().AddRange(xtime.Range{Start: testStart, End: testStart.Add(11 * time.Hour)})
}

func writeGoodFiles(t *testing.T, dir string, shard uint32) {
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
		writeTSDBFiles(t, dir, shard, input.start, input.id, input.data)
	}
}

func writeTSDBFiles(t *testing.T, dir string, shard uint32, start time.Time, id string, data []byte) {
	w := fs.NewWriter(testBlockSize, dir, testWriterBufferSize, nil)
	require.NoError(t, w.Open(shard, start))
	require.NoError(t, w.Write(id, data))
	require.NoError(t, w.Close())
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

func TestGetAvailabilityEmptyRangeError(t *testing.T) {
	fss := newFileSystemSource("foo", storage.NewDatabaseOptions())
	res := fss.GetAvailability(0, nil)
	require.Nil(t, res)
}

func TestGetAvailabilityPatternError(t *testing.T) {
	fss := newFileSystemSource("[[", storage.NewDatabaseOptions())
	res := fss.GetAvailability(0, createTimeRanges())
	require.Nil(t, res)
}

func TestGetAvailabilityReadInfoError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeTSDBFiles(t, dir, shard, testStart, "foo", []byte{0x1})
	// Intentionally corrupt the info file
	writeInfoFile(t, dir, shard, testStart, []byte{0x1, 0x2})

	fss := newFileSystemSource(dir, storage.NewDatabaseOptions())
	res := fss.GetAvailability(shard, createTimeRanges())
	require.Nil(t, res)
}

func TestGetAvailabilityDigestOfDigestMismatch(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeTSDBFiles(t, dir, shard, testStart, "foo", []byte{0x1})
	// Intentionally corrupt the digest file
	writeDigestFile(t, dir, shard, testStart, nil)

	fss := newFileSystemSource(dir, storage.NewDatabaseOptions())
	res := fss.GetAvailability(shard, createTimeRanges())
	require.Nil(t, res)
}

func TestGetAvailabilityTimeRangeFilter(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeGoodFiles(t, dir, shard)

	fss := newFileSystemSource(dir, storage.NewDatabaseOptions())
	res := fss.GetAvailability(shard, createTimeRanges())

	expected := []xtime.Range{
		{Start: testStart, End: testStart.Add(2 * time.Hour)},
		{Start: testStart.Add(10 * time.Hour), End: testStart.Add(12 * time.Hour)},
	}
	validateTimeRanges(t, res, expected)
}

func TestGetAvailabilityTimeRangePartialError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeGoodFiles(t, dir, shard)
	// Intentionally write a corrupted info file
	writeInfoFile(t, dir, shard, testStart.Add(4*time.Hour), []byte{0x1, 0x2})

	fss := newFileSystemSource(dir, storage.NewDatabaseOptions())
	res := fss.GetAvailability(shard, createTimeRanges())

	expected := []xtime.Range{
		{Start: testStart, End: testStart.Add(2 * time.Hour)},
		{Start: testStart.Add(10 * time.Hour), End: testStart.Add(12 * time.Hour)},
	}
	validateTimeRanges(t, res, expected)
}

func TestReadDataEmptyRangeErr(t *testing.T) {
	fss := newFileSystemSource("foo", storage.NewDatabaseOptions())
	res, unfulfilled := fss.ReadData(0, nil)
	require.Nil(t, res)
	require.Nil(t, unfulfilled)
}

func TestReadDataPatternError(t *testing.T) {
	fss := newFileSystemSource("[[", storage.NewDatabaseOptions())
	res, unfulfilled := fss.ReadData(0, xtime.NewRanges())
	require.Nil(t, res)
	require.True(t, unfulfilled.IsEmpty())
}

func TestReadDataOpenFileError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeTSDBFiles(t, dir, shard, testStart, "foo", []byte{0x1})
	// Intentionally truncate the info file
	writeInfoFile(t, dir, shard, testStart, nil)

	fss := newFileSystemSource(dir, storage.NewDatabaseOptions())
	res, unfulfilled := fss.ReadData(shard, createTimeRanges())
	require.Nil(t, res)
	expected := []xtime.Range{
		{Start: testStart, End: testStart.Add(11 * time.Hour)},
	}
	validateTimeRanges(t, unfulfilled, expected)
}

func TestReadDataDataCorruptionError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeTSDBFiles(t, dir, shard, testStart, "foo", []byte{0x1})
	// Intentionally corrupt the data file
	writeDataFile(t, dir, shard, testStart, []byte{0x1})

	fss := newFileSystemSource(dir, storage.NewDatabaseOptions())
	tr := createTimeRanges()
	res, unfulfilled := fss.ReadData(shard, tr)
	require.True(t, res.IsEmpty())
	require.Equal(t, tr, unfulfilled)
}

func validateReadResults(t *testing.T, fss m3db.Source, dir string, shard uint32) {
	tr := createTimeRanges()
	expected := []xtime.Range{
		{Start: testStart.Add(2 * time.Hour), End: testStart.Add(10 * time.Hour)},
	}
	res, unfulfilled := fss.ReadData(shard, tr)
	require.Equal(t, 2, len(res.GetAllSeries()))
	validateTimeRanges(t, unfulfilled, expected)

	allSeries := res.GetAllSeries()
	require.Equal(t, 2, len(allSeries))

	ids := []string{"foo", "bar"}
	data := [][]byte{
		{1, 2, 3},
		{4, 5, 6},
	}
	times := []time.Time{testStart, testStart.Add(10 * time.Hour)}
	for i, id := range ids {
		allBlocks := allSeries[id].GetAllBlocks()
		require.Equal(t, 1, len(allBlocks))
		block := allBlocks[times[i]]
		stream, err := block.Stream(nil)
		require.NoError(t, err)
		var b [100]byte
		n, err := stream.Read(b[:])
		require.NoError(t, err)
		require.Equal(t, data[i], b[:n])
	}
}

func TestReadDataTimeFilter(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeGoodFiles(t, dir, shard)

	fss := newFileSystemSource(dir, storage.NewDatabaseOptions())
	validateReadResults(t, fss, dir, shard)
}

func TestReadDataPartialError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shard := uint32(0)
	writeGoodFiles(t, dir, shard)
	// Intentionally corrupt the data file
	writeDataFile(t, dir, shard, testStart.Add(4*time.Hour), []byte{0x1})

	fss := newFileSystemSource(dir, storage.NewDatabaseOptions())
	validateReadResults(t, fss, dir, shard)
}

func TestReadDataValidateError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	reader := mocks.NewMockFileSetReader(ctrl)
	fss := newFileSystemSource(dir, storage.NewDatabaseOptions()).(*fileSystemSource)
	fss.newReaderFn = func(filePathPrefix string, readerBufferSize int) m3db.FileSetReader {
		return reader
	}

	shard := uint32(0)
	writeTSDBFiles(t, dir, shard, testStart, "foo", []byte{0x1})
	reader.EXPECT().Open(shard, testStart).Return(nil)
	reader.EXPECT().Range().Return(xtime.Range{Start: testStart, End: testStart.Add(2 * time.Hour)})
	reader.EXPECT().Entries().Return(0)
	reader.EXPECT().Validate().Return(errors.New("foo"))
	reader.EXPECT().Close().Return(nil)

	res, unfulfilled := fss.ReadData(shard, createTimeRanges())
	require.True(t, res.IsEmpty())
	expected := []xtime.Range{
		{Start: testStart, End: testStart.Add(11 * time.Hour)},
	}
	validateTimeRanges(t, unfulfilled, expected)
}
