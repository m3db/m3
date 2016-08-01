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
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	schema "github.com/m3db/m3db/generated/proto/schema"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3x/time"

	"github.com/golang/protobuf/proto"
	"github.com/m3db/m3db/storage"
	"github.com/stretchr/testify/require"
)

var (
	testStart      = time.Now()
	testBlockSize  = 2 * time.Hour
	testBlockTimes = []time.Time{testStart, testStart.Add(10 * time.Hour), testStart.Add(20 * time.Hour)}
)

func createTempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "foo")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func createShardDir(t *testing.T, prefix string, shard int) string {
	shardDirPath := path.Join(prefix, strconv.Itoa(shard))
	err := os.Mkdir(shardDirPath, os.ModeDir|os.FileMode(0755))
	require.Nil(t, err)
	return shardDirPath
}

func createFile(t *testing.T, filePath string) *os.File {
	f, err := os.Create(filePath)
	require.NoError(t, err)
	return f
}

func createTimeRanges() xtime.Ranges {
	return xtime.NewRanges().AddRange(xtime.Range{Start: testStart, End: testStart.Add(11 * time.Hour)})
}

func createBadInfoFile(t *testing.T, shardDirPath string, blockStartTime time.Time) {
	f := createInfoFileWithCheckpoint(t, shardDirPath, blockStartTime)
	_, err := f.Write([]byte{0x1, 0x2})
	require.NoError(t, err)
	f.Close()
}

func createInfoFileWithCheckpoint(t *testing.T, shardDirPath string, blockStartTime time.Time) *os.File {
	checkpointPath := path.Join(shardDirPath, fmt.Sprintf("%d-checkpoint.db", xtime.ToNanoseconds(blockStartTime)))
	checkpointFile := createFile(t, checkpointPath)
	checkpointFile.Close()

	infoPath := path.Join(shardDirPath, fmt.Sprintf("%d-info.db", xtime.ToNanoseconds(blockStartTime)))
	infoFile := createFile(t, infoPath)
	return infoFile
}

func writeGoodInfoFiles(t *testing.T, shardDirPath string, blockStartTimes []time.Time) {
	for _, blockStartTime := range blockStartTimes {
		infoFile := createInfoFileWithCheckpoint(t, shardDirPath, blockStartTime)
		writeInfoFile(t, infoFile, blockStartTime)
		infoFile.Close()
	}
}

func writeInfoFile(t *testing.T, f *os.File, start time.Time) {
	info := &schema.IndexInfo{
		Start:     xtime.ToNanoseconds(start),
		BlockSize: int64(testBlockSize),
		Entries:   10,
	}
	data, err := proto.Marshal(info)
	require.NoError(t, err)

	_, err = f.Write(data)
	require.NoError(t, err)
}

func writeFilesForTimeRaw(t *testing.T, shardDirPath string, start time.Time, data []byte) {
	timeInNano := xtime.ToNanoseconds(start)

	f := createFile(t, path.Join(shardDirPath, fmt.Sprintf("%d-info.db", timeInNano)))
	writeInfoFile(t, f, start)
	f.Close()

	f = createFile(t, path.Join(shardDirPath, fmt.Sprintf("%d-index.db", timeInNano)))
	f.Write(data)
	f.Close()

	f = createFile(t, path.Join(shardDirPath, fmt.Sprintf("%d-data.db", timeInNano)))
	f.Close()

	f = createFile(t, path.Join(shardDirPath, fmt.Sprintf("%d-checkpoint.db", timeInNano)))
	f.Close()
}

func writeFilesForTimeUsingWriter(t *testing.T, dir string, start time.Time, id string, data []byte) {
	w := fs.NewWriter(testBlockSize, dir, nil)
	err := w.Open(0, start)
	require.NoError(t, err)
	require.NoError(t, w.Write(id, data))
	require.NoError(t, w.Close())
}

func writeGoodFiles(t *testing.T, dir string) {
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
		writeFilesForTimeUsingWriter(t, dir, input.start, input.id, input.data)
	}
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

func TestGetAvailabilityOpenFileError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shardDirPath := createShardDir(t, dir, 0)
	fpath := path.Join(shardDirPath, "1000-info.db")
	f := createFile(t, fpath)
	f.Close()

	os.Chmod(fpath, os.FileMode(0333))

	fss := newFileSystemSource(dir, storage.NewDatabaseOptions())
	res := fss.GetAvailability(0, createTimeRanges())
	require.True(t, res.IsEmpty())
}

func TestGetAvailabilityReadInfoError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shardDirPath := createShardDir(t, dir, 0)
	createBadInfoFile(t, shardDirPath, testStart.Add(4*time.Hour))

	fss := newFileSystemSource(dir, storage.NewDatabaseOptions())
	res := fss.GetAvailability(0, createTimeRanges())
	require.True(t, res.IsEmpty())
}

func TestGetAvailabilityTimeRangeFilter(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shardDirPath := createShardDir(t, dir, 0)
	writeGoodInfoFiles(t, shardDirPath, testBlockTimes)

	fss := newFileSystemSource(dir, storage.NewDatabaseOptions())
	res := fss.GetAvailability(0, createTimeRanges())

	expected := []xtime.Range{
		{Start: testStart, End: testStart.Add(2 * time.Hour)},
		{Start: testStart.Add(10 * time.Hour), End: testStart.Add(12 * time.Hour)},
	}
	validateTimeRanges(t, res, expected)
}

func TestGetAvailabilityTimeRangePartialError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shardDirPath := createShardDir(t, dir, 0)
	writeGoodInfoFiles(t, shardDirPath, testBlockTimes)
	createBadInfoFile(t, shardDirPath, testStart.Add(4*time.Hour))

	fss := newFileSystemSource(dir, storage.NewDatabaseOptions())
	res := fss.GetAvailability(0, createTimeRanges())

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

	shardDirPath := createShardDir(t, dir, 0)
	createInfoFileWithCheckpoint(t, shardDirPath, testStart).Close()

	fss := newFileSystemSource(dir, storage.NewDatabaseOptions())
	res, unfulfilled := fss.ReadData(0, createTimeRanges())
	require.True(t, res.IsEmpty())
	expected := []xtime.Range{
		{Start: testStart, End: testStart.Add(11 * time.Hour)},
	}
	validateTimeRanges(t, unfulfilled, expected)
}

func TestReadDataDataCorruptionError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shardDirPath := createShardDir(t, dir, 0)
	writeFilesForTimeRaw(t, shardDirPath, testStart, []byte{0x1})

	fss := newFileSystemSource(dir, storage.NewDatabaseOptions())
	tr := createTimeRanges()
	res, unfulfilled := fss.ReadData(0, tr)
	require.True(t, res.IsEmpty())
	require.Equal(t, tr, unfulfilled)
}

func validateReadResults(t *testing.T, dir string, timeInNano int) {
	fss := newFileSystemSource(dir, storage.NewDatabaseOptions())
	tr := createTimeRanges()
	expected := []xtime.Range{
		{Start: testStart.Add(2 * time.Hour), End: testStart.Add(10 * time.Hour)},
	}
	res, unfulfilled := fss.ReadData(0, tr)
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

	writeGoodFiles(t, dir)
	validateReadResults(t, dir, 0)
}

func TestReadDataPartialError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shardDirPath := createShardDir(t, dir, 0)
	writeGoodFiles(t, dir)
	writeFilesForTimeRaw(t, shardDirPath, testStart.Add(4*time.Hour), []byte{0x1})

	validateReadResults(t, dir, 0)
}
