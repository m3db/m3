package fs

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"code.uber.internal/infra/memtsdb/persist/fs"
	schema "code.uber.internal/infra/memtsdb/persist/fs/proto"
	xtime "code.uber.internal/infra/memtsdb/x/time"

	"code.uber.internal/infra/memtsdb/storage"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

var (
	testStart  = time.Now()
	testWindow = 2 * time.Hour
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
	return xtime.NewRanges().AddRange(xtime.Range{Start: testStart, End: testStart.Add(15 * time.Hour)})
}

func writeInfoFile(t *testing.T, f *os.File, start time.Time) {
	info := &schema.IndexInfo{
		Start:   xtime.ToNanoseconds(start),
		Window:  int64(testWindow),
		Entries: 10,
	}
	data, err := proto.Marshal(info)
	require.NoError(t, err)

	_, err = f.Write(data)
	require.NoError(t, err)
}

func writeBadInfoFile(t *testing.T, shardDirPath string, version int) {
	fpath := path.Join(shardDirPath, fmt.Sprintf("%d-info.db", version))
	f := createFile(t, fpath)
	_, err := f.Write([]byte{0x1, 0x2})
	require.NoError(t, err)
	f.Close()
}

func writeGoodInfoFiles(t *testing.T, shardDirPath string, versions []int) {
	for _, version := range versions {
		fpath := path.Join(shardDirPath, fmt.Sprintf("%d-info.db", version))
		f := createFile(t, fpath)
		writeInfoFile(t, f, testStart.Add(time.Hour*time.Duration(version)))
		f.Close()
	}
}

func writeFilesForVersionRaw(t *testing.T, shardDirPath string, version int, start time.Time, data []byte) {
	f := createFile(t, path.Join(shardDirPath, fmt.Sprintf("%d-info.db", version)))
	writeInfoFile(t, f, start)
	f.Close()

	f = createFile(t, path.Join(shardDirPath, fmt.Sprintf("%d-index.db", version)))
	f.Write(data)
	f.Close()

	f = createFile(t, path.Join(shardDirPath, fmt.Sprintf("%d-data.db", version)))
	f.Close()
}

func writeFilesForVersionUsingWriter(t *testing.T, dir string, start time.Time, id string, data []byte) {
	w := fs.NewWriter(start, testWindow, dir, nil)
	err := w.Open(0)
	require.NoError(t, err)
	require.NoError(t, w.Write(id, data))
	require.NoError(t, w.Close())
}

func writeGoodVersionedFiles(t *testing.T, dir string) {
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
		writeFilesForVersionUsingWriter(t, dir, input.start, input.id, input.data)
	}
}

func validateTimeRanges(t *testing.T, tr xtime.Ranges, expected []xtime.Range) {
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
	res := fss.GetAvailability(0, xtime.NewRanges())
	require.Nil(t, res)
}

func TestGetAvailabilityOpenFileError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shardDirPath := createShardDir(t, dir, 0)
	fpath := path.Join(shardDirPath, "0-info.db")
	f := createFile(t, fpath)
	f.Close()

	os.Chmod(fpath, os.FileMode(0333))

	fss := newFileSystemSource(dir, storage.NewDatabaseOptions())
	res := fss.GetAvailability(0, xtime.NewRanges())
	require.True(t, res.IsEmpty())
}

func TestGetAvailabilityReadInfoError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shardDirPath := createShardDir(t, dir, 0)
	writeBadInfoFile(t, shardDirPath, 0)

	fss := newFileSystemSource(dir, storage.NewDatabaseOptions())
	res := fss.GetAvailability(0, xtime.NewRanges())
	require.True(t, res.IsEmpty())
}

func TestGetAvailabilityTimeRangeFilter(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shardDirPath := createShardDir(t, dir, 0)
	versions := []int{0, 10, 20}
	writeGoodInfoFiles(t, shardDirPath, versions)

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
	versions := []int{0, 10, 20}
	writeGoodInfoFiles(t, shardDirPath, versions)
	writeBadInfoFile(t, shardDirPath, 30)

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
	fpath := path.Join(shardDirPath, "0-info.db")
	f := createFile(t, fpath)
	f.Close()

	fss := newFileSystemSource(dir, storage.NewDatabaseOptions())
	res, unfulfilled := fss.ReadData(0, createTimeRanges())
	require.True(t, res.IsEmpty())
	expected := []xtime.Range{
		{Start: testStart, End: testStart.Add(15 * time.Hour)},
	}
	validateTimeRanges(t, unfulfilled, expected)
}

func TestReadDataDataCorruptionError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shardDirPath := createShardDir(t, dir, 0)
	writeFilesForVersionRaw(t, shardDirPath, 0, testStart, []byte{0x1})

	fss := newFileSystemSource(dir, storage.NewDatabaseOptions())
	tr := createTimeRanges()
	res, unfulfilled := fss.ReadData(0, tr)
	require.True(t, res.IsEmpty())
	require.Equal(t, tr, unfulfilled)
}

func validateReadResults(t *testing.T, dir string, version int) {
	fss := newFileSystemSource(dir, storage.NewDatabaseOptions())
	tr := createTimeRanges()
	expected := []xtime.Range{
		{Start: testStart.Add(2 * time.Hour), End: testStart.Add(10 * time.Hour)},
		{Start: testStart.Add(12 * time.Hour), End: testStart.Add(15 * time.Hour)},
	}
	res, unfulfilled := fss.ReadData(0, tr)
	require.Equal(t, 2, len(res.GetAllSeries()))
	validateTimeRanges(t, unfulfilled, expected)

	allSeries := res.GetAllSeries()
	require.Equal(t, 2, len(allSeries))

	ids := []string{"foo", "bar"}
	data := [][]byte{
		{0x1, 0x2, 0x3, 0x80, 0x0},
		{0x4, 0x5, 0x6, 0x80, 0x0},
	}
	times := []time.Time{testStart, testStart.Add(10 * time.Hour)}
	for i, id := range ids {
		allBlocks := allSeries[id].GetAllBlocks()
		require.Equal(t, 1, len(allBlocks))
		var b [100]byte
		n, err := allBlocks[times[i]].Stream().Read(b[:])
		require.NoError(t, err)
		require.Equal(t, data[i], b[:n])
	}
}

func TestReadDataTimeFilter(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	writeGoodVersionedFiles(t, dir)
	validateReadResults(t, dir, 0)
}

func TestReadDataPartialError(t *testing.T) {
	dir := createTempDir(t)
	defer os.RemoveAll(dir)

	shardDirPath := createShardDir(t, dir, 0)
	writeGoodVersionedFiles(t, dir)
	writeFilesForVersionRaw(t, shardDirPath, 10, testStart, []byte{0x1})

	validateReadResults(t, dir, 0)
}
