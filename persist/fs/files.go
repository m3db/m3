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
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/persist/fs/msgpack"
	"github.com/m3db/m3db/persist/schema"
	xclose "github.com/m3db/m3x/close"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
)

var timeZero time.Time

const (
	dataDirName       = "data"
	snapshotDirName   = "snapshots"
	commitLogsDirName = "commitlogs"

	commitLogComponentPosition = 2
	snapshotComponentPosition  = 3
)

type fileOpener func(filePath string) (*os.File, error)

// FilesetFile represents a set of Fileset files for a given block start
type FilesetFile struct {
	ID                FilesetFileIdentifier
	AbsoluteFilepaths []string
}

// FilesetFilesSlice is a slice of FilesetFile
type FilesetFilesSlice []FilesetFile

// Filepaths flattens a slice of FilesetFiles to a single slice of filepaths.
// All paths returned are absolute.
func (f FilesetFilesSlice) Filepaths() []string {
	flattened := []string{}
	for _, fileset := range f {
		flattened = append(flattened, fileset.AbsoluteFilepaths...)
	}

	return flattened
}

// NewFilesetFile creates a new Fileset file
func NewFilesetFile(id FilesetFileIdentifier) FilesetFile {
	return FilesetFile{
		ID:                id,
		AbsoluteFilepaths: []string{},
	}
}

// SnapshotFile represents a set of Snapshot files for a given block start
type SnapshotFile struct {
	FilesetFile
}

// HasCheckpointFile returns a bool indicating whether the given set of
// snapshot files has a checkpoint file.
func (s SnapshotFile) HasCheckpointFile() bool {
	for _, fileName := range s.AbsoluteFilepaths {
		if strings.Contains(fileName, checkpointFileSuffix) {
			return true
		}
	}

	return false
}

// SnapshotFilesSlice is a slice of SnapshotFile
type SnapshotFilesSlice []SnapshotFile

// Filepaths flattens a slice of SnapshotFiles to a single slice of filepaths.
// All paths returned are absolute.
func (f SnapshotFilesSlice) Filepaths() []string {
	flattened := []string{}
	for _, snapshot := range f {
		flattened = append(flattened, snapshot.AbsoluteFilepaths...)
	}

	return flattened
}

// LatestForBlock returns the latest (highest index) SnapshotFile in the
// slice for a given block start.
func (f SnapshotFilesSlice) LatestForBlock(blockStart time.Time) (SnapshotFile, bool) {
	// Make sure we're already sorted
	f.sortByTimeAndIndexAscending()

	for i, curr := range f {
		if curr.ID.BlockStart.Equal(blockStart) {
			isEnd := i == len(f)-1
			isHighestIdx := true
			if !isEnd {
				next := f[i+1]
				if next.ID.BlockStart.Equal(blockStart) && next.ID.Index > curr.ID.Index {
					isHighestIdx = false
				}
			}

			if isEnd || isHighestIdx {
				return curr, true
			}
		}
	}

	return SnapshotFile{}, false
}

func (f SnapshotFilesSlice) sortByTimeAndIndexAscending() {
	sort.Slice(f, func(i, j int) bool {
		if f[i].ID.BlockStart.Equal(f[j].ID.BlockStart) {
			return f[i].ID.Index < f[j].ID.Index
		}

		return f[i].ID.BlockStart.Before(f[j].ID.BlockStart)
	})
}

// NewSnapshotFile creates a new Snapshot file
func NewSnapshotFile(id FilesetFileIdentifier) SnapshotFile {
	return SnapshotFile{
		FilesetFile: NewFilesetFile(id),
	}
}

func openFiles(opener fileOpener, fds map[string]**os.File) error {
	var firstErr error
	for filePath, fdPtr := range fds {
		fd, err := opener(filePath)
		if err != nil {
			firstErr = err
			break
		}
		*fdPtr = fd
	}

	if firstErr == nil {
		return nil
	}

	// If we have encountered an error when opening the files,
	// close the ones that have been opened.
	for _, fdPtr := range fds {
		if *fdPtr != nil {
			(*fdPtr).Close()
		}
	}

	return firstErr
}

// TODO(xichen): move closeAll to m3x/close.
func closeAll(closers ...xclose.Closer) error {
	multiErr := xerrors.NewMultiError()
	for _, closer := range closers {
		if err := closer.Close(); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	return multiErr.FinalError()
}

// DeleteFiles delete a set of files, returning all the errors encountered during
// the deletion process.
func DeleteFiles(filePaths []string) error {
	multiErr := xerrors.NewMultiError()
	for _, file := range filePaths {
		if err := os.Remove(file); err != nil {
			detailedErr := fmt.Errorf("failed to remove file %s: %v", file, err)
			multiErr = multiErr.Add(detailedErr)
		}
	}
	return multiErr.FinalError()
}

// DeleteDirectories delets a set of directories and its contents, returning all
// of the errors encountered during the deletion process.
func DeleteDirectories(dirPaths []string) error {
	multiErr := xerrors.NewMultiError()
	for _, dir := range dirPaths {
		if err := os.RemoveAll(dir); err != nil {
			detailedErr := fmt.Errorf("failed to remove dir %s: %v", dir, err)
			multiErr = multiErr.Add(detailedErr)
		}
	}
	return multiErr.FinalError()
}

// byTimeAscending sorts files by their block start times in ascending order.
// If the files do not have block start times in their names, the result is undefined.
type byTimeAscending []string

func (a byTimeAscending) Len() int      { return len(a) }
func (a byTimeAscending) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byTimeAscending) Less(i, j int) bool {
	ti, _ := TimeFromFileName(a[i])
	tj, _ := TimeFromFileName(a[j])
	return ti.Before(tj)
}

// commitlogsByTimeAndIndexAscending sorts commitlogs by their block start times and index in ascending
// order. If the files do not have block start times or indexes in their names, the result is undefined.
type commitlogsByTimeAndIndexAscending []string

func (a commitlogsByTimeAndIndexAscending) Len() int      { return len(a) }
func (a commitlogsByTimeAndIndexAscending) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a commitlogsByTimeAndIndexAscending) Less(i, j int) bool {
	ti, ii, _ := TimeAndIndexFromCommitlogFilename(a[i])
	tj, ij, _ := TimeAndIndexFromCommitlogFilename(a[j])
	if ti.Before(tj) {
		return true
	}
	return ti.Equal(tj) && ii < ij
}

// snapshotsByTimeAndIndexAscending sorts snapshots by their block start times and index in ascending
// order. If the files do not have block start times or indexes in their names, the result is undefined.
type snapshotsByTimeAndIndexAscending []string

func (a snapshotsByTimeAndIndexAscending) Len() int      { return len(a) }
func (a snapshotsByTimeAndIndexAscending) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a snapshotsByTimeAndIndexAscending) Less(i, j int) bool {
	ti, ii, _ := TimeAndIndexFromSnapshotFilename(a[i])
	tj, ij, _ := TimeAndIndexFromSnapshotFilename(a[j])
	if ti.Before(tj) {
		return true
	}
	return ti.Equal(tj) && ii < ij
}

func componentsAndTimeFromFileName(fname string) ([]string, time.Time, error) {
	components := strings.Split(filepath.Base(fname), separator)
	if len(components) < 3 {
		return nil, timeZero, fmt.Errorf("unexpected file name %s", fname)
	}
	str := strings.Replace(components[1], fileSuffix, "", 1)
	nanoseconds, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return nil, timeZero, err
	}
	return components, time.Unix(0, nanoseconds), nil
}

// TimeFromFileName extracts the block start time from file name.
func TimeFromFileName(fname string) (time.Time, error) {
	_, t, err := componentsAndTimeFromFileName(fname)
	return t, err
}

// TimeAndIndexFromCommitlogFilename extracts the block start and index from file name for a commitlog.
func TimeAndIndexFromCommitlogFilename(fname string) (time.Time, int, error) {
	return timeAndIndexFromFileName(fname, commitLogComponentPosition)
}

// TimeAndIndexFromSnapshotFilename extracts the block start and index from file name for a Snapshot.
func TimeAndIndexFromSnapshotFilename(fname string) (time.Time, int, error) {
	return timeAndIndexFromFileName(fname, snapshotComponentPosition)
}

func timeAndIndexFromFileName(fname string, componentPosition int) (time.Time, int, error) {
	components, t, err := componentsAndTimeFromFileName(fname)
	if err != nil {
		return timeZero, 0, err
	}

	if componentPosition > len(components)-1 {
		return timeZero, 0, fmt.Errorf("malformed filename: %s", fname)
	}

	str := strings.Replace(components[componentPosition], fileSuffix, "", 1)
	index, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return timeZero, 0, err
	}
	return t, int(index), nil
}

type infoFileFn func(fname string, infoData []byte)

func forEachInfoFile(filePathPrefix string, namespace ident.ID, shard uint32, readerBufferSize int, fn infoFileFn) {
	matched, err := filesetFiles(filePathPrefix, namespace, shard, infoFilePattern)
	if err != nil {
		return
	}

	shardDir := ShardDataDirPath(filePathPrefix, namespace, shard)
	digestBuf := digest.NewBuffer()
	for i := range matched {
		t := matched[i].ID.BlockStart
		checkpointFilePath := filesetPathFromTime(shardDir, t, checkpointFileSuffix)
		if !FileExists(checkpointFilePath) {
			continue
		}
		checkpointFd, err := os.Open(checkpointFilePath)
		if err != nil {
			continue
		}
		// Read digest of digests from the checkpoint file
		expectedDigestOfDigest, err := digestBuf.ReadDigestFromFile(checkpointFd)
		checkpointFd.Close()
		if err != nil {
			continue
		}
		// Read and validate the digest file
		digestData, err := readAndValidate(shardDir, t, digestFileSuffix, readerBufferSize, expectedDigestOfDigest)
		if err != nil {
			continue
		}
		// Read and validate the info file
		expectedInfoDigest := digest.ToBuffer(digestData).ReadDigest()
		infoData, err := readAndValidate(shardDir, t, infoFileSuffix, readerBufferSize, expectedInfoDigest)
		if err != nil {
			continue
		}
		if len(matched[i].AbsoluteFilepaths) != 1 {
			continue
		}

		fn(matched[i].AbsoluteFilepaths[0], infoData)
	}
}

// ReadInfoFileResult is the result of reading an info file
type ReadInfoFileResult struct {
	Info schema.IndexInfo
	Err  ReadInfoFileResultError
}

// ReadInfoFileResultError is the interface for obtaining information about an error
// that occurred trying to read an info file
type ReadInfoFileResultError interface {
	Error() error
	Filepath() string
}

type readInfoFileResultError struct {
	err      error
	filepath string
}

// Error returns the error that occurred reading the info file
func (r readInfoFileResultError) Error() error {
	return r.err
}

// FilePath returns the filepath for the problematic file
func (r readInfoFileResultError) Filepath() string {
	return r.filepath
}

// ReadInfoFiles reads all the valid info entries. Even if ReadInfoFiles returns an error,
// there may be some valid entries in the returned slice.
func ReadInfoFiles(
	filePathPrefix string,
	namespace ident.ID,
	shard uint32,
	readerBufferSize int,
	decodingOpts msgpack.DecodingOptions,
) []ReadInfoFileResult {
	var infoFileResults []ReadInfoFileResult
	decoder := msgpack.NewDecoder(decodingOpts)
	forEachInfoFile(filePathPrefix, namespace, shard, readerBufferSize, func(filepath string, data []byte) {
		decoder.Reset(msgpack.NewDecoderStream(data))
		info, err := decoder.DecodeIndexInfo()
		infoFileResults = append(infoFileResults, ReadInfoFileResult{
			Info: info,
			Err: readInfoFileResultError{
				err:      err,
				filepath: filepath,
			},
		})
	})
	return infoFileResults
}

// SnapshotFiles returns a slice of all the names for all the fileset files
// for a given namespace and shard combination.
func SnapshotFiles(filePathPrefix string, namespace ident.ID, shard uint32) (SnapshotFilesSlice, error) {
	return snapshotFiles(filePathPrefix, namespace, shard, filesetFilePattern)
}

// FilesetBefore returns all the fileset files whose timestamps are earlier than a given time.
func FilesetBefore(filePathPrefix string, namespace ident.ID, shard uint32, t time.Time) ([]string, error) {
	matched, err := filesetFiles(filePathPrefix, namespace, shard, filesetFilePattern)
	if err != nil {
		return nil, err
	}
	return FilesBefore(matched.Filepaths(), t)
}

// DeleteInactiveDirectories deletes any directories that are not currently active, as defined by the
// inputed active directories within the parent directory
func DeleteInactiveDirectories(parentDirectoryPath string, activeDirectories []string) error {
	var toDelete []string
	activeDirNames := make(map[string]struct{})
	allSubDirs, err := findSubDirectoriesAndPaths(parentDirectoryPath)
	if err != nil {
		return nil
	}

	// Create shard set, might also be useful to just send in as strings?
	for _, dir := range activeDirectories {
		activeDirNames[dir] = struct{}{}
	}

	for dirName, dirPath := range allSubDirs {
		if _, ok := activeDirNames[dirName]; !ok {
			toDelete = append(toDelete, dirPath)
		}
	}
	return DeleteDirectories(toDelete)
}

// CommitLogFiles returns all the commit log files in the commit logs directory.
func CommitLogFiles(commitLogsDir string) ([]string, error) {
	return commitlogFiles(commitLogsDir, commitLogFilePattern)
}

// CommitLogFilesForTime returns all the commit log files for a given time.
func CommitLogFilesForTime(commitLogsDir string, t time.Time) ([]string, error) {
	commitLogFileForTimePattern := fmt.Sprintf(commitLogFileForTimeTemplate, t.UnixNano())
	return commitlogFiles(commitLogsDir, commitLogFileForTimePattern)
}

// CommitLogFilesBefore returns all the commit log files whose timestamps are earlier than a given time.
func CommitLogFilesBefore(commitLogsDir string, t time.Time) ([]string, error) {
	commitLogs, err := CommitLogFiles(commitLogsDir)
	if err != nil {
		return nil, err
	}
	return FilesBefore(commitLogs, t)
}

type toSortableFn func(files []string) sort.Interface

func findFiles(fileDir string, pattern string, fn toSortableFn) ([]string, error) {
	matched, err := filepath.Glob(path.Join(fileDir, pattern))
	if err != nil {
		return nil, err
	}
	sort.Sort(fn(matched))
	return matched, nil
}

type directoryNamesToPaths map[string]string

func findSubDirectoriesAndPaths(directoryPath string) (directoryNamesToPaths, error) {
	parent, err := os.Open(directoryPath)
	if err != nil {
		return nil, err
	}

	subDirectoriesToPaths := make(directoryNamesToPaths)
	subDirNames, err := parent.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	err = parent.Close()
	if err != nil {
		return nil, err
	}

	for _, dirName := range subDirNames {
		subDirectoriesToPaths[dirName] = path.Join(directoryPath, dirName)
	}
	return subDirectoriesToPaths, nil
}

func snapshotFiles(filePathPrefix string, namespace ident.ID, shard uint32, pattern string) ([]SnapshotFile, error) {
	shardDir := ShardSnapshotsDirPath(filePathPrefix, namespace, shard)
	byTimeAsc, err := findFiles(shardDir, pattern, func(files []string) sort.Interface {
		return snapshotsByTimeAndIndexAscending(files)
	})

	if err != nil {
		return nil, err
	}

	if len(byTimeAsc) == 0 {
		return nil, nil
	}

	var (
		latestBlockStart   time.Time
		latestIndex        int
		latestSnapshotFile SnapshotFile
		snapshotFiles      = []SnapshotFile{}
	)
	for _, file := range byTimeAsc {
		currentFileBlockStart, index, err := TimeAndIndexFromSnapshotFilename(file)
		if err != nil {
			return nil, err
		}

		if latestBlockStart.IsZero() {
			latestSnapshotFile = NewSnapshotFile(FilesetFileIdentifier{
				Namespace:  namespace,
				Shard:      shard,
				BlockStart: currentFileBlockStart,
			})
			latestIndex = index
		} else if !currentFileBlockStart.Equal(latestBlockStart) || latestIndex != index {
			snapshotFiles = append(snapshotFiles, latestSnapshotFile)
			latestSnapshotFile = NewSnapshotFile(FilesetFileIdentifier{
				Namespace:  namespace,
				Shard:      shard,
				BlockStart: currentFileBlockStart,
			})
			latestIndex = index
		}
		latestBlockStart = currentFileBlockStart

		latestSnapshotFile.ID.Index = index
		latestSnapshotFile.AbsoluteFilepaths = append(latestSnapshotFile.AbsoluteFilepaths, file)
	}
	snapshotFiles = append(snapshotFiles, latestSnapshotFile)

	return snapshotFiles, nil
}

func filesetFiles(filePathPrefix string, namespace ident.ID, shard uint32, pattern string) (FilesetFilesSlice, error) {
	shardDir := ShardDataDirPath(filePathPrefix, namespace, shard)
	byTimeAsc, err := findFiles(shardDir, pattern, func(files []string) sort.Interface {
		return byTimeAscending(files)
	})

	if err != nil {
		return nil, err
	}

	if len(byTimeAsc) == 0 {
		return nil, nil
	}

	var (
		latestBlockStart  time.Time
		latestFilesetFile FilesetFile
		filesetFiles      = []FilesetFile{}
	)
	for _, file := range byTimeAsc {
		currentFileBlockStart, err := TimeFromFileName(file)
		if err != nil {
			return nil, err
		}

		if latestBlockStart.IsZero() {
			latestFilesetFile = NewFilesetFile(FilesetFileIdentifier{
				Namespace:  namespace,
				Shard:      shard,
				BlockStart: currentFileBlockStart,
			})
		} else if !currentFileBlockStart.Equal(latestBlockStart) {
			filesetFiles = append(filesetFiles, latestFilesetFile)
			latestFilesetFile = NewFilesetFile(FilesetFileIdentifier{
				Namespace:  namespace,
				Shard:      shard,
				BlockStart: currentFileBlockStart,
			})
		}
		latestBlockStart = currentFileBlockStart

		latestFilesetFile.AbsoluteFilepaths = append(latestFilesetFile.AbsoluteFilepaths, file)
	}
	filesetFiles = append(filesetFiles, latestFilesetFile)

	return filesetFiles, nil
}

func commitlogFiles(commitLogsDir string, pattern string) ([]string, error) {
	return findFiles(commitLogsDir, pattern, func(files []string) sort.Interface {
		return commitlogsByTimeAndIndexAscending(files)
	})
}

// FilesBefore filters the list of files down to those whose name indicate they are
// before a given time period. Mutates the provided slice.
func FilesBefore(files []string, t time.Time) ([]string, error) {
	var (
		j        int
		multiErr xerrors.MultiError
	)
	// Matched files are sorted by their timestamps in ascending order.
	for i := range files {
		ft, err := TimeFromFileName(files[i])
		if err != nil {
			multiErr = multiErr.Add(err)
			continue
		}
		if !ft.Before(t) {
			break
		}
		files[j] = files[i]
		j++
	}
	return files[:j], multiErr.FinalError()
}

func readAndValidate(
	prefix string,
	t time.Time,
	suffix string,
	readerBufferSize int,
	expectedDigest uint32,
) ([]byte, error) {
	filePath := filesetPathFromTime(prefix, t, suffix)
	fd, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	stat, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}

	size := int(stat.Size())
	buf := make([]byte, size)

	fwd := digest.NewFdWithDigestReader(readerBufferSize)
	fwd.Reset(fd)
	n, err := fwd.ReadAllAndValidate(buf, expectedDigest)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

// DataDirPath returns the path to the data directory belonging to a db
func DataDirPath(prefix string) string {
	return path.Join(prefix, dataDirName)
}

// SnapshotDirPath returns the path to the snapshot directory belong to a db
func SnapshotDirPath(prefix string) string {
	return path.Join(prefix, snapshotDirName)
}

// NamespaceDataDirPath returns the path to the data directory for a given namespace.
func NamespaceDataDirPath(prefix string, namespace ident.ID) string {
	return path.Join(prefix, dataDirName, namespace.String())
}

// NamespaceSnapshotsDirPath returns the path to the snapshots directory for a given namespace.
func NamespaceSnapshotsDirPath(prefix string, namespace ident.ID) string {
	return path.Join(prefix, snapshotDirName, namespace.String())
}

// ShardDataDirPath returns the path to the data directory for a given shard.
func ShardDataDirPath(prefix string, namespace ident.ID, shard uint32) string {
	namespacePath := NamespaceDataDirPath(prefix, namespace)
	return path.Join(namespacePath, strconv.Itoa(int(shard)))
}

// ShardSnapshotsDirPath returns the path to the snapshots directory for a given shard.
func ShardSnapshotsDirPath(prefix string, namespace ident.ID, shard uint32) string {
	namespacePath := NamespaceSnapshotsDirPath(prefix, namespace)
	return path.Join(namespacePath, strconv.Itoa(int(shard)))
}

// CommitLogsDirPath returns the path to commit logs.
func CommitLogsDirPath(prefix string) string {
	return path.Join(prefix, commitLogsDirName)
}

// DataFilesetExistsAt determines whether data fileset files exist for the given namespace, shard, and block start time.
func DataFilesetExistsAt(prefix string, namespace ident.ID, shard uint32, blockStart time.Time) bool {
	shardDir := ShardDataDirPath(prefix, namespace, shard)
	checkpointFile := filesetPathFromTime(shardDir, blockStart, checkpointFileSuffix)
	return FileExists(checkpointFile)
}

// SnapshotFilesetExistsAt determines whether snapshot fileset files exist for the given namespace, shard, and block start time.
func SnapshotFilesetExistsAt(prefix string, namespace ident.ID, shard uint32, blockStart time.Time) (bool, error) {
	snapshotFiles, err := SnapshotFiles(prefix, namespace, shard)
	if err != nil {
		return false, err
	}

	latest, ok := snapshotFiles.LatestForBlock(blockStart)
	if !ok {
		return false, nil
	}

	return latest.HasCheckpointFile(), nil
}

// NextCommitLogsFile returns the next commit logs file.
func NextCommitLogsFile(prefix string, start time.Time) (string, int) {
	for i := 0; ; i++ {
		entry := fmt.Sprintf("%d%s%d", start.UnixNano(), separator, i)
		fileName := fmt.Sprintf("%s%s%s%s", commitLogFilePrefix, separator, entry, fileSuffix)
		filePath := path.Join(CommitLogsDirPath(prefix), fileName)
		if !FileExists(filePath) {
			return filePath, i
		}
	}
}

// NextSnapshotFileIndex returns the next snapshot file index for a given
// namespace/shard/blockStart combination.
func NextSnapshotFileIndex(filePathPrefix string, namespace ident.ID, shard uint32, blockStart time.Time) (int, error) {
	snapshotFiles, err := SnapshotFiles(filePathPrefix, namespace, shard)
	if err != nil {
		return -1, err
	}

	var currentSnapshotIndex = -1
	for _, snapshot := range snapshotFiles {
		if snapshot.ID.BlockStart.Equal(blockStart) {
			currentSnapshotIndex = snapshot.ID.Index
			break
		}
	}

	return currentSnapshotIndex + 1, nil
}

// FileExists returns whether a file at the given path exists.
func FileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return err == nil
}

// OpenWritable opens a file for writing and truncating as necessary.
func OpenWritable(filePath string, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
}

func filesetPathFromTime(prefix string, t time.Time, suffix string) string {
	name := fmt.Sprintf("%s%s%d%s%s%s", filesetFilePrefix, separator, t.UnixNano(), separator, suffix, fileSuffix)
	return path.Join(prefix, name)
}

func snapshotPathFromTimeAndIndex(prefix string, t time.Time, suffix string, index int) string {
	name := fmt.Sprintf("%s%s%d%s%s%s%d%s", filesetFilePrefix, separator, t.UnixNano(), separator, suffix, separator, index, fileSuffix)
	return path.Join(prefix, name)
}
