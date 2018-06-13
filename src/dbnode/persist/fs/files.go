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

	"github.com/m3db/m3db/src/dbnode/digest"
	"github.com/m3db/m3db/src/dbnode/generated/proto/index"
	"github.com/m3db/m3db/src/dbnode/persist"
	"github.com/m3db/m3db/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3db/src/dbnode/persist/schema"
	idxpersist "github.com/m3db/m3db/src/m3ninx/persist"
	xclose "github.com/m3db/m3x/close"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
)

var timeZero time.Time

const (
	dataDirName       = "data"
	indexDirName      = "index"
	snapshotDirName   = "snapshots"
	commitLogsDirName = "commitlogs"

	commitLogComponentPosition    = 2
	indexFileSetComponentPosition = 2
)

type fileOpener func(filePath string) (*os.File, error)

// FileSetFile represents a set of FileSet files for a given block start
type FileSetFile struct {
	ID                FileSetFileIdentifier
	AbsoluteFilepaths []string
}

// HasCheckpointFile returns a bool indicating whether the given set of
// fileset files has a checkpoint file.
func (f FileSetFile) HasCheckpointFile() bool {
	for _, fileName := range f.AbsoluteFilepaths {
		if strings.Contains(fileName, checkpointFileSuffix) {
			return true
		}
	}

	return false
}

// FileSetFilesSlice is a slice of FileSetFile
type FileSetFilesSlice []FileSetFile

// Filepaths flattens a slice of FileSetFiles to a single slice of filepaths.
// All paths returned are absolute.
func (f FileSetFilesSlice) Filepaths() []string {
	flattened := []string{}
	for _, fileset := range f {
		flattened = append(flattened, fileset.AbsoluteFilepaths...)
	}

	return flattened
}

// LatestVolumeForBlock returns the latest (highest index) FileSetFile in the
// slice for a given block start, only applicable for index and snapshot file set files.
func (f FileSetFilesSlice) LatestVolumeForBlock(blockStart time.Time) (FileSetFile, bool) {
	// Make sure we're already sorted
	f.sortByTimeAndVolumeIndexAscending()

	for i, curr := range f {
		if curr.ID.BlockStart.Equal(blockStart) {
			isEnd := i == len(f)-1
			isHighestIdx := true
			if !isEnd {
				next := f[i+1]
				if next.ID.BlockStart.Equal(blockStart) && next.ID.VolumeIndex > curr.ID.VolumeIndex {
					isHighestIdx = false
				}
			}

			if isEnd || isHighestIdx {
				return curr, true
			}
		}
	}

	return FileSetFile{}, false
}

// ignores the index in the FileSetFileIdentifier because fileset files should
// always have index 0.
func (f FileSetFilesSlice) sortByTimeAscending() {
	sort.Slice(f, func(i, j int) bool {
		return f[i].ID.BlockStart.Before(f[j].ID.BlockStart)
	})
}

func (f FileSetFilesSlice) sortByTimeAndVolumeIndexAscending() {
	sort.Slice(f, func(i, j int) bool {
		if f[i].ID.BlockStart.Equal(f[j].ID.BlockStart) {
			return f[i].ID.VolumeIndex < f[j].ID.VolumeIndex
		}

		return f[i].ID.BlockStart.Before(f[j].ID.BlockStart)
	})
}

// NewFileSetFile creates a new FileSet file
func NewFileSetFile(id FileSetFileIdentifier) FileSetFile {
	return FileSetFile{
		ID:                id,
		AbsoluteFilepaths: []string{},
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

// fileSetFilesByTimeAndIndexAscending sorts file sets files by their block start times and volume
// index in ascending order. If the files do not have block start times or indexes in their names,
// the result is undefined.
type fileSetFilesByTimeAndVolumeIndexAscending []string

func (a fileSetFilesByTimeAndVolumeIndexAscending) Len() int      { return len(a) }
func (a fileSetFilesByTimeAndVolumeIndexAscending) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a fileSetFilesByTimeAndVolumeIndexAscending) Less(i, j int) bool {
	ti, ii, _ := TimeAndVolumeIndexFromFileSetFilename(a[i])
	tj, ij, _ := TimeAndVolumeIndexFromFileSetFilename(a[j])
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

// TimeAndVolumeIndexFromFileSetFilename extracts the block start and volume index from file name.
func TimeAndVolumeIndexFromFileSetFilename(fname string) (time.Time, int, error) {
	return timeAndIndexFromFileName(fname, indexFileSetComponentPosition)
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

type forEachInfoFileSelector struct {
	fileSetType    persist.FileSetType
	contentType    persist.FileSetContentType
	filePathPrefix string
	namespace      ident.ID
	shard          uint32 // shard only applicable for data content type
}

type infoFileFn func(fname string, id FileSetFileIdentifier, infoData []byte)

func forEachInfoFile(
	args forEachInfoFileSelector,
	readerBufferSize int,
	fn infoFileFn,
) {
	matched, err := filesetFiles(filesetFilesSelector{
		fileSetType:    args.fileSetType,
		contentType:    args.contentType,
		filePathPrefix: args.filePathPrefix,
		namespace:      args.namespace,
		shard:          args.shard,
		pattern:        infoFilePattern,
	})
	if err != nil {
		return
	}

	var dir string
	switch args.fileSetType {
	case persist.FileSetFlushType:
		switch args.contentType {
		case persist.FileSetDataContentType:
			dir = ShardDataDirPath(args.filePathPrefix, args.namespace, args.shard)
		case persist.FileSetIndexContentType:
			dir = NamespaceIndexDataDirPath(args.filePathPrefix, args.namespace)
		default:
			return
		}
	case persist.FileSetSnapshotType:
		switch args.contentType {
		case persist.FileSetDataContentType:
			dir = ShardSnapshotsDirPath(args.filePathPrefix, args.namespace, args.shard)
		case persist.FileSetIndexContentType:
			dir = NamespaceIndexSnapshotDirPath(args.filePathPrefix, args.namespace)
		default:
			return
		}
	default:
		return
	}

	var indexDigests index.IndexDigests
	digestBuf := digest.NewBuffer()
	for i := range matched {
		t := matched[i].ID.BlockStart
		volume := matched[i].ID.VolumeIndex

		var (
			checkpointFilePath string
			digestsFilePath    string
			infoFilePath       string
		)
		switch args.fileSetType {
		case persist.FileSetFlushType:
			switch args.contentType {
			case persist.FileSetDataContentType:
				checkpointFilePath = filesetPathFromTime(dir, t, checkpointFileSuffix)
				digestsFilePath = filesetPathFromTime(dir, t, digestFileSuffix)
				infoFilePath = filesetPathFromTime(dir, t, infoFileSuffix)
			case persist.FileSetIndexContentType:
				checkpointFilePath = filesetPathFromTimeAndIndex(dir, t, volume, checkpointFileSuffix)
				digestsFilePath = filesetPathFromTimeAndIndex(dir, t, volume, digestFileSuffix)
				infoFilePath = filesetPathFromTimeAndIndex(dir, t, volume, infoFileSuffix)
			}
		case persist.FileSetSnapshotType:
			checkpointFilePath = filesetPathFromTimeAndIndex(dir, t, volume, checkpointFileSuffix)
			digestsFilePath = filesetPathFromTimeAndIndex(dir, t, volume, digestFileSuffix)
			infoFilePath = filesetPathFromTimeAndIndex(dir, t, volume, infoFileSuffix)
		}
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
		digestData, err := readAndValidate(digestsFilePath, readerBufferSize,
			expectedDigestOfDigest)
		if err != nil {
			continue
		}

		// Read and validate the info file
		var expectedInfoDigest uint32
		switch args.contentType {
		case persist.FileSetDataContentType:
			expectedInfoDigest = digest.ToBuffer(digestData).ReadDigest()
		case persist.FileSetIndexContentType:
			if err := indexDigests.Unmarshal(digestData); err != nil {
				continue
			}
			expectedInfoDigest = indexDigests.GetInfoDigest()
		}

		infoData, err := readAndValidate(infoFilePath, readerBufferSize,
			expectedInfoDigest)
		if err != nil {
			continue
		}
		if len(matched[i].AbsoluteFilepaths) != 1 {
			continue
		}

		fn(matched[i].AbsoluteFilepaths[0], matched[i].ID, infoData)
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
	forEachInfoFile(
		forEachInfoFileSelector{
			fileSetType:    persist.FileSetFlushType,
			contentType:    persist.FileSetDataContentType,
			filePathPrefix: filePathPrefix,
			namespace:      namespace,
			shard:          shard,
		},
		readerBufferSize,
		func(filepath string, id FileSetFileIdentifier, data []byte) {
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

// ReadIndexInfoFileResult is the result of reading an info file
type ReadIndexInfoFileResult struct {
	ID   FileSetFileIdentifier
	Info index.IndexInfo
	Err  ReadInfoFileResultError
}

// ReadIndexInfoFiles reads all the valid index info entries. Even if ReadIndexInfoFiles returns an error,
// there may be some valid entries in the returned slice.
func ReadIndexInfoFiles(
	filePathPrefix string,
	namespace ident.ID,
	readerBufferSize int,
) []ReadIndexInfoFileResult {
	var infoFileResults []ReadIndexInfoFileResult
	forEachInfoFile(
		forEachInfoFileSelector{
			fileSetType:    persist.FileSetFlushType,
			contentType:    persist.FileSetIndexContentType,
			filePathPrefix: filePathPrefix,
			namespace:      namespace,
		},
		readerBufferSize,
		func(filepath string, id FileSetFileIdentifier, data []byte) {
			var info index.IndexInfo
			err := info.Unmarshal(data)
			infoFileResults = append(infoFileResults, ReadIndexInfoFileResult{
				ID:   id,
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
func SnapshotFiles(filePathPrefix string, namespace ident.ID, shard uint32) (FileSetFilesSlice, error) {
	return filesetFiles(filesetFilesSelector{
		fileSetType:    persist.FileSetSnapshotType,
		contentType:    persist.FileSetDataContentType,
		filePathPrefix: filePathPrefix,
		namespace:      namespace,
		shard:          shard,
		pattern:        filesetFilePattern,
	})
}

// IndexSnapshotFiles returns a slice of all the names for all the index fileset files
// for a given namespace.
func IndexSnapshotFiles(filePathPrefix string, namespace ident.ID) (FileSetFilesSlice, error) {
	return filesetFiles(filesetFilesSelector{
		fileSetType:    persist.FileSetSnapshotType,
		contentType:    persist.FileSetIndexContentType,
		filePathPrefix: filePathPrefix,
		namespace:      namespace,
		pattern:        filesetFilePattern,
	})
}

// FileSetAt returns a FileSetFile for the given namespace/shard/blockStart combination if it exists.
func FileSetAt(filePathPrefix string, namespace ident.ID, shard uint32, blockStart time.Time) (FileSetFile, bool, error) {
	matched, err := filesetFiles(filesetFilesSelector{
		fileSetType:    persist.FileSetFlushType,
		contentType:    persist.FileSetDataContentType,
		filePathPrefix: filePathPrefix,
		namespace:      namespace,
		shard:          shard,
		pattern:        filesetFileForTime(blockStart, anyLowerCaseCharsPattern),
	})
	if err != nil {
		return FileSetFile{}, false, err
	}

	matched.sortByTimeAscending()
	for i, fileset := range matched {
		if fileset.ID.BlockStart.Equal(blockStart) {
			nextIdx := i + 1
			if nextIdx < len(matched) && matched[nextIdx].ID.BlockStart.Equal(blockStart) {
				// Should never happen
				return FileSetFile{}, false, fmt.Errorf(
					"found multiple fileset files for blockStart: %d",
					blockStart.Unix(),
				)
			}

			if !fileset.HasCheckpointFile() {
				continue
			}

			return fileset, true, nil
		}
	}

	return FileSetFile{}, false, nil
}

// IndexFileSetsAt returns all FileSetFile(s) for the given namespace/blockStart combination.
// NB: It returns all complete Volumes found on disk.
func IndexFileSetsAt(filePathPrefix string, namespace ident.ID, blockStart time.Time) (FileSetFilesSlice, error) {
	matches, err := filesetFiles(filesetFilesSelector{
		fileSetType:    persist.FileSetFlushType,
		contentType:    persist.FileSetIndexContentType,
		filePathPrefix: filePathPrefix,
		namespace:      namespace,
		pattern:        filesetFileForTime(blockStart, anyLowerCaseCharsNumbersPattern),
	})
	if err != nil {
		return nil, err
	}

	filesets := make(FileSetFilesSlice, 0, len(matches))
	matches.sortByTimeAscending()
	for _, fileset := range matches {
		if fileset.ID.BlockStart.Equal(blockStart) {
			if !fileset.HasCheckpointFile() {
				continue
			}
			filesets = append(filesets, fileset)
		}
	}

	return filesets, nil
}

// DeleteFileSetAt deletes a FileSetFile for a given namespace/shard/blockStart combination if it exists.
func DeleteFileSetAt(filePathPrefix string, namespace ident.ID, shard uint32, t time.Time) error {
	fileset, ok, err := FileSetAt(filePathPrefix, namespace, shard, t)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("fileset for blockStart: %d does not exist", t.Unix())
	}

	return DeleteFiles(fileset.AbsoluteFilepaths)
}

// DataFileSetsBefore returns all the flush data fileset files whose timestamps are earlier than a given time.
func DataFileSetsBefore(filePathPrefix string, namespace ident.ID, shard uint32, t time.Time) ([]string, error) {
	matched, err := filesetFiles(filesetFilesSelector{
		fileSetType:    persist.FileSetFlushType,
		contentType:    persist.FileSetDataContentType,
		filePathPrefix: filePathPrefix,
		namespace:      namespace,
		shard:          shard,
		pattern:        filesetFilePattern,
	})
	if err != nil {
		return nil, err
	}
	return FilesBefore(matched.Filepaths(), t)
}

// IndexFileSetsBefore returns all the flush index fileset files whose timestamps are earlier than a given time.
func IndexFileSetsBefore(filePathPrefix string, namespace ident.ID, t time.Time) ([]string, error) {
	matched, err := filesetFiles(filesetFilesSelector{
		fileSetType:    persist.FileSetFlushType,
		contentType:    persist.FileSetIndexContentType,
		filePathPrefix: filePathPrefix,
		namespace:      namespace,
		pattern:        filesetFilePattern,
	})
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

// SortedCommitLogFiles returns all the commit log files in the commit logs directory.
func SortedCommitLogFiles(commitLogsDir string) ([]string, error) {
	return sortedCommitlogFiles(commitLogsDir, commitLogFilePattern)
}

// CommitLogFilesForTime returns all the commit log files for a given time.
func CommitLogFilesForTime(commitLogsDir string, t time.Time) ([]string, error) {
	commitLogFileForTimePattern := fmt.Sprintf(commitLogFileForTimeTemplate, t.UnixNano())
	return sortedCommitlogFiles(commitLogsDir, commitLogFileForTimePattern)
}

// SortedCommitLogFilesBefore returns all the commit log files whose timestamps are earlier than a given time.
func SortedCommitLogFilesBefore(commitLogsDir string, t time.Time) ([]string, error) {
	commitLogs, err := SortedCommitLogFiles(commitLogsDir)
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

type filesetFilesSelector struct {
	fileSetType    persist.FileSetType
	contentType    persist.FileSetContentType
	filePathPrefix string
	namespace      ident.ID
	shard          uint32
	pattern        string
}

func filesetFiles(args filesetFilesSelector) (FileSetFilesSlice, error) {
	var (
		byTimeAsc []string
		err       error
	)
	switch args.fileSetType {
	case persist.FileSetFlushType:
		switch args.contentType {
		case persist.FileSetDataContentType:
			dir := ShardDataDirPath(args.filePathPrefix, args.namespace, args.shard)
			byTimeAsc, err = findFiles(dir, args.pattern, func(files []string) sort.Interface {
				return byTimeAscending(files)
			})
		case persist.FileSetIndexContentType:
			dir := NamespaceIndexDataDirPath(args.filePathPrefix, args.namespace)
			byTimeAsc, err = findFiles(dir, args.pattern, func(files []string) sort.Interface {
				return fileSetFilesByTimeAndVolumeIndexAscending(files)
			})
		default:
			return nil, fmt.Errorf("unknown content type: %d", args.contentType)
		}
	case persist.FileSetSnapshotType:
		var dir string
		switch args.contentType {
		case persist.FileSetDataContentType:
			dir = ShardSnapshotsDirPath(args.filePathPrefix, args.namespace, args.shard)
		case persist.FileSetIndexContentType:
			dir = NamespaceIndexSnapshotDirPath(args.filePathPrefix, args.namespace)
		default:
			return nil, fmt.Errorf("unknown content type: %d", args.contentType)
		}
		byTimeAsc, err = findFiles(dir, args.pattern, func(files []string) sort.Interface {
			return fileSetFilesByTimeAndVolumeIndexAscending(files)
		})
	default:
		return nil, fmt.Errorf("unknown type: %d", args.fileSetType)
	}
	if err != nil {
		return nil, err
	}

	if len(byTimeAsc) == 0 {
		return nil, nil
	}

	var (
		latestBlockStart  time.Time
		latestVolumeIndex int
		latestFileSetFile FileSetFile
		filesetFiles      = []FileSetFile{}
	)
	for _, file := range byTimeAsc {
		var (
			currentFileBlockStart time.Time
			volumeIndex           int
			err                   error
		)
		switch args.fileSetType {
		case persist.FileSetFlushType:
			switch args.contentType {
			case persist.FileSetDataContentType:
				currentFileBlockStart, err = TimeFromFileName(file)
			case persist.FileSetIndexContentType:
				currentFileBlockStart, volumeIndex, err = TimeAndVolumeIndexFromFileSetFilename(file)
			default:
				return nil, fmt.Errorf("unknown content type: %d", args.contentType)
			}
		case persist.FileSetSnapshotType:
			currentFileBlockStart, volumeIndex, err = TimeAndVolumeIndexFromFileSetFilename(file)
		default:
			return nil, fmt.Errorf("unknown type: %d", args.fileSetType)
		}
		if err != nil {
			return nil, err
		}

		if latestBlockStart.IsZero() {
			latestFileSetFile = NewFileSetFile(FileSetFileIdentifier{
				Namespace:   args.namespace,
				BlockStart:  currentFileBlockStart,
				Shard:       args.shard,
				VolumeIndex: volumeIndex,
			})
		} else if !currentFileBlockStart.Equal(latestBlockStart) || latestVolumeIndex != volumeIndex {
			filesetFiles = append(filesetFiles, latestFileSetFile)
			latestFileSetFile = NewFileSetFile(FileSetFileIdentifier{
				Namespace:   args.namespace,
				BlockStart:  currentFileBlockStart,
				Shard:       args.shard,
				VolumeIndex: volumeIndex,
			})
		}
		latestBlockStart = currentFileBlockStart
		latestVolumeIndex = volumeIndex

		latestFileSetFile.AbsoluteFilepaths = append(latestFileSetFile.AbsoluteFilepaths, file)
	}
	filesetFiles = append(filesetFiles, latestFileSetFile)

	return filesetFiles, nil
}

func sortedCommitlogFiles(commitLogsDir string, pattern string) ([]string, error) {
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
	filePath string,
	readerBufferSize int,
	expectedDigest uint32,
) ([]byte, error) {
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

// NamespaceIndexDataDirPath returns the path to the data directory for a given namespace.
func NamespaceIndexDataDirPath(prefix string, namespace ident.ID) string {
	return path.Join(prefix, indexDirName, dataDirName, namespace.String())
}

// NamespaceIndexSnapshotDirPath returns the path to the data directory for a given namespace.
func NamespaceIndexSnapshotDirPath(prefix string, namespace ident.ID) string {
	return path.Join(prefix, indexDirName, snapshotDirName, namespace.String())
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

// DataFileSetExistsAt determines whether data fileset files exist for the given namespace, shard, and block start.
func DataFileSetExistsAt(prefix string, namespace ident.ID, shard uint32, blockStart time.Time) (bool, error) {
	_, ok, err := FileSetAt(prefix, namespace, shard, blockStart)
	if err != nil {
		return false, err
	}

	return ok, nil
}

// SnapshotFileSetExistsAt determines whether snapshot fileset files exist for the given namespace, shard, and block start time.
func SnapshotFileSetExistsAt(prefix string, namespace ident.ID, shard uint32, blockStart time.Time) (bool, error) {
	snapshotFiles, err := SnapshotFiles(prefix, namespace, shard)
	if err != nil {
		return false, err
	}

	latest, ok := snapshotFiles.LatestVolumeForBlock(blockStart)
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

// NextSnapshotFileSetVolumeIndex returns the next snapshot file set index for a given
// namespace/shard/blockStart combination.
func NextSnapshotFileSetVolumeIndex(filePathPrefix string, namespace ident.ID, shard uint32, blockStart time.Time) (int, error) {
	snapshotFiles, err := SnapshotFiles(filePathPrefix, namespace, shard)
	if err != nil {
		return -1, err
	}

	latestFile, ok := snapshotFiles.LatestVolumeForBlock(blockStart)
	if !ok {
		return 0, nil
	}

	return latestFile.ID.VolumeIndex + 1, nil
}

// NextIndexFileSetVolumeIndex returns the next index file set index for a given
// namespace/blockStart combination.
func NextIndexFileSetVolumeIndex(filePathPrefix string, namespace ident.ID, blockStart time.Time) (int, error) {
	files, err := filesetFiles(filesetFilesSelector{
		fileSetType:    persist.FileSetFlushType,
		contentType:    persist.FileSetIndexContentType,
		filePathPrefix: filePathPrefix,
		namespace:      namespace,
		pattern:        filesetFileForTime(blockStart, anyLowerCaseCharsNumbersPattern),
	})
	if err != nil {
		return -1, err
	}

	latestFile, ok := files.LatestVolumeForBlock(blockStart)
	if !ok {
		return 0, nil
	}

	return latestFile.ID.VolumeIndex + 1, nil
}

// NextIndexSnapshotFileIndex returns the next snapshot file index for a given
// namespace/shard/blockStart combination.
func NextIndexSnapshotFileIndex(filePathPrefix string, namespace ident.ID, blockStart time.Time) (int, error) {
	snapshotFiles, err := IndexSnapshotFiles(filePathPrefix, namespace)
	if err != nil {
		return -1, err
	}

	var currentSnapshotIndex = -1
	for _, snapshot := range snapshotFiles {
		if snapshot.ID.BlockStart.Equal(blockStart) {
			currentSnapshotIndex = snapshot.ID.VolumeIndex
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

func filesetFileForTime(t time.Time, suffix string) string {
	return fmt.Sprintf("%s%s%d%s%s%s", filesetFilePrefix, separator, t.UnixNano(), separator, suffix, fileSuffix)
}

func filesetPathFromTime(prefix string, t time.Time, suffix string) string {
	return path.Join(prefix, filesetFileForTime(t, suffix))
}

func filesetPathFromTimeAndIndex(prefix string, t time.Time, index int, suffix string) string {
	return path.Join(prefix, filesetFileForTime(t, fmt.Sprintf("%d%s%s", index, separator, suffix)))
}

func filesetIndexSegmentFileSuffixFromTime(
	t time.Time,
	segmentIndex int,
	segmentFileType idxpersist.IndexSegmentFileType,
) string {
	return fmt.Sprintf("%s%s%d%s%s", segmentFileSetFilePrefix, separator, segmentIndex, separator, segmentFileType)
}

func filesetIndexSegmentFilePathFromTime(
	prefix string,
	t time.Time,
	volumeIndex int,
	segmentIndex int,
	segmentFileType idxpersist.IndexSegmentFileType,
) string {
	suffix := filesetIndexSegmentFileSuffixFromTime(t, segmentIndex, segmentFileType)
	return filesetPathFromTimeAndIndex(prefix, t, volumeIndex, suffix)
}

func snapshotIndexSegmentFilePathFromTimeAndIndex(
	prefix string,
	t time.Time,
	snapshotIndex int,
	segmentIndex int,
	segmentFileType idxpersist.IndexSegmentFileType,
) string {
	suffix := filesetIndexSegmentFileSuffixFromTime(t, segmentIndex, segmentFileType)
	return filesetPathFromTimeAndIndex(prefix, t, snapshotIndex, suffix)
}
