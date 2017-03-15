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
	"github.com/m3db/m3db/persist/encoding/msgpack"
	"github.com/m3db/m3db/persist/schema"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/close"
	"github.com/m3db/m3x/errors"
)

var timeZero time.Time

const (
	dataDirName       = "data"
	commitLogsDirName = "commitlogs"
)

type fileOpener func(filePath string) (*os.File, error)

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

// byTimeAndIndexAscending sorts files by their block start times and index in ascending
// order. If the files do not have block start times or indexes in their names, the result
// is undefined.
type byTimeAndIndexAscending []string

func (a byTimeAndIndexAscending) Len() int      { return len(a) }
func (a byTimeAndIndexAscending) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byTimeAndIndexAscending) Less(i, j int) bool {
	ti, ii, _ := TimeAndIndexFromFileName(a[i])
	tj, ij, _ := TimeAndIndexFromFileName(a[j])
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

// TimeAndIndexFromFileName extracts the block start and index from file name.
func TimeAndIndexFromFileName(fname string) (time.Time, int, error) {
	components, t, err := componentsAndTimeFromFileName(fname)
	if err != nil {
		return timeZero, 0, err
	}
	str := strings.Replace(components[2], fileSuffix, "", 1)
	index, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return timeZero, 0, err
	}
	return t, int(index), nil
}

type infoFileFn func(fname string, infoData []byte)

func forEachInfoFile(filePathPrefix string, namespace ts.ID, shard uint32, readerBufferSize int, fn infoFileFn) {
	matched, err := filesetFiles(filePathPrefix, namespace, shard, infoFilePattern)
	if err != nil {
		return
	}

	shardDir := ShardDirPath(filePathPrefix, namespace, shard)
	digestBuf := digest.NewBuffer()
	for i := range matched {
		t, err := TimeFromFileName(matched[i])
		if err != nil {
			continue
		}
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
		fn(matched[i], infoData)
	}
}

// ReadInfoFiles reads all the valid info entries.
func ReadInfoFiles(
	filePathPrefix string,
	namespace ts.ID,
	shard uint32,
	readerBufferSize int,
	decodingOpts msgpack.DecodingOptions,
) []schema.IndexInfo {
	var indexEntries []schema.IndexInfo
	decoder := msgpack.NewDecoder(decodingOpts)
	forEachInfoFile(filePathPrefix, namespace, shard, readerBufferSize, func(_ string, data []byte) {
		decoder.Reset(data)
		info, err := decoder.DecodeIndexInfo()
		if err != nil {
			return
		}
		indexEntries = append(indexEntries, info)
	})
	return indexEntries
}

// FilesetBefore returns all the fileset files whose timestamps are earlier than a given time.
func FilesetBefore(filePathPrefix string, namespace ts.ID, shard uint32, t time.Time) ([]string, error) {
	matched, err := filesetFiles(filePathPrefix, namespace, shard, filesetFilePattern)
	if err != nil {
		return nil, err
	}
	return filesBefore(matched, t)
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
	var multiErr xerrors.MultiError

	commitLogs, err := CommitLogFiles(commitLogsDir)
	if err != nil {
		multiErr = multiErr.Add(err)
	}
	res, err := filesBefore(commitLogs, t)
	if err != nil {
		multiErr = multiErr.Add(err)
	}
	return res, multiErr.FinalError()
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

func filesetFiles(filePathPrefix string, namespace ts.ID, shard uint32, pattern string) ([]string, error) {
	shardDir := ShardDirPath(filePathPrefix, namespace, shard)
	return findFiles(shardDir, pattern, func(files []string) sort.Interface {
		return byTimeAscending(files)
	})
}

func commitlogFiles(commitLogsDir string, pattern string) ([]string, error) {
	return findFiles(commitLogsDir, pattern, func(files []string) sort.Interface {
		return byTimeAndIndexAscending(files)
	})
}

func filesBefore(files []string, t time.Time) ([]string, error) {
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

// NamespaceDirPath returns the path to a given namespace.
func NamespaceDirPath(prefix string, namespace ts.ID) string {
	return path.Join(prefix, dataDirName, namespace.String())
}

// ShardDirPath returns the path to a given shard.
func ShardDirPath(prefix string, namespace ts.ID, shard uint32) string {
	namespacePath := NamespaceDirPath(prefix, namespace)
	return path.Join(namespacePath, strconv.Itoa(int(shard)))
}

// CommitLogsDirPath returns the path to commit logs.
func CommitLogsDirPath(prefix string) string {
	return path.Join(prefix, commitLogsDirName)
}

// FilesetExistsAt determines whether a data file exists for the given namespace, shard, and block start time.
func FilesetExistsAt(prefix string, namespace ts.ID, shard uint32, blockStart time.Time) bool {
	shardDir := ShardDirPath(prefix, namespace, shard)
	checkpointFile := filesetPathFromTime(shardDir, blockStart, checkpointFileSuffix)
	return FileExists(checkpointFile)
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
