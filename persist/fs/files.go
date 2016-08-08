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
	"github.com/m3db/m3db/generated/proto/schema"
	"github.com/m3db/m3x/close"
	"github.com/m3db/m3x/errors"

	"github.com/golang/protobuf/proto"
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

// ForEachInfoFile iterates over each valid info file and applies the function passed in.
func ForEachInfoFile(filePathPrefix string, shard uint32, readerBufferSize int, fn infoFileFn) {
	shardDir := ShardDirPath(filePathPrefix, shard)
	matched, err := filepath.Glob(path.Join(shardDir, infoFilePattern))
	if err != nil {
		return
	}

	sort.Sort(byTimeAscending(matched))
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
func ReadInfoFiles(filePathPrefix string, shard uint32, readerBufferSize int) []*schema.IndexInfo {
	var indexEntries []*schema.IndexInfo
	ForEachInfoFile(filePathPrefix, shard, readerBufferSize, func(_ string, data []byte) {
		info, err := readInfo(data)
		if err != nil {
			return
		}
		indexEntries = append(indexEntries, info)
	})
	return indexEntries
}

// CommitLogFiles returns all the commit log files in the commit logs directory.
func CommitLogFiles(commitLogsDir string) ([]string, error) {
	matched, err := filepath.Glob(path.Join(commitLogsDir, commitLogFilePattern))
	if err != nil {
		return nil, err
	}
	sort.Sort(byTimeAndIndexAscending(matched))
	return matched, err
}

// readInfo reads the data stored in the info file and returns an index entry.
func readInfo(data []byte) (*schema.IndexInfo, error) {
	info := &schema.IndexInfo{}
	if err := proto.Unmarshal(data, info); err != nil {
		return nil, err
	}
	return info, nil
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

	fwd := digest.NewFdWithDigestReader(readerBufferSize)
	fwd.Reset(fd)
	return fwd.ReadAllAndValidate(expectedDigest)
}

// ShardDirPath returns the path to a given shard.
func ShardDirPath(prefix string, shard uint32) string {
	return path.Join(prefix, dataDirName, strconv.Itoa(int(shard)))
}

// CommitLogsDirPath returns the path to commit logs.
func CommitLogsDirPath(prefix string) string {
	return path.Join(prefix, commitLogsDirName)
}

// FileExistsAt determines whether a data file exists for the given shard and block start time.
func FileExistsAt(prefix string, shard uint32, blockStart time.Time) bool {
	shardDir := ShardDirPath(prefix, shard)
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
