// Copyright (c) 2017 Uber Technologies, Inc.
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

package commitlog

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
)

// ReadLogInfo reads the commit log info out of a commitlog file
func ReadLogInfo(filePath string, opts Options) (time.Time, time.Duration, int64, error) {
	var fd *os.File
	var err error
	defer func() {
		if fd == nil {
			fd.Close()
		}
	}()

	fd, err = os.Open(filePath)
	if err != nil {
		return time.Time{}, 0, 0, fsError{err}
	}

	chunkReader := newChunkReader(opts.FlushSize())
	chunkReader.reset(fd)
	size, err := binary.ReadUvarint(chunkReader)
	if err != nil {
		return time.Time{}, 0, 0, err
	}

	bytes := make([]byte, size)
	_, err = chunkReader.Read(bytes)
	if err != nil {
		return time.Time{}, 0, 0, err
	}
	logDecoder := msgpack.NewDecoder(nil)
	logDecoder.Reset(msgpack.NewDecoderStream(bytes))
	logInfo, decoderErr := logDecoder.DecodeLogInfo()

	err = fd.Close()
	fd = nil
	if err != nil {
		return time.Time{}, 0, 0, fsError{err}
	}

	return time.Unix(0, logInfo.Start), time.Duration(logInfo.Duration), logInfo.Index, decoderErr
}

// Files returns a slice of all available commit log files on disk along with
// their associated metadata.
func Files(opts Options) ([]persist.CommitlogFile, []ErrorWithPath, error) {
	commitLogsDir := fs.CommitLogsDirPath(
		opts.FilesystemOptions().FilePathPrefix())
	filePaths, err := fs.SortedCommitLogFiles(commitLogsDir)
	if err != nil {
		return nil, nil, err
	}

	commitLogFiles := make([]persist.CommitlogFile, 0, len(filePaths))
	errorsWithPath := make([]ErrorWithPath, 0)
	for _, filePath := range filePaths {
		file := persist.CommitlogFile{
			FilePath: filePath,
		}

		start, duration, index, err := ReadLogInfo(filePath, opts)
		if _, ok := err.(fsError); ok {
			return nil, nil, err
		}

		if err != nil {
			errorsWithPath = append(errorsWithPath, NewErrorWithPath(
				err, filePath))
			continue
		}

		if err == nil {
			file.Start = start
			file.Duration = duration
			file.Index = index
		}

		commitLogFiles = append(commitLogFiles, persist.CommitlogFile{
			FilePath: filePath,
			Start:    start,
			Duration: duration,
			Index:    index,
		})
	}

	sort.Slice(commitLogFiles, func(i, j int) bool {
		// Sorting is best effort here since we may not know the start.
		return commitLogFiles[i].Start.Before(commitLogFiles[j].Start)
	})

	return commitLogFiles, errorsWithPath, nil
}

// ErrorWithPath is an error that includes the path of the file that
// had the error.
type ErrorWithPath struct {
	err  error
	path string
}

// Error returns the error.
func (e ErrorWithPath) Error() string {
	return fmt.Sprintf("%s: %s", e.path, e.err)
}

// Path returns the path of the file that the error is associated with.
func (e ErrorWithPath) Path() string {
	return e.path
}

// NewErrorWithPath creates a new ErrorWithPath.
func NewErrorWithPath(err error, path string) ErrorWithPath {
	return ErrorWithPath{
		err:  err,
		path: path,
	}
}

type fsError struct {
	err error
}

func (e fsError) Error() string {
	return e.err.Error()
}
