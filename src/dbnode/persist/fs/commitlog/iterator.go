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

package commitlog

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/m3db/m3/src/dbnode/persist"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	errIndexDoesNotMatch = errors.New("commit log file index does not match filename")
)

type iteratorMetrics struct {
	readsErrors tally.Counter
}

type iterator struct {
	iterOpts IteratorOpts
	opts     Options
	scope    tally.Scope
	metrics  iteratorMetrics
	log      *zap.Logger
	files    []persist.CommitLogFile
	reader   CommitLogReader
	read     LogEntry
	err      error
	setRead  bool
	closed   bool
}

// ReadAllPredicate can be passed as the ReadCommitLogPredicate for callers
// that want a convenient way to read all the commitlogs
func ReadAllPredicate() FileFilterPredicate {
	return func(f FileFilterInfo) bool { return true }
}

// NewIterator creates a new commit log iterator
func NewIterator(iterOpts IteratorOpts) (iter Iterator, corruptFiles []ErrorWithPath, err error) {
	opts := iterOpts.CommitLogOptions
	iops := opts.InstrumentOptions()
	iops = iops.SetMetricsScope(iops.MetricsScope().SubScope("iterator"))

	files, corruptFiles, err := Files(opts)
	if err != nil {
		return nil, nil, err
	}
	filteredFiles := filterFiles(files, iterOpts.FileFilterPredicate)
	filteredCorruptFiles := filterCorruptFiles(corruptFiles, iterOpts.FileFilterPredicate)

	scope := iops.MetricsScope()
	log := iops.Logger()
	log.Info("found commit log files to read",
		zap.Int("fileCount", len(filteredFiles)),
		zap.Strings("commitlog-files", commitLogFilesForLogging(filteredFiles)))
	return &iterator{
		iterOpts: iterOpts,
		opts:     opts,
		scope:    scope,
		metrics: iteratorMetrics{
			readsErrors: scope.Counter("reads.errors"),
		},
		log:   log,
		files: filteredFiles,
	}, filteredCorruptFiles, nil
}

func (i *iterator) Next() bool {
	if i.hasError() || i.closed {
		return false
	}
	if i.reader == nil {
		if !i.nextReader() {
			return false
		}
	}
	var err error
	i.read, err = i.reader.Read()
	if err == io.EOF {
		closeErr := i.closeAndResetReader()
		if closeErr != nil {
			i.err = closeErr
		}
		// Try the next reader
		return i.Next()
	}
	if err != nil {
		// Try the next reader, this enables restoring with best effort from commit logs
		i.metrics.readsErrors.Inc(1)
		i.log.Error("commit log reader returned error, iterator moving to next file", zap.Error(err))
		i.err = err
		closeErr := i.closeAndResetReader()
		if closeErr != nil {
			i.err = closeErr
		}
		return i.Next()
	}
	i.setRead = true
	return true
}

func (i *iterator) Current() LogEntry {
	read := i.read
	if i.hasError() || i.closed || !i.setRead {
		read = LogEntry{}
	}
	return read
}

func (i *iterator) Err() error {
	return i.err
}

// TODO: Refactor codebase so that it can handle Close() returning an error
func (i *iterator) Close() {
	if i.closed {
		return
	}
	i.closed = true
	i.closeAndResetReader()
}

func (i *iterator) hasError() bool {
	return i.err != nil
}

func (i *iterator) nextReader() bool {
	if len(i.files) == 0 {
		return false
	}

	err := i.closeAndResetReader()
	if err != nil {
		i.err = err
		return false
	}

	file := i.files[0]
	i.files = i.files[1:]

	reader := NewCommitLogReader(CommitLogReaderOptions{
		commitLogOptions:    i.opts,
		returnMetadataAsRef: i.iterOpts.ReturnMetadataAsRef,
	})
	index, err := reader.Open(file.FilePath)
	if err != nil {
		i.err = err
		return false
	}
	if index != file.Index {
		i.err = errIndexDoesNotMatch
		return false
	}

	i.log.Info("reading commit log file", zap.String("file", file.FilePath))
	i.reader = reader
	return true
}

func filterFiles(files []persist.CommitLogFile, predicate FileFilterPredicate) []persist.CommitLogFile {
	filtered := make([]persist.CommitLogFile, 0, len(files))
	for _, f := range files {
		info := FileFilterInfo{File: f}
		if predicate(info) {
			filtered = append(filtered, f)
		}
	}
	return filtered
}

func filterCorruptFiles(corruptFiles []ErrorWithPath, predicate FileFilterPredicate) []ErrorWithPath {
	filtered := make([]ErrorWithPath, 0, len(corruptFiles))
	for _, errWithPath := range corruptFiles {
		info := FileFilterInfo{
			Err:       errWithPath,
			IsCorrupt: true,
		}
		if predicate(info) {
			filtered = append(filtered, errWithPath)
		}
	}
	return filtered
}

func commitLogFilesForLogging(files []persist.CommitLogFile) []string {
	result := make([]string, 0, len(files))
	for _, f := range files {
		var fileSize int64
		if fi, err := os.Stat(f.FilePath); err == nil {
			fileSize = fi.Size()
		}
		result = append(result, fmt.Sprintf("path=%s, len=%d", f.FilePath, fileSize))
	}
	return result
}

func (i *iterator) closeAndResetReader() error {
	if i.reader == nil {
		return nil
	}
	reader := i.reader
	i.reader = nil
	return reader.Close()
}
