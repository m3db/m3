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
	"io"

	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/ts"
	xlog "github.com/m3db/m3x/log"
	xtime "github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
)

var (
	errStartDoesNotMatch    = errors.New("commit log file start does not match filename")
	errDurationDoesNotMatch = errors.New("commit log file duration does not match filename")
	errIndexDoesNotMatch    = errors.New("commit log file index does not match filename")
)

type iteratorMetrics struct {
	readsErrors tally.Counter
}

type iterator struct {
	opts    Options
	scope   tally.Scope
	metrics iteratorMetrics
	log     xlog.Logger
	files   []string
	reader  commitLogReader
	read    iteratorRead
	setRead bool
	err     error
	closed  bool
}

type iteratorRead struct {
	series     Series
	datapoint  ts.Datapoint
	unit       xtime.Unit
	annotation []byte
}

// NewIterator creates a new commit log iterator
func NewIterator(opts Options) (Iterator, error) {
	iops := opts.InstrumentOptions()
	iops = iops.SetMetricsScope(iops.MetricsScope().SubScope("iterator"))
	fsopts := opts.FilesystemOptions()
	files, err := fs.CommitLogFiles(fs.CommitLogsDirPath(fsopts.FilePathPrefix()))
	if err != nil {
		return nil, err
	}
	scope := iops.MetricsScope()
	return &iterator{
		opts:  opts,
		scope: scope,
		metrics: iteratorMetrics{
			readsErrors: scope.Counter("reads.errors"),
		},
		log:   iops.Logger(),
		files: files,
	}, nil
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
	i.read.series, i.read.datapoint, i.read.unit, i.read.annotation, err = i.reader.Read()
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
		i.log.Errorf("commit log reader returned error, iterator moving to next file: %v", err)
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

func (i *iterator) Current() (Series, ts.Datapoint, xtime.Unit, ts.Annotation) {
	read := i.read
	if i.hasError() || i.closed || !i.setRead {
		read = iteratorRead{}
	}
	return read.series, read.datapoint, read.unit, read.annotation
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

	t, idx, err := fs.TimeAndIndexFromFileName(file)
	if err != nil {
		i.err = err
		return false
	}

	reader := newCommitLogReader(i.opts)
	start, duration, index, err := reader.Open(file)
	if err != nil {
		i.err = err
		return false
	}
	if !t.Equal(start) {
		i.err = errStartDoesNotMatch
		return false
	}
	if duration != i.opts.BlockSize() {
		i.err = errDurationDoesNotMatch
		return false
	}
	if index != idx {
		i.err = errIndexDoesNotMatch
		return false
	}

	i.reader = reader
	return true
}

func (i *iterator) closeAndResetReader() error {
	if i.reader == nil {
		return nil
	}
	reader := i.reader
	i.reader = nil
	return reader.Close()
}
