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
	"os"

	"github.com/m3db/m3db/instrument"
	"github.com/m3db/m3db/retention"
)

const (
	// defaultWriterBufferSize is the default buffer size for writing TSDB files
	defaultWriterBufferSize = 65536

	// defaultReaderBufferSize is the default buffer size for reading TSDB files
	defaultReaderBufferSize = 65536
)

var (
	defaultFilePathPrefix   = os.TempDir()
	defaultNewFileMode      = os.FileMode(0666)
	defaultNewDirectoryMode = os.ModeDir | os.FileMode(0755)
)

type options struct {
	instrumentOpts   instrument.Options
	retentionOpts    retention.Options
	filePathPrefix   string
	newFileMode      os.FileMode
	newDirectoryMode os.FileMode
	writerBufferSize int
	readerBufferSize int
}

// NewOptions creates a new set of fs options
func NewOptions() Options {
	return &options{
		instrumentOpts:   instrument.NewOptions(),
		retentionOpts:    retention.NewOptions(),
		filePathPrefix:   defaultFilePathPrefix,
		newFileMode:      defaultNewFileMode,
		newDirectoryMode: defaultNewDirectoryMode,
		writerBufferSize: defaultWriterBufferSize,
		readerBufferSize: defaultReaderBufferSize,
	}
}

func (o *options) InstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) GetInstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) RetentionOptions(value retention.Options) Options {
	opts := *o
	opts.retentionOpts = value
	return &opts
}

func (o *options) GetRetentionOptions() retention.Options {
	return o.retentionOpts
}

func (o *options) FilePathPrefix(value string) Options {
	opts := *o
	opts.filePathPrefix = value
	return &opts
}

func (o *options) GetFilePathPrefix() string {
	return o.filePathPrefix
}

func (o *options) NewFileMode(value os.FileMode) Options {
	opts := *o
	opts.newFileMode = value
	return &opts
}

func (o *options) GetNewFileMode() os.FileMode {
	return o.newFileMode
}

func (o *options) NewDirectoryMode(value os.FileMode) Options {
	opts := *o
	opts.newDirectoryMode = value
	return &opts
}

func (o *options) GetNewDirectoryMode() os.FileMode {
	return o.newDirectoryMode
}

func (o *options) WriterBufferSize(value int) Options {
	opts := *o
	opts.writerBufferSize = value
	return &opts
}

func (o *options) GetWriterBufferSize() int {
	return o.writerBufferSize
}

func (o *options) ReaderBufferSize(value int) Options {
	opts := *o
	opts.readerBufferSize = value
	return &opts
}

func (o *options) GetReaderBufferSize() int {
	return o.readerBufferSize
}
