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
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/persist/encoding/msgpack"
	"github.com/m3db/m3db/ratelimit"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3x/instrument"
)

const (
	// defaultWriterBufferSize is the default buffer size for writing TSDB files
	defaultWriterBufferSize = 65536

	// defaultReaderBufferSize is the default buffer size for reading TSDB files
	defaultReaderBufferSize = 65536

	// defaultThroughputCheckInterval is the default throughput check interval
	defaultThroughputCheckInterval = 100 * time.Millisecond

	// defaultThroughputLimitMbps is the default throughput limit in Mb/s
	defaultThroughputLimitMbps = 50.0
)

var (
	defaultFilePathPrefix   = os.TempDir()
	defaultNewFileMode      = os.FileMode(0666)
	defaultNewDirectoryMode = os.ModeDir | os.FileMode(0755)
)

type options struct {
	clockOpts        clock.Options
	instrumentOpts   instrument.Options
	retentionOpts    retention.Options
	rateLimitOpts    ratelimit.Options
	decodingOpts     msgpack.DecodingOptions
	filePathPrefix   string
	newFileMode      os.FileMode
	newDirectoryMode os.FileMode
	writerBufferSize int
	readerBufferSize int
}

// NewOptions creates a new set of fs options
func NewOptions() Options {
	return &options{
		clockOpts:        clock.NewOptions(),
		instrumentOpts:   instrument.NewOptions(),
		retentionOpts:    retention.NewOptions(),
		rateLimitOpts:    ratelimit.NewOptions(),
		decodingOpts:     msgpack.NewDecodingOptions(),
		filePathPrefix:   defaultFilePathPrefix,
		newFileMode:      defaultNewFileMode,
		newDirectoryMode: defaultNewDirectoryMode,
		writerBufferSize: defaultWriterBufferSize,
		readerBufferSize: defaultReaderBufferSize,
	}
}

func (o *options) SetClockOptions(value clock.Options) Options {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *options) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetRetentionOptions(value retention.Options) Options {
	opts := *o
	opts.retentionOpts = value
	return &opts
}

func (o *options) RetentionOptions() retention.Options {
	return o.retentionOpts
}

func (o *options) SetRateLimitOptions(value ratelimit.Options) Options {
	opts := *o
	opts.rateLimitOpts = value
	return &opts
}

func (o *options) RateLimitOptions() ratelimit.Options {
	return o.rateLimitOpts
}

func (o *options) SetDecodingOptions(value msgpack.DecodingOptions) Options {
	opts := *o
	opts.decodingOpts = value
	return &opts
}

func (o *options) DecodingOptions() msgpack.DecodingOptions {
	return o.decodingOpts
}

func (o *options) SetFilePathPrefix(value string) Options {
	opts := *o
	opts.filePathPrefix = value
	return &opts
}

func (o *options) FilePathPrefix() string {
	return o.filePathPrefix
}

func (o *options) SetNewFileMode(value os.FileMode) Options {
	opts := *o
	opts.newFileMode = value
	return &opts
}

func (o *options) NewFileMode() os.FileMode {
	return o.newFileMode
}

func (o *options) SetNewDirectoryMode(value os.FileMode) Options {
	opts := *o
	opts.newDirectoryMode = value
	return &opts
}

func (o *options) NewDirectoryMode() os.FileMode {
	return o.newDirectoryMode
}

func (o *options) SetWriterBufferSize(value int) Options {
	opts := *o
	opts.writerBufferSize = value
	return &opts
}

func (o *options) WriterBufferSize() int {
	return o.writerBufferSize
}

func (o *options) SetReaderBufferSize(value int) Options {
	opts := *o
	opts.readerBufferSize = value
	return &opts
}

func (o *options) ReaderBufferSize() int {
	return o.readerBufferSize
}
