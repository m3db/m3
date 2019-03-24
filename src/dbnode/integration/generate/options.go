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

package generate

import (
	"os"
	"time"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
)

const (
	// defaultRetentionPeriod is how long we keep data in memory by default.
	defaultRetentionPeriod = 2 * 24 * time.Hour

	// defaultBlockSize is the default block size
	defaultBlockSize = 2 * time.Hour

	// defaultWriterBufferSize is the default buffer size for writing TSDB files
	defaultWriterBufferSize = 65536

	// defaultNewFileMode is the file mode used for new files by default
	defaultNewFileMode = os.FileMode(0666)

	// defaultNewDirectoryMode is the file mode used for new directories by default
	defaultNewDirectoryMode = os.ModeDir | os.FileMode(0755)

	// defaultWriteEmptyShards is the writeEmptyShards value by default
	defaultWriteEmptyShards = true

	// defaultWriteSnapshot determines whether the writer writes snapshots instead
	// of data files.
	defaultWriteSnapshot = false
)

var (
	defaultFilePathPrefix = os.TempDir()
)

type options struct {
	clockOpts        clock.Options
	retentionPeriod  time.Duration
	blockSize        time.Duration
	filePathPrefix   string
	newFileMode      os.FileMode
	newDirectoryMode os.FileMode
	writerBufferSize int
	writeEmptyShards bool
	writeSnapshot    bool
	encoderPool      encoding.EncoderPool
}

// NewOptions creates a new set of fs options
func NewOptions() Options {
	encoderPool := encoding.NewEncoderPool(nil)
	encodingOpts := encoding.NewOptions().SetEncoderPool(encoderPool)
	encoderPool.Init(func() encoding.Encoder {
		return m3tsz.NewEncoder(time.Time{}, nil, nil, m3tsz.DefaultIntOptimizationEnabled, encodingOpts)
	})

	return &options{
		clockOpts:        clock.NewOptions(),
		retentionPeriod:  defaultRetentionPeriod,
		blockSize:        defaultBlockSize,
		filePathPrefix:   defaultFilePathPrefix,
		newFileMode:      defaultNewFileMode,
		newDirectoryMode: defaultNewDirectoryMode,
		writerBufferSize: defaultWriterBufferSize,
		writeEmptyShards: defaultWriteEmptyShards,
		writeSnapshot:    defaultWriteSnapshot,
		encoderPool:      encoderPool,
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

func (o *options) SetRetentionPeriod(value time.Duration) Options {
	opts := *o
	opts.retentionPeriod = value
	return &opts
}

func (o *options) RetentionPeriod() time.Duration {
	return o.retentionPeriod
}

func (o *options) SetBlockSize(value time.Duration) Options {
	opts := *o
	opts.blockSize = value
	return &opts
}

func (o *options) BlockSize() time.Duration {
	return o.blockSize
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

func (o *options) SetWriteEmptyShards(value bool) Options {
	opts := *o
	opts.writeEmptyShards = value
	return &opts
}

func (o *options) WriteEmptyShards() bool {
	return o.writeEmptyShards
}

func (o *options) SetWriteSnapshot(value bool) Options {
	opts := *o
	opts.writeSnapshot = value
	return &opts
}

func (o *options) WriteSnapshot() bool {
	return o.writeSnapshot
}

func (o *options) SetEncoderPool(value encoding.EncoderPool) Options {
	opts := *o
	opts.encoderPool = value
	return &opts
}

func (o *options) EncoderPool() encoding.EncoderPool {
	return o.encoderPool
}
