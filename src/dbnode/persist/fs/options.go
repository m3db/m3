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
	"errors"
	"fmt"
	"os"

	"github.com/m3db/m3/src/dbnode/clock"
	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/serialize"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
)

const (
	// defaultIndexSummariesPercent is the default percent of series for which an entry will be written into the metadata summary
	defaultIndexSummariesPercent = 0.03

	// defaultIndexBloomFilterFalsePositivePercent is the false positive percent to use to calculate size for when writing bloom filters
	defaultIndexBloomFilterFalsePositivePercent = 0.02

	// defaultWriterBufferSize is the default buffer size for writing TSDB files
	defaultWriterBufferSize = 65536

	// defaultDataReaderBufferSize is the default buffer size for reading TSDB data and index files
	defaultDataReaderBufferSize = 65536

	// defaultInfoReaderBufferSize is the default buffer size for reading TSDB info, checkpoint and digest files
	defaultInfoReaderBufferSize = 64

	// defaultSeekReaderBufferSize is the default buffer size for fs seeker's data buffer
	defaultSeekReaderBufferSize = 4096

	// defaultMmapEnableHugePages is the default setting whether to enable huge pages or not
	defaultMmapEnableHugePages = false

	// defaultMmapHugePagesThreshold is the default threshold for when to enable huge pages if enabled
	defaultMmapHugePagesThreshold = 2 << 14 // 32kb (or when eclipsing 8 pages of default 4096 page size)
)

var (
	defaultFilePathPrefix   = os.TempDir()
	defaultNewFileMode      = os.FileMode(0666)
	defaultNewDirectoryMode = os.ModeDir | os.FileMode(0755)

	errTagEncoderPoolNotSet = errors.New("tag encoder pool is not set")
	errTagDecoderPoolNotSet = errors.New("tag decoder pool is not set")
)

type options struct {
	clockOpts                            clock.Options
	instrumentOpts                       instrument.Options
	runtimeOptsMgr                       runtime.OptionsManager
	decodingOpts                         msgpack.DecodingOptions
	filePathPrefix                       string
	newFileMode                          os.FileMode
	newDirectoryMode                     os.FileMode
	indexSummariesPercent                float64
	indexBloomFilterFalsePositivePercent float64
	writerBufferSize                     int
	dataReaderBufferSize                 int
	infoReaderBufferSize                 int
	seekReaderBufferSize                 int
	mmapEnableHugePages                  bool
	mmapHugePagesThreshold               int64
	tagEncoderPool                       serialize.TagEncoderPool
	tagDecoderPool                       serialize.TagDecoderPool
	fstOptions                           fst.Options
}

// NewOptions creates a new set of fs options
func NewOptions() Options {
	tagEncoderPool := serialize.NewTagEncoderPool(
		serialize.NewTagEncoderOptions(), pool.NewObjectPoolOptions())
	tagEncoderPool.Init()
	tagDecoderPool := serialize.NewTagDecoderPool(
		serialize.NewTagDecoderOptions(), pool.NewObjectPoolOptions())
	tagDecoderPool.Init()
	fstOptions := fst.NewOptions()

	return &options{
		clockOpts:                            clock.NewOptions(),
		instrumentOpts:                       instrument.NewOptions(),
		runtimeOptsMgr:                       runtime.NewOptionsManager(),
		decodingOpts:                         msgpack.NewDecodingOptions(),
		filePathPrefix:                       defaultFilePathPrefix,
		newFileMode:                          defaultNewFileMode,
		newDirectoryMode:                     defaultNewDirectoryMode,
		indexSummariesPercent:                defaultIndexSummariesPercent,
		indexBloomFilterFalsePositivePercent: defaultIndexBloomFilterFalsePositivePercent,
		writerBufferSize:                     defaultWriterBufferSize,
		dataReaderBufferSize:                 defaultDataReaderBufferSize,
		infoReaderBufferSize:                 defaultInfoReaderBufferSize,
		seekReaderBufferSize:                 defaultSeekReaderBufferSize,
		mmapEnableHugePages:                  defaultMmapEnableHugePages,
		mmapHugePagesThreshold:               defaultMmapHugePagesThreshold,
		tagEncoderPool:                       tagEncoderPool,
		tagDecoderPool:                       tagDecoderPool,
		fstOptions:                           fstOptions,
	}
}

func (o *options) Validate() error {
	if o.indexSummariesPercent < 0 || o.indexSummariesPercent > 1.0 {
		return fmt.Errorf(
			"invalid index summaries percent, must be >= 0 and <= 1: instead %f",
			o.indexSummariesPercent)
	}
	if o.indexBloomFilterFalsePositivePercent < 0 || o.indexBloomFilterFalsePositivePercent > 1.0 {
		return fmt.Errorf(
			"invalid index bloom filter false positive percent, must be >= 0 and <= 1: instead %f",
			o.indexBloomFilterFalsePositivePercent)
	}
	if o.tagEncoderPool == nil {
		return errTagEncoderPoolNotSet
	}
	if o.tagDecoderPool == nil {
		return errTagDecoderPoolNotSet
	}
	return nil
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

func (o *options) SetRuntimeOptionsManager(value runtime.OptionsManager) Options {
	opts := *o
	opts.runtimeOptsMgr = value
	return &opts
}

func (o *options) RuntimeOptionsManager() runtime.OptionsManager {
	return o.runtimeOptsMgr
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

func (o *options) SetIndexSummariesPercent(value float64) Options {
	opts := *o
	opts.indexSummariesPercent = value
	return &opts
}

func (o *options) IndexSummariesPercent() float64 {
	return o.indexSummariesPercent
}

func (o *options) SetIndexBloomFilterFalsePositivePercent(value float64) Options {
	opts := *o
	opts.indexBloomFilterFalsePositivePercent = value
	return &opts
}

func (o *options) IndexBloomFilterFalsePositivePercent() float64 {
	return o.indexBloomFilterFalsePositivePercent
}

func (o *options) SetWriterBufferSize(value int) Options {
	opts := *o
	opts.writerBufferSize = value
	return &opts
}

func (o *options) WriterBufferSize() int {
	return o.writerBufferSize
}

func (o *options) SetDataReaderBufferSize(value int) Options {
	opts := *o
	opts.dataReaderBufferSize = value
	return &opts
}

func (o *options) DataReaderBufferSize() int {
	return o.dataReaderBufferSize
}

func (o *options) SetInfoReaderBufferSize(value int) Options {
	opts := *o
	opts.infoReaderBufferSize = value
	return &opts
}

func (o *options) InfoReaderBufferSize() int {
	return o.infoReaderBufferSize
}

func (o *options) SetSeekReaderBufferSize(value int) Options {
	opts := *o
	opts.seekReaderBufferSize = value
	return &opts
}

func (o *options) SeekReaderBufferSize() int {
	return o.seekReaderBufferSize
}

func (o *options) SetMmapEnableHugeTLB(value bool) Options {
	opts := *o
	opts.mmapEnableHugePages = value
	return &opts
}

func (o *options) MmapEnableHugeTLB() bool {
	return o.mmapEnableHugePages
}

func (o *options) SetMmapHugeTLBThreshold(value int64) Options {
	opts := *o
	opts.mmapHugePagesThreshold = value
	return &opts
}

func (o *options) MmapHugeTLBThreshold() int64 {
	return o.mmapHugePagesThreshold
}

func (o *options) SetTagEncoderPool(value serialize.TagEncoderPool) Options {
	opts := *o
	opts.tagEncoderPool = value
	return &opts
}

func (o *options) TagEncoderPool() serialize.TagEncoderPool {
	return o.tagEncoderPool
}

func (o *options) SetTagDecoderPool(value serialize.TagDecoderPool) Options {
	opts := *o
	opts.tagDecoderPool = value
	return &opts
}

func (o *options) TagDecoderPool() serialize.TagDecoderPool {
	return o.tagDecoderPool
}

func (o *options) SetFSTOptions(value fst.Options) Options {
	opts := *o
	opts.fstOptions = value
	return &opts
}

func (o *options) FSTOptions() fst.Options {
	return o.fstOptions
}
