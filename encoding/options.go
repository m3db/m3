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

package encoding

import (
	"github.com/m3db/m3db/pool"
	xio "github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/time"
)

const (
	defaultDefaultTimeUnit = xtime.Second
)

var (
	// default encoding options
	defaultOptions = newOptions()
)

type options struct {
	defaultTimeUnit      xtime.Unit
	timeEncodingSchemes  TimeEncodingSchemes
	markerEncodingScheme MarkerEncodingScheme
	encoderPool          EncoderPool
	readerIteratorPool   ReaderIteratorPool
	bytesPool            pool.BytesPool
	segmentReaderPool    xio.SegmentReaderPool
}

func newOptions() Options {
	return &options{
		defaultTimeUnit:      defaultDefaultTimeUnit,
		timeEncodingSchemes:  defaultTimeEncodingSchemes,
		markerEncodingScheme: defaultMarkerEncodingScheme,
	}
}

// NewOptions creates a new options.
func NewOptions() Options {
	return defaultOptions
}

func (o *options) DefaultTimeUnit(value xtime.Unit) Options {
	opts := *o
	opts.defaultTimeUnit = value
	return &opts
}

func (o *options) GetDefaultTimeUnit() xtime.Unit {
	return o.defaultTimeUnit
}

func (o *options) TimeEncodingSchemes(value TimeEncodingSchemes) Options {
	opts := *o
	opts.timeEncodingSchemes = value
	return &opts
}

func (o *options) GetTimeEncodingSchemes() TimeEncodingSchemes {
	return o.timeEncodingSchemes
}

func (o *options) MarkerEncodingScheme(value MarkerEncodingScheme) Options {
	opts := *o
	opts.markerEncodingScheme = value
	return &opts
}

func (o *options) GetMarkerEncodingScheme() MarkerEncodingScheme {
	return o.markerEncodingScheme
}

func (o *options) EncoderPool(value EncoderPool) Options {
	opts := *o
	opts.encoderPool = value
	return &opts
}

func (o *options) GetEncoderPool() EncoderPool {
	return o.encoderPool
}

func (o *options) ReaderIteratorPool(value ReaderIteratorPool) Options {
	opts := *o
	opts.readerIteratorPool = value
	return &opts
}

func (o *options) GetReaderIteratorPool() ReaderIteratorPool {
	return o.readerIteratorPool
}

func (o *options) BytesPool(value pool.BytesPool) Options {
	opts := *o
	opts.bytesPool = value
	return &opts
}

func (o *options) GetBytesPool() pool.BytesPool {
	return o.bytesPool
}

func (o *options) SegmentReaderPool(value xio.SegmentReaderPool) Options {
	opts := *o
	opts.segmentReaderPool = value
	return &opts
}

func (o *options) GetSegmentReaderPool() xio.SegmentReaderPool {
	return o.segmentReaderPool
}
