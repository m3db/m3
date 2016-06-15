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

package tsz

import (
	"github.com/m3db/m3db/interfaces/m3db"
)

var (
	// default encoding options
	defaultOptions = newOptions()
)

// Options represents different options for encoding time as well as markers.
type Options interface {
	// TimeEncodingSchemes sets the time encoding schemes for different time units.
	TimeEncodingSchemes(value TimeEncodingSchemes) Options

	// GetTimeEncodingSchemes returns the time encoding schemes for different time units.
	GetTimeEncodingSchemes() TimeEncodingSchemes

	// MarkerEncodingScheme sets the marker encoding scheme.
	MarkerEncodingScheme(value MarkerEncodingScheme) Options

	// GetMarkerEncodingScheme returns the marker encoding scheme.
	GetMarkerEncodingScheme() MarkerEncodingScheme

	// Pool sets the encoder pool.
	Pool(value m3db.EncoderPool) Options

	// GetPool returns the encoder pool.
	GetPool() m3db.EncoderPool

	// IteratorPool sets the iterator pool.
	IteratorPool(value m3db.IteratorPool) Options

	// GetIteratorPool returns the iterator pool.
	GetIteratorPool() m3db.IteratorPool

	// BytesPool sets the bytes pool.
	BytesPool(value m3db.BytesPool) Options

	// GetBytesPool returns the bytes pool.
	GetBytesPool() m3db.BytesPool

	// SegmentReaderPool sets the segment reader pool.
	SegmentReaderPool(value m3db.SegmentReaderPool) Options

	// GetSegmentReaderPool returns the segment reader pool.
	GetSegmentReaderPool() m3db.SegmentReaderPool
}

type options struct {
	timeEncodingSchemes  TimeEncodingSchemes
	markerEncodingScheme MarkerEncodingScheme
	pool                 m3db.EncoderPool
	iteratorPool         m3db.IteratorPool
	bytesPool            m3db.BytesPool
	segmentReaderPool    m3db.SegmentReaderPool
}

func newOptions() Options {
	return &options{
		timeEncodingSchemes:  defaultTimeEncodingSchemes,
		markerEncodingScheme: defaultMarkerEncodingScheme,
	}
}

// NewOptions creates a new options.
func NewOptions() Options {
	return defaultOptions
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

func (o *options) Pool(value m3db.EncoderPool) Options {
	opts := *o
	opts.pool = value
	return &opts
}

func (o *options) GetPool() m3db.EncoderPool {
	return o.pool
}

func (o *options) IteratorPool(value m3db.IteratorPool) Options {
	opts := *o
	opts.iteratorPool = value
	return &opts
}

func (o *options) GetIteratorPool() m3db.IteratorPool {
	return o.iteratorPool
}

func (o *options) BytesPool(value m3db.BytesPool) Options {
	opts := *o
	opts.bytesPool = value
	return &opts
}

func (o *options) GetBytesPool() m3db.BytesPool {
	return o.bytesPool
}

func (o *options) SegmentReaderPool(value m3db.SegmentReaderPool) Options {
	opts := *o
	opts.segmentReaderPool = value
	return &opts
}

func (o *options) GetSegmentReaderPool() m3db.SegmentReaderPool {
	return o.segmentReaderPool
}
