// Copyright (c) 2018 Uber Technologies, Inc.
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

package fst

import (
	"io"

	"github.com/m3db/m3/src/m3ninx/index"
	sgmt "github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/x/bytes"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"
)

const (
	magicNumber = 0x6D33D0C5

	// MajorVersion is the currently supported MajorVersion.
	MajorVersion = 1

	// MinorVersion is the current MinorVersion.
	MinorVersion = 0
)

// Segment represents a FST segment.
type Segment interface {
	sgmt.Segment
	index.Readable
}

// Writer writes out a FST segment from the provided elements.
type Writer interface {
	// Reset sets the Writer to persist the provide segment.
	// NB(prateek): the provided segment must be a Sealed Mutable segment.
	Reset(s sgmt.MutableSegment) error

	// MajorVersion is the major version for the writer.
	MajorVersion() int

	// MinorVersion is the minor version for the writer.
	MinorVersion() int

	// Metadata returns metadata about the writer.
	Metadata() []byte

	// WriteDocumentsData writes out the documents data to the provided writer.
	WriteDocumentsData(w io.Writer) error

	// WriteDocumentsIndex writes out the documents index to the provided writer.
	// NB(prateek): this must be called after WriteDocumentsData().
	WriteDocumentsIndex(w io.Writer) error

	// WritePostingsOffsets writes out the postings offset file to the provided
	// writer.
	WritePostingsOffsets(w io.Writer) error

	// WriteFSTTerms writes out the FSTTerms file using the provided writer.
	// NB(prateek): this must be called after WritePostingsOffsets().
	WriteFSTTerms(w io.Writer) error

	// WriteFSTFields writes out the FSTFields file using the provided writer.
	// NB(prateek): this must be called after WriteFSTTerm().
	WriteFSTFields(w io.Writer) error
}

// Options is a collection of knobs for a fs segment.
type Options interface {
	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetBytesSliceArrayPool sets the bytes slice array pool.
	SetBytesSliceArrayPool(value bytes.SliceArrayPool) Options

	// BytesSliceArrayPool returns the bytes slice array pool.
	BytesSliceArrayPool() bytes.SliceArrayPool

	// SetBytesPool sets the bytes pool.
	SetBytesPool(value pool.BytesPool) Options

	// BytesPool returns the bytes pool.
	BytesPool() pool.BytesPool

	// SetPostingsListPool sets the postings list pool.
	SetPostingsListPool(value postings.Pool) Options

	// PostingsListPool returns the postings list pool.
	PostingsListPool() postings.Pool
}
