// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE

package wide

import (
	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
)

// Options represents the options for mismatch calculation.
type Options interface {
	// Validate will validate the options and return an error if not valid.
	Validate() error

	// SetBatchSize sets the batch size.
	SetBatchSize(value int) Options

	// BatchSize returns the batch size.
	BatchSize() int

	// SetStrict sets strict mode. If strict mode is enabled, computing mismatches
	// causes an error if entries are not called in lexicographically increasing
	// ID order. This adds additional overhead.
	SetStrict(value bool) Options

	// Strict returns the strict setting.
	Strict() bool

	// SetBytesPool sets the bytes pool.
	SetBytesPool(value pool.BytesPool) Options

	// BytesPool returns the bytes pool.
	BytesPool() pool.BytesPool

	// SetDecodingOptions sets the decoding options.
	SetDecodingOptions(value msgpack.DecodingOptions) Options

	// DecodingOptions returns the decoding options.
	DecodingOptions() msgpack.DecodingOptions

	// SetInstrumentOptions sets the instrumentation options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrumentation options.
	InstrumentOptions() instrument.Options
}

// Cleanup is a cleanup function to be called when underlying bytes are
// no longer necessary.
type Cleanup func()

// ReadMismatch describes a metric that does not match a given summary,
// with descriptor of the mismatch.
type ReadMismatch struct {
	// Mismatched indicates this is a mismatched read.
	Mismatched bool
	// Checksum is the checksum for the mismatched series.
	Checksum int64
	// Data is the data for this query.
	// NB: only present for entry mismatches.
	Data checked.Bytes
	// EncodedTags are the tags for this query.
	// NB: only present for entry mismatches.
	EncodedTags []byte
	// ID is the ID for this query.
	// NB: only present for entry mismatches.
	ID []byte

	cleanup Cleanup
}

// IndexChecksumBlockBuffer is a buffer accross IndexChecksumBlocks.
type IndexChecksumBlockBuffer interface {
	// Next moves to the next IndexChecksumBlock element.
	Next() bool
	// Current yields the current IndexChecksumBlock.
	Current() ident.IndexChecksumBlock
	// Close closes the buffer.
	Close()
	// Push adds an IndexChecksumBlock to the buffer.
	Push(bl ident.IndexChecksumBlock)
}

// EntryChecksumMismatchChecker checks if a given entry should yield a mismatch.
type EntryChecksumMismatchChecker interface {
	// ComputeMismatchForEntry determines if the given index entry is a mismatch.
	ComputeMismatchForEntry(entry schema.IndexEntry) ([]ReadMismatch, error)
	// Drain returns any unconsumed IndexChecksumBlocks as mismatches.
	Drain() []ReadMismatch
}
