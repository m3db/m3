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
	"fmt"

	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
)

// Options represents the options for mismatch calculation.
type Options interface {
	// Validate will validate the options and return an error if not valid.
	Validate() error

	// SetBatchSize sets the batch size.
	SetBatchSize(value int) Options

	// BatchSize returns the batch size.
	BatchSize() int

	// SetDecodingOptions sets the decoding options.
	SetDecodingOptions(value msgpack.DecodingOptions) Options

	// DecodingOptions returns the decoding options.
	DecodingOptions() msgpack.DecodingOptions

	// SetInstrumentOptions sets the instrumentation options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrumentation options.
	InstrumentOptions() instrument.Options
}

// ReadMismatch describes a series that does not match the expected wide index
// checksum, with a descriptor of the mismatch. This can indicate both scenarios
// where the expected checksum was not found, and when there is a mismatch.
type ReadMismatch struct {
	// Checksum is the wide index checksum for the mismatched series. All
	// ReadMismatches will have this set.
	Checksum int64
	// EncodedTags are the tags for this read mismatch.
	// NB: This is only present when there is an explicit mismatch, and tags
	// and IDs have to be returned for quorum reads.
	EncodedTags []byte
	// ID is the ID for this read mismatch.
	// NB: This is only present when there is an explicit mismatch, and tags
	// and IDs have to be returned for quorum reads.
	ID []byte
}

func (c ReadMismatch) String() string {
	if len(c.EncodedTags) > 0 {
		return fmt.Sprintf("{c: %d, e: true}", c.Checksum)
	}
	return fmt.Sprintf("{c: %d, e: false}", c.Checksum)
}

// IndexChecksumBlockBatchReader is a reader across IndexChecksumBlockBatches.
type IndexChecksumBlockBatchReader interface {
	// Next moves to the next IndexChecksumBlockBatch element.
	Next() bool
	// Current yields the current IndexChecksumBlockBatch.
	Current() ident.IndexChecksumBlockBatch
}

// EntryChecksumMismatchChecker checks if a given entry should yield a mismatch.
type EntryChecksumMismatchChecker interface {
	// ComputeMismatchesForEntry determines if the given index entry is a mismatch.
	ComputeMismatchesForEntry(entry schema.IndexEntry) ([]ReadMismatch, error)
	// Drain returns any unconsumed IndexChecksumBlockBatches as mismatches.
	Drain() []ReadMismatch
}
