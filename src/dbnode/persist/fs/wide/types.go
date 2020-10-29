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
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/checked"
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
	// ReadMismatch extends IndexChecksum with additional mismatch fields.
	xio.IndexChecksum
	// Data is the data for the read mismatch. Set only on reader mismatches.
	Data checked.Bytes
}

// IsReaderMismatch is true if this mismatch is this mismatch is on the reader
// side.
func (r ReadMismatch) IsReaderMismatch() bool {
	return r.IndexChecksum.ID != nil ||
		r.IndexChecksum.EncodedTags != nil
}

// IndexChecksumBlockBatchReader is a reader across IndexChecksumBlockBatches.
type IndexChecksumBlockBatchReader interface {
	// Next moves to the next IndexChecksumBlockBatch element.
	Next() bool
	// Current yields the current IndexChecksumBlockBatch.
	Current() IndexChecksumBlockBatch
	// Close closes the reader, draining any incoming reads without using them.
	Close()
}

// EntryChecksumMismatchChecker checks if a given entry should yield a mismatch.
type EntryChecksumMismatchChecker interface {
	// ComputeMismatchesForEntry determines if the given index entry is a mismatch.
	ComputeMismatchesForEntry(entry xio.IndexChecksum) ([]ReadMismatch, error)
	// Drain returns any unconsumed IndexChecksumBlockBatches as mismatches.
	Drain() []ReadMismatch
	// Lock sets a mutex on this mismatch checker.
	Lock()
	// Unlock unlocks the mutex on the mismatch checker.
	Unlock()
}

// StreamedMismatch yields a ReadMismatch value asynchronously,
// and any errors encountered during execution.
type StreamedMismatch interface {
	// RetrieveMismatch retrieves the mismatch.
	RetrieveMismatch() (ReadMismatch, error)
}

type emptyStreamedMismatch struct{}

func (emptyStreamedMismatch) RetrieveMismatch() (ReadMismatch, error) {
	return ReadMismatch{}, nil
}

// EmptyStreamedMismatch is an empty streamed mismatch batch.
var EmptyStreamedMismatch StreamedMismatch = emptyStreamedMismatch{}

// IndexChecksumBlockBatch represents a batch of index checksums originating
// from a single series block.
type IndexChecksumBlockBatch struct {
	// Checksums is the list of index checksums.
	Checksums []int64
	// EndMarker is a batch marker, signifying the ID of the
	// last element in the batch.
	EndMarker []byte
}
