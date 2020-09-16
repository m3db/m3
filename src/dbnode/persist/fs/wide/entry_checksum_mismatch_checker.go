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
	"bytes"
	"fmt"

	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"

	"go.uber.org/zap"
)

type entry struct {
	idChecksum int64
	entry      schema.IndexEntry
}

type entryChecksumMismatchChecker struct {
	blockBuffer  IndexChecksumBlockBuffer
	mismatches   []ReadMismatch
	bytesPool    pool.BytesPool
	strictLastID []byte

	decodeOpts msgpack.DecodingOptions
	iOpts      instrument.Options

	strict    bool
	batchIdx  int
	exhausted bool
	started   bool
}

// NewEntryChecksumMismatchChecker creates a new entry checksum mismatch
// checker, backed by the given block buffer.
// NB: index entries MUST be checked in lexicographical order by ID.
func NewEntryChecksumMismatchChecker(
	blockBuffer IndexChecksumBlockBuffer,
	opts Options,
) EntryChecksumMismatchChecker {
	return &entryChecksumMismatchChecker{
		blockBuffer: blockBuffer,
		mismatches:  make([]ReadMismatch, 0, opts.BatchSize()),
		bytesPool:   opts.BytesPool(),
		decodeOpts:  opts.DecodingOptions(),
		iOpts:       opts.InstrumentOptions(),
		strict:      opts.Strict(),
	}
}

func (w *entryChecksumMismatchChecker) entryMismatch(e entry) ReadMismatch {
	var cleanup Cleanup
	if w.bytesPool != nil {
		cleanup = func() {
			w.bytesPool.Put(e.entry.ID)
			w.bytesPool.Put(e.entry.EncodedTags)
		}
	}

	return ReadMismatch{
		Mismatched: true,

		Checksum:    e.idChecksum,
		EncodedTags: e.entry.EncodedTags,
		ID:          e.entry.ID,

		cleanup: cleanup,
	}
}

func (w *entryChecksumMismatchChecker) entryMismatches(entries ...entry) []ReadMismatch {
	for _, e := range entries {
		w.mismatches = append(w.mismatches, w.entryMismatch(e))
	}

	return w.mismatches
}

func (w *entryChecksumMismatchChecker) indexMismatches(checksums ...int64) []ReadMismatch {
	for _, c := range checksums {
		w.mismatches = append(w.mismatches,
			ReadMismatch{Mismatched: true, Checksum: c})
	}

	return w.mismatches
}

func (w *entryChecksumMismatchChecker) invariant(
	marker []byte,
	checksum int64,
	entry entry,
) error {
	// Checksums match but IDs do not. Treat as invariant violation.
	err := fmt.Errorf("checksum collision")
	instrument.EmitAndLogInvariantViolation(w.iOpts, func(l *zap.Logger) {
		l.Error(
			err.Error(),
			zap.Int64("checksum", checksum),
			zap.Binary("marker", marker),
			zap.Any("entry", entry.entry),
		)
	})
	return err
}

func (w *entryChecksumMismatchChecker) ComputeMismatchForEntry(
	e schema.IndexEntry,
) ([]ReadMismatch, error) {
	if w.strict {
		if w.strictLastID == nil {
			w.strictLastID = append(make([]byte, 0, len(e.ID)), e.ID...)
		} else {
			if bytes.Compare(w.strictLastID, e.ID) != -1 {
				return nil, fmt.Errorf("entry ID (%s) must lexicographically "+
					"follow previous entry ID (%s)", string(e.ID), string(w.strictLastID))
			}

			w.strictLastID = w.strictLastID[:0]
			w.strictLastID = append(w.strictLastID, e.ID...)
		}
	}

	hash := w.decodeOpts.Hash32()
	checksum := e.Hash(hash)
	hash.Reset()

	entry := entry{entry: e, idChecksum: checksum}
	w.mismatches = w.mismatches[:0]
	if w.exhausted {
		// NB: no remaining batches in the index checksum block; any further
		// elements are mismatches (missing from primary).
		return w.entryMismatches(entry), nil
	}

	if !w.started {
		w.started = true
		if !w.blockBuffer.Next() {
			// NB: no index checksum blocks available; any further
			// elements are mismatches (missing from primary).
			w.exhausted = true
			return w.entryMismatches(entry), nil
		}

		w.batchIdx = 0
	}

	batch := w.blockBuffer.Current()
	markerIdx := len(batch.Checksums) - 1
	for {
		checksum := batch.Checksums[w.batchIdx]
		compare := bytes.Compare(batch.Marker, entry.entry.ID)
		if w.batchIdx == markerIdx {
			// NB: this is the last element in the batch. Check ID against MARKER.
			if entry.idChecksum == checksum {
				if compare != 0 {
					// Checksums match but IDs do not. Treat as invariant violation.
					return nil, w.invariant(batch.Marker, checksum, entry)
				}

				// ID and checksum match. Advance the block iter and return gathered mismatches.
				if !w.blockBuffer.Next() {
					w.exhausted = true
				} else {
					batch = w.blockBuffer.Current()
					markerIdx = len(batch.Checksums) - 1
					w.batchIdx = 0
				}

				return w.mismatches, nil
			}

			// Checksum mismatch.
			if compare == 0 {
				// IDs match but checksums do not. Advance the block iter and return
				// mismatch.
				if !w.blockBuffer.Next() {
					w.exhausted = true
				} else {
					batch = w.blockBuffer.Current()
					markerIdx = len(batch.Checksums) - 1
					w.batchIdx = 0
				}

				return w.entryMismatches(entry), nil
			} else if compare > 0 {
				// This is a mismatch on primary that appears before the
				// marker element. Return mismatch but do not advance iter.
				return w.entryMismatches(entry), nil
			}

			// The current batch here is exceeded. Emit the current batch marker as
			// a mismatch on primary, and advance the block iter.
			w.indexMismatches(checksum)
			if !w.blockBuffer.Next() {
				// If no further values, add the current entry as a mismatch and return.
				w.exhausted = true
				return w.entryMismatches(entry), nil
			}

			batch = w.blockBuffer.Current()
			markerIdx = len(batch.Checksums) - 1
			w.batchIdx = 0
			continue
		}

		if checksum == entry.idChecksum {
			// Matches: increment batch index and return any gathered mismatches.
			w.batchIdx = w.batchIdx + 1
			return w.mismatches, nil
		}

		for nextBatchIdx := w.batchIdx + 1; nextBatchIdx < markerIdx; nextBatchIdx++ {
			// NB: read next hashes, checking for index checksum matches.
			nextChecksum := batch.Checksums[nextBatchIdx]
			if entry.idChecksum != nextChecksum {
				continue
			}

			// Checksum match. Add previous checksums as mismatches.
			mismatches := w.indexMismatches(batch.Checksums[w.batchIdx:nextBatchIdx]...)
			w.batchIdx = nextBatchIdx + 1
			return mismatches, nil
		}

		checksum = batch.Checksums[markerIdx]
		// NB: this is the last element in the batch. Check ID against MARKER.
		if entry.idChecksum == checksum {
			if compare != 0 {
				// Checksums match but IDs do not. Treat as invariant violation.
				return nil, w.invariant(batch.Marker, checksum, entry)
			}

			w.indexMismatches(batch.Checksums[w.batchIdx:markerIdx]...)
			// ID and checksum match. Advance the block iter and return empty.
			if !w.blockBuffer.Next() {
				w.exhausted = true
			} else {
				batch = w.blockBuffer.Current()
				markerIdx = len(batch.Checksums) - 1
				w.batchIdx = 0
			}

			return w.mismatches, nil
		}

		// Checksums do not match.
		if compare > 0 {
			// This is a mismatch on primary that appears before the
			// marker element. Return mismatch but do not advance iter.
			return w.entryMismatches(entry), nil
		}

		// Current value is past the end of this batch. Mark all in batch as
		// mismatches, and receive next batch.
		w.indexMismatches(batch.Checksums[w.batchIdx:]...)
		if !w.blockBuffer.Next() {
			// If no further values, add the current entry as a mismatch and return.
			w.exhausted = true
			return w.entryMismatches(entry), nil
		}

		batch = w.blockBuffer.Current()
		markerIdx = len(batch.Checksums) - 1
		w.batchIdx = 0
	}
}

func (w *entryChecksumMismatchChecker) Drain() []ReadMismatch {
	if w.exhausted {
		return nil
	}

	w.mismatches = w.mismatches[:0]
	curr := w.blockBuffer.Current()
	w.indexMismatches(curr.Checksums[w.batchIdx:]...)
	for w.blockBuffer.Next() {
		curr := w.blockBuffer.Current()
		w.indexMismatches(curr.Checksums...)
	}

	return w.mismatches
}
