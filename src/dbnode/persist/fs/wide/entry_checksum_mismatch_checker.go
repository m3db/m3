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
	"sync"

	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/instrument"

	"go.uber.org/zap"
)

type entryWithChecksum struct {
	idChecksum int64
	entry      schema.IndexEntry
}

type entryChecksumMismatchChecker struct {
	mu sync.Mutex

	blockReader  IndexChecksumBlockBatchReader
	mismatches   []ReadMismatch
	strictLastID []byte

	decodeOpts msgpack.DecodingOptions
	iOpts      instrument.Options

	batchIdx  int
	exhausted bool
	started   bool
}

// FIXME: remove once this is changed to single output.
func (c *entryChecksumMismatchChecker) Lock() {
	c.mu.Lock()
}

// FIXME: remove once this is changed to single output.
func (c *entryChecksumMismatchChecker) Unlock() {
	c.mu.Unlock()
}

// NewEntryChecksumMismatchChecker creates a new entry checksum mismatch
// checker, backed by the given block reader.
// NB: index entries MUST be checked in lexicographical order by ID.
func NewEntryChecksumMismatchChecker(
	blockReader IndexChecksumBlockBatchReader,
	opts Options,
) EntryChecksumMismatchChecker {
	return &entryChecksumMismatchChecker{
		blockReader: blockReader,
		mismatches:  make([]ReadMismatch, 0, opts.BatchSize()),
		decodeOpts:  opts.DecodingOptions(),
		iOpts:       opts.InstrumentOptions(),
	}
}

func checksumMismatch(checksum xio.IndexChecksum) ReadMismatch {
	return ReadMismatch{
		IndexChecksum: checksum,
	}
}

func (c *entryChecksumMismatchChecker) checksumMismatches(
	checksums ...xio.IndexChecksum,
) []ReadMismatch {
	for _, checksum := range checksums {
		c.mismatches = append(c.mismatches, checksumMismatch(checksum))
	}

	return c.mismatches
}

func (c *entryChecksumMismatchChecker) recordIndexMismatches(checksums ...int64) {
	for _, checksum := range checksums {
		c.mismatches = append(c.mismatches, ReadMismatch{
			IndexChecksum: xio.IndexChecksum{
				MetadataChecksum: checksum,
			},
		})
	}
}

func (c *entryChecksumMismatchChecker) emitInvariantViolation(
	marker []byte,
	checksum int64,
	entry xio.IndexChecksum,
) error {
	// Checksums match but IDs do not. Treat as an invariant violation.
	err := fmt.Errorf("checksum collision")
	instrument.EmitAndLogInvariantViolation(c.iOpts, func(l *zap.Logger) {
		l.Error(
			err.Error(),
			zap.Int64("checksum", checksum),
			zap.Binary("marker", marker),
			zap.Any("entry", entry),
		)
	})
	return err
}

func (c *entryChecksumMismatchChecker) readNextBatch() IndexChecksumBlockBatch {
	if !c.blockReader.Next() {
		c.exhausted = true
		// NB: set exhausted to true and return an empty since there are no
		// more available checksum blocks.
		return IndexChecksumBlockBatch{}
	}

	c.batchIdx = 0
	batch := c.blockReader.Current()
	return batch
}

func (c *entryChecksumMismatchChecker) ComputeMismatchesForEntry(
	entry xio.IndexChecksum,
) ([]ReadMismatch, error) {
	c.mismatches = c.mismatches[:0]
	if c.exhausted {
		// NB: no remaining batches in the index checksum block; any further
		// elements are mismatches (missing from primary).
		return c.checksumMismatches(entry), nil
	}

	if !c.started {
		c.started = true
		if !c.blockReader.Next() {
			// NB: no index checksum blocks available; any further
			// elements are mismatches (missing from primary).
			c.exhausted = true
			return c.checksumMismatches(entry), nil
		}

		c.batchIdx = 0
	}

	batch := c.blockReader.Current()
	for {
		markerIdx := len(batch.Checksums) - 1

		// NB: If the incoming checksum block is empty, move to the next one.
		if len(batch.Checksums) == 0 {
			batch = c.readNextBatch()
			if c.exhausted {
				return c.mismatches, nil
			}

			continue
		}

		checksum := batch.Checksums[c.batchIdx]
		markerCompare := bytes.Compare(batch.EndMarker, entry.ID.Bytes())
		if c.batchIdx < markerIdx {
			if checksum == entry.MetadataChecksum {
				// Matches: increment batch index and return any gathered mismatches.
				c.batchIdx++
				return c.mismatches, nil
			}

			for nextBatchIdx := c.batchIdx + 1; nextBatchIdx < markerIdx; nextBatchIdx++ {
				// NB: read next hashes, checking for index checksum matches.
				nextChecksum := batch.Checksums[nextBatchIdx]
				if entry.MetadataChecksum != nextChecksum {
					continue
				}

				// Checksum match. Add previous checksums as mismatches.
				c.recordIndexMismatches(batch.Checksums[c.batchIdx:nextBatchIdx]...)
				c.batchIdx = nextBatchIdx + 1
				return c.mismatches, nil
			}

			checksum = batch.Checksums[markerIdx]
			// NB: this is the last element in the batch. Check ID against MARKER.
			if entry.MetadataChecksum == checksum {
				if markerCompare != 0 {
					// Checksums match but IDs do not. Treat as emitInvariantViolation violation.
					return nil, c.emitInvariantViolation(batch.EndMarker, checksum, entry)
				}

				c.recordIndexMismatches(batch.Checksums[c.batchIdx:markerIdx]...)
				// ID and checksum match. Advance the block iter and return empty.
				batch = c.readNextBatch()
				return c.mismatches, nil
			}

			// Checksums do not match.
			if markerCompare > 0 {
				// This is a mismatch on primary that appears before the
				// marker element. Return mismatch but do not advance iter.
				return c.checksumMismatches(entry), nil
			}

			// Current value is past the end of this batch. Mark all in batch as
			// mismatches, and receive next batch.
			c.recordIndexMismatches(batch.Checksums[c.batchIdx:]...)
			batch = c.readNextBatch()
			if c.exhausted {
				// If no further values, add the current entry as a mismatch and return.
				return c.checksumMismatches(entry), nil
			}

			// All mismatches marked for the current batch, check entry against next
			// batch.
			continue
		}

		// NB: this is the last element in the batch. Check ID against MARKER.
		if entry.MetadataChecksum == checksum {
			if markerCompare != 0 {
				// Checksums match but IDs do not. Treat as emitInvariantViolation violation.
				return nil, c.emitInvariantViolation(batch.EndMarker, checksum, entry)
			}

			// ID and checksum match. Advance the block iter and return gathered mismatches.
			batch = c.readNextBatch()
			return c.mismatches, nil
		}

		// Checksum mismatch.
		if markerCompare == 0 {
			// IDs match but checksums do not. Advance the block iter and return
			// mismatch.
			batch = c.readNextBatch()
			return c.checksumMismatches(entry), nil
		} else if markerCompare > 0 {
			// This is a mismatch on primary that appears before the
			// marker element. Return mismatch but do not advance iter.
			return c.checksumMismatches(entry), nil
		}

		// The current batch here is exceeded. Emit the current batch marker as
		// a mismatch on primary, and advance the block iter.
		c.recordIndexMismatches(checksum)
		batch = c.readNextBatch()
		if c.exhausted {
			// If no further values, add the current entry as a mismatch and return.
			return c.checksumMismatches(entry), nil
		}
	}
}

func (c *entryChecksumMismatchChecker) Drain() []ReadMismatch {
	if c.exhausted {
		return nil
	}

	c.mismatches = c.mismatches[:0]
	curr := c.blockReader.Current()
	c.recordIndexMismatches(curr.Checksums[c.batchIdx:]...)
	for c.blockReader.Next() {
		curr := c.blockReader.Current()
		c.recordIndexMismatches(curr.Checksums...)
	}

	return c.mismatches
}
