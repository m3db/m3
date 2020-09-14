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
	"errors"

	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
)

var errFinishedStreaming = errors.New("mismatch_streaming_finished")

type entry struct {
	idChecksum int64
	entry      schema.IndexEntry
}

type entryReader interface {
	next() bool
	current() entry
}

type streamMismatchWriter struct {
	decodeOpts msgpack.DecodingOptions
	iOpts      instrument.Options
	bytesPool  pool.BytesPool
}

func (w *streamMismatchWriter) toMismatch(e entry) ReadMismatch {
	var cleanup cleanup
	if w.bytesPool != nil {
		cleanup = func() {
			w.bytesPool.Put(e.entry.ID)
			w.bytesPool.Put(e.entry.EncodedTags)
		}
	}

	return ReadMismatch{
		Checksum:    e.idChecksum,
		Data:        nil, // TODO: add this correctly.
		EncodedTags: e.entry.EncodedTags,
		ID:          e.entry.ID,

		cleanup: cleanup,
	}
}

func newStreamMismatchWriter(
	decodeOpts msgpack.DecodingOptions,
	iOpts instrument.Options,
) *streamMismatchWriter {
	return &streamMismatchWriter{
		decodeOpts: decodeOpts,
		iOpts:      iOpts,
	}
}

func (w *streamMismatchWriter) drainBatchChecksums(
	checksums []int64,
	outStream chan<- ReadMismatch,
) {
	for _, c := range checksums {
		outStream <- ReadMismatch{Checksum: c}
	}
}

func (w *streamMismatchWriter) drainRemainingBatchtreamAndClose(
	currentBatch []int64,
	inStream <-chan ident.IndexChecksumBlock,
	outStream chan<- ReadMismatch,
) {
	w.drainBatchChecksums(currentBatch, outStream)

	for batch := range inStream {
		w.drainBatchChecksums(batch.Checksums, outStream)
	}

	close(outStream)
}

func (w *streamMismatchWriter) readRemainingReadersAndClose(
	current entry,
	reader entryReader,
	outStream chan<- ReadMismatch,
) {
	outStream <- w.toMismatch(current)
	for reader.next() {
		outStream <- w.toMismatch(reader.current())
	}

	close(outStream)
}

func (w *streamMismatchWriter) emitChecksumMismatches(
	e entry,
	checksum int64,
	outStream chan<- ReadMismatch,
) {
	// NB: If data checksums match, this entry matches.
	if checksum == e.entry.Hash(w.decodeOpts.Hash32()) {
		return
	}

	outStream <- w.toMismatch(e)
}

func (w *streamMismatchWriter) loadNextValidIndexChecksumBatch(
	inStream <-chan ident.IndexChecksumBlock,
	r entryReader,
	outStream chan<- ReadMismatch,
) (ident.IndexChecksumBlock, bool) {
	var (
		batch ident.IndexChecksumBlock
		ok    bool
	)

	reader := r.current()
	for {
		batch, ok = <-inStream
		if !ok {
			// NB: finished streaming from hash block. Mark remaining entries as
			// ONLY_SECONDARY and return.
			w.readRemainingReadersAndClose(reader, r, outStream)
			return ident.IndexChecksumBlock{}, false
		}

		if len(batch.Checksums) == 0 {
			continue
		}

		if compare := bytes.Compare(batch.Marker, reader.entry.ID); compare > 0 {
			// NB: current element is before the current MARKER element;
			// this is a valid index hash block for comparison.
			return batch, true
		} else if compare < 0 {
			// NB: all elements from the current idxHashBatch are before hte current
			// element; mark all elements in batch as ONLY_ON_PRIMARY and fetch the
			// next idxHashBatch.
			w.drainBatchChecksums(batch.Checksums, outStream)
			continue
		}

		// NB: the last (i.e. MARKER) element is the first one in the index batch
		// to match the current element.
		lastIdx := len(batch.Checksums) - 1
		if lastIdx >= 1 {
			// NB: Mark all preceeding entries as ONLY_PRIMARY mismatches.
			w.drainBatchChecksums(batch.Checksums[:lastIdx], outStream)
		}

		w.emitChecksumMismatches(reader, batch.Checksums[lastIdx], outStream)

		// NB: Increment entry read.
		if !w.nextReader(nil, inStream, r, outStream) {
			return ident.IndexChecksumBlock{}, false
		}

		reader = r.current()
	}
}

// nextReader increments the entry reader; if there are no additional entries,
// input stream and current batch are streamed to output as mismatches.
func (w *streamMismatchWriter) nextReader(
	currentBatch []int64,
	inStream <-chan ident.IndexChecksumBlock,
	r entryReader,
	outStream chan<- ReadMismatch,
) bool {
	if r.next() {
		return true
	}

	// NB: if no next reader, drain remaining batches as mismatches
	// and close the input stream.
	w.drainRemainingBatchtreamAndClose(currentBatch, inStream, outStream)
	return false
}

func (w *streamMismatchWriter) merge(
	inStream <-chan ident.IndexChecksumBlock,
	r entryReader,
	outStream chan<- ReadMismatch,
) error {
	if !w.nextReader(nil, inStream, r, outStream) {
		// NB: no elements in the reader, channels drained.
		return nil
	}

	batch, hasBatch := w.loadNextValidIndexChecksumBatch(inStream, r, outStream)
	if !hasBatch {
		// NB: no remaining batches in the input stream, channels drained.
		return nil
	}

	batchIdx := 0
	markerIdx := len(batch.Checksums) - 1
	for {
		entry := r.current()
		checksum := batch.Checksums[batchIdx]

		// NB: this is the last element in the batch. Check against MARKER.
		if batchIdx == markerIdx {
			if entry.idChecksum == checksum {
				if !bytes.Equal(batch.Marker, entry.entry.ID) {
					outStream <- w.toMismatch(entry)
				}

				// NB: advance to next reader element.
				if !w.nextReader(nil, inStream, r, outStream) {
					return nil
				}
			} else {
				compare := bytes.Compare(batch.Marker, entry.entry.ID)
				if compare == 0 {
					outStream <- w.toMismatch(entry)
				} else if compare > 0 {
					outStream <- w.toMismatch(entry)
					// NB: advance to next reader element.
					if !w.nextReader(batch.Checksums[markerIdx:], inStream, r, outStream) {
						return nil
					}

					continue
				} else {
					outStream <- ReadMismatch{Checksum: checksum}
				}
			}

			batch, hasBatch = w.loadNextValidIndexChecksumBatch(inStream, r, outStream)
			if !hasBatch {
				return nil
			}

			batchIdx = 0
			markerIdx = len(batch.Checksums) - 1
			continue
		}

		if entry.idChecksum == checksum {
			// NB: advance to next batch checksum.
			batchIdx++
			// NB: advance to next reader element.
			if !w.nextReader(batch.Checksums[batchIdx:], inStream, r, outStream) {
				return nil
			}

			continue
		}

		foundMatching := false
		for nextBatchIdx := batchIdx + 1; nextBatchIdx <= markerIdx; nextBatchIdx++ {
			// NB: read next hashes, checking for index checksum matches.
			nextChecksum := batch.Checksums[nextBatchIdx]
			if entry.idChecksum != nextChecksum {
				continue
			}

			// NB: found matching checksum; add all indexHash entries between
			// batchIdx and nextBatchIdx as ONLY_PRIMARY mismatches.
			w.drainBatchChecksums(batch.Checksums[batchIdx:nextBatchIdx], outStream)
			if nextBatchIdx != markerIdx {
				// NB: advance to next reader element.
				if !w.nextReader(nil, inStream, r, outStream) {
					return nil
				}

				batchIdx = nextBatchIdx + 1
			} else {
				if !bytes.Equal(batch.Marker, entry.entry.ID) {
					outStream <- w.toMismatch(entry)
				}

				// NB: advance to next reader element.
				if !w.nextReader(nil, inStream, r, outStream) {
					return nil
				}

				batch, hasBatch = w.loadNextValidIndexChecksumBatch(inStream, r, outStream)
				if !hasBatch {
					return nil
				}

				batchIdx = 0
				markerIdx = len(batch.Checksums) - 1
			}

			foundMatching = true
			break
		}

		if foundMatching {
			continue
		}

		// NB: reached end of current batch with no match; use MARKER to determine
		// if the current element is missing from the batch checksums or if the
		// next batch should be loaded, instead.
		compare := bytes.Compare(batch.Marker, entry.entry.ID)
		if compare == 1 {
			// NB: this entry is missing from the batch; emit as a mismatch and
			// move to the next.
			outStream <- w.toMismatch(entry)
			// NB: advance to next reader element.
			if !w.nextReader(batch.Checksums[batchIdx:], inStream, r, outStream) {
				return nil
			}

			continue
		} else if compare < 0 {
			// NB: this entry is after the last entry in the batch. Drain remaining
			// batch elements as mismatches and increment the batch.
			w.drainBatchChecksums(batch.Checksums[batchIdx:], outStream)
		} else if compare == 0 {
			//  entry.idChecksum != batch.Checksums[markerIdx] {
			// NB: entry ID and marker IDs match, but checksums mismatch. Although
			// this means there is a mismatch here since matching IDs checksums
			// necessarily mismatch above, verify defensively here.
			w.drainBatchChecksums(batch.Checksums[batchIdx:markerIdx], outStream)
			if entry.idChecksum != batch.Checksums[markerIdx] {
				outStream <- w.toMismatch(entry)
			}

			if !w.nextReader(batch.Checksums[batchIdx:markerIdx-1], inStream, r, outStream) {
				return nil
			}
		} else {
			if entry.idChecksum != batch.Checksums[markerIdx] {
				outStream <- w.toMismatch(entry)
			}
		}

		// NB: get next index hash block, and reset batch and marker indices.
		batch, hasBatch = w.loadNextValidIndexChecksumBatch(inStream, r, outStream)
		if !hasBatch {
			return nil
		}

		batchIdx = 0
		markerIdx = len(batch.Checksums) - 1
	}
}
