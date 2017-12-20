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
	"bytes"
	"os"

	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/persist/encoding"
	"github.com/m3db/m3db/ts"

	"gopkg.in/vmihailenco/msgpack.v2"
)

// indexLookup provides a way of quickly determining the nearest offset of an
// ID in the index file. It is not safe for concurrent use
type indexLookup struct {
	summaryIDsOffsets []encoding.IndexSummaryToken
	// bytes from file mmap'd into anonymous region
	summariesBytes []byte
	// reusable decoder stream
	decoderStream  encoding.DecoderStream
	decoder        encoding.Decoder
	msgpackDecoder *msgpack.Decoder
}

func newIndexLookupImpl(
	summaryIDsOffsets []encoding.IndexSummaryToken,
	summariesBytes []byte,
	decoder encoding.Decoder,
	decoderStream encoding.DecoderStream,
) *indexLookup {
	return &indexLookup{
		summaryIDsOffsets: summaryIDsOffsets,
		summariesBytes:    summariesBytes,
		decoderStream:     decoderStream,
		decoder:           decoder,
		msgpackDecoder:    msgpack.NewDecoder(nil),
	}
}

// getNearestIndexFileOffset returns either:
//     1. The offset in the index file for the specified series
//     2. The offset in the index file for the the series in the summaries file
//        that satisfies the following two constraints:
//            1. Is closest to the desired series in the index file
//            2. Is BEFORE the desired series in the index file (because we
//               we scan the index file sequentially in a forward-moving manner)
func (il *indexLookup) getNearestIndexFileOffset(id ts.ID) (int64, bool, error) {
	idBytes := id.Data().Get()

	min := 0
	max := len(il.summaryIDsOffsets) - 1
	bestMatchSoFar := int64(-1)

	for {
		if min > max {
			if bestMatchSoFar == -1 {
				return -1, false, nil
			}
			return bestMatchSoFar, true, nil
		}

		idx := (max + min) / 2
		summaryBytesMetadata := il.summaryIDsOffsets[idx]
		compBytes := summaryBytesMetadata.ID(il.summariesBytes)
		comparison := bytes.Compare(idBytes, compBytes)

		// Found it
		if comparison == 0 {
			indexOffset, err := summaryBytesMetadata.IndexOffset(
				il.summariesBytes, il.decoderStream, il.msgpackDecoder)
			// Should never happen, either something is really wrong with the code or
			// the file on disk was corrupted
			if err != nil {
				return -1, false, err
			}
			return indexOffset, true, nil
		}

		// idBytes is smaller than compBytes, go left
		if comparison == -1 {
			max = idx - 1
			continue
		}

		// idBytes is larger than compBytes, go right
		if comparison == 1 {
			min = idx + 1
			// The summaries file only contains a fraction of the series that are in
			// the index file itself. Because of that, the binary search that we're
			// performing is "optimistic". We're trying to find either an exact match,
			// OR the nearest match that is to the left of the series we're searching
			// for (so we keep track of it everytime we move right).
			indexOffset, err := summaryBytesMetadata.IndexOffset(
				il.summariesBytes, il.decoderStream, il.msgpackDecoder)
			if err != nil {
				return -1, false, nil
			}
			bestMatchSoFar = indexOffset
			continue
		}
	}
}

func (il *indexLookup) close() error {
	return munmap(il.summariesBytes)
}

// mmap file into memory
// then msgpack decode everything in the mmap to create the offset
func readIndexLookupFromSummariesFile(
	summariesFd *os.File,
	summariesFdWithDigest digest.FdWithDigestReader,
	expectedSummariesDigest uint32,
	decoder encoding.Decoder,
	numEntries int,
) (*indexLookup, error) {
	summariesFdWithDigest.Reset(summariesFd)
	stat, err := summariesFd.Stat()
	if err != nil {
		return nil, err
	}
	numBytes := stat.Size()

	// Request an anonymous (non-file-backed) mmap region. Note that we're going
	// to use the mmap'd region to store the read-only summaries data, but the mmap
	// region itself needs to be writable so we can copy the bytes from disk
	// into it
	anonMmap, err := mmapBytes(numBytes, mmapOptions{read: true, write: true})
	if err != nil {
		return nil, err
	}

	// Validate the bytes on disk using the digest, and read them into
	// the mmap'd region
	_, err = summariesFdWithDigest.ReadAllAndValidate(anonMmap, expectedSummariesDigest)
	if err != nil {
		return nil, err
	}

	// Msgpack decode the entire summaries file (we need to store the offsets
	// for the entries so we can binary-search it)
	var (
		decoderStream    = encoding.NewDecoderStream(anonMmap)
		summariesOffsets = make([]encoding.IndexSummaryToken, 0, numEntries)
		lastReadID       []byte
		needsSort        = false
	)
	decoder.Reset(decoderStream)

	for read := 0; read < numEntries; read++ {
		// We ignore the entry itself because we don't need any information from it
		entry, entryMetadata, err := decoder.DecodeIndexSummary()
		if err != nil {
			return nil, err
		}

		// Should never happen as files should be sorted on disk
		if lastReadID != nil && bytes.Compare(lastReadID, entry.ID) != -1 {
			needsSort = true
		}
		summariesOffsets = append(summariesOffsets, entryMetadata)
	}

	// Again, should never happen because files should be sorted on disk
	if needsSort {
		// TODO: Emit log
		summariesOffsets = encoding.NewIndexSummaryTokensSortableCollection(
			summariesOffsets,
			anonMmap,
		).Sorted()
	}

	return newIndexLookupImpl(summariesOffsets, anonMmap, decoder, decoderStream), nil
}
