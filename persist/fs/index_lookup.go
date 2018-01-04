// Copyright (c) 2017 Uber Technologies, Inc.
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
	"fmt"
	"os"

	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/persist/encoding"
	"github.com/m3db/m3db/ts"

	"gopkg.in/vmihailenco/msgpack.v2"
)

// nearestIndexOffsetLookup provides a way of quickly determining the nearest offset of an
// ID in the index file. It is not safe for concurrent use
type nearestIndexOffsetLookup struct {
	summaryIDsOffsets []encoding.IndexSummaryToken
	// bytes from file mmap'd into anonymous region
	summariesBytes []byte
	// reusable decoder stream
	decoderStream  encoding.DecoderStream
	decoder        encoding.Decoder
	msgpackDecoder *msgpack.Decoder
}

func newNearestIndexOffsetLookup(
	summaryIDsOffsets []encoding.IndexSummaryToken,
	summariesBytes []byte,
	decoder encoding.Decoder,
	decoderStream encoding.DecoderStream,
) *nearestIndexOffsetLookup {
	return &nearestIndexOffsetLookup{
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
// In other words, the returned offset can always be used as a starting point to
// begin scanning the index file for the desired series.
func (il *nearestIndexOffsetLookup) getNearestIndexFileOffset(id ts.ID) (int64, bool, error) {
	idBytes := id.Data().Get()

	min := 0
	max := len(il.summaryIDsOffsets) - 1

	// The summaries file only contains a fraction of the series that are in
	// the index file itself. Because of that, the binary search that we're
	// performing is "optimistic". We're trying to find either an exact match,
	// OR the nearest match that is to the left of the series we're searching
	// for (so we keep track of it everytime we move right).
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
			indexOffset, err := summaryBytesMetadata.IndexOffset(
				il.summariesBytes, il.decoderStream, il.msgpackDecoder)
			if err != nil {
				return -1, false, nil
			}
			// update the bestMatchSoFar everytime we move right
			bestMatchSoFar = indexOffset
			continue
		}
	}
}

func (il *nearestIndexOffsetLookup) close() error {
	return munmap(il.summariesBytes)
}

// readNearestIndexOffsetLookupFromSummaries creates an nearestIndexOffsetLookup
// from an index summaries file by reading the summaries file into an anonymous
// mmap'd region, and also creating the slice of summaries offsets which is
// required to binary search the data structure. It will also make sure that
// the summaries file is sorted (which it always should be).
func readNearestIndexOffsetLookupFromSummaries(
	summariesFd *os.File,
	summariesFdWithDigest digest.FdWithDigestReader,
	expectedSummariesDigest uint32,
	decoder encoding.Decoder,
	numEntries int,
) (*nearestIndexOffsetLookup, error) {
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
		munmap(anonMmap)
		return nil, err
	}

	// Msgpack decode the entire summaries file (we need to store the offsets
	// for the entries so we can binary-search it)
	var (
		decoderStream    = encoding.NewDecoderStream(anonMmap)
		summariesOffsets = make([]encoding.IndexSummaryToken, 0, numEntries)
		lastReadID       []byte
	)
	decoder.Reset(decoderStream)

	for read := 0; read < numEntries; read++ {
		// We ignore the entry itself because we don't need any information from it
		entry, entryMetadata, err := decoder.DecodeIndexSummary()
		if err != nil {
			munmap(anonMmap)
			return nil, err
		}

		// Make sure that all the IDs are sorted as we iterate, and return an error
		// if they're not. This should never happen as files should be sorted on disk.
		if lastReadID != nil && bytes.Compare(lastReadID, entry.ID) != -1 {
			munmap(anonMmap)
			return nil, fmt.Errorf("summaries file is not sorted: %s", summariesFd.Name())
		}
		summariesOffsets = append(summariesOffsets, entryMetadata)
	}

	return newNearestIndexOffsetLookup(summariesOffsets, anonMmap, decoder, decoderStream), nil
}
