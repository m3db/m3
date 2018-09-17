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
	"errors"
	"fmt"

	"github.com/m3db/m3/src/dbnode/digest"
	xmsgpack "github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/x/mmap"
	"github.com/m3db/m3x/ident"

	"gopkg.in/vmihailenco/msgpack.v2"
)

var errCloneShouldNotBeCloned = errors.New("clones should not be cloned")

// nearestIndexOffsetLookup provides a way of quickly determining the nearest offset of an
// ID in the index file. It is not safe for concurrent use
type nearestIndexOffsetLookup struct {
	summaryIDsOffsets []xmsgpack.IndexSummaryToken
	// bytes from file mmap'd into anonymous region
	summariesMmap []byte
	// reusable decoder stream
	decoderStream  xmsgpack.DecoderStream
	msgpackDecoder *msgpack.Decoder
	isClone        bool
}

func newNearestIndexOffsetLookup(
	summaryIDsOffsets []xmsgpack.IndexSummaryToken,
	summariesMmap []byte,
	decoderStream xmsgpack.DecoderStream,
) *nearestIndexOffsetLookup {
	return &nearestIndexOffsetLookup{
		summaryIDsOffsets: summaryIDsOffsets,
		summariesMmap:     summariesMmap,
		decoderStream:     decoderStream,
		msgpackDecoder:    msgpack.NewDecoder(nil),
		isClone:           false,
	}
}

func (il *nearestIndexOffsetLookup) concurrentClone() (*nearestIndexOffsetLookup, error) {
	if il.isClone {
		return nil, errCloneShouldNotBeCloned
	}

	return &nearestIndexOffsetLookup{
		summaryIDsOffsets: il.summaryIDsOffsets,
		summariesMmap:     il.summariesMmap,
		decoderStream:     xmsgpack.NewDecoderStream(nil),
		msgpackDecoder:    msgpack.NewDecoder(nil),
		isClone:           true,
	}, nil
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
func (il *nearestIndexOffsetLookup) getNearestIndexFileOffset(id ident.ID) (int64, error) {
	idBytes := id.Bytes()

	min := 0
	max := len(il.summaryIDsOffsets) - 1

	// The summaries file only contains a fraction of the series that are in
	// the index file itself. Because of that, the binary search that we're
	// performing is "optimistic". We're trying to find either an exact match,
	// OR the nearest match that is to the left of the series we're searching
	// for (so we keep track of it everytime we move right). We start with an
	// assumption that the best match so far is at index 0, because in the worst
	// case scenario if we don't find a single "match", then the caller should
	// start at index 0 and scan until they encounter an entry that tells them
	// that the ID they're looking for does not exist (because the IDs in the
	// index are sorted).
	bestMatchSoFar := int64(0)

	for {
		if min > max {
			return bestMatchSoFar, nil
		}

		idx := (max + min) / 2
		summaryBytesMetadata := il.summaryIDsOffsets[idx]
		compBytes := summaryBytesMetadata.ID(il.summariesMmap)
		comparison := bytes.Compare(idBytes, compBytes)

		// Found it
		if comparison == 0 {
			indexOffset, err := summaryBytesMetadata.IndexOffset(
				il.summariesMmap, il.decoderStream, il.msgpackDecoder)
			// Should never happen, either something is really wrong with the code or
			// the file on disk was corrupted
			if err != nil {
				return -1, err
			}
			return indexOffset, nil
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
				il.summariesMmap, il.decoderStream, il.msgpackDecoder)
			if err != nil {
				return -1, err
			}
			// update the bestMatchSoFar everytime we move right
			bestMatchSoFar = indexOffset
			continue
		}
	}
}

func (il *nearestIndexOffsetLookup) close() error {
	// Parent should clean up shared resources
	if il.isClone {
		return nil
	}
	return mmap.Munmap(il.summariesMmap)
}

// newNearestIndexOffsetLookupFromSummariesFile creates an nearestIndexOffsetLookup
// from an index summaries file by reading the summaries file into an anonymous
// mmap'd region, and also creating the slice of summaries offsets which is
// required to binary search the data structure. It will also make sure that
// the summaries file is sorted (which it always should be).
func newNearestIndexOffsetLookupFromSummariesFile(
	summariesFdWithDigest digest.FdWithDigestReader,
	expectedSummariesDigest uint32,
	decoder *xmsgpack.Decoder,
	numEntries int,
) (*nearestIndexOffsetLookup, error) {
	summariesFd := summariesFdWithDigest.Fd()
	stat, err := summariesFd.Stat()
	if err != nil {
		return nil, err
	}
	numBytes := stat.Size()

	// Request an anonymous (non-file-backed) mmap region. Note that we're going
	// to use the mmap'd region to store the read-only summaries data, but the mmap
	// region itself needs to be writable so we can copy the bytes from disk
	// into it
	mmapResult, err := mmap.Bytes(numBytes, mmap.Options{Read: true, Write: true})
	if err != nil {
		return nil, err
	}
	summariesMmap := mmapResult.Result

	// Validate the bytes on disk using the digest, and read them into
	// the mmap'd region
	_, err = summariesFdWithDigest.ReadAllAndValidate(summariesMmap, expectedSummariesDigest)
	if err != nil {
		mmap.Munmap(summariesMmap)
		return nil, err
	}

	// Msgpack decode the entire summaries file (we need to store the offsets
	// for the entries so we can binary-search it)
	var (
		decoderStream = xmsgpack.NewDecoderStream(summariesMmap)
		summaryTokens = make([]xmsgpack.IndexSummaryToken, 0, numEntries)
		lastReadID    []byte
	)
	decoder.Reset(decoderStream)

	for read := 0; read < numEntries; read++ {
		// We ignore the entry itself because we don't need any information from it
		entry, summaryToken, err := decoder.DecodeIndexSummary()
		if err != nil {
			mmap.Munmap(summariesMmap)
			return nil, err
		}

		// Make sure that all the IDs are sorted as we iterate, and return an error
		// if they're not. This should never happen as files should be sorted on disk.
		if lastReadID != nil && bytes.Compare(lastReadID, entry.ID) != -1 {
			mmap.Munmap(summariesMmap)
			return nil, fmt.Errorf("summaries file is not sorted: %s", summariesFd.Name())
		}
		summaryTokens = append(summaryTokens, summaryToken)
		lastReadID = entry.ID
	}

	return newNearestIndexOffsetLookup(summaryTokens, summariesMmap, decoderStream), nil
}
