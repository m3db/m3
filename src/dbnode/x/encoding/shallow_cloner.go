// Copyright (c) 2024 Uber Technologies, Inc.
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

package encoding

import (
	"time"

	enc "github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	xtime "github.com/m3db/m3/src/x/time"
)

// Cloner makes a clone of the given encoding type.
type Cloner interface {
	CloneSeriesIterator(enc.SeriesIterator) (enc.SeriesIterator, error)
}

type shallowCloner struct {
	pools enc.IteratorPools

	cloneBlockReaderFn cloneBlockReaderFn
}

// NewShallowCloner returns a cloner which makes a shallow copy of the given type.
func NewShallowCloner(pools enc.IteratorPools) Cloner {
	cloner := shallowCloner{
		pools: pools,
	}
	cloner.cloneBlockReaderFn = cloner.cloneBlockReader
	return cloner
}

// CloneSeriesIterator makes a shallow copy of the given series iterator.
// i.e. The returned iterator can be iterated independently of the original iterator.
// It does NOT copy the underlying tsz data, only points to the original iterator's data.
// nb:
//   - The lifecycle of the returned iterator is only valid until the original iterator is valid.
//   - Do NOT iterate the original iterator, as it can release resources held on to by the cloned
//     iterators.
func (s shallowCloner) CloneSeriesIterator(iter enc.SeriesIterator) (enc.SeriesIterator, error) {
	replicas, err := iter.Replicas()
	if err != nil {
		return nil, err
	}

	replicaCopies := s.pools.MultiReaderIteratorArray().Get(len(replicas))
	replicaCopies = replicaCopies[:0]
	for _, replica := range replicas {
		replicaCopy, err := s.cloneMultiReaderIterator(replica)
		if err != nil {
			return nil, err
		}
		replicaCopies = append(replicaCopies, replicaCopy)
	}

	iterCopy := s.pools.SeriesIterator().Get()
	iterCopy.Reset(enc.SeriesIteratorOptions{
		ID:             s.pools.ID().Clone(iter.ID()),
		Tags:           iter.Tags().Duplicate(),
		StartInclusive: iter.Start(),
		EndExclusive:   iter.End(),
		Replicas:       replicaCopies,
	})

	return iterCopy, nil
}

const (
	_initReadersSize = 5
)

func (s shallowCloner) cloneMultiReaderIterator(
	iter enc.MultiReaderIterator,
) (enc.MultiReaderIterator, error) {
	// TODO: pool these slice allocations
	blockReaderCopies := make([][]xio.BlockReader, 0, _initReadersSize)

	// nb: the implementation below requires iteration of the blocksIters
	// underlying the MultiReaderIterator. While we make sure to reset the
	// state of the iterator back to what it was originally, one important
	// caveat to callout: this iteration is not-threadsafe. i.e. if copies
	// are going to be made by different go-routines, the synchronization
	// has to be done at a level above this.
	readers := iter.Readers()
	initIdx := readers.Index()

	// nb: we start by assuming next=true for the first pass through the readersIterator
	// as the multiReaderiterator already calls Next() on the readersIterator when
	// it acquires the iter.
	for next := true; next; next = readers.Next() {
		currLen, currStart, currBlockSize := readers.CurrentReaders()
		currentCopies := make([]xio.BlockReader, 0, currLen)
		for i := 0; i < currLen; i++ {
			currReader := readers.CurrentReaderAt(i)
			currCopy, err := s.cloneBlockReaderFn(currReader, currStart, currBlockSize)
			if err != nil {
				return nil, err
			}
			currentCopies = append(currentCopies, currCopy)
		}
		blockReaderCopies = append(blockReaderCopies, currentCopies)
	}

	// reset the reader to the original state
	readers.RewindToIndex(initIdx)

	// TODO: pool this type
	sliceOfSlicesIter := xio.NewReaderSliceOfSlicesFromBlockReadersIterator(blockReaderCopies)
	multiReaderIterCopy := s.pools.MultiReaderIterator().Get()
	multiReaderIterCopy.ResetSliceOfSlices(sliceOfSlicesIter, iter.Schema())

	return multiReaderIterCopy, nil
}

type cloneBlockReaderFn func(xio.BlockReader, xtime.UnixNano, time.Duration) (xio.BlockReader, error)

func (s shallowCloner) cloneBlockReader(
	reader xio.BlockReader,
	start xtime.UnixNano,
	blockSize time.Duration,
) (xio.BlockReader, error) {
	// nb: we cannot rely on the reader.Segment.Clone() method as that performs a deep copy.
	// i.e. it copies the underlying data []byte as well. The copy we provide only
	// copies the wrapper constructs, it still points at the same []byte as the original.

	seg, err := reader.Segment()
	if err != nil {
		return xio.BlockReader{}, err
	}

	head := s.pools.CheckedBytesWrapper().Get(seg.Head.Bytes())
	tail := s.pools.CheckedBytesWrapper().Get(seg.Tail.Bytes())
	checksum := seg.CalculateChecksum()
	segCopy := ts.NewSegment(head, tail, checksum, ts.FinalizeNone)
	segReader := xio.NewSegmentReader(segCopy)

	return xio.BlockReader{
		SegmentReader: segReader,
		Start:         start,
		BlockSize:     blockSize,
	}, nil
}
