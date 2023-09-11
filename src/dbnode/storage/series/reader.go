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

package series

import (
	"errors"
	"fmt"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

var (
	errSeriesReadInvalidRange = errors.New(
		"series invalid time range read argument specified")
)

// Reader reads results from a series, or a series block
// retriever or both.
// It is implemented as a struct so it can be allocated on
// the stack.
type Reader struct {
	opts       Options
	id         ident.ID
	retriever  QueryableBlockRetriever
	onRetrieve block.OnRetrieveBlock
	onRead     block.OnReadBlock
}

// NewReaderUsingRetriever returns a reader for a series
// block retriever, it will use the block retriever as the
// source to read blocks from.
func NewReaderUsingRetriever(
	id ident.ID,
	retriever QueryableBlockRetriever,
	onRetrieveBlock block.OnRetrieveBlock,
	onReadBlock block.OnReadBlock,
	opts Options,
) Reader {
	return Reader{
		opts:       opts,
		id:         id,
		retriever:  retriever,
		onRetrieve: onRetrieveBlock,
		onRead:     onReadBlock,
	}
}

// ReadEncoded reads encoded blocks using just a block retriever.
func (r *Reader) ReadEncoded(
	ctx context.Context,
	start, end xtime.UnixNano,
	nsCtx namespace.Context,
) (BlockReaderIter, error) {
	return r.readersWithBlocksMapAndBuffer(ctx, start, end, nil, nil, nsCtx)
}

func (r *Reader) readersWithBlocksMapAndBuffer(
	ctx context.Context,
	start, end xtime.UnixNano,
	seriesBlocks block.DatabaseSeriesBlocks,
	seriesBuffer databaseBuffer,
	nsCtx namespace.Context,
) (BlockReaderIter, error) {
	if end.Before(start) {
		return nil, xerrors.NewInvalidParamsError(errSeriesReadInvalidRange)
	}

	var (
		nowFn        = r.opts.ClockOptions().NowFn()
		now          = xtime.ToUnixNano(nowFn())
		ropts        = r.opts.RetentionOptions()
		size         = ropts.BlockSize()
		alignedStart = start.Truncate(size)
		alignedEnd   = end.Truncate(size)
	)

	if alignedEnd.Equal(end) {
		// Move back to make range [start, end)
		alignedEnd = alignedEnd.Add(-1 * size)
	}

	// Squeeze the lookup window by what's available to make range queries like [0, infinity) possible
	earliest := retention.FlushTimeStart(ropts, now)
	if alignedStart.Before(earliest) {
		alignedStart = earliest
	}
	latest := now.Add(ropts.BufferFuture()).Truncate(size)
	if alignedEnd.After(latest) {
		alignedEnd = latest
	}

	return r.readersWithBlocksMapAndBufferAligned(ctx, alignedStart, alignedEnd,
		seriesBlocks, seriesBuffer, nsCtx)
}

// BlockReaderIter provides an Iterator interface to a collection of BlockReaders.
//
// The Iterator allows disk read to be lazily requested when Next() is called. This allows the system to limit an
// expensive Query before it issues all its disk reads.
//
// The Iterator does not hold any locks so it's safe to pause or even abandon iterating through the results. Any
// in-memory blocks (cache, series buffer) are eagerly loaded to avoid holding read locks while iterating.
//
// This iterator is not thread safe and should be only be used by a single go routine.
type BlockReaderIter interface {
	// Next requests the next BlockReaders, returning if any more existed.
	// The BlockReaders are available with Current.
	// If an error occurs, false is returned and it is available with Err.
	Next(ctx context.Context) bool

	// Current returns the current set of BlockReaders for a given blockStart time, guaranteed to be not empty.
	// Results are returned in block start time asc ordering. Within the same block start time there is no guaranteed
	// ordering.
	Current() []xio.BlockReader

	// Err is non-nil if an error occurred when calling Next.
	Err() error

	// ToSlices eagerly loads all BlockReaders into the legacy slices of slices for backward compatibility.
	// TODO: remove this and convert everything to the iterator pattern.
	ToSlices(ctx context.Context) ([][]xio.BlockReader, error)
}

type blockReaderIterOpts struct {
	start     xtime.UnixNano
	end       xtime.UnixNano
	blockSize time.Duration
	reader    *Reader
	nsCtx     namespace.Context
	cached    []xio.BlockReader
	buffer    [][]xio.BlockReader
}

type blockReaderIter struct {
	blockReaderIterOpts

	blockAt xtime.UnixNano
	curr    []xio.BlockReader
	err     error
}

func (i *blockReaderIter) Err() error {
	return i.err
}

func (i *blockReaderIter) Current() []xio.BlockReader {
	return i.curr
}

// nolint: gocyclo
func (i *blockReaderIter) Next(ctx context.Context) bool {
	if i.blockAt.IsZero() {
		i.blockAt = i.start
	}
	i.curr = make([]xio.BlockReader, 0)
	for !i.blockAt.After(i.end) && len(i.curr) == 0 {
		// first checks the blocks from the series buffer at this time.
		// blocks are sorted by start time so we only need to look at the head.
		for len(i.buffer) > 0 && len(i.buffer[0]) > 0 && i.buffer[0][0].Start.Equal(i.blockAt) {
			i.curr = append(i.curr, i.buffer[0][0])
			i.buffer[0] = i.buffer[0][1:]
			if len(i.buffer[0]) == 0 {
				i.buffer = i.buffer[1:]
			}
		}

		// next check for the disk block at this time.
		// check the cache first.
		// blocks are sorted by start time so we only need to look at the head.
		if len(i.cached) > 0 && i.cached[0].Start.Equal(i.blockAt) {
			i.curr = append(i.curr, i.cached[0])
			i.cached = i.cached[1:]
		} else {
			// if not in the cache, request a load from disk.
			blockReader, found, err := i.reader.streamBlock(ctx, i.blockAt, i.reader.onRetrieve, i.nsCtx)
			if err != nil {
				i.err = err
				return false
			}
			if found {
				i.curr = append(i.curr, blockReader)
			}
		}
		i.blockAt = i.blockAt.Add(i.blockSize)
	}
	return len(i.curr) != 0
}

func (i *blockReaderIter) ToSlices(ctx context.Context) ([][]xio.BlockReader, error) {
	var results [][]xio.BlockReader
	for i.Next(ctx) {
		results = append(results, i.Current())
	}
	if i.Err() != nil {
		return nil, i.Err()
	}
	return results, nil
}

func (r *Reader) readersWithBlocksMapAndBufferAligned(
	ctx context.Context,
	start, end xtime.UnixNano,
	seriesBlocks block.DatabaseSeriesBlocks,
	seriesBuffer databaseBuffer,
	nsCtx namespace.Context,
) (BlockReaderIter, error) {
	var (
		nowFn       = r.opts.ClockOptions().NowFn()
		now         = xtime.ToUnixNano(nowFn())
		ropts       = r.opts.RetentionOptions()
		blockSize   = ropts.BlockSize()
		readerCount = end.Sub(start) / blockSize
		buffer      [][]xio.BlockReader
		cached      []xio.BlockReader
	)

	if readerCount < 0 {
		readerCount = 0
	}

	for blockAt := start; !blockAt.After(end); blockAt = blockAt.Add(blockSize) {
		// Eagerly load the readers from the series buffer and disk cache to avoid holding locks in the iterator.
		//
		// It is important to look for data in the series buffer one block at
		// a time within this loop so that the returned results contain data
		// from blocks in chronological order. Failure to do this will result
		// in an out of order error in the MultiReaderIterator on query.

		blockReader, blk, found, err := retrieveCached(ctx, blockAt, seriesBlocks)
		if err != nil {
			return nil, err
		}

		if found {
			// NB(r): Mark this block as read now
			blk.SetLastReadTime(now)
			if r.onRead != nil {
				r.onRead.OnReadBlock(blk)
			}
			cached = append(cached, blockReader)
		}

		if seriesBuffer != nil {
			var bufferReaders []xio.BlockReader
			bufferResults, err := seriesBuffer.ReadEncoded(ctx, blockAt, blockAt.Add(blockSize), nsCtx)
			if err != nil {
				return nil, err
			}

			// Multiple block results may be returned here (for the same block
			// start) - one for warm writes and another for cold writes.
			for _, bufferRes := range bufferResults {
				bufferReaders = append(bufferReaders, bufferRes...)
			}
			if len(bufferReaders) > 0 {
				buffer = append(buffer, bufferReaders)
			}
		}
	}

	return &blockReaderIter{
		blockReaderIterOpts: blockReaderIterOpts{
			start:     start,
			end:       end,
			blockSize: blockSize,
			reader:    r,
			nsCtx:     nsCtx,
			buffer:    buffer,
			cached:    cached,
		},
	}, nil
}

// FetchBlocks returns data blocks given a list of block start times using
// just a block retriever.
func (r *Reader) FetchBlocks(
	ctx context.Context,
	starts []xtime.UnixNano,
	nsCtx namespace.Context,
) ([]block.FetchBlockResult, error) {
	return r.fetchBlocksWithBlocksMapAndBuffer(ctx, starts, nil, nil, nsCtx)
}

func (r *Reader) fetchBlocksWithBlocksMapAndBuffer(
	ctx context.Context,
	starts []xtime.UnixNano,
	seriesBlocks block.DatabaseSeriesBlocks,
	seriesBuffer databaseBuffer,
	nsCtx namespace.Context,
) ([]block.FetchBlockResult, error) {
	res := r.resolveBlockResults(ctx, starts, seriesBlocks, nsCtx)
	if seriesBuffer != nil && !seriesBuffer.IsEmpty() {
		bufferResults := seriesBuffer.FetchBlocks(ctx, starts, nsCtx)

		// Ensure both slices are sorted before merging as two sorted lists.
		block.SortFetchBlockResultByTimeAscending(res)
		block.SortFetchBlockResultByTimeAscending(bufferResults)
		bufferIdx := 0
		for i := range res {
			blockResult := res[i]
			if !(bufferIdx < len(bufferResults)) {
				break
			}

			currBufferResult := bufferResults[bufferIdx]
			if blockResult.Start.Equal(currBufferResult.Start) {
				if currBufferResult.Err != nil {
					res[i].Err = currBufferResult.Err
				} else {
					res[i].Blocks = append(res[i].Blocks, currBufferResult.Blocks...)
				}
				bufferIdx++
				continue
			}
		}

		// Add any buffer results for which there was no existing blockstart
		// to the end.
		if bufferIdx < len(bufferResults) {
			res = append(res, bufferResults[bufferIdx:]...)
		}
	}

	// Should still be sorted but do it again for sanity.
	block.SortFetchBlockResultByTimeAscending(res)
	return res, nil
}

func (r *Reader) resolveBlockResults(
	ctx context.Context,
	starts []xtime.UnixNano,
	seriesBlocks block.DatabaseSeriesBlocks,
	nsCtx namespace.Context,
) []block.FetchBlockResult {
	// Two-dimensional slice (each block.FetchBlockResult has a []xio.BlockReader internally)
	// such that the first dimension is unique by blockstart and the second dimension is blocks
	// of data for that blockstart (not necessarily in chronological order).
	//
	// ex. (querying 2P.M -> 6P.M with a 2-hour blocksize):
	// []block.FetchBlockResult{
	//   block.FetchBlockResult{
	//     Start: 2P.M,
	//     Blocks: []xio.BlockReader{block0, block1, block2},
	//   },
	//   block.FetchBlockResult{
	//     Start: 4P.M,
	//     Blocks: []xio.BlockReader{block0},
	//   },
	// }
	res := make([]block.FetchBlockResult, 0, len(starts))
	for _, start := range starts {
		// Slice of xio.BlockReader such that all data belong to the same blockstart.
		var blockReaders []xio.BlockReader

		blockReader, _, found, err := retrieveCached(ctx, start, seriesBlocks)
		if err != nil {
			// Short-circuit this entire blockstart if an error was encountered.
			r := block.NewFetchBlockResult(start, nil,
				fmt.Errorf("unable to retrieve block stream for series %s time %v: %w",
					r.id.String(), start, err))
			res = append(res, r)
			continue
		}

		if !found {
			// NB(r): Always use nil for OnRetrieveBlock so we don't cache the
			// series after fetching it from disk, the fetch blocks API is called
			// during streaming so to cache it in memory would mean we would
			// eventually cache all series in memory when we stream results to a
			// peer.
			blockReader, found, err = r.streamBlock(ctx, start, nil, nsCtx)
			if err != nil {
				// Short-circuit this entire blockstart if an error was encountered.
				r := block.NewFetchBlockResult(start, nil,
					fmt.Errorf("unable to retrieve block stream for series %s time %v: %w",
						r.id.String(), start, err))
				res = append(res, r)
				continue
			}
		}

		if found {
			blockReaders = append(blockReaders, blockReader)
		}

		if len(blockReaders) > 0 {
			res = append(res, block.NewFetchBlockResult(start, blockReaders, nil))
		}
	}

	return res
}

func retrieveCached(
	ctx context.Context,
	start xtime.UnixNano,
	seriesBlocks block.DatabaseSeriesBlocks,
) (xio.BlockReader, block.DatabaseBlock, bool, error) {
	if seriesBlocks != nil {
		if b, exists := seriesBlocks.BlockAt(start); exists {
			streamedBlock, err := b.Stream(ctx)
			if err != nil {
				return xio.BlockReader{}, b, false, err
			}

			if streamedBlock.IsNotEmpty() {
				return streamedBlock, b, true, nil
			}
		}
	}

	return xio.BlockReader{}, nil, false, nil
}

func (r *Reader) streamBlock(
	ctx context.Context,
	start xtime.UnixNano,
	onRetrieve block.OnRetrieveBlock,
	nsCtx namespace.Context,
) (xio.BlockReader, bool, error) {
	cachePolicy := r.opts.CachePolicy()
	switch {
	case cachePolicy == CacheAll:
		// No-op, block metadata should have been in-memory
	case r.retriever != nil:
		// Try to stream from disk
		isRetrievable, err := r.retriever.IsBlockRetrievable(start)
		if err != nil {
			return xio.BlockReader{}, false, err
		}

		if isRetrievable {
			streamedBlock, err := r.retriever.Stream(ctx, r.id, start, onRetrieve, nsCtx)
			if err != nil {
				// Short-circuit this entire blockstart if an error was encountered.
				return xio.BlockReader{}, false, err
			}

			if streamedBlock.IsNotEmpty() {
				return streamedBlock, true, nil
			}
		}
	}

	return xio.BlockReader{}, false, nil
}
