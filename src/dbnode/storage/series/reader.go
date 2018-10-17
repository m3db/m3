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

	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
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
func (r Reader) ReadEncoded(
	ctx context.Context,
	start, end time.Time,
) ([][]xio.BlockReader, error) {
	return r.readersWithBlocksMapAndBuffer(ctx, start, end, nil, nil)
}

func (r Reader) readersWithBlocksMapAndBuffer(
	ctx context.Context,
	start, end time.Time,
	seriesBlocks block.DatabaseSeriesBlocks,
	seriesBuffer databaseBuffer,
) ([][]xio.BlockReader, error) {
	// TODO(r): pool these results arrays
	var results [][]xio.BlockReader

	if end.Before(start) {
		return nil, xerrors.NewInvalidParamsError(errSeriesReadInvalidRange)
	}

	var (
		nowFn        = r.opts.ClockOptions().NowFn()
		now          = nowFn()
		cachePolicy  = r.opts.CachePolicy()
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

	first, last := alignedStart, alignedEnd
	for blockAt := first; !blockAt.After(last); blockAt = blockAt.Add(size) {
		if seriesBlocks != nil {
			if block, ok := seriesBlocks.BlockAt(blockAt); ok {
				// Block served from in-memory or in-memory metadata
				// will defer to disk read
				streamedBlock, err := block.Stream(ctx)
				if err != nil {
					return nil, err
				}
				if streamedBlock.IsNotEmpty() {
					results = append(results, []xio.BlockReader{streamedBlock})
					// NB(r): Mark this block as read now
					block.SetLastReadTime(now)
					if r.onRead != nil {
						r.onRead.OnReadBlock(block)
					}
				}
				continue
			}
		}

		switch {
		case cachePolicy == CacheAll:
			// No-op, block metadata should have been in-memory
		case r.retriever != nil:
			// Try to stream from disk
			if r.retriever.IsBlockRetrievable(blockAt) {
				streamedBlock, err := r.retriever.Stream(ctx, r.id, blockAt, r.onRetrieve)
				if err != nil {
					return nil, err
				}
				if streamedBlock.IsNotEmpty() {
					results = append(results, []xio.BlockReader{streamedBlock})
				}
			}
		}
	}

	if seriesBuffer != nil {
		bufferResults := seriesBuffer.ReadEncoded(ctx, start, end)
		if len(bufferResults) > 0 {
			results = append(results, bufferResults...)
		}
	}

	return results, nil
}

// FetchBlocks returns data blocks given a list of block start times using
// just a block retriever.
func (r Reader) FetchBlocks(
	ctx context.Context,
	starts []time.Time,
) ([]block.FetchBlockResult, error) {
	return r.fetchBlocksWithBlocksMapAndBuffer(ctx, starts, nil, nil)
}

func (r Reader) fetchBlocksWithBlocksMapAndBuffer(
	ctx context.Context,
	starts []time.Time,
	seriesBlocks block.DatabaseSeriesBlocks,
	seriesBuffer databaseBuffer,
) ([]block.FetchBlockResult, error) {
	var (
		// TODO(r): pool these results arrays
		res         = make([]block.FetchBlockResult, 0, len(starts))
		cachePolicy = r.opts.CachePolicy()
		// NB(r): Always use nil for OnRetrieveBlock so we don't cache the
		// series after fetching it from disk, the fetch blocks API is called
		// during streaming so to cache it in memory would mean we would
		// eventually cache all series in memory when we stream results to a
		// peer.
		onRetrieve block.OnRetrieveBlock
	)
	for _, start := range starts {
		if seriesBlocks != nil {
			if b, exists := seriesBlocks.BlockAt(start); exists {
				streamedBlock, err := b.Stream(ctx)
				if err != nil {
					r := block.NewFetchBlockResult(start, nil,
						fmt.Errorf("unable to retrieve block stream for series %s time %v: %v",
							r.id.String(), start, err))
					res = append(res, r)
				}
				if streamedBlock.IsNotEmpty() {
					b := []xio.BlockReader{streamedBlock}
					r := block.NewFetchBlockResult(start, b, nil)
					res = append(res, r)
				}
				continue
			}
		}
		switch {
		case cachePolicy == CacheAll:
			// No-op, block metadata should have been in-memory
		case r.retriever != nil:
			// Try to stream from disk
			if r.retriever.IsBlockRetrievable(start) {
				streamedBlock, err := r.retriever.Stream(ctx, r.id, start, onRetrieve)
				if err != nil {
					r := block.NewFetchBlockResult(start, nil,
						fmt.Errorf("unable to retrieve block stream for series %s time %v: %v",
							r.id.String(), start, err))
					res = append(res, r)
				}
				if streamedBlock.IsNotEmpty() {
					b := []xio.BlockReader{streamedBlock}
					r := block.NewFetchBlockResult(start, b, nil)
					res = append(res, r)
				}
			}
		}
	}

	if seriesBuffer != nil && !seriesBuffer.IsEmpty() {
		bufferResults := seriesBuffer.FetchBlocks(ctx, starts)
		res = append(res, bufferResults...)
	}

	block.SortFetchBlockResultByTimeAscending(res)

	return res, nil
}
