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

package series

import (
	"errors"
	"fmt"
	"time"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"
	xerrors "github.com/m3db/m3x/errors"
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
	opts        Options
	cloneableID ts.ID
	retriever   QueryableBlockRetriever
}

// NewReaderForRetriever returns a reader for a series
// block retriever, it will only return data from the block
// retriever.
func NewReaderForRetriever(
	id ts.ID,
	retriever QueryableBlockRetriever,
	opts Options,
) Reader {
	return Reader{opts: opts, cloneableID: id, retriever: retriever}
}

// ReadEncoded reads encoded blocks using just a block retriever.
func (r Reader) ReadEncoded(
	ctx context.Context,
	start, end time.Time,
) ([][]xio.SegmentReader, error) {
	return r.readersWithBlocksMapAndBuffer(ctx, start, end, nil, nil)
}

func (r Reader) readersWithBlocksMapAndBuffer(
	ctx context.Context,
	start, end time.Time,
	seriesBlocks block.DatabaseSeriesBlocks,
	seriesBuffer databaseBuffer,
) ([][]xio.SegmentReader, error) {
	// TODO(r): pool these results arrays
	var results [][]xio.SegmentReader

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
	if earliest := retention.FlushTimeStart(ropts, now); alignedStart.Before(earliest) {
		alignedStart = earliest
	}
	if latest := retention.FlushTimeEnd(ropts, now); alignedEnd.After(latest) {
		alignedEnd = latest
	}

	var (
		first, last = alignedStart, alignedEnd
		clonedID    ts.ID
	)
	for blockAt := first; !blockAt.After(last); blockAt = blockAt.Add(size) {
		if seriesBlocks != nil {
			if block, ok := seriesBlocks.BlockAt(blockAt); ok {
				// Block served from in-memory or in-memory metadata
				// will defer to disk read
				stream, err := block.Stream(ctx)
				if err != nil {
					return nil, err
				}
				if stream != nil {
					results = append(results, []xio.SegmentReader{stream})
					// NB(r): Mark this block as read now
					block.SetLastReadTime(now)
				}
				continue
			}
		}

		switch {
		case cachePolicy == CacheAll:
			// No-op, block metadata should have been in-memory
		case cachePolicy == CacheAllMetadata:
			// No-op, block metadata should have been in-memory
		case r.retriever != nil:
			// Try to stream from disk
			if r.retriever.IsBlockRetrievable(blockAt) {
				if clonedID == nil {
					// Clone ID as the block retriever uses the ID async so we cannot
					// be sure about owner not finalizing the cloneableID passed to
					// the Reader
					clonedID = r.opts.IdentifierPool().Clone(r.cloneableID)
					ctx.RegisterFinalizer(clonedID)
				}
				stream, err := r.retriever.Stream(clonedID, blockAt, nil)
				if err != nil {
					return nil, err
				}
				if stream != nil {
					results = append(results, []xio.SegmentReader{stream})
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
	res := make([]block.FetchBlockResult, 0, len(starts))

	var clonedID ts.ID
	cachePolicy := r.opts.CachePolicy()

	for _, start := range starts {
		if seriesBlocks != nil {
			if b, exists := seriesBlocks.BlockAt(start); exists {
				stream, err := b.Stream(ctx)
				if err != nil {
					r := block.NewFetchBlockResult(start, nil,
						fmt.Errorf("unable to retrieve block stream for series %s time %v: %v",
							r.cloneableID.String(), start, err))
					res = append(res, r)
				}
				if stream != nil {
					r := block.NewFetchBlockResult(start, []xio.SegmentReader{stream}, nil)
					res = append(res, r)
				}
				continue
			}
		}

		switch {
		case cachePolicy == CacheAll:
			// No-op, block metadata should have been in-memory
		case cachePolicy == CacheAllMetadata:
			// No-op, block metadata should have been in-memory
		case r.retriever != nil:
			// Try to stream from disk
			if r.retriever.IsBlockRetrievable(start) {
				if clonedID == nil {
					// Clone ID as the block retriever uses the ID async so we cannot
					// be sure about owner not finalizing the cloneableID passed to
					// the Reader
					clonedID = r.opts.IdentifierPool().Clone(r.cloneableID)
					ctx.RegisterFinalizer(clonedID)
				}
				stream, err := r.retriever.Stream(clonedID, start, nil)
				if err != nil {
					r := block.NewFetchBlockResult(start, nil,
						fmt.Errorf("unable to retrieve block stream for series %s time %v: %v",
							r.cloneableID.String(), start, err))
					res = append(res, r)
				}
				if stream != nil {
					// TODO: work out how to pass checksum here or defer till later somehow
					r := block.NewFetchBlockResult(start, []xio.SegmentReader{stream}, nil)
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
