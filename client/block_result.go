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

package client

import (
	"io"
	"sync"
	"time"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/ts"
	xio "github.com/m3db/m3db/x/io"

	"github.com/m3db/m3x/pool"
)

type blocksResult struct {
	sync.RWMutex
	opts                    Options
	blockOpts               block.Options
	blockAllocSize          int
	contextPool             context.Pool
	encoderPool             encoding.EncoderPool
	multiReaderIteratorPool encoding.MultiReaderIteratorPool
	bytesPool               pool.BytesPool
	result                  bootstrap.ShardResult
}

func newBlocksResult(
	opts Options,
	bootstrapOpts bootstrap.Options,
	multiReaderIteratorPool encoding.MultiReaderIteratorPool,
) *blocksResult {
	blockOpts := bootstrapOpts.DatabaseBlockOptions()
	return &blocksResult{
		opts:                    opts,
		blockOpts:               blockOpts,
		blockAllocSize:          blockOpts.DatabaseBlockAllocSize(),
		contextPool:             opts.ContextPool(),
		encoderPool:             blockOpts.EncoderPool(),
		multiReaderIteratorPool: multiReaderIteratorPool,
		bytesPool:               blockOpts.BytesPool(),
		result:                  bootstrap.NewShardResult(4096, bootstrapOpts),
	}
}

func (r *blocksResult) addBlockFromPeer(id ts.ID, block *rpc.Block) error {
	var (
		start    = time.Unix(0, block.Start)
		segments = block.Segments
		result   = r.blockOpts.DatabaseBlockPool().Get()
	)

	if segments == nil {
		return errSessionBadBlockResultFromPeer
	}

	switch {
	case segments.Merged != nil:
		// Unmerged, can insert directly into a single block
		size := len(segments.Merged.Head) + len(segments.Merged.Tail)
		data := r.bytesPool.Get(size)[:size]
		n := copy(data, segments.Merged.Head)
		copy(data[n:], segments.Merged.Tail)

		encoder := r.encoderPool.Get()
		encoder.ResetSetData(start, data, false)

		result.Reset(start, encoder)

	case segments.Unmerged != nil:
		// Must merge to provide a single block
		readers := make([]io.Reader, len(segments.Unmerged))
		for i := range segments.Unmerged {
			readers[i] = xio.NewSegmentReader(ts.Segment{
				Head: segments.Unmerged[i].Head,
				Tail: segments.Unmerged[i].Tail,
			})
		}
		encoder, err := r.mergeReaders(start, readers)
		if err != nil {
			return err
		}
		result.Reset(start, encoder)

	default:
		return errSessionBadBlockResultFromPeer

	}

	// No longer need the encoder, seal the block
	result.Seal()

	resultCtx := r.contextPool.Get()
	defer resultCtx.Close()

	resultReader, err := result.Stream(resultCtx)
	if err != nil {
		return err
	}
	if resultReader == nil {
		return nil
	}

	var tmpCtx context.Context

	for {
		r.Lock()
		currBlock, exists := r.result.BlockAt(id, start)
		if !exists {
			r.result.AddBlock(id, result)
			r.Unlock()
			break
		}

		// Remove the existing block from the result so it doesn't get
		// merged again
		r.result.RemoveBlockAt(id, start)
		r.Unlock()

		// If we've already received data for this block, merge them
		// with the new block if possible
		if tmpCtx == nil {
			tmpCtx = r.contextPool.Get()
		} else {
			tmpCtx.Reset()
		}

		currReader, err := currBlock.Stream(tmpCtx)
		if err != nil {
			return err
		}

		// If there are no data in the current block, there is no
		// need to merge
		if currReader == nil {
			continue
		}

		readers := []io.Reader{currReader, resultReader}
		encoder, err := r.mergeReaders(start, readers)
		tmpCtx.BlockingClose()

		if err != nil {
			return err
		}

		result.Reset(start, encoder)
		result.Seal()
	}

	return nil
}

func (r *blocksResult) mergeReaders(start time.Time, readers []io.Reader) (encoding.Encoder, error) {
	iter := r.multiReaderIteratorPool.Get()
	iter.Reset(readers)
	defer iter.Close()

	encoder := r.encoderPool.Get()
	encoder.Reset(start, r.blockAllocSize)

	for iter.Next() {
		dp, unit, annotation := iter.Current()
		if err := encoder.Encode(dp, unit, annotation); err != nil {
			encoder.Close()
			return nil, err
		}
	}
	if err := iter.Err(); err != nil {
		encoder.Close()
		return nil, err
	}

	return encoder, nil
}
