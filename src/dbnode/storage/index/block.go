// Copyright (c) 2018 Uber Technologies, Inc.
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

package index

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	m3ninxindex "github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/mem"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/search"
	"github.com/m3db/m3/src/m3ninx/search/executor"
	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	xtime "github.com/m3db/m3x/time"
)

var (
	errUnableToWriteBlockClosed     = errors.New("unable to write, index block is closed")
	errUnableToWriteBlockSealed     = errors.New("unable to write, index block is sealed")
	errUnableToQueryBlockClosed     = errors.New("unable to query, index block is closed")
	errUnableToBootstrapBlockClosed = errors.New("unable to bootstrap, block is closed")
	errUnableToTickBlockClosed      = errors.New("unable to tick, block is closed")
	errBlockAlreadyClosed           = errors.New("unable to close, block already closed")

	errUnableToSealBlockIllegalStateFmtString  = "unable to seal, index block state: %v"
	errUnableToWriteBlockUnknownStateFmtString = "unable to write, unknown index block state: %v"
)

type blockState byte

const (
	blockStateClosed blockState = iota
	blockStateOpen
	blockStateSealed
)

type newExecutorFn func() (search.Executor, error)

type block struct {
	sync.RWMutex
	state               blockState
	activeSegment       segment.MutableSegment
	shardRangesSegments []blockShardRangesSegments

	newExecutorFn newExecutorFn
	startTime     time.Time
	endTime       time.Time
	blockSize     time.Duration
	opts          Options
	nsMD          namespace.Metadata
}

// blockShardsSegments is a collection of segments that has a mapping of what shards
// and time ranges they completely cover, this can only ever come from computing
// from data that has come from shards, either on an index flush or a bootstrap.
type blockShardRangesSegments struct {
	shardTimeRanges result.ShardTimeRanges
	segments        []segment.Segment
}

// NewBlock returns a new Block, representing a complete reverse index for the
// duration of time specified. It is backed by one or more segments.
func NewBlock(
	startTime time.Time,
	md namespace.Metadata,
	opts Options,
) (Block, error) {
	var (
		blockSize = md.Options().IndexOptions().BlockSize()
	)

	// FOLLOWUP(prateek): use this to track segments when we have multiple segments in a Block.
	postingsOffset := postings.ID(0)
	seg, err := mem.NewSegment(postingsOffset, opts.MemSegmentOptions())
	if err != nil {
		return nil, err
	}

	b := &block{
		state:         blockStateOpen,
		activeSegment: seg,

		startTime: startTime,
		endTime:   startTime.Add(blockSize),
		blockSize: blockSize,
		opts:      opts,
		nsMD:      md,
	}
	b.newExecutorFn = b.executorWithRLock

	return b, nil
}

func (b *block) StartTime() time.Time {
	return b.startTime
}

func (b *block) EndTime() time.Time {
	return b.endTime
}

func (b *block) WriteBatch(inserts *WriteBatch) (WriteBatchResult, error) {
	b.Lock()
	defer b.Unlock()

	if b.state != blockStateOpen {
		err := b.writeBatchErrorInvalidState(b.state)
		inserts.MarkUnmarkedEntriesError(err)
		return WriteBatchResult{
			NumError: int64(inserts.Len()),
		}, err
	}

	// NB: we're guaranteed the block (i.e. has a valid activeSegment) because
	// of the state check above. the if check below is additional paranoia.
	if b.activeSegment == nil { // should never happen
		err := b.openBlockHasNilActiveSegmentInvariantErrorWithRLock()
		inserts.MarkUnmarkedEntriesError(err)
		return WriteBatchResult{
			NumError: int64(inserts.Len()),
		}, err
	}

	err := b.activeSegment.InsertBatch(m3ninxindex.Batch{
		Docs:                inserts.PendingDocs(),
		AllowPartialUpdates: true,
	})
	if err == nil {
		inserts.MarkUnmarkedEntriesSuccess()
		return WriteBatchResult{
			NumSuccess: int64(inserts.Len()),
		}, nil
	}

	partialErr, ok := err.(*m3ninxindex.BatchPartialError)
	if !ok { // should never happen
		err := b.unknownWriteBatchInvariantError(err)
		// NB: marking all the inserts as failure, cause we don't know which ones failed
		inserts.MarkUnmarkedEntriesError(err)
		return WriteBatchResult{NumError: int64(inserts.Len())}, err
	}

	numErr := len(partialErr.Errs())
	for _, err := range partialErr.Errs() {
		// Avoid marking these as success
		inserts.MarkUnmarkedEntryError(err.Err, err.Idx)
	}

	// mark all non-error inserts success, so we don't repeatedly index them
	inserts.MarkUnmarkedEntriesSuccess()
	return WriteBatchResult{
		NumSuccess: int64(inserts.Len() - numErr),
		NumError:   int64(numErr),
	}, partialErr
}

func (b *block) executorWithRLock() (search.Executor, error) {
	var expectedReaders int
	if b.activeSegment != nil {
		expectedReaders++
	}
	for _, group := range b.shardRangesSegments {
		expectedReaders += len(group.segments)
	}

	var (
		readers = make([]m3ninxindex.Reader, 0, expectedReaders)
		success = false
	)

	// cleanup in case any of the readers below fail.
	defer func() {
		if !success {
			for _, reader := range readers {
				reader.Close()
			}
		}
	}()

	// start with the segment that's being actively written to (if we have one)
	if b.activeSegment != nil {
		reader, err := b.activeSegment.Reader()
		if err != nil {
			return nil, err
		}
		readers = append(readers, reader)
	}

	// loop over the segments associated to shard time ranges
	for _, group := range b.shardRangesSegments {
		for _, seg := range group.segments {
			reader, err := seg.Reader()
			if err != nil {
				return nil, err
			}
			readers = append(readers, reader)
		}
	}

	success = true
	return executor.NewExecutor(readers), nil
}

func (b *block) Query(
	query Query,
	opts QueryOptions,
	results Results,
) (bool, error) {
	b.RLock()
	defer b.RUnlock()
	if b.state == blockStateClosed {
		return false, errUnableToQueryBlockClosed
	}

	exec, err := b.newExecutorFn()
	if err != nil {
		return false, err
	}

	// FOLLOWUP(prateek): push down QueryOptions to restrict results
	// TODO(jeromefroe): Use the idx query directly once we implement an index in m3ninx
	// and don't need to use the segments anymore.
	iter, err := exec.Execute(query.Query.SearchQuery())
	if err != nil {
		exec.Close()
		return false, err
	}

	var (
		size       = results.Size()
		brokeEarly = false
	)
	execCloser := safeCloser{closable: exec}
	iterCloser := safeCloser{closable: iter}

	defer func() {
		iterCloser.Close()
		execCloser.Close()
	}()

	for iter.Next() {
		if opts.Limit > 0 && size >= opts.Limit {
			brokeEarly = true
			break
		}
		d := iter.Current()
		_, size, err = results.Add(d)
		if err != nil {
			return false, err
		}
	}

	if err := iter.Err(); err != nil {
		return false, err
	}

	if err := iterCloser.Close(); err != nil {
		return false, err
	}

	if err := execCloser.Close(); err != nil {
		return false, err
	}

	exhaustive := !brokeEarly
	return exhaustive, nil
}

func (b *block) AddResults(
	results result.IndexBlock,
) error {
	b.Lock()
	defer b.Unlock()

	// NB(prateek): we have to allow bootstrap to succeed even if we're Sealed because
	// of topology changes. i.e. if the current m3db process is assigned new shards,
	// we need to include their data in the index.

	// i.e. the only state we do not accept bootstrapped data is if we are closed.
	if b.state == blockStateClosed {
		return errUnableToBootstrapBlockClosed
	}

	// First check fulfilled is correct
	min, max := results.Fulfilled().MinMax()
	if min.Before(b.startTime) || max.After(b.endTime) {
		blockRange := xtime.Range{Start: b.startTime, End: b.endTime}
		return fmt.Errorf("fulfilled range %s is outside of index block range: %s",
			results.Fulfilled().SummaryString(), blockRange.String())
	}

	// NB: need to check if the current block has been marked 'Sealed' and if so,
	// mark all incoming mutable segments the same.
	isSealed := b.IsSealedWithRLock()

	var multiErr xerrors.MultiError
	for _, seg := range results.Segments() {
		if x, ok := seg.(segment.MutableSegment); ok {
			if isSealed {
				_, err := x.Seal()
				if err != nil {
					// if this happens it means a Mutable segment was marked sealed
					// in the bootstrappers, this should never happen.
					err := b.bootstrappingSealedMutableSegmentInvariant(err)
					multiErr = multiErr.Add(err)
				}
			}
		}
	}

	entry := blockShardRangesSegments{
		shardTimeRanges: results.Fulfilled(),
		segments:        results.Segments(),
	}

	// First see if this block can cover all our current blocks covering shard
	// time ranges
	currFulfilled := make(result.ShardTimeRanges)
	for _, existing := range b.shardRangesSegments {
		currFulfilled.AddRanges(existing.shardTimeRanges)
	}

	unfulfilledBySegments := currFulfilled.Copy()
	unfulfilledBySegments.Subtract(results.Fulfilled())
	if !unfulfilledBySegments.IsEmpty() {
		// This is the case where it cannot wholly replace the current set of blocks
		// so simply append the segments in this case
		b.shardRangesSegments = append(b.shardRangesSegments, entry)
		return multiErr.FinalError()
	}

	// This is the case where the new segments can wholly replace the
	// current set of blocks since unfullfilled by the new segments is zero
	for i, group := range b.shardRangesSegments {
		for _, seg := range group.segments {
			// Make sure to close the existing segments
			multiErr = multiErr.Add(seg.Close())
		}
		b.shardRangesSegments[i] = blockShardRangesSegments{}
	}
	b.shardRangesSegments = append(b.shardRangesSegments[:0], entry)

	return multiErr.FinalError()
}

func (b *block) Tick(c context.Cancellable, tickStart time.Time) (BlockTickResult, error) {
	b.RLock()
	defer b.RUnlock()
	result := BlockTickResult{}
	if b.state == blockStateClosed {
		return result, errUnableToTickBlockClosed
	}

	// active segment, can be nil incase we've evicted it already.
	if b.activeSegment != nil {
		result.NumSegments++
		result.NumDocs += b.activeSegment.Size()
	}

	// any other segments
	for _, group := range b.shardRangesSegments {
		for _, seg := range group.segments {
			result.NumSegments++
			result.NumDocs += seg.Size()
		}
	}

	return result, nil
}

func (b *block) Seal() error {
	b.Lock()
	defer b.Unlock()

	// ensure we only Seal if we're marked Open
	if b.state != blockStateOpen {
		return fmt.Errorf(errUnableToSealBlockIllegalStateFmtString, b.state)
	}
	b.state = blockStateSealed

	var multiErr xerrors.MultiError

	// seal active mutable segment.
	_, err := b.activeSegment.Seal()
	multiErr = multiErr.Add(err)

	// loop over any added mutable segments and seal them too.
	for _, group := range b.shardRangesSegments {
		for _, seg := range group.segments {
			if unsealed, ok := seg.(segment.MutableSegment); ok {
				_, err := unsealed.Seal()
				multiErr = multiErr.Add(err)
			}
		}
	}

	return multiErr.FinalError()
}

func (b *block) IsSealedWithRLock() bool {
	return b.state == blockStateSealed
}

func (b *block) IsSealed() bool {
	b.RLock()
	defer b.RUnlock()
	return b.IsSealedWithRLock()
}

func (b *block) NeedsMutableSegmentsEvicted() bool {
	b.RLock()
	defer b.RUnlock()
	anyMutableSegmentNeedsEviction := b.activeSegment != nil && b.activeSegment.Size() > 0

	// can early terminate if we already know we need to flush.
	if anyMutableSegmentNeedsEviction {
		return true
	}

	// otherwise we check all the boostrapped segments and to see if any of them
	// need a flush
	for _, shardRangeSegments := range b.shardRangesSegments {
		for _, seg := range shardRangeSegments.segments {
			if mutableSeg, ok := seg.(segment.MutableSegment); ok {
				anyMutableSegmentNeedsEviction = anyMutableSegmentNeedsEviction || mutableSeg.Size() > 0
			}
		}
	}

	return anyMutableSegmentNeedsEviction
}

func (b *block) EvictMutableSegments() (EvictMutableSegmentResults, error) {
	var results EvictMutableSegmentResults
	b.Lock()
	defer b.Unlock()
	if b.state != blockStateSealed {
		return results, fmt.Errorf("unable to evict mutable segments, block must be sealed, found: %v", b.state)
	}
	var multiErr xerrors.MultiError

	// close active segment.
	if b.activeSegment != nil {
		results.NumMutableSegments++
		results.NumDocs += b.activeSegment.Size()
		multiErr = multiErr.Add(b.activeSegment.Close())
		b.activeSegment = nil
	}

	// close any other mutable segments too.
	for idx := range b.shardRangesSegments {
		segments := make([]segment.Segment, 0, len(b.shardRangesSegments[idx].segments))
		for _, seg := range b.shardRangesSegments[idx].segments {
			mutableSeg, ok := seg.(segment.MutableSegment)
			if !ok {
				segments = append(segments, seg)
				continue
			}
			results.NumMutableSegments++
			results.NumDocs += mutableSeg.Size()
			multiErr = multiErr.Add(mutableSeg.Close())
		}
		b.shardRangesSegments[idx].segments = segments
	}

	return results, multiErr.FinalError()
}

func (b *block) Close() error {
	b.Lock()
	defer b.Unlock()
	if b.state == blockStateClosed {
		return errBlockAlreadyClosed
	}
	b.state = blockStateClosed

	var multiErr xerrors.MultiError

	// close active segment.
	if b.activeSegment != nil {
		multiErr = multiErr.Add(b.activeSegment.Close())
		b.activeSegment = nil
	}

	// close any other added segments too.
	for _, group := range b.shardRangesSegments {
		for _, seg := range group.segments {
			multiErr = multiErr.Add(seg.Close())
		}
	}
	b.shardRangesSegments = nil

	return multiErr.FinalError()
}

func (b *block) writeBatchErrorInvalidState(state blockState) error {
	switch state {
	case blockStateClosed:
		return errUnableToWriteBlockClosed
	case blockStateSealed:
		return errUnableToWriteBlockSealed
	default: // should never happen
		err := fmt.Errorf(errUnableToWriteBlockUnknownStateFmtString, state)
		instrument.EmitAndLogInvariantViolation(b.opts.InstrumentOptions(), func(l xlog.Logger) {
			l.Errorf(err.Error())
		})
		return err
	}
}

func (b *block) unknownWriteBatchInvariantError(err error) error {
	wrappedErr := fmt.Errorf("unexpected non-BatchPartialError from m3ninx InsertBatch: %v", err)
	instrument.EmitAndLogInvariantViolation(b.opts.InstrumentOptions(), func(l xlog.Logger) {
		l.Errorf(wrappedErr.Error())
	})
	return wrappedErr
}

func (b *block) bootstrappingSealedMutableSegmentInvariant(err error) error {
	wrapped := fmt.Errorf("internal error: bootstrapping a mutable segment already marked sealed: %v", err)
	instrument.EmitAndLogInvariantViolation(b.opts.InstrumentOptions(), func(l xlog.Logger) {
		l.Errorf(wrapped.Error())
	})
	return wrapped
}

func (b *block) openBlockHasNilActiveSegmentInvariantErrorWithRLock() error {
	err := fmt.Errorf("internal error: block has open block state [%v] has nil active segment", b.state)
	instrument.EmitAndLogInvariantViolation(b.opts.InstrumentOptions(), func(l xlog.Logger) {
		l.Errorf(err.Error())
	})
	return err
}

type closable interface {
	Close() error
}

type safeCloser struct {
	closable
	closed bool
}

func (c *safeCloser) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.closable.Close()
}
