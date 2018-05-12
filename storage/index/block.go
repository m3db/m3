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

	"github.com/m3db/m3db/storage/index/convert"
	m3ninxindex "github.com/m3db/m3ninx/index"
	"github.com/m3db/m3ninx/index/segment"
	"github.com/m3db/m3ninx/index/segment/mem"
	"github.com/m3db/m3ninx/postings"
	"github.com/m3db/m3ninx/search"
	"github.com/m3db/m3ninx/search/executor"
	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/instrument"
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
	state                blockState
	bootstrappedSegments []segment.Segment
	segment              segment.MutableSegment

	newExecutorFn newExecutorFn
	startTime     time.Time
	endTime       time.Time
	opts          Options
}

// NewBlock returns a new Block, representing a complete reverse index for the
// duration of time specified. It is backed by one or more segments.
func NewBlock(
	startTime time.Time,
	blockSize time.Duration,
	opts Options,
) (Block, error) {
	// FOLLOWUP(prateek): use this to track segments when we have multiple segments in a Block.
	postingsOffset := postings.ID(0)
	seg, err := mem.NewSegment(postingsOffset, opts.MemSegmentOptions())
	if err != nil {
		return nil, err
	}

	b := &block{
		state:   blockStateOpen,
		segment: seg,

		startTime: startTime,
		endTime:   startTime.Add(blockSize),
		opts:      opts,
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

func (b *block) WriteBatch(inserts WriteBatchEntryByBlockStartAndID) (WriteBatchResult, error) {
	b.RLock()
	defer b.RUnlock()

	if b.state != blockStateOpen {
		// NB: releasing all references to inserts
		WriteBatchEntriesFinalizer(inserts).Finalize()
		return WriteBatchResult{
			NumError: int64(len(inserts)),
		}, b.writeBatchErrorInvalidState(b.state)
	}

	var (
		multiErr xerrors.MultiError
		result   WriteBatchResult
	)
	inserts.ForEachID(func(writesForID WriteBatchEntryByBlockStartAndID) {
		// all writes are guaranteed to have the same ID by this point, and further
		// we're guaranteed that at least a single element exists in the slice.
		id := writesForID[0].ID
		tags := writesForID[0].Tags

		if id == nil {
			return
		}

		// define some helper functions to help keep the code below cleaner.
		failFn := func(err error) {
			multiErr = multiErr.Add(err)
			result.NumError += int64(len(writesForID))
			// finalize all refs
			WriteBatchEntriesFinalizer(writesForID).Finalize()
		}
		successFn := func() {
			result.NumSuccess += int64(len(writesForID))
			// mark the first ref success (can mark any ref success here, because they're backed by
			// by the same entry). Could also mark all of them success but it wouldn't buy us anything.
			writesForID[0].OnIndexSeries.OnIndexSuccess(xtime.ToUnixNano(b.startTime))
			// we do need to finalize all refs as each is an extra inc we need to dec
			WriteBatchEntriesFinalizer(writesForID).Finalize()
		}

		contains, err := b.segment.ContainsID(id.Bytes())
		if contains && err == nil {
			// can early terminate as the active segment already has the ID
			successFn()
			return
		}

		// NB(prateek): we delay the conversion from ident types -> doc as we want to minimize the allocs
		// of idents until we're sure we actually need to index a series. This helps keep memory usage low
		// when we receive a large spike of new metrics.
		d, err := convert.FromMetric(id, tags)
		if err != nil {
			failFn(err)
			return
		}

		// now actually perform the insert
		if _, err := b.segment.Insert(d); err != nil {
			failFn(err)
			return
		}

		successFn()
	})

	return result, multiErr.FinalError()
}

func (b *block) executorWithRLock() (search.Executor, error) {
	var (
		readers = make([]m3ninxindex.Reader, 0, 1+len(b.bootstrappedSegments))
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

	// start with the actively written to segment
	reader, err := b.segment.Reader()
	if err != nil {
		return nil, err
	}
	readers = append(readers, reader)

	// include all bootstrapped segments
	for _, seg := range b.bootstrappedSegments {
		reader, err := seg.Reader()
		if err != nil {
			return nil, err
		}
		readers = append(readers, reader)
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

type closable interface {
	Close() error
}

func (b *block) Bootstrap(
	segments []segment.Segment,
) error {
	// NB(prateek): we have to allow bootstrap to succeed even if we're Sealed
	// because of topology changes.
	b.Lock()
	defer b.Unlock()
	if b.state == blockStateClosed {
		return errUnableToBootstrapBlockClosed
	}

	b.bootstrappedSegments = append(b.bootstrappedSegments, segments...)
	return nil
}

func (b *block) Tick(c context.Cancellable) (BlockTickResult, error) {
	b.RLock()
	defer b.RUnlock()
	result := BlockTickResult{}
	if b.state == blockStateClosed {
		return result, errUnableToTickBlockClosed
	}

	// active segment
	result.NumSegments++
	result.NumDocs += b.segment.Size()

	// bootstrapped segments
	for _, seg := range b.bootstrappedSegments {
		result.NumSegments++
		result.NumDocs += seg.Size()
	}

	return result, nil
}

func (b *block) Seal() error {
	b.Lock()
	defer b.Unlock()
	if b.state != blockStateOpen {
		return fmt.Errorf(errUnableToSealBlockIllegalStateFmtString, b.state)
	}
	b.state = blockStateSealed
	return nil
}

func (b *block) IsSealed() bool {
	b.RLock()
	defer b.RUnlock()
	return b.state == blockStateSealed
}

func (b *block) Close() error {
	b.Lock()
	defer b.Unlock()
	if b.state == blockStateClosed {
		return errBlockAlreadyClosed
	}
	b.state = blockStateClosed

	var multiErr xerrors.MultiError
	multiErr = multiErr.Add(b.segment.Close())
	for _, seg := range b.bootstrappedSegments {
		multiErr = multiErr.Add(seg.Close())
	}
	b.bootstrappedSegments = nil
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
		instrument.EmitInvariantViolationAndGetLogger(b.opts.InstrumentOptions()).Errorf(err.Error())
		return err
	}
}

func (b *block) unknownWriteBatchInvariantError(err error) error {
	wrappedErr := fmt.Errorf("received non BatchPartialError from m3ninx InsertBatch [%T]", err)
	instrument.EmitInvariantViolationAndGetLogger(b.opts.InstrumentOptions()).Errorf(wrappedErr.Error())
	return wrappedErr
}
