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

	"github.com/m3db/m3ninx/doc"
	m3ninxindex "github.com/m3db/m3ninx/index"
	"github.com/m3db/m3ninx/index/segment"
	"github.com/m3db/m3ninx/index/segment/mem"
	"github.com/m3db/m3ninx/postings"
	"github.com/m3db/m3ninx/search"
	"github.com/m3db/m3ninx/search/executor"
	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
	xlog "github.com/m3db/m3x/log"
)

var (
	errUnableToWriteBlockClosed     = errors.New("unable to write, index block is closed")
	errUnableToWriteBlockSealed     = errors.New("unable to write, index block is sealed")
	errUnableToQueryBlockClosed     = errors.New("unable to query, index block is closed")
	errUnableToBootstrapBlockClosed = errors.New("unable to bootstrap, block is closed")
	errUnableToTickBlockClosed      = errors.New("unable to tick, block is closed")
	errBlockAlreadyClosed           = errors.New("unable to close, block already closed")

	errUnableToSealBlockIllegalStateFmtString  = "unable to seal, index block state: %v"
	errUnableToWriteBlockUnknownStateFmtString = "[invariant violated] unable to write, unknown index block state: %v"
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
	state blockState

	bootstrappedSegments []segment.Segment
	segment              segment.MutableSegment

	newExecutorFn newExecutorFn
	startTime     time.Time
	endTime       time.Time
	opts          Options
	logger        xlog.Logger
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
		logger:    opts.InstrumentOptions().Logger(),
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

func (b *block) WriteBatch(inserts []WriteBatchEntry) (WriteBatchResult, error) {
	b.RLock()
	defer b.RUnlock()
	if b.state != blockStateOpen {
		// NB: releasing all references to inserts
		for _, insert := range inserts {
			insert.OnIndexSeries.OnIndexFinalize()
		}
		return WriteBatchResult{
			NumError: int64(len(inserts)),
		}, writeBatchErrorInvalidState(b.state)
	}

	// FOLLOWUP(prateek): docs array pooling
	batch := m3ninxindex.Batch{
		Docs:                make([]doc.Document, 0, len(inserts)),
		AllowPartialUpdates: true,
	}
	for _, insert := range inserts {
		batch.Docs = append(batch.Docs, insert.Document)
	}

	err := b.segment.InsertBatch(batch)
	if err == nil {
		for idx, insert := range inserts {
			insert.OnIndexSeries.OnIndexSuccess(b.endTime)
			insert.OnIndexSeries.OnIndexFinalize()
			batch.Docs[idx] = doc.Document{}
		}
		return WriteBatchResult{
			NumSuccess: int64(len(inserts)),
		}, nil
	}

	partialErr, ok := err.(*m3ninxindex.BatchPartialError)
	if !ok {
		// should never happen
		b.logger.Errorf(
			"[invariant violated] received non BatchPartialError from m3ninx InsertBatch [%T]", err)
		// NB: marking all the inserts as failure, cause we don't know which ones failed
		for _, insert := range inserts {
			insert.OnIndexSeries.OnIndexFinalize()
			insert.Document = doc.Document{}
			insert.OnIndexSeries = nil
		}
		return WriteBatchResult{
			NumError: int64(len(inserts)),
		}, fmt.Errorf("unexpected error: %v", err)
	}

	// first finalize all the responses which were errors, and mark them
	// nil to indicate they're done.
	numErr := len(partialErr.Indices())
	for _, idx := range partialErr.Indices() {
		inserts[idx].OnIndexSeries.OnIndexFinalize()
		inserts[idx].OnIndexSeries = nil
		inserts[idx].Document = doc.Document{}
	}

	// mark all non-error inserts success, so we don't repeatedly index them,
	// and then finalize any held references.
	for _, insert := range inserts {
		if insert.OnIndexSeries == nil {
			continue
		}
		insert.OnIndexSeries.OnIndexSuccess(b.endTime)
		insert.OnIndexSeries.OnIndexFinalize()
		insert.OnIndexSeries = nil
		insert.Document = doc.Document{}
	}

	return WriteBatchResult{
		NumSuccess: int64(len(inserts) - numErr),
		NumError:   int64(numErr),
	}, partialErr
}

func (b *block) executorWithRLock() (search.Executor, error) {
	var (
		readers = make([]m3ninxindex.Reader, 0, 1+len(b.bootstrappedSegments))
		success = false
	)

	// cleanup in case shit breaks
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

func writeBatchErrorInvalidState(state blockState) error {
	switch state {
	case blockStateClosed:
		return errUnableToWriteBlockClosed
	case blockStateSealed:
		return errUnableToWriteBlockSealed
	default:
		return fmt.Errorf(errUnableToWriteBlockUnknownStateFmtString, state)
	}
}
