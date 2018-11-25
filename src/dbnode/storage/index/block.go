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
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/dbnode/storage/index/segments"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/m3ninx/doc"
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

	"github.com/uber-go/tally"
)

var (
	// ErrUnableToQueryBlockClosed is returned when querying closed block.
	ErrUnableToQueryBlockClosed = errors.New("unable to query, index block is closed")

	errUnableToWriteBlockClosed     = errors.New("unable to write, index block is closed")
	errUnableToWriteBlockSealed     = errors.New("unable to write, index block is sealed")
	errUnableToBootstrapBlockClosed = errors.New("unable to bootstrap, block is closed")
	errUnableToTickBlockClosed      = errors.New("unable to tick, block is closed")
	errBlockAlreadyClosed           = errors.New("unable to close, block already closed")
	errUnableReportStatsBlockClosed = errors.New("unable to report stats, block is closed")

	errUnableToSealBlockIllegalStateFmtString  = "unable to seal, index block state: %v"
	errUnableToWriteBlockUnknownStateFmtString = "unable to write, unknown index block state: %v"
)

type blockState uint

const (
	blockStateOpen blockState = iota
	blockStateSealed
	blockStateClosed

	compactDebugLogEvery = 1 // Emit debug log for every compaction
)

func (s blockState) String() string {
	switch s {
	case blockStateOpen:
		return "open"
	case blockStateSealed:
		return "sealed"
	case blockStateClosed:
		return "closed"
	}
	return "unknown"
}

type newExecutorFn func() (search.Executor, error)

type block struct {
	sync.RWMutex

	state                             blockState
	hasEvictedMutableSegmentsAnyTimes bool

	activeSegment       *mutableReadableSeg
	frozenSegments      []*readableSeg
	shardRangesSegments []blockShardRangesSegments

	newExecutorFn newExecutorFn
	startTime     time.Time
	endTime       time.Time
	blockSize     time.Duration
	opts          Options
	iopts         instrument.Options
	nsMD          namespace.Metadata
	docsPool      doc.DocumentArrayPool

	compacting         bool
	compactions        int
	compactionPlanOpts compaction.PlannerOptions
	compactor          *compaction.Compactor

	metrics blockMetrics
	logger  xlog.Logger
}

type blockMetrics struct {
	rotateActiveSegment      tally.Counter
	rotateActiveSegmentAge   tally.Timer
	rotateActiveSegmentSize  tally.Histogram
	compactionPlanRunLatency tally.Timer
	compactionTaskRunLatency tally.Timer
}

func newBlockMetrics(s tally.Scope) blockMetrics {
	s = s.SubScope("index").SubScope("block")
	return blockMetrics{
		rotateActiveSegment:    s.Counter("rotate-active-segment"),
		rotateActiveSegmentAge: s.Timer("rotate-active-segment-age"),
		rotateActiveSegmentSize: s.Histogram("rotate-active-segment-size",
			append(tally.ValueBuckets{0}, tally.MustMakeExponentialValueBuckets(100, 2, 16)...)),
		compactionPlanRunLatency: s.Timer("compaction-plan-run-latency"),
		compactionTaskRunLatency: s.Timer("compaction-task-run-latency"),
	}
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
	docsPool := opts.DocumentArrayPool()

	compactor, err := compaction.NewCompactor(docsPool,
		documentArrayPoolCapacity,
		opts.MemSegmentOptions(),
		opts.FSTSegmentOptions())
	if err != nil {
		return nil, err
	}

	blockSize := md.Options().IndexOptions().BlockSize()
	iopts := opts.InstrumentOptions()
	b := &block{
		state:     blockStateOpen,
		startTime: startTime,
		endTime:   startTime.Add(blockSize),
		blockSize: blockSize,
		opts:      opts,
		iopts:     iopts,
		nsMD:      md,
		docsPool:  docsPool,
		compactor: compactor,
		metrics:   newBlockMetrics(iopts.MetricsScope()),
		logger:    iopts.Logger(),
	}
	b.newExecutorFn = b.executorWithRLock

	if _, err := b.rotateActiveSegment(); err != nil {
		return nil, err
	}

	return b, nil
}

func (b *block) newMutableSegment() (segment.MutableSegment, error) {
	postingsOffset := postings.ID(0)
	seg, err := mem.NewSegment(postingsOffset, b.opts.MemSegmentOptions())
	if err != nil {
		return nil, err
	}

	return seg, nil
}

func (b *block) rotateActiveSegment() (*mutableReadableSeg, error) {
	// NB(r): This may be nil on the first call on initialization
	prev := b.activeSegment

	seg, err := b.newMutableSegment()
	if err != nil {
		return nil, err
	}

	b.activeSegment = newMutableReadableSeg(seg)

	if prev != nil {
		b.metrics.rotateActiveSegment.Inc(1)
		b.metrics.rotateActiveSegmentAge.Record(prev.Age())
		b.metrics.rotateActiveSegmentSize.RecordValue(float64(prev.Segment().Size()))
	}

	return prev, nil
}

func (b *block) StartTime() time.Time {
	return b.startTime
}

func (b *block) EndTime() time.Time {
	return b.endTime
}

func (b *block) maybeCompactWithLock() {
	if b.compacting || b.state != blockStateOpen {
		return
	}

	// Create a logical plan
	segs := make([]compaction.Segment, 0, 1+len(b.frozenSegments))
	if b.activeSegment != nil && b.activeSegment.Segment().Size() > 0 {
		segs = append(segs, compaction.Segment{
			Age:     b.activeSegment.Age(),
			Size:    b.activeSegment.Segment().Size(),
			Type:    segments.MutableType,
			Segment: b.activeSegment.Segment(),
		})
	}

	for _, seg := range b.frozenSegments {
		segs = append(segs, compaction.Segment{
			Age:     seg.Age(),
			Size:    seg.Segment().Size(),
			Type:    segments.FSTType,
			Segment: seg.Segment(),
		})
	}

	plan, err := compaction.NewPlan(segs, b.opts.CompactionPlannerOptions())
	if err != nil {
		instrument.EmitAndLogInvariantViolation(b.iopts, func(l xlog.Logger) {
			l.Errorf("could not create index compaction plan: %v", err)
		})
		return
	}

	if len(plan.Tasks) == 0 {
		return
	}

	var compactingActiveSegment bool
	if b.activeSegment != nil {
		for _, task := range plan.Tasks {
			for _, seg := range task.Segments {
				if seg.Segment == b.activeSegment.Segment() {
					compactingActiveSegment = true
					break
				}
			}
			if compactingActiveSegment {
				break
			}
		}
	}

	if compactingActiveSegment {
		// Rotate the current active segment so it's not written to while
		// we're compacting it
		prev, err := b.rotateActiveSegment()
		if err != nil {
			instrument.EmitAndLogInvariantViolation(b.iopts, func(l xlog.Logger) {
				l.Errorf("could not rotate active segment for compaction: %v", err)
			})
			return
		}

		b.frozenSegments = append(b.frozenSegments, newReadableSeg(prev.Segment()))
	}

	// Kick off compaction
	b.startCompactWithLock(plan)
}

func (b *block) startCompactWithLock(plan *compaction.Plan) {
	b.compacting = true
	go func() {
		b.compactWithPlan(plan)

		b.Lock()
		b.cleanupCompactWithLock()
		b.Unlock()
	}()
}

func (b *block) cleanupCompactWithLock() {
	b.compacting = false

	if b.state == blockStateOpen {
		// See if we need to trigger another compaction
		b.maybeCompactWithLock()
		return
	}

	// Check if need to close all the frozen segments due to
	// having evicted mutable segments or the block being closed.
	// NB(r): The frozen/compacted segments are derived segments of the
	// active mutable segment, if we ever evict that segment then
	// we don't need the frozen/compacted segments either and should
	// shed them from memory.
	shouldEvictCompactedSegments := b.state == blockStateClosed ||
		b.hasEvictedMutableSegmentsAnyTimes
	if !shouldEvictCompactedSegments {
		return
	}

	for _, seg := range b.frozenSegments {
		err := seg.Segment().Close()
		if err != nil {
			instrument.EmitAndLogInvariantViolation(b.iopts, func(l xlog.Logger) {
				l.Errorf("could not close frozen segment: %v", err)
			})
		}
	}
	b.frozenSegments = nil

	// Free compactor resources
	err := b.compactor.Close()
	if err != nil {
		instrument.EmitAndLogInvariantViolation(b.iopts, func(l xlog.Logger) {
			l.Errorf("error closing index block compactor: %v", err)
		})
	}
}

func (b *block) compactWithPlan(plan *compaction.Plan) {
	sw := b.metrics.compactionPlanRunLatency.Start()
	defer sw.Stop()

	n := b.compactions
	b.compactions++

	logger := b.logger.WithFields(xlog.NewField("compaction", n))
	log := n%compactDebugLogEvery == 0
	if log {
		for i, task := range plan.Tasks {
			summary := task.Summary()
			logger.WithFields(
				xlog.NewField("task", i),
				xlog.NewField("numMutable", summary.NumMutable),
				xlog.NewField("numFST", summary.NumFST),
				xlog.NewField("cumulativeMutableAge", summary.CumulativeMutableAge.String()),
				xlog.NewField("cumulativeSize", summary.CumulativeSize),
			).Debug("planned compaction task")
		}
	}

	for i, task := range plan.Tasks {
		taskLogger := logger.WithFields(xlog.NewField("task", i))
		if log {
			taskLogger.Debug("start compaction task")
		}

		err := b.compactWithTask(task)

		if log {
			taskLogger.Debug("done compaction task")
		}

		if err != nil {
			instrument.EmitAndLogInvariantViolation(b.iopts, func(l xlog.Logger) {
				l.Errorf("error compacting segments: %v", err)
			})
			return
		}
	}
}

func (b *block) compactWithTask(task compaction.Task) error {
	segments := make([]segment.Segment, 0, len(task.Segments))
	for _, seg := range task.Segments {
		segments = append(segments, seg.Segment)
	}

	sw := b.metrics.compactionTaskRunLatency.Start()
	compacted, err := b.compactor.Compact(segments)
	sw.Stop()

	if err != nil {
		return err
	}

	// Rotate out the replaced frozen segments and add the compacted one
	b.Lock()
	defer b.Unlock()

	newFrozenSegments := make([]*readableSeg, 0, len(b.frozenSegments))
	for _, frozen := range b.frozenSegments {
		keepCurr := true
		for _, seg := range segments {
			if frozen.Segment() == seg {
				// Do not keep this one, it was compacted just then
				keepCurr = false
				break
			}
		}

		if keepCurr {
			newFrozenSegments = append(newFrozenSegments, frozen)
			continue
		}

		err := frozen.Segment().Close()
		if err != nil {
			// Already compacted, not much we can do about not closing it
			instrument.EmitAndLogInvariantViolation(b.iopts, func(l xlog.Logger) {
				l.Errorf("unable to close compacted block: %v", err)
			})
		}
	}

	b.frozenSegments = append(newFrozenSegments, newReadableSeg(compacted))

	return nil
}

func (b *block) WriteBatch(inserts *WriteBatch) (WriteBatchResult, error) {
	b.Lock()
	defer func() {
		b.maybeCompactWithLock()
		b.Unlock()
	}()

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

	err := b.activeSegment.Segment().InsertBatch(m3ninxindex.Batch{
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
		reader, err := b.activeSegment.Segment().Reader()
		if err != nil {
			return nil, err
		}
		readers = append(readers, reader)
	}

	// loop over frozen segments
	for _, seg := range b.frozenSegments {
		reader, err := seg.Segment().Reader()
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
		return false, ErrUnableToQueryBlockClosed
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

	size := results.Size()
	limitedResults := false
	iterCloser := safeCloser{closable: iter}
	execCloser := safeCloser{closable: exec}

	defer func() {
		iterCloser.Close()
		execCloser.Close()
	}()

	for iter.Next() {
		if opts.LimitExceeded(size) {
			limitedResults = true
			break
		}

		d := iter.Current()
		_, size, err = results.AddDocument(d)
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

	exhaustive := !limitedResults
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
		return nil
	}

	// This is the case where the new segments can wholly replace the
	// current set of blocks since unfullfilled by the new segments is zero
	multiErr := xerrors.NewMultiError()
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
		result.NumDocs += b.activeSegment.Segment().Size()
	}

	// add frozen segments
	for _, seg := range b.frozenSegments {
		result.NumSegments++
		result.NumDocs += seg.Segment().Size()
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

	// seal active mutable segment.
	_, err := b.activeSegment.Segment().Seal()

	// all frozen segments and added mutable segments can't actually be
	// written to and they don't need to be sealed since we don't
	// flush these segments

	return err
}

func (b *block) Stats(reporter BlockStatsReporter) error {
	b.RLock()
	defer b.RUnlock()

	if b.state != blockStateOpen {
		return errUnableReportStatsBlockClosed
	}

	if b.activeSegment != nil {
		reporter.ReportSegmentStats(BlockSegmentStats{
			Type:    ActiveOpenSegment,
			Mutable: true,
			Age:     b.activeSegment.Age(),
			Size:    b.activeSegment.Segment().Size(),
		})
	}

	for _, frozen := range b.frozenSegments {
		_, mutable := frozen.Segment().(segment.MutableSegment)
		reporter.ReportSegmentStats(BlockSegmentStats{
			Type:    ActiveFrozenSegment,
			Mutable: mutable,
			Age:     frozen.Age(),
			Size:    frozen.Segment().Size(),
		})
	}

	for _, shardRangeSegments := range b.shardRangesSegments {
		for _, seg := range shardRangeSegments.segments {
			_, mutable := seg.(segment.MutableSegment)
			reporter.ReportSegmentStats(BlockSegmentStats{
				Type:    FlushedSegment,
				Mutable: mutable,
				Size:    seg.Size(),
			})
		}
	}

	return nil
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
	anyMutableSegmentNeedsEviction := b.activeSegment != nil &&
		b.activeSegment.Segment().Size() > 0

	// can early terminate if we already know we need to flush.
	if anyMutableSegmentNeedsEviction {
		return true
	}

	// check any frozen segments need to be flushed.
	for _, seg := range b.frozenSegments {
		anyMutableSegmentNeedsEviction = anyMutableSegmentNeedsEviction || seg.Segment().Size() > 0
	}

	// check boostrapped segments and to see if any of them need an eviction
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

	b.hasEvictedMutableSegmentsAnyTimes = true

	var multiErr xerrors.MultiError

	// close active segment.
	if b.activeSegment != nil {
		results.NumMutableSegments++
		results.NumDocs += b.activeSegment.Segment().Size()
		multiErr = multiErr.Add(b.activeSegment.Segment().Close())
		b.activeSegment = nil
	}

	// if not compacting, trigger a cleanup so that all frozen segments get
	// closed, otherwise after the current running compaction the frozen
	// segments will get closed
	if !b.compacting {
		b.cleanupCompactWithLock()
	}

	// close any other mutable segments that was added.
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
		multiErr = multiErr.Add(b.activeSegment.Segment().Close())
		b.activeSegment = nil
	}

	// if not compacting, trigger a cleanup so that all frozen segments get
	// closed, otherwise after the current running compaction the frozen
	// segments will get closed
	if !b.compacting {
		b.cleanupCompactWithLock()
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
