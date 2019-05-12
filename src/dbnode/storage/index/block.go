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
	"bytes"
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
	"github.com/m3db/m3/src/m3ninx/index/segment/builder"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/m3ninx/search"
	"github.com/m3db/m3/src/m3ninx/search/executor"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/resource"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	// ErrUnableToQueryBlockClosed is returned when querying closed block.
	ErrUnableToQueryBlockClosed = errors.New("unable to query, index block is closed")
	// ErrUnableReportStatsBlockClosed is returned from Stats when the block is closed.
	ErrUnableReportStatsBlockClosed = errors.New("unable to report stats, block is closed")

	errUnableToWriteBlockClosed                = errors.New("unable to write, index block is closed")
	errUnableToWriteBlockSealed                = errors.New("unable to write, index block is sealed")
	errUnableToWriteBlockConcurrent            = errors.New("unable to write, index block is being written to already")
	errUnableToBootstrapBlockClosed            = errors.New("unable to bootstrap, block is closed")
	errUnableToTickBlockClosed                 = errors.New("unable to tick, block is closed")
	errBlockAlreadyClosed                      = errors.New("unable to close, block already closed")
	errForegroundCompactorNoPlan               = errors.New("index foreground compactor failed to generate a plan")
	errForegroundCompactorBadPlanFirstTask     = errors.New("index foreground compactor generated plan without mutable segment in first task")
	errForegroundCompactorBadPlanSecondaryTask = errors.New("index foreground compactor generated plan with mutable segment a secondary task")
	errCancelledQuery                          = errors.New("query was cancelled")

	errUnableToSealBlockIllegalStateFmtString  = "unable to seal, index block state: %v"
	errUnableToWriteBlockUnknownStateFmtString = "unable to write, unknown index block state: %v"
)

type blockState uint

const (
	blockStateOpen blockState = iota
	blockStateSealed
	blockStateClosed

	defaultQueryDocsBatchSize             = 256
	defaultAggregateResultsEntryBatchSize = 256

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

// nolint: maligned
type block struct {
	sync.RWMutex

	state                             blockState
	hasEvictedMutableSegmentsAnyTimes bool

	foregroundSegments  []*readableSeg
	backgroundSegments  []*readableSeg
	shardRangesSegments []blockShardRangesSegments

	newFieldsAndTermsIteratorFn newFieldsAndTermsIteratorFn
	newExecutorFn               newExecutorFn
	blockStart                  time.Time
	blockEnd                    time.Time
	blockSize                   time.Duration
	blockOpts                   BlockOptions
	opts                        Options
	iopts                       instrument.Options
	nsMD                        namespace.Metadata

	compact blockCompact

	metrics blockMetrics
	logger  *zap.Logger
}

type blockMetrics struct {
	rotateActiveSegment                tally.Counter
	rotateActiveSegmentAge             tally.Timer
	rotateActiveSegmentSize            tally.Histogram
	foregroundCompactionPlanRunLatency tally.Timer
	foregroundCompactionTaskRunLatency tally.Timer
	backgroundCompactionPlanRunLatency tally.Timer
	backgroundCompactionTaskRunLatency tally.Timer
}

func newBlockMetrics(s tally.Scope) blockMetrics {
	s = s.SubScope("index").SubScope("block")
	foregroundScope := s.Tagged(map[string]string{"compaction-type": "foreground"})
	backgroundScope := s.Tagged(map[string]string{"compaction-type": "background"})
	return blockMetrics{
		rotateActiveSegment:    s.Counter("rotate-active-segment"),
		rotateActiveSegmentAge: s.Timer("rotate-active-segment-age"),
		rotateActiveSegmentSize: s.Histogram("rotate-active-segment-size",
			append(tally.ValueBuckets{0}, tally.MustMakeExponentialValueBuckets(100, 2, 16)...)),
		foregroundCompactionPlanRunLatency: foregroundScope.Timer("compaction-plan-run-latency"),
		foregroundCompactionTaskRunLatency: foregroundScope.Timer("compaction-task-run-latency"),
		backgroundCompactionPlanRunLatency: backgroundScope.Timer("compaction-plan-run-latency"),
		backgroundCompactionTaskRunLatency: backgroundScope.Timer("compaction-task-run-latency"),
	}
}

// blockShardsSegments is a collection of segments that has a mapping of what shards
// and time ranges they completely cover, this can only ever come from computing
// from data that has come from shards, either on an index flush or a bootstrap.
type blockShardRangesSegments struct {
	shardTimeRanges result.ShardTimeRanges
	segments        []segment.Segment
}

// BlockOptions is a set of options used when constructing an index block.
type BlockOptions struct {
	ForegroundCompactorMmapDocsData bool
	BackgroundCompactorMmapDocsData bool
}

// NewBlock returns a new Block, representing a complete reverse index for the
// duration of time specified. It is backed by one or more segments.
func NewBlock(
	blockStart time.Time,
	md namespace.Metadata,
	opts BlockOptions,
	indexOpts Options,
) (Block, error) {
	blockSize := md.Options().IndexOptions().BlockSize()
	iopts := indexOpts.InstrumentOptions()
	b := &block{
		state:      blockStateOpen,
		blockStart: blockStart,
		blockEnd:   blockStart.Add(blockSize),
		blockSize:  blockSize,
		opts:       indexOpts,
		iopts:      iopts,
		nsMD:       md,
		metrics:    newBlockMetrics(iopts.MetricsScope()),
		logger:     iopts.Logger(),
	}
	b.newFieldsAndTermsIteratorFn = newFieldsAndTermsIterator
	b.newExecutorFn = b.executorWithRLock

	return b, nil
}

func (b *block) StartTime() time.Time {
	return b.blockStart
}

func (b *block) EndTime() time.Time {
	return b.blockEnd
}

func (b *block) maybeBackgroundCompactWithLock() {
	if b.compact.compactingBackground || b.state != blockStateOpen {
		return
	}

	// Create a logical plan.
	segs := make([]compaction.Segment, 0, len(b.backgroundSegments))
	for _, seg := range b.backgroundSegments {
		segs = append(segs, compaction.Segment{
			Age:     seg.Age(),
			Size:    seg.Segment().Size(),
			Type:    segments.FSTType,
			Segment: seg.Segment(),
		})
	}

	plan, err := compaction.NewPlan(segs, b.opts.BackgroundCompactionPlannerOptions())
	if err != nil {
		instrument.EmitAndLogInvariantViolation(b.iopts, func(l *zap.Logger) {
			l.Error("index background compaction plan error", zap.Error(err))
		})
		return
	}

	if len(plan.Tasks) == 0 {
		return
	}

	// Kick off compaction.
	b.compact.compactingBackground = true
	go func() {
		b.backgroundCompactWithPlan(plan)

		b.Lock()
		b.compact.compactingBackground = false
		b.cleanupBackgroundCompactWithLock()
		b.Unlock()
	}()
}

func (b *block) shouldEvictCompactedSegmentsWithLock() bool {
	// NB(r): The frozen/compacted segments are derived segments of the
	// active mutable segment, if we ever evict that segment then
	// we don't need the frozen/compacted segments either and should
	// shed them from memory.
	return b.state == blockStateClosed ||
		b.hasEvictedMutableSegmentsAnyTimes
}

func (b *block) cleanupBackgroundCompactWithLock() {
	if b.state == blockStateOpen {
		// See if we need to trigger another compaction.
		b.maybeBackgroundCompactWithLock()
		return
	}

	// Check if need to close all the compacted segments due to
	// having evicted mutable segments or the block being closed.
	if !b.shouldEvictCompactedSegmentsWithLock() {
		return
	}

	// Evict compacted segments.
	b.closeCompactedSegments(b.backgroundSegments)
	b.backgroundSegments = nil

	// Free compactor resources.
	if b.compact.backgroundCompactor == nil {
		return
	}

	if err := b.compact.backgroundCompactor.Close(); err != nil {
		instrument.EmitAndLogInvariantViolation(b.iopts, func(l *zap.Logger) {
			l.Error("error closing index block background compactor", zap.Error(err))
		})
	}
	b.compact.backgroundCompactor = nil
}

func (b *block) closeCompactedSegments(segments []*readableSeg) {
	for _, seg := range segments {
		err := seg.Segment().Close()
		if err != nil {
			instrument.EmitAndLogInvariantViolation(b.iopts, func(l *zap.Logger) {
				l.Error("could not close compacted segment", zap.Error(err))
			})
		}
	}
}

func (b *block) backgroundCompactWithPlan(plan *compaction.Plan) {
	sw := b.metrics.backgroundCompactionPlanRunLatency.Start()
	defer sw.Stop()

	n := b.compact.numBackground
	b.compact.numBackground++

	logger := b.logger.With(
		zap.Time("block", b.blockStart),
		zap.Int("numBackgroundCompaction", n),
	)
	log := n%compactDebugLogEvery == 0
	if log {
		for i, task := range plan.Tasks {
			summary := task.Summary()
			logger.Debug("planned background compaction task",
				zap.Int("task", i),
				zap.Int("numMutable", summary.NumMutable),
				zap.Int("numFST", summary.NumFST),
				zap.String("cumulativeMutableAge", summary.CumulativeMutableAge.String()),
				zap.Int64("cumulativeSize", summary.CumulativeSize),
			)
		}
	}

	for i, task := range plan.Tasks {
		err := b.backgroundCompactWithTask(task, log,
			logger.With(zap.Int("task", i)))
		if err != nil {
			instrument.EmitAndLogInvariantViolation(b.iopts, func(l *zap.Logger) {
				l.Error("error compacting segments", zap.Error(err))
			})
			return
		}
	}
}

func (b *block) backgroundCompactWithTask(
	task compaction.Task,
	log bool,
	logger *zap.Logger,
) error {
	if log {
		logger.Debug("start compaction task")
	}

	segments := make([]segment.Segment, 0, len(task.Segments))
	for _, seg := range task.Segments {
		segments = append(segments, seg.Segment)
	}

	start := time.Now()
	compacted, err := b.compact.backgroundCompactor.Compact(segments)
	took := time.Since(start)
	b.metrics.backgroundCompactionTaskRunLatency.Record(took)

	if log {
		logger.Debug("done compaction task", zap.Duration("took", took))
	}

	if err != nil {
		return err
	}

	// Rotate out the replaced frozen segments and add the compacted one.
	b.Lock()
	defer b.Unlock()

	result := b.addCompactedSegmentFromSegments(b.backgroundSegments,
		segments, compacted)
	b.backgroundSegments = result

	return nil
}

func (b *block) addCompactedSegmentFromSegments(
	current []*readableSeg,
	segmentsJustCompacted []segment.Segment,
	compacted segment.Segment,
) []*readableSeg {
	result := make([]*readableSeg, 0, len(current))
	for _, existing := range current {
		keepCurr := true
		for _, seg := range segmentsJustCompacted {
			if existing.Segment() == seg {
				// Do not keep this one, it was compacted just then.
				keepCurr = false
				break
			}
		}

		if keepCurr {
			result = append(result, existing)
			continue
		}

		err := existing.Segment().Close()
		if err != nil {
			// Already compacted, not much we can do about not closing it.
			instrument.EmitAndLogInvariantViolation(b.iopts, func(l *zap.Logger) {
				l.Error("unable to close compacted block", zap.Error(err))
			})
		}
	}

	// Return all the ones we kept plus the new compacted segment
	return append(result, newReadableSeg(compacted, b.opts))
}

func (b *block) WriteBatch(inserts *WriteBatch) (WriteBatchResult, error) {
	b.Lock()
	if b.state != blockStateOpen {
		b.Unlock()
		return b.writeBatchResult(inserts, b.writeBatchErrorInvalidState(b.state))
	}
	if b.compact.compactingForeground {
		b.Unlock()
		return b.writeBatchResult(inserts, errUnableToWriteBlockConcurrent)
	}
	// Lazily allocate the segment builder and compactors
	err := b.compact.allocLazyBuilderAndCompactors(b.blockOpts, b.opts)
	if err != nil {
		b.Unlock()
		return b.writeBatchResult(inserts, err)
	}

	b.compact.compactingForeground = true
	builder := b.compact.segmentBuilder
	b.Unlock()

	defer func() {
		b.Lock()
		b.compact.compactingForeground = false
		b.cleanupForegroundCompactWithLock()
		b.Unlock()
	}()

	builder.Reset(0)
	insertResultErr := builder.InsertBatch(m3ninxindex.Batch{
		Docs:                inserts.PendingDocs(),
		AllowPartialUpdates: true,
	})
	if len(builder.Docs()) == 0 {
		// No inserts, no need to compact.
		return b.writeBatchResult(inserts, insertResultErr)
	}

	// We inserted some documents, need to compact immediately into a
	// foreground segment from the segment builder before we can serve reads
	// from an FST segment.
	err = b.foregroundCompactWithBuilder(builder)
	if err != nil {
		return b.writeBatchResult(inserts, err)
	}

	// Return result from the original insertion since compaction was successful.
	return b.writeBatchResult(inserts, insertResultErr)
}

func (b *block) writeBatchResult(
	inserts *WriteBatch,
	err error,
) (WriteBatchResult, error) {
	if err == nil {
		inserts.MarkUnmarkedEntriesSuccess()
		return WriteBatchResult{
			NumSuccess: int64(inserts.Len()),
		}, nil
	}

	partialErr, ok := err.(*m3ninxindex.BatchPartialError)
	if !ok {
		// NB: marking all the inserts as failure, cause we don't know which ones failed.
		inserts.MarkUnmarkedEntriesError(err)
		return WriteBatchResult{NumError: int64(inserts.Len())}, err
	}

	numErr := len(partialErr.Errs())
	for _, err := range partialErr.Errs() {
		// Avoid marking these as success.
		inserts.MarkUnmarkedEntryError(err.Err, err.Idx)
	}

	// Mark all non-error inserts success, so we don't repeatedly index them.
	inserts.MarkUnmarkedEntriesSuccess()
	return WriteBatchResult{
		NumSuccess: int64(inserts.Len() - numErr),
		NumError:   int64(numErr),
	}, partialErr
}

func (b *block) foregroundCompactWithBuilder(builder segment.DocumentsBuilder) error {
	// We inserted some documents, need to compact immediately into a
	// foreground segment.
	b.Lock()
	foregroundSegments := b.foregroundSegments
	b.Unlock()

	segs := make([]compaction.Segment, 0, len(foregroundSegments)+1)
	segs = append(segs, compaction.Segment{
		Age:     0,
		Size:    int64(len(builder.Docs())),
		Type:    segments.MutableType,
		Builder: builder,
	})
	for _, seg := range foregroundSegments {
		segs = append(segs, compaction.Segment{
			Age:     seg.Age(),
			Size:    seg.Segment().Size(),
			Type:    segments.FSTType,
			Segment: seg.Segment(),
		})
	}

	plan, err := compaction.NewPlan(segs, b.opts.ForegroundCompactionPlannerOptions())
	if err != nil {
		return err
	}

	// Check plan
	if len(plan.Tasks) == 0 {
		// Should always generate a task when a mutable builder is passed to planner
		return errForegroundCompactorNoPlan
	}
	if taskNumBuilders(plan.Tasks[0]) != 1 {
		// First task of plan must include the builder, so we can avoid resetting it
		// for the first task, but then safely reset it in consequent tasks
		return errForegroundCompactorBadPlanFirstTask
	}

	// Move any unused segments to the background.
	b.Lock()
	b.maybeMoveForegroundSegmentsToBackgroundWithLock(plan.UnusedSegments)
	b.Unlock()

	n := b.compact.numForeground
	b.compact.numForeground++

	logger := b.logger.With(
		zap.Time("block", b.blockStart),
		zap.Int("numForegroundCompaction", n),
	)
	log := n%compactDebugLogEvery == 0
	if log {
		for i, task := range plan.Tasks {
			summary := task.Summary()
			logger.Debug("planned foreground compaction task",
				zap.Int("task", i),
				zap.Int("numMutable", summary.NumMutable),
				zap.Int("numFST", summary.NumFST),
				zap.Duration("cumulativeMutableAge", summary.CumulativeMutableAge),
				zap.Int64("cumulativeSize", summary.CumulativeSize),
			)
		}
	}

	// Run the plan.
	sw := b.metrics.foregroundCompactionPlanRunLatency.Start()
	defer sw.Stop()

	// Run the first task, without resetting the builder.
	if err := b.foregroundCompactWithTask(
		builder, plan.Tasks[0],
		log, logger.With(zap.Int("task", 0)),
	); err != nil {
		return err
	}

	// Now run each consequent task, resetting the builder each time since
	// the results from the builder have already been compacted in the first
	// task.
	for i := 1; i < len(plan.Tasks); i++ {
		task := plan.Tasks[i]
		if taskNumBuilders(task) > 0 {
			// Only the first task should compact the builder
			return errForegroundCompactorBadPlanSecondaryTask
		}
		// Now use the builder after resetting it.
		builder.Reset(0)
		if err := b.foregroundCompactWithTask(
			builder, task,
			log, logger.With(zap.Int("task", i)),
		); err != nil {
			return err
		}
	}

	return nil
}

func (b *block) maybeMoveForegroundSegmentsToBackgroundWithLock(
	segments []compaction.Segment,
) {
	if len(segments) == 0 {
		return
	}
	if b.compact.backgroundCompactor == nil {
		// No longer performing background compaction due to evict/close.
		return
	}

	b.logger.Debug("moving segments from foreground to background",
		zap.Int("numSegments", len(segments)))

	// If background compaction is still active, then we move any unused
	// foreground segments into the background so that they might be
	// compacted by the background compactor at some point.
	i := 0
	for _, currForeground := range b.foregroundSegments {
		movedToBackground := false
		for _, seg := range segments {
			if currForeground.Segment() == seg.Segment {
				b.backgroundSegments = append(b.backgroundSegments, currForeground)
				movedToBackground = true
				break
			}
		}
		if movedToBackground {
			continue // No need to keep this segment, we moved it.
		}

		b.foregroundSegments[i] = currForeground
		i++
	}

	b.foregroundSegments = b.foregroundSegments[:i]

	// Potentially kick off a background compaction.
	b.maybeBackgroundCompactWithLock()
}

func (b *block) foregroundCompactWithTask(
	builder segment.DocumentsBuilder,
	task compaction.Task,
	log bool,
	logger *zap.Logger,
) error {
	if log {
		logger.Debug("start compaction task")
	}

	segments := make([]segment.Segment, 0, len(task.Segments))
	for _, seg := range task.Segments {
		if seg.Segment == nil {
			continue // This means the builder is being used.
		}
		segments = append(segments, seg.Segment)
	}

	start := time.Now()
	compacted, err := b.compact.foregroundCompactor.CompactUsingBuilder(builder, segments)
	took := time.Since(start)
	b.metrics.foregroundCompactionTaskRunLatency.Record(took)

	if log {
		logger.Debug("done compaction task", zap.Duration("took", took))
	}

	if err != nil {
		return err
	}

	// Rotate in the ones we just compacted.
	b.Lock()
	defer b.Unlock()

	result := b.addCompactedSegmentFromSegments(b.foregroundSegments,
		segments, compacted)
	b.foregroundSegments = result

	return nil
}

func (b *block) cleanupForegroundCompactWithLock() {
	// Check if we need to close all the compacted segments due to
	// having evicted mutable segments or the block being closed.
	if !b.shouldEvictCompactedSegmentsWithLock() {
		return
	}

	// Evict compacted segments.
	b.closeCompactedSegments(b.foregroundSegments)
	b.foregroundSegments = nil

	// Free compactor resources.
	if b.compact.foregroundCompactor == nil {
		return
	}

	if err := b.compact.foregroundCompactor.Close(); err != nil {
		instrument.EmitAndLogInvariantViolation(b.iopts, func(l *zap.Logger) {
			l.Error("error closing index block foreground compactor", zap.Error(err))
		})
	}

	b.compact.foregroundCompactor = nil
	b.compact.segmentBuilder = nil
}

func (b *block) executorWithRLock() (search.Executor, error) {
	expectedReaders := len(b.foregroundSegments) + len(b.backgroundSegments)
	for _, group := range b.shardRangesSegments {
		expectedReaders += len(group.segments)
	}

	var (
		readers = make([]m3ninxindex.Reader, 0, expectedReaders)
		success = false
	)
	defer func() {
		// Cleanup in case any of the readers below fail.
		if !success {
			for _, reader := range readers {
				reader.Close()
			}
		}
	}()

	// Add foreground and background segments.
	var foregroundErr, backgroundErr error
	readers, foregroundErr = addReadersFromReadableSegments(readers,
		b.foregroundSegments)
	readers, backgroundErr = addReadersFromReadableSegments(readers,
		b.backgroundSegments)
	if err := xerrors.FirstError(foregroundErr, backgroundErr); err != nil {
		return nil, err
	}

	// Loop over the segments associated to shard time ranges.
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

func (b *block) segmentsWithRLock() []segment.Segment {
	numSegments := len(b.foregroundSegments) + len(b.backgroundSegments)
	for _, group := range b.shardRangesSegments {
		numSegments += len(group.segments)
	}

	segments := make([]segment.Segment, 0, numSegments)
	// Add foreground & background segments.
	for _, seg := range b.foregroundSegments {
		segments = append(segments, seg.Segment())
	}
	for _, seg := range b.backgroundSegments {
		segments = append(segments, seg.Segment())
	}

	// Loop over the segments associated to shard time ranges.
	for _, group := range b.shardRangesSegments {
		for _, seg := range group.segments {
			segments = append(segments, seg)
		}
	}

	return segments
}

// Query acquires a read lock on the block so that the segments
// are guaranteed to not be freed/released while accumulating results.
// This allows references to the mmap'd segment data to be accumulated
// and then copied into the results before this method returns (it is not
// safe to return docs directly from the segments from this method, the
// results datastructure is used to copy it every time documents are added
// to the results datastructure).
func (b *block) Query(
	cancellable *resource.CancellableLifetime,
	query Query,
	opts QueryOptions,
	results BaseResults,
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
	iter, err := exec.Execute(query.Query.SearchQuery())
	if err != nil {
		exec.Close()
		return false, err
	}

	var (
		iterCloser = safeCloser{closable: iter}
		execCloser = safeCloser{closable: exec}
		size       = results.Size()
		docsPool   = b.opts.DocumentArrayPool()
		batch      = docsPool.Get()
		batchSize  = cap(batch)
	)
	if batchSize == 0 {
		batchSize = defaultQueryDocsBatchSize
	}

	defer func() {
		iterCloser.Close()
		execCloser.Close()
		docsPool.Put(batch)
	}()

	for iter.Next() {
		if opts.LimitExceeded(size) {
			break
		}

		batch = append(batch, iter.Current())
		if len(batch) < batchSize {
			continue
		}

		batch, size, err = b.addQueryResults(cancellable, results, batch)
		if err != nil {
			return false, err
		}
	}

	// Add last batch to results if remaining.
	if len(batch) > 0 {
		batch, size, err = b.addQueryResults(cancellable, results, batch)
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

	exhaustive := !opts.LimitExceeded(size)
	return exhaustive, nil
}

func (b *block) addQueryResults(
	cancellable *resource.CancellableLifetime,
	results BaseResults,
	batch []doc.Document,
) ([]doc.Document, int, error) {
	// checkout the lifetime of the query before adding results.
	queryValid := cancellable.TryCheckout()
	if !queryValid {
		// query not valid any longer, do not add results and return early.
		return batch, 0, errCancelledQuery
	}

	// try to add the docs to the resource.
	size, err := results.AddDocuments(batch)

	// immediately release the checkout on the lifetime of query.
	cancellable.ReleaseCheckout()

	// reset batch.
	var emptyDoc doc.Document
	for i := range batch {
		batch[i] = emptyDoc
	}
	batch = batch[:0]

	// return results.
	return batch, size, err
}

// Aggregate acquires a read lock on the block so that the segments
// are guaranteed to not be freed/released while accumulating results.
// NB: Aggregate is an optimization of the general aggregate Query approach
// for the case when we can skip going to raw documents, and instead rely on
// pre-aggregated results via the FST underlying the index.
func (b *block) Aggregate(
	cancellable *resource.CancellableLifetime,
	opts QueryOptions,
	results AggregateResults,
) (bool, error) {
	b.RLock()
	defer b.RUnlock()

	if b.state == blockStateClosed {
		return false, ErrUnableToQueryBlockClosed
	}

	aggOpts := results.AggregateResultsOptions()
	iterateTerms := aggOpts.Type == AggregateTagNamesAndValues
	iterateOpts := fieldsAndTermsIteratorOpts{
		iterateTerms: iterateTerms,
		allowFn: func(field []byte) bool {
			// skip any field names that we shouldn't allow.
			if bytes.Equal(field, doc.IDReservedFieldName) {
				return false
			}
			return aggOpts.FieldFilter.Allow(field)
		},
		fieldIterFn: func(s segment.Segment) (segment.FieldsIterator, error) {
			// NB(prateek): we default to using the regular (FST) fields iterator
			// unless we have a predefined list of fields we know we need to restrict
			// our search to, in which case we iterate that list and check if known values
			// in the FST to restrict our search. This is going to be significantly faster
			// while len(FieldsFilter) < 5-10 elements;
			// but there will exist a ratio between the len(FieldFilter) v size(FST) after which
			// iterating the entire FST is faster.
			// Here, we chose to avoid factoring that in to our choice because almost all input
			// to this function is expected to have (FieldsFilter) pretty small. If that changes
			// in the future, we can revisit this.
			if len(aggOpts.FieldFilter) == 0 {
				return s.FieldsIterable().Fields()
			}
			return newFilterFieldsIterator(s, aggOpts.FieldFilter)
		},
	}

	iter, err := b.newFieldsAndTermsIteratorFn(nil, iterateOpts)
	if err != nil {
		return false, err
	}

	var (
		size       = results.Size()
		batch      = b.opts.AggregateResultsEntryArrayPool().Get()
		batchSize  = cap(batch)
		iterClosed = false // tracking whether we need to free the iterator at the end.
	)
	if batchSize == 0 {
		batchSize = defaultAggregateResultsEntryBatchSize
	}

	// cleanup at the end
	defer func() {
		b.opts.AggregateResultsEntryArrayPool().Put(batch)
		if !iterClosed {
			iter.Close()
		}
	}()

	segs := b.segmentsWithRLock()
	for _, s := range segs {
		if opts.LimitExceeded(size) {
			break
		}

		err = iter.Reset(s, iterateOpts)
		if err != nil {
			return false, err
		}
		iterClosed = false // only once the iterator has been successfully Reset().

		for iter.Next() {
			if opts.LimitExceeded(size) {
				break
			}

			field, term := iter.Current()
			batch = b.appendFieldAndTermToBatch(batch, field, term, iterateTerms)
			if len(batch) < batchSize {
				continue
			}

			batch, size, err = b.addAggregateResults(cancellable, results, batch)
			if err != nil {
				return false, err
			}
		}

		if err := iter.Err(); err != nil {
			return false, err
		}

		iterClosed = true
		if err := iter.Close(); err != nil {
			return false, err
		}
	}

	// Add last batch to results if remaining.
	if len(batch) > 0 {
		batch, size, err = b.addAggregateResults(cancellable, results, batch)
		if err != nil {
			return false, err
		}
	}

	exhaustive := !opts.LimitExceeded(size)
	return exhaustive, nil
}

func (b *block) appendFieldAndTermToBatch(
	batch []AggregateResultsEntry,
	field, term []byte,
	includeTerms bool,
) []AggregateResultsEntry {
	// NB(prateek): we make a copy of the (field, term) entries returned
	// by the iterator during traversal, because the []byte are only valid per entry during
	// the traversal (i.e. calling Next() invalidates the []byte). We choose to do this
	// instead of checking if the entry is required (duplicates may exist in the results map
	// already), as it reduces contention on the map itself. Further, the ownership of these
	// idents is transferred to the results map, which either hangs on to them (if they are new),
	// or finalizes them if they are duplicates.
	var (
		entry            AggregateResultsEntry
		lastField        []byte
		lastFieldIsValid bool
		reuseLastEntry   bool
	)
	// we are iterating multiple segments so we may receive duplicates (same field/term), but
	// as we are iterating one segment at a time, and because the underlying index structures
	// are FSTs, we rely on the fact that iterator traversal is in order to avoid creating duplicate
	// entries for the same fields, by checking the last batch entry to see if the bytes are
	// the same.
	// It's easier to consider an example, say we have a segment with fields/terms:
	// (f1, t1), (f1, t2), ..., (fn, t1), ..., (fn, tn)
	// as we iterate in order, we receive (f1, t1) and then (f1, t2) we can avoid the repeated f1
	// allocation if the previous entry has the same value.
	// NB: this isn't strictly true because when we switch iterating between segments,
	// the fields/terms switch in an order which doesn't have to be strictly lexicographic. In that
	// instance however, the only downside is we would be allocating more. i.e. this is just an
	// optimisation, it doesn't affect correctness.
	if len(batch) > 0 {
		lastFieldIsValid = true
		lastField = batch[len(batch)-1].Field.Bytes()
	}
	if lastFieldIsValid && bytes.Equal(lastField, field) {
		reuseLastEntry = true
		entry = batch[len(batch)-1] // avoid alloc cause we already have the field
	} else {
		entry.Field = b.pooledID(field) // allocate id because this is the first time we've seen it
	}

	if includeTerms {
		// terms are always new (as far we know without checking the map for duplicates), so we allocate
		entry.Terms = append(entry.Terms, b.pooledID(term))
	}

	if reuseLastEntry {
		batch[len(batch)-1] = entry
	} else {
		batch = append(batch, entry)
	}
	return batch
}

func (b *block) pooledID(id []byte) ident.ID {
	data := b.opts.CheckedBytesPool().Get(len(id))
	data.IncRef()
	data.AppendAll(id)
	data.DecRef()
	return b.opts.IdentifierPool().BinaryID(data)
}

func (b *block) addAggregateResults(
	cancellable *resource.CancellableLifetime,
	results AggregateResults,
	batch []AggregateResultsEntry,
) ([]AggregateResultsEntry, int, error) {
	// checkout the lifetime of the query before adding results.
	queryValid := cancellable.TryCheckout()
	if !queryValid {
		// query not valid any longer, do not add results and return early.
		return batch, 0, errCancelledQuery
	}

	// try to add the docs to the resource.
	size := results.AddFields(batch)

	// immediately release the checkout on the lifetime of query.
	cancellable.ReleaseCheckout()

	// reset batch.
	var emptyField AggregateResultsEntry
	for i := range batch {
		batch[i] = emptyField
	}
	batch = batch[:0]

	// return results.
	return batch, size, nil
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
	if min.Before(b.blockStart) || max.After(b.blockEnd) {
		blockRange := xtime.Range{Start: b.blockStart, End: b.blockEnd}
		return fmt.Errorf("fulfilled range %s is outside of index block range: %s",
			results.Fulfilled().SummaryString(), blockRange.String())
	}

	var (
		plCache         = b.opts.PostingsListCache()
		readThroughOpts = b.opts.ReadThroughSegmentOptions()
		segments        = results.Segments()
	)
	readThroughSegments := make([]segment.Segment, 0, len(segments))
	for _, seg := range segments {
		readThroughSeg := seg
		if _, ok := seg.(segment.MutableSegment); !ok {
			// only wrap the immutable segments with a read through cache.
			readThroughSeg = NewReadThroughSegment(seg, plCache, readThroughOpts)
		}
		readThroughSegments = append(readThroughSegments, readThroughSeg)
	}

	entry := blockShardRangesSegments{
		shardTimeRanges: results.Fulfilled(),
		segments:        readThroughSegments,
	}

	// first see if this block can cover all our current blocks covering shard
	// time ranges.
	currFulfilled := make(result.ShardTimeRanges)
	for _, existing := range b.shardRangesSegments {
		currFulfilled.AddRanges(existing.shardTimeRanges)
	}

	unfulfilledBySegments := currFulfilled.Copy()
	unfulfilledBySegments.Subtract(results.Fulfilled())
	if !unfulfilledBySegments.IsEmpty() {
		// This is the case where it cannot wholly replace the current set of blocks
		// so simply append the segments in this case.
		b.shardRangesSegments = append(b.shardRangesSegments, entry)
		return nil
	}

	// This is the case where the new segments can wholly replace the
	// current set of blocks since unfullfilled by the new segments is zero.
	multiErr := xerrors.NewMultiError()
	for i, group := range b.shardRangesSegments {
		for _, seg := range group.segments {
			// Make sure to close the existing segments.
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

	// Add foreground/background segments.
	for _, seg := range b.foregroundSegments {
		result.NumSegments++
		result.NumDocs += seg.Segment().Size()
	}
	for _, seg := range b.backgroundSegments {
		result.NumSegments++
		result.NumDocs += seg.Segment().Size()
	}

	// Any segments covering persisted shard ranges.
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

	// Ensure we only Seal if we're marked Open.
	if b.state != blockStateOpen {
		return fmt.Errorf(errUnableToSealBlockIllegalStateFmtString, b.state)
	}
	b.state = blockStateSealed

	// All foreground/background segments and added mutable segments can't
	// be written to and they don't need to be sealed since we don't flush
	// these segments.
	return nil
}

func (b *block) Stats(reporter BlockStatsReporter) error {
	b.RLock()
	defer b.RUnlock()

	if b.state != blockStateOpen {
		return ErrUnableReportStatsBlockClosed
	}

	for _, seg := range b.foregroundSegments {
		_, mutable := seg.Segment().(segment.MutableSegment)
		reporter.ReportSegmentStats(BlockSegmentStats{
			Type:    ActiveForegroundSegment,
			Mutable: mutable,
			Age:     seg.Age(),
			Size:    seg.Segment().Size(),
		})
	}
	for _, seg := range b.backgroundSegments {
		_, mutable := seg.Segment().(segment.MutableSegment)
		reporter.ReportSegmentStats(BlockSegmentStats{
			Type:    ActiveBackgroundSegment,
			Mutable: mutable,
			Age:     seg.Age(),
			Size:    seg.Segment().Size(),
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

	// Check any foreground/background segments that can be evicted after a flush.
	var anyMutableSegmentNeedsEviction bool
	for _, seg := range b.foregroundSegments {
		anyMutableSegmentNeedsEviction = anyMutableSegmentNeedsEviction || seg.Segment().Size() > 0
	}
	for _, seg := range b.backgroundSegments {
		anyMutableSegmentNeedsEviction = anyMutableSegmentNeedsEviction || seg.Segment().Size() > 0
	}

	// Check boostrapped segments and to see if any of them need an eviction.
	for _, shardRangeSegments := range b.shardRangesSegments {
		for _, seg := range shardRangeSegments.segments {
			if mutableSeg, ok := seg.(segment.MutableSegment); ok {
				anyMutableSegmentNeedsEviction = anyMutableSegmentNeedsEviction || mutableSeg.Size() > 0
			}
		}
	}

	return anyMutableSegmentNeedsEviction
}

func (b *block) EvictMutableSegments() error {
	b.Lock()
	defer b.Unlock()
	if b.state != blockStateSealed {
		return fmt.Errorf("unable to evict mutable segments, block must be sealed, found: %v", b.state)
	}

	b.hasEvictedMutableSegmentsAnyTimes = true

	// If not compacting, trigger a cleanup so that all frozen segments get
	// closed, otherwise after the current running compaction the compacted
	// segments will get closed.
	if !b.compact.compactingForeground {
		b.cleanupForegroundCompactWithLock()
	}
	if !b.compact.compactingBackground {
		b.cleanupBackgroundCompactWithLock()
	}

	// Close any other mutable segments that was added.
	multiErr := xerrors.NewMultiError()
	for idx := range b.shardRangesSegments {
		segments := make([]segment.Segment, 0, len(b.shardRangesSegments[idx].segments))
		for _, seg := range b.shardRangesSegments[idx].segments {
			mutableSeg, ok := seg.(segment.MutableSegment)
			if !ok {
				segments = append(segments, seg)
				continue
			}
			multiErr = multiErr.Add(mutableSeg.Close())
		}
		b.shardRangesSegments[idx].segments = segments
	}

	return multiErr.FinalError()
}

func (b *block) Close() error {
	b.Lock()
	defer b.Unlock()
	if b.state == blockStateClosed {
		return errBlockAlreadyClosed
	}
	b.state = blockStateClosed

	// If not compacting, trigger a cleanup so that all frozen segments get
	// closed, otherwise after the current running compaction the compacted
	// segments will get closed.
	if !b.compact.compactingForeground {
		b.cleanupForegroundCompactWithLock()
	}
	if !b.compact.compactingBackground {
		b.cleanupBackgroundCompactWithLock()
	}

	// Close any other added segments too.
	var multiErr xerrors.MultiError
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
		instrument.EmitAndLogInvariantViolation(b.opts.InstrumentOptions(), func(l *zap.Logger) {
			l.Error(err.Error())
		})
		return err
	}
}

// blockCompact has several lazily allocated compaction components.
type blockCompact struct {
	segmentBuilder       segment.DocumentsBuilder
	foregroundCompactor  *compaction.Compactor
	backgroundCompactor  *compaction.Compactor
	compactingForeground bool
	compactingBackground bool
	numForeground        int
	numBackground        int
}

func (b *blockCompact) allocLazyBuilderAndCompactors(
	blockOpts BlockOptions,
	opts Options,
) error {
	var (
		err      error
		docsPool = opts.DocumentArrayPool()
	)
	if b.segmentBuilder == nil {
		b.segmentBuilder, err = builder.NewBuilderFromDocuments(opts.SegmentBuilderOptions())
		if err != nil {
			return err
		}
	}

	if b.foregroundCompactor == nil {
		b.foregroundCompactor, err = compaction.NewCompactor(docsPool,
			documentArrayPoolCapacity,
			opts.SegmentBuilderOptions(),
			opts.FSTSegmentOptions(),
			compaction.CompactorOptions{
				FSTWriterOptions: &fst.WriterOptions{
					// DisableRegistry is set to true to trade a larger FST size
					// for a faster FST compaction since we want to reduce the end
					// to end latency for time to first index a metric.
					DisableRegistry: true,
				},
				MmapDocsData: blockOpts.ForegroundCompactorMmapDocsData,
			})
		if err != nil {
			return err
		}
	}

	if b.backgroundCompactor == nil {
		b.backgroundCompactor, err = compaction.NewCompactor(docsPool,
			documentArrayPoolCapacity,
			opts.SegmentBuilderOptions(),
			opts.FSTSegmentOptions(),
			compaction.CompactorOptions{
				MmapDocsData: blockOpts.BackgroundCompactorMmapDocsData,
			})
		if err != nil {
			return err
		}
	}

	return nil
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

func taskNumBuilders(task compaction.Task) int {
	builders := 0
	for _, seg := range task.Segments {
		if seg.Builder != nil {
			builders++
			continue
		}
	}
	return builders
}

func addReadersFromReadableSegments(
	readers []m3ninxindex.Reader,
	segments []*readableSeg,
) ([]m3ninxindex.Reader, error) {
	for _, seg := range segments {
		reader, err := seg.Segment().Reader()
		if err != nil {
			return nil, err
		}
		readers = append(readers, reader)
	}
	return readers, nil
}
