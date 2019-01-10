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
	"github.com/m3db/m3/src/m3ninx/index/segment/builder"
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
	// ErrUnableReportStatsBlockClosed is returned from Stats when the block is closed.
	ErrUnableReportStatsBlockClosed = errors.New("unable to report stats, block is closed")

	errUnableToWriteBlockClosed                = errors.New("unable to write, index block is closed")
	errUnableToWriteBlockSealed                = errors.New("unable to write, index block is sealed")
	errUnableToWriteBlockConcurrent            = errors.New("unable to write, index block is being written to already")
	errUnableToBootstrapBlockClosed            = errors.New("unable to bootstrap, block is closed")
	errUnableToTickBlockClosed                 = errors.New("unable to tick, block is closed")
	errBlockAlreadyClosed                      = errors.New("unable to close, block already closed")
	errForegroundCompactorNoBuilderDocs        = errors.New("index foreground compactor has no builder documents to compact")
	errForegroundCompactorNoPlan               = errors.New("index foreground compactor failed to generate a plan")
	errForegroundCompactorBadPlanFirstTask     = errors.New("index foreground compactor generated plan without mutable segment in first task")
	errForegroundCompactorBadPlanSecondaryTask = errors.New("index foreground compactor generated plan with mutable segment a secondary task")

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

	segmentBuilder      segment.DocumentsBuilder
	foregroundSegments  []*readableSeg
	backgroundSegments  []*readableSeg
	shardRangesSegments []blockShardRangesSegments

	newExecutorFn newExecutorFn
	startTime     time.Time
	endTime       time.Time
	blockSize     time.Duration
	opts          Options
	iopts         instrument.Options
	nsMD          namespace.Metadata
	docsPool      doc.DocumentArrayPool

	compactingForeground  bool
	compactingBackground  bool
	compactionsForeground int
	compactionsBackground int
	foregroundCompactor   *compaction.Compactor
	backgroundCompactor   *compaction.Compactor

	metrics blockMetrics
	logger  xlog.Logger
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

// NewBlock returns a new Block, representing a complete reverse index for the
// duration of time specified. It is backed by one or more segments.
func NewBlock(
	startTime time.Time,
	md namespace.Metadata,
	opts Options,
) (Block, error) {
	docsPool := opts.DocumentArrayPool()

	foregroundCompactor, err := compaction.NewCompactor(docsPool,
		documentArrayPoolCapacity,
		opts.SegmentBuilderOptions(),
		opts.FSTSegmentOptions())
	if err != nil {
		return nil, err
	}

	backgroundCompactor, err := compaction.NewCompactor(docsPool,
		documentArrayPoolCapacity,
		opts.SegmentBuilderOptions(),
		opts.FSTSegmentOptions())
	if err != nil {
		return nil, err
	}

	segmentBuilder, err := builder.NewBuilderFromDocuments(opts.SegmentBuilderOptions())
	if err != nil {
		return nil, err
	}

	blockSize := md.Options().IndexOptions().BlockSize()
	iopts := opts.InstrumentOptions()
	b := &block{
		state:               blockStateOpen,
		segmentBuilder:      segmentBuilder,
		startTime:           startTime,
		endTime:             startTime.Add(blockSize),
		blockSize:           blockSize,
		opts:                opts,
		iopts:               iopts,
		nsMD:                md,
		docsPool:            docsPool,
		foregroundCompactor: foregroundCompactor,
		backgroundCompactor: backgroundCompactor,
		metrics:             newBlockMetrics(iopts.MetricsScope()),
		logger:              iopts.Logger(),
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

func (b *block) maybeBackgroundCompactWithLock() {
	if b.compactingBackground || b.state != blockStateOpen {
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
		instrument.EmitAndLogInvariantViolation(b.iopts, func(l xlog.Logger) {
			l.Errorf("index background compaction plan error: %v", err)
		})
		return
	}

	if len(plan.Tasks) == 0 {
		return
	}

	// Kick off compaction.
	b.compactingBackground = true
	go func() {
		b.backgroundCompactWithPlan(plan)

		b.Lock()
		b.compactingBackground = false
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
	if b.backgroundCompactor == nil {
		return
	}

	if err := b.backgroundCompactor.Close(); err != nil {
		instrument.EmitAndLogInvariantViolation(b.iopts, func(l xlog.Logger) {
			l.Errorf("error closing index block background compactor: %v", err)
		})
	}
	b.backgroundCompactor = nil
}

func (b *block) closeCompactedSegments(segments []*readableSeg) {
	for _, seg := range segments {
		err := seg.Segment().Close()
		if err != nil {
			instrument.EmitAndLogInvariantViolation(b.iopts, func(l xlog.Logger) {
				l.Errorf("could not close compacted segment: %v", err)
			})
		}
	}
}

func (b *block) backgroundCompactWithPlan(plan *compaction.Plan) {
	sw := b.metrics.backgroundCompactionPlanRunLatency.Start()
	defer sw.Stop()

	n := b.compactionsBackground
	b.compactionsBackground++

	logger := b.logger.WithFields(
		xlog.NewField("block", b.startTime.String()),
		xlog.NewField("numBackgroundCompaction", n),
	)
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
			).Debug("planned background compaction task")
		}
	}

	for i, task := range plan.Tasks {
		err := b.backgroundCompactWithTask(task, log,
			logger.WithFields(xlog.NewField("task", i)))
		if err != nil {
			instrument.EmitAndLogInvariantViolation(b.iopts, func(l xlog.Logger) {
				l.Errorf("error compacting segments: %v", err)
			})
			return
		}
	}
}

func (b *block) backgroundCompactWithTask(
	task compaction.Task,
	log bool,
	logger xlog.Logger,
) error {
	if log {
		logger.Debug("start compaction task")
	}

	segments := make([]segment.Segment, 0, len(task.Segments))
	for _, seg := range task.Segments {
		segments = append(segments, seg.Segment)
	}

	start := time.Now()
	compacted, err := b.backgroundCompactor.Compact(segments)
	took := time.Since(start)
	b.metrics.backgroundCompactionTaskRunLatency.Record(took)

	if log {
		logger.WithFields(xlog.NewField("took", took.String())).
			Debug("done compaction task")
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
			instrument.EmitAndLogInvariantViolation(b.iopts, func(l xlog.Logger) {
				l.Errorf("unable to close compacted block: %v", err)
			})
		}
	}

	// Return all the ones we kept plus the new compacted segment.
	return append(result, newReadableSeg(compacted))
}

func (b *block) WriteBatch(inserts *WriteBatch) (WriteBatchResult, error) {
	b.Lock()
	if b.state != blockStateOpen {
		b.Unlock()
		return b.writeBatchResult(inserts, b.writeBatchErrorInvalidState(b.state))
	}
	if b.compactingForeground {
		b.Unlock()
		return b.writeBatchResult(inserts, errUnableToWriteBlockConcurrent)
	}

	b.compactingForeground = true
	builder := b.segmentBuilder
	b.Unlock()

	defer func() {
		b.Lock()
		b.compactingForeground = false
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
	err := b.foregroundCompactWithBuilder(builder)
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
		Age:  0,
		Size: int64(len(builder.Docs())),
		Type: segments.MutableType,
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

	// Check plan.
	var planErr error
	switch {
	case len(builder.Docs()) == 0:
		// Must be compacting with a builder that has some inserted documents.
		planErr = errForegroundCompactorNoBuilderDocs
	case len(plan.Tasks) == 0:
		// Must generate a plan since we gave it a mutable segment type to compact.
		planErr = errForegroundCompactorNoPlan
	case !taskHasNilSegment(plan.Tasks[0]):
		// First task of plan must include the builder, so we can avoid resetting it
		// for the first task, but then safely reset it in consequent tasks.
		planErr = errForegroundCompactorBadPlanFirstTask
	}
	if planErr != nil {
		return planErr
	}

	// Move any unused segments to the background.
	b.Lock()
	b.maybeMoveForegroundSegmentsToBackgroundWithLock(plan.UnusedSegments)
	b.Unlock()

	n := b.compactionsForeground
	b.compactionsForeground++

	logger := b.logger.WithFields(
		xlog.NewField("block", b.startTime.String()),
		xlog.NewField("numForegroundCompaction", n),
	)
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
			).Debug("planned foreground compaction task")
		}
	}

	// Run the plan.
	sw := b.metrics.foregroundCompactionPlanRunLatency.Start()
	defer sw.Stop()

	// Run the first task, without resetting the builder.
	if err := b.foregroundCompactWithTask(
		builder, plan.Tasks[0],
		log, logger.WithFields(xlog.NewField("task", 0)),
	); err != nil {
		return err
	}

	// Now run each consequent task, resetting the builder each time since
	// the results from the builder have already been compacted in the first
	// task.
	for i := 1; i < len(plan.Tasks); i++ {
		task := plan.Tasks[i]
		if taskHasNilSegment(task) { // Only the first task should have the nil segment.
			return errForegroundCompactorBadPlanSecondaryTask
		}
		// Now use the builder after resetting it.
		builder.Reset(0)
		if err := b.foregroundCompactWithTask(
			builder, task,
			log, logger.WithFields(xlog.NewField("task", i)),
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
	if b.backgroundCompactor == nil {
		// No longer performing background compaction due to evict/close.
		return
	}

	b.logger.Debugf("moving %d segments from foreground to background",
		len(segments))

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
	logger xlog.Logger,
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
	compacted, err := b.foregroundCompactor.CompactUsingBuilder(builder, segments)
	took := time.Since(start)
	b.metrics.foregroundCompactionTaskRunLatency.Record(took)

	if log {
		logger.WithFields(xlog.NewField("took", took.String())).
			Debug("done compaction task")
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
	// Check if need to close all the compacted segments due to
	// having evicted mutable segments or the block being closed.
	if !b.shouldEvictCompactedSegmentsWithLock() {
		return
	}

	// Evict compacted segments.
	b.closeCompactedSegments(b.foregroundSegments)
	b.foregroundSegments = nil

	// Free compactor resources.
	if b.foregroundCompactor == nil {
		return
	}

	if err := b.foregroundCompactor.Close(); err != nil {
		instrument.EmitAndLogInvariantViolation(b.iopts, func(l xlog.Logger) {
			l.Errorf("error closing index block foreground compactor: %v", err)
		})
	}

	b.foregroundCompactor = nil
	b.segmentBuilder = nil
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
	if !b.compactingForeground {
		b.cleanupForegroundCompactWithLock()
	}
	if !b.compactingBackground {
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
	if !b.compactingForeground {
		b.cleanupForegroundCompactWithLock()
	}
	if !b.compactingBackground {
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

func taskHasNilSegment(task compaction.Task) bool {
	for _, seg := range task.Segments {
		if seg.Segment == nil {
			return true
		}
	}
	return false
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
