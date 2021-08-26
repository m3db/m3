// Copyright (c) 2020 Uber Technologies, Inc.
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
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/dbnode/storage/index/segments"
	"github.com/m3db/m3/src/m3ninx/doc"
	m3ninxindex "github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/builder"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/m3ninx/x"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/mmap"
	xresource "github.com/m3db/m3/src/x/resource"
	xtime "github.com/m3db/m3/src/x/time"
)

var (
	errUnableToWriteBlockConcurrent            = errors.New("unable to write, index block is being written to already")
	errMutableSegmentsAlreadyClosed            = errors.New("mutable segments already closed")
	errForegroundCompactorNoPlan               = errors.New("index foreground compactor failed to generate a plan")
	errForegroundCompactorBadPlanFirstTask     = errors.New("index foreground compactor generated plan without mutable segment in first task")
	errForegroundCompactorBadPlanSecondaryTask = errors.New("index foreground compactor generated plan with mutable segment a secondary task")

	numBackgroundCompactorsStandard       = 1
	numBackgroundCompactorsGarbageCollect = 1
)

type mutableSegmentsState uint

const (
	mutableSegmentsStateOpen   mutableSegmentsState = iota
	mutableSegmentsStateClosed mutableSegmentsState = iota
)

// nolint: maligned
type mutableSegments struct {
	sync.RWMutex

	state mutableSegmentsState

	foregroundSegments []*readableSeg
	backgroundSegments []*readableSeg

	compact                  mutableSegmentsCompact
	blockStart               xtime.UnixNano
	blockSize                time.Duration
	blockOpts                BlockOptions
	opts                     Options
	iopts                    instrument.Options
	optsListener             xresource.SimpleCloser
	writeIndexingConcurrency int

	seriesActiveFn segment.DocumentsFilter

	metrics mutableSegmentsMetrics
	logger  *zap.Logger
}

type mutableSegmentsMetrics struct {
	foregroundCompactionPlanRunLatency    tally.Timer
	foregroundCompactionTaskRunLatency    tally.Timer
	backgroundCompactionPlanRunLatency    tally.Timer
	backgroundCompactionTaskRunLatency    tally.Timer
	activeBlockIndexNew                   tally.Counter
	activeBlockGarbageCollectSegment      tally.Counter
	activeBlockGarbageCollectSeries       tally.Counter
	activeBlockGarbageCollectEmptySegment tally.Counter
}

func newMutableSegmentsMetrics(s tally.Scope) mutableSegmentsMetrics {
	foregroundScope := s.Tagged(map[string]string{"compaction-type": "foreground"})
	backgroundScope := s.Tagged(map[string]string{"compaction-type": "background"})
	activeBlockScope := s.SubScope("active-block")
	return mutableSegmentsMetrics{
		foregroundCompactionPlanRunLatency: foregroundScope.Timer("compaction-plan-run-latency"),
		foregroundCompactionTaskRunLatency: foregroundScope.Timer("compaction-task-run-latency"),
		backgroundCompactionPlanRunLatency: backgroundScope.Timer("compaction-plan-run-latency"),
		backgroundCompactionTaskRunLatency: backgroundScope.Timer("compaction-task-run-latency"),
		activeBlockIndexNew: activeBlockScope.Tagged(map[string]string{
			"result_type": "new",
		}).Counter("index-result"),
		activeBlockGarbageCollectSegment:      activeBlockScope.Counter("gc-segment"),
		activeBlockGarbageCollectSeries:       activeBlockScope.Counter("gc-series"),
		activeBlockGarbageCollectEmptySegment: activeBlockScope.Counter("gc-empty-segment"),
	}
}

// newMutableSegments returns a new Block, representing a complete reverse index
// for the duration of time specified. It is backed by one or more segments.
func newMutableSegments(
	md namespace.Metadata,
	blockStart xtime.UnixNano,
	opts Options,
	blockOpts BlockOptions,
	namespaceRuntimeOptsMgr namespace.RuntimeOptionsManager,
	iopts instrument.Options,
) *mutableSegments {
	m := &mutableSegments{
		blockStart: blockStart,
		blockSize:  md.Options().IndexOptions().BlockSize(),
		opts:       opts,
		blockOpts:  blockOpts,
		compact:    mutableSegmentsCompact{opts: opts, blockOpts: blockOpts},
		iopts:      iopts,
		metrics:    newMutableSegmentsMetrics(iopts.MetricsScope()),
		logger:     iopts.Logger(),
	}
	m.seriesActiveFn = segment.DocumentsFilterFn(m.seriesActive)
	m.optsListener = namespaceRuntimeOptsMgr.RegisterListener(m)
	return m
}

func (m *mutableSegments) SetNamespaceRuntimeOptions(opts namespace.RuntimeOptions) {
	m.Lock()
	// Update current runtime opts for segment builders created in future.
	perCPUFraction := opts.WriteIndexingPerCPUConcurrencyOrDefault()
	cpus := math.Ceil(perCPUFraction * float64(runtime.NumCPU()))
	m.writeIndexingConcurrency = int(math.Max(1, cpus))
	segmentBuilder := m.compact.segmentBuilder
	m.Unlock()

	// Reset any existing segment builder to new concurrency, do this
	// out of the lock since builder can be used for foreground compaction
	// outside the lock and does it's own locking.
	if segmentBuilder != nil {
		segmentBuilder.SetIndexConcurrency(m.writeIndexingConcurrency)
	}

	// Set the global concurrency control we have (we may need to fork
	// github.com/twotwotwo/sorts to control this on a per segment builder
	// basis).
	builder.SetSortConcurrency(m.writeIndexingConcurrency)
}

func (m *mutableSegments) seriesActive(d doc.Metadata) bool {
	// Filter out any documents that only were indexed for
	// sealed blocks.
	if d.OnIndexSeries == nil {
		instrument.EmitAndLogInvariantViolation(m.iopts, func(l *zap.Logger) {
			l.Error("unexpected nil for document index entry for background compact")
		})
		return true
	}

	return !d.OnIndexSeries.TryMarkIndexGarbageCollected()
}

func (m *mutableSegments) WriteBatch(inserts *WriteBatch) (MutableSegmentsStats, error) {
	m.Lock()
	if m.state == mutableSegmentsStateClosed {
		m.Unlock()
		return MutableSegmentsStats{}, errMutableSegmentsAlreadyClosed
	}

	if m.compact.compactingForeground {
		m.Unlock()
		return MutableSegmentsStats{}, errUnableToWriteBlockConcurrent
	}

	// Lazily allocate the segment builder and compactors.
	err := m.compact.allocLazyBuilderAndCompactorsWithLock(m.writeIndexingConcurrency)
	if err != nil {
		m.Unlock()
		return MutableSegmentsStats{}, err
	}

	m.compact.compactingForeground = true
	segmentBuilder := m.compact.segmentBuilder
	m.Unlock()

	defer func() {
		m.Lock()
		m.compact.compactingForeground = false
		m.cleanupForegroundCompactWithLock()
		m.Unlock()
	}()

	docs := inserts.PendingDocs()
	entries := inserts.PendingEntries()

	// Set the doc ref for later recall.
	for i := range entries {
		docs[i].OnIndexSeries = entries[i].OnIndexSeries
	}

	segmentBuilder.Reset()
	insertResultErr := segmentBuilder.InsertBatch(m3ninxindex.Batch{
		Docs:                docs,
		AllowPartialUpdates: true,
	})
	n := len(segmentBuilder.Docs())
	if n == 0 {
		// No inserts, no need to compact.
		return MutableSegmentsStats{}, insertResultErr
	}

	// We inserted some documents, need to compact immediately into a
	// foreground segment from the segment builder before we can serve reads
	// from an FST segment.
	result, err := m.foregroundCompactWithBuilder(segmentBuilder)
	if err != nil {
		return MutableSegmentsStats{}, err
	}

	m.metrics.activeBlockIndexNew.Inc(int64(n))

	// Return result from the original insertion since compaction was successful.
	return result, insertResultErr
}

func (m *mutableSegments) AddReaders(readers []segment.Reader) ([]segment.Reader, error) {
	m.RLock()
	defer m.RUnlock()

	var err error
	readers, err = m.addReadersWithLock(m.foregroundSegments, readers)
	if err != nil {
		return nil, err
	}

	readers, err = m.addReadersWithLock(m.backgroundSegments, readers)
	if err != nil {
		return nil, err
	}

	return readers, nil
}

func (m *mutableSegments) addReadersWithLock(src []*readableSeg, dst []segment.Reader) ([]segment.Reader, error) {
	for _, seg := range src {
		reader, err := seg.Segment().Reader()
		if err != nil {
			return nil, err
		}
		dst = append(dst, reader)
	}
	return dst, nil
}

func (m *mutableSegments) Len() int {
	m.RLock()
	defer m.RUnlock()

	return len(m.foregroundSegments) + len(m.backgroundSegments)
}

func (m *mutableSegments) MemorySegmentsData(ctx context.Context) ([]fst.SegmentData, error) {
	m.RLock()
	defer m.RUnlock()

	// NB(r): This is for debug operations, do not bother about allocations.
	var results []fst.SegmentData
	for _, segs := range [][]*readableSeg{
		m.foregroundSegments,
		m.backgroundSegments,
	} {
		for _, seg := range segs {
			fstSegment, ok := seg.Segment().(fst.Segment)
			if !ok {
				return nil, fmt.Errorf("segment not fst segment: created=%v", seg.createdAt)
			}

			segmentData, err := fstSegment.SegmentData(ctx)
			if err != nil {
				return nil, err
			}

			results = append(results, segmentData)
		}
	}
	return results, nil
}

func (m *mutableSegments) NeedsEviction() bool {
	m.RLock()
	defer m.RUnlock()

	var needsEviction bool
	for _, seg := range m.foregroundSegments {
		needsEviction = needsEviction || seg.Segment().Size() > 0
	}
	for _, seg := range m.backgroundSegments {
		needsEviction = needsEviction || seg.Segment().Size() > 0
	}
	return needsEviction
}

func (m *mutableSegments) NumSegmentsAndDocs() (int64, int64) {
	m.RLock()
	defer m.RUnlock()

	foregroundNumSegments, foregroundNumDocs := numSegmentsAndDocs(m.foregroundSegments)
	backgroundNumSegments, backgroundNumDocs := numSegmentsAndDocs(m.backgroundSegments)
	numSegments := foregroundNumSegments + backgroundNumSegments
	numDocs := foregroundNumDocs + backgroundNumDocs
	return numSegments, numDocs
}

func numSegmentsAndDocs(segs []*readableSeg) (int64, int64) {
	var (
		numSegments, numDocs int64
	)
	for _, seg := range segs {
		numSegments++
		numDocs += seg.Segment().Size()
	}
	return numSegments, numDocs
}

func (m *mutableSegments) Stats(reporter BlockStatsReporter) {
	m.RLock()
	defer m.RUnlock()

	for _, seg := range m.foregroundSegments {
		_, mutable := seg.Segment().(segment.MutableSegment)
		reporter.ReportSegmentStats(BlockSegmentStats{
			Type:    ActiveForegroundSegment,
			Mutable: mutable,
			Age:     seg.Age(),
			Size:    seg.Segment().Size(),
		})
	}
	for _, seg := range m.backgroundSegments {
		_, mutable := seg.Segment().(segment.MutableSegment)
		reporter.ReportSegmentStats(BlockSegmentStats{
			Type:    ActiveBackgroundSegment,
			Mutable: mutable,
			Age:     seg.Age(),
			Size:    seg.Segment().Size(),
		})
	}

	reporter.ReportIndexingStats(BlockIndexingStats{
		IndexConcurrency: m.writeIndexingConcurrency,
	})
}

func (m *mutableSegments) Close() {
	m.Lock()
	defer m.Unlock()
	m.state = mutableSegmentsStateClosed
	m.cleanupCompactWithLock()
	m.optsListener.Close()
}

func (m *mutableSegments) maybeBackgroundCompactWithLock() {
	if m.compact.compactingBackgroundStandard {
		return
	}

	m.backgroundCompactWithLock()
}

func (m *mutableSegments) backgroundCompactWithLock() {
	// Create a logical plan.
	segs := make([]compaction.Segment, 0, len(m.backgroundSegments))
	for _, seg := range m.backgroundSegments {
		if seg.garbageCollecting {
			// Do not try to compact something that we are background
			// garbage collecting documents from (that have been phased out).
			continue
		}
		segs = append(segs, compaction.Segment{
			Age:     seg.Age(),
			Size:    seg.Segment().Size(),
			Type:    segments.FSTType,
			Segment: seg.Segment(),
		})
	}

	plan, err := compaction.NewPlan(segs, m.opts.BackgroundCompactionPlannerOptions())
	if err != nil {
		instrument.EmitAndLogInvariantViolation(m.iopts, func(l *zap.Logger) {
			l.Error("index background compaction plan error", zap.Error(err))
		})
		return
	}

	var (
		gcRequired       = false
		gcPlan           = &compaction.Plan{}
		gcAlreadyRunning = m.compact.compactingBackgroundGarbageCollect
	)
	if !gcAlreadyRunning {
		gcRequired = true

		for _, seg := range m.backgroundSegments {
			alreadyHasTask := false
			for _, task := range plan.Tasks {
				for _, taskSegment := range task.Segments {
					if taskSegment.Segment == seg.Segment() {
						alreadyHasTask = true
						break
					}
				}
			}
			if alreadyHasTask {
				// Skip needing to check if segment needs filtering.
				continue
			}

			// Ensure that segment has some series that need to be GC'd.
			hasAnyInactiveSeries, err := m.segmentAnyInactiveSeries(seg.Segment())
			if err != nil {
				instrument.EmitAndLogInvariantViolation(m.iopts, func(l *zap.Logger) {
					l.Error("error detecting needs background gc segment", zap.Error(err))
				})
				continue
			}
			if !hasAnyInactiveSeries {
				// Skip background GC since all series are still active and no
				// series need to be removed.
				continue
			}

			// The active block starts are outdated, need to compact
			// and remove any old data from the segment.
			var task compaction.Task
			if len(gcPlan.Tasks) > 0 {
				task = gcPlan.Tasks[0]
			}

			task.Segments = append(task.Segments, compaction.Segment{
				Age:     seg.Age(),
				Size:    seg.Segment().Size(),
				Type:    segments.FSTType,
				Segment: seg.Segment(),
			})

			if len(gcPlan.Tasks) == 0 {
				gcPlan.Tasks = make([]compaction.Task, 1)
			}
			gcPlan.Tasks[0] = task

			// Mark as not-compactable for standard compactions
			// since this will be async compacted into a smaller
			// segment.
			seg.garbageCollecting = true
		}
	}

	if len(plan.Tasks) != 0 {
		// Kick off compaction.
		m.compact.compactingBackgroundStandard = true
		go func() {
			m.backgroundCompactWithPlan(plan, m.compact.backgroundCompactors, gcRequired)

			m.Lock()
			m.compact.compactingBackgroundStandard = false
			m.cleanupBackgroundCompactWithLock()
			m.Unlock()
		}()
	}

	if len(gcPlan.Tasks) != 0 {
		// Run non-GC tasks separately so the standard loop is not blocked.
		m.compact.compactingBackgroundGarbageCollect = true
		go func() {
			compactors, err := m.compact.allocBackgroundCompactorsGarbageCollect()
			if err != nil {
				instrument.EmitAndLogInvariantViolation(m.iopts, func(l *zap.Logger) {
					l.Error("error background gc segments", zap.Error(err))
				})
			} else {
				m.backgroundCompactWithPlan(gcPlan, compactors, gcRequired)
				m.closeCompactors(compactors)
			}

			m.Lock()
			m.compact.compactingBackgroundGarbageCollect = false
			m.cleanupBackgroundCompactWithLock()
			m.Unlock()
		}()
	}
}

func (m *mutableSegments) segmentAnyInactiveSeries(seg segment.Segment) (bool, error) {
	reader, err := seg.Reader()
	if err != nil {
		return false, err
	}

	defer func() {
		_ = reader.Close()
	}()

	docs, err := reader.AllDocs()
	if err != nil {
		return false, err
	}

	docsCloser := x.NewSafeCloser(docs)
	defer func() {
		// In case of early return cleanup
		_ = docsCloser.Close()
	}()

	var result bool
	for docs.Next() {
		d := docs.Current()
		indexEntry := d.OnIndexSeries
		if indexEntry == nil {
			return false, fmt.Errorf("document has no index entry: %s", d.ID)
		}
		if indexEntry.NeedsIndexGarbageCollected() {
			result = true
			break
		}
	}

	if err := docs.Err(); err != nil {
		return false, err
	}

	return result, docsCloser.Close()
}

func (m *mutableSegments) shouldEvictCompactedSegmentsWithLock() bool {
	return m.state == mutableSegmentsStateClosed
}

func (m *mutableSegments) cleanupBackgroundCompactWithLock() {
	if m.state == mutableSegmentsStateOpen {
		// See if we need to trigger another compaction.
		m.maybeBackgroundCompactWithLock()
		return
	}

	// Check if need to close all the compacted segments due to
	// mutableSegments being closed.
	if !m.shouldEvictCompactedSegmentsWithLock() {
		return
	}

	// Close compacted segments.
	m.closeCompactedSegmentsWithLock(m.backgroundSegments)
	m.backgroundSegments = nil

	// Free compactor resources.
	if m.compact.backgroundCompactors == nil {
		return
	}

	m.closeCompactors(m.compact.backgroundCompactors)
	m.compact.backgroundCompactors = nil
}

func (m *mutableSegments) closeCompactors(compactors chan *compaction.Compactor) {
	close(compactors)
	for compactor := range compactors {
		err := compactor.Close()
		if err == nil {
			continue
		}

		instrument.EmitAndLogInvariantViolation(m.iopts, func(l *zap.Logger) {
			l.Error("error closing index block background compactor", zap.Error(err))
		})
	}
}

func (m *mutableSegments) closeCompactedSegmentsWithLock(segments []*readableSeg) {
	for _, seg := range segments {
		err := seg.Segment().Close()
		if err != nil {
			instrument.EmitAndLogInvariantViolation(m.iopts, func(l *zap.Logger) {
				l.Error("could not close compacted segment", zap.Error(err))
			})
		}
	}
}

func (m *mutableSegments) backgroundCompactWithPlan(
	plan *compaction.Plan,
	compactors chan *compaction.Compactor,
	gcRequired bool,
) {
	sw := m.metrics.backgroundCompactionPlanRunLatency.Start()
	defer sw.Stop()

	n := m.compact.numBackground
	m.compact.numBackground++

	logger := m.logger.With(
		zap.Time("blockStart", m.blockStart.ToTime()),
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
				zap.Stringer("cumulativeMutableAge", summary.CumulativeMutableAge),
				zap.Int64("cumulativeSize", summary.CumulativeSize),
			)
		}
	}

	var wg sync.WaitGroup
	for i, task := range plan.Tasks {
		i, task := i, task
		wg.Add(1)
		compactor := <-compactors
		go func() {
			defer func() {
				compactors <- compactor
				wg.Done()
			}()
			err := m.backgroundCompactWithTask(task, compactor, gcRequired,
				log, logger.With(zap.Int("task", i)))
			if err != nil {
				instrument.EmitAndLogInvariantViolation(m.iopts, func(l *zap.Logger) {
					l.Error("error compacting segments", zap.Error(err))
				})
			}
		}()
	}

	wg.Wait()
}

func (m *mutableSegments) newReadThroughSegment(seg fst.Segment) *ReadThroughSegment {
	readThroughOpts := m.opts.ReadThroughSegmentOptions()
	return NewReadThroughSegment(seg, m.opts.PostingsListCache(), readThroughOpts)
}

func (m *mutableSegments) backgroundCompactWithTask(
	task compaction.Task,
	compactor *compaction.Compactor,
	gcRequired bool,
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

	var documentsFilter segment.DocumentsFilter
	if gcRequired {
		// Only actively filter out documents if GC is required.
		documentsFilter = m.seriesActiveFn
	}

	start := time.Now()
	compactResult, err := compactor.Compact(segments, documentsFilter,
		m.metrics.activeBlockGarbageCollectSeries,
		mmap.ReporterOptions{
			Context: mmap.Context{
				Name: mmapIndexBlockName,
			},
			Reporter: m.opts.MmapReporter(),
		})
	took := time.Since(start)
	m.metrics.backgroundCompactionTaskRunLatency.Record(took)

	if log {
		logger.Debug("done compaction task", zap.Duration("took", took))
	}

	// Check if result would have resulted in an empty segment.
	empty := errors.Is(err, compaction.ErrCompactorBuilderEmpty)
	if empty {
		// Don't return the error since we need to remove the old segments
		// by calling addCompactedSegmentFromSegmentsWithLock.
		err = nil
	}
	if err != nil {
		return err
	}

	var (
		compacted = compactResult.Compacted
		// segMetas   = compactResult.SegmentMetadatas
		replaceSeg segment.Segment
	)
	if empty {
		m.metrics.activeBlockGarbageCollectEmptySegment.Inc(1)
	} else {
		m.metrics.activeBlockGarbageCollectSegment.Inc(1)

		// Add a read through cache for repeated expensive queries against
		// background compacted segments since they can live for quite some
		// time and accrue a large set of documents.
		readThroughSeg := m.newReadThroughSegment(compacted)
		replaceSeg = readThroughSeg

		// NB(r): Before replacing the old segments with the compacted segment
		// we rebuild all the cached postings lists that the previous segment had
		// to avoid latency spikes during segment rotation.
		// Note: There was very obvious peaks of latency (p99 of <500ms spiking
		// to 8 times that at first replace of large segments after a block
		// rotation) without this optimization.

		// TODO: port populating cached searches
		// if err := m.populateCachedSearches(readThroughSeg, segMetas); err != nil {
		// 	instrument.EmitAndLogInvariantViolation(m.iopts, func(l *zap.Logger) {
		// 		l.Error("failed to populate cached searches", zap.Error(err))
		// 	})
		// }
	}

	// Rotate out the replaced frozen segments and add the compacted one.
	m.Lock()
	defer m.Unlock()

	result := m.addCompactedSegmentFromSegmentsWithLock(m.backgroundSegments,
		segments, replaceSeg)
	m.backgroundSegments = result

	return nil
}

func (m *mutableSegments) addCompactedSegmentFromSegmentsWithLock(
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
			instrument.EmitAndLogInvariantViolation(m.iopts, func(l *zap.Logger) {
				l.Error("unable to close compacted block", zap.Error(err))
			})
		}
	}

	if compacted == nil {
		return result
	}

	// Return all the ones we kept plus the new compacted segment
	return append(result, newReadableSeg(compacted, m.opts))
}

func (m *mutableSegments) foregroundCompactWithBuilder(
	builder segment.DocumentsBuilder,
) (MutableSegmentsStats, error) {
	// We inserted some documents, need to compact immediately into a
	// foreground segment.
	m.Lock()
	foregroundSegments := m.foregroundSegments
	m.Unlock()

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

	plan, err := compaction.NewPlan(segs, m.opts.ForegroundCompactionPlannerOptions())
	if err != nil {
		return MutableSegmentsStats{}, err
	}

	// Check plan
	if len(plan.Tasks) == 0 {
		// Should always generate a task when a mutable builder is passed to planner
		return MutableSegmentsStats{}, errForegroundCompactorNoPlan
	}
	if taskNumBuilders(plan.Tasks[0]) != 1 {
		// First task of plan must include the builder, so we can avoid resetting it
		// for the first task, but then safely reset it in consequent tasks
		return MutableSegmentsStats{}, errForegroundCompactorBadPlanFirstTask
	}

	// Move any unused segments to the background.
	m.Lock()
	m.maybeMoveForegroundSegmentsToBackgroundWithLock(plan.UnusedSegments)
	m.Unlock()

	n := m.compact.numForeground
	m.compact.numForeground++

	logger := m.logger.With(
		zap.Time("blockStart", m.blockStart.ToTime()),
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
	sw := m.metrics.foregroundCompactionPlanRunLatency.Start()
	defer sw.Stop()

	// Run the first task, without resetting the builder.
	result, err := m.foregroundCompactWithTask(builder, plan.Tasks[0],
		log, logger.With(zap.Int("task", 0)))
	if err != nil {
		return result, err
	}

	// Now run each consequent task, resetting the builder each time since
	// the results from the builder have already been compacted in the first
	// task.
	for i := 1; i < len(plan.Tasks); i++ {
		task := plan.Tasks[i]
		if taskNumBuilders(task) > 0 {
			// Only the first task should compact the builder
			return result, errForegroundCompactorBadPlanSecondaryTask
		}
		// Now use the builder after resetting it.
		builder.Reset()
		result, err = m.foregroundCompactWithTask(builder, task,
			log, logger.With(zap.Int("task", i)))
		if err != nil {
			return result, err
		}
	}

	return result, nil
}

func (m *mutableSegments) maybeMoveForegroundSegmentsToBackgroundWithLock(
	segments []compaction.Segment,
) {
	if len(segments) == 0 {
		return
	}
	if m.compact.backgroundCompactors == nil {
		// No longer performing background compaction due to evict/close.
		return
	}

	m.logger.Debug("moving segments from foreground to background",
		zap.Int("numSegments", len(segments)))

	// If background compaction is still active, then we move any unused
	// foreground segments into the background so that they might be
	// compacted by the background compactor at some point.
	i := 0
	for _, currForeground := range m.foregroundSegments {
		movedToBackground := false
		for _, seg := range segments {
			if currForeground.Segment() == seg.Segment {
				m.backgroundSegments = append(m.backgroundSegments, currForeground)
				movedToBackground = true
				break
			}
		}
		if movedToBackground {
			continue // No need to keep this segment, we moved it.
		}

		m.foregroundSegments[i] = currForeground
		i++
	}

	m.foregroundSegments = m.foregroundSegments[:i]

	// Potentially kick off a background compaction.
	m.maybeBackgroundCompactWithLock()
}

func (m *mutableSegments) foregroundCompactWithTask(
	builder segment.DocumentsBuilder,
	task compaction.Task,
	log bool,
	logger *zap.Logger,
) (MutableSegmentsStats, error) {
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
	compacted, err := m.compact.foregroundCompactor.CompactUsingBuilder(builder, segments, mmap.ReporterOptions{
		Context: mmap.Context{
			Name: mmapIndexBlockName,
		},
		Reporter: m.opts.MmapReporter(),
	})
	took := time.Since(start)
	m.metrics.foregroundCompactionTaskRunLatency.Record(took)

	if log {
		logger.Debug("done compaction task", zap.Duration("took", took))
	}

	if err != nil {
		return MutableSegmentsStats{}, err
	}

	// Add a read through cache for repeated expensive queries against
	// compacted segments since they can live for quite some time during
	// block rotations while a burst of segments are created.
	segment := m.newReadThroughSegment(compacted)

	// Rotate in the ones we just compacted.
	m.Lock()
	defer m.Unlock()

	result := m.addCompactedSegmentFromSegmentsWithLock(m.foregroundSegments,
		segments, segment)
	m.foregroundSegments = result

	foregroundNumSegments, foregroundNumDocs := numSegmentsAndDocs(m.foregroundSegments)
	backgroundNumSegments, backgroundNumDocs := numSegmentsAndDocs(m.backgroundSegments)
	return MutableSegmentsStats{
		Foreground: MutableSegmentsSegmentStats{
			NumSegments: foregroundNumSegments,
			NumDocs:     foregroundNumDocs,
		},
		Background: MutableSegmentsSegmentStats{
			NumSegments: backgroundNumSegments,
			NumDocs:     backgroundNumDocs,
		},
	}, nil
}

func (m *mutableSegments) cleanupForegroundCompactWithLock() {
	// Check if need to close all the compacted segments due to
	// mutableSegments being closed.
	if !m.shouldEvictCompactedSegmentsWithLock() {
		return
	}

	// Close compacted segments.
	m.closeCompactedSegmentsWithLock(m.foregroundSegments)
	m.foregroundSegments = nil

	// Free compactor resources.
	if m.compact.foregroundCompactor != nil {
		if err := m.compact.foregroundCompactor.Close(); err != nil {
			instrument.EmitAndLogInvariantViolation(m.iopts, func(l *zap.Logger) {
				l.Error("error closing index block foreground compactor", zap.Error(err))
			})
		}
		m.compact.foregroundCompactor = nil
	}

	// Free segment builder resources.
	if m.compact.segmentBuilder != nil {
		if err := m.compact.segmentBuilder.Close(); err != nil {
			instrument.EmitAndLogInvariantViolation(m.iopts, func(l *zap.Logger) {
				l.Error("error closing index block segment builder", zap.Error(err))
			})
		}
		m.compact.segmentBuilder = nil
	}
}
func (m *mutableSegments) cleanupCompactWithLock() {
	// If not compacting, trigger a cleanup so that all frozen segments get
	// closed, otherwise after the current running compaction the compacted
	// segments will get closed.
	if !m.compact.compactingForeground {
		m.cleanupForegroundCompactWithLock()
	}
	if !m.compact.compactingBackgroundStandard && !m.compact.compactingBackgroundGarbageCollect {
		m.cleanupBackgroundCompactWithLock()
	}
}

// mutableSegmentsCompact has several lazily allocated compaction components.
type mutableSegmentsCompact struct {
	opts      Options
	blockOpts BlockOptions

	segmentBuilder                     segment.CloseableDocumentsBuilder
	foregroundCompactor                *compaction.Compactor
	backgroundCompactors               chan *compaction.Compactor
	compactingForeground               bool
	compactingBackgroundStandard       bool
	compactingBackgroundGarbageCollect bool
	numForeground                      int
	numBackground                      int
}

func (m *mutableSegmentsCompact) allocLazyBuilderAndCompactorsWithLock(
	concurrency int,
) error {
	var (
		err          error
		metadataPool = m.opts.MetadataArrayPool()
	)
	if m.segmentBuilder == nil {
		builderOpts := m.opts.SegmentBuilderOptions().
			SetConcurrency(concurrency)

		m.segmentBuilder, err = builder.NewBuilderFromDocuments(builderOpts)
		if err != nil {
			return err
		}
	}

	if m.foregroundCompactor == nil {
		m.foregroundCompactor, err = compaction.NewCompactor(metadataPool,
			MetadataArrayPoolCapacity,
			m.opts.SegmentBuilderOptions(),
			m.opts.FSTSegmentOptions(),
			compaction.CompactorOptions{
				FSTWriterOptions: &fst.WriterOptions{
					// DisableRegistry is set to true to trade a larger FST size
					// for a faster FST compaction since we want to reduce the end
					// to end latency for time to first index a metric.
					DisableRegistry: true,
				},
				MmapDocsData: m.blockOpts.ForegroundCompactorMmapDocsData,
			})
		if err != nil {
			return err
		}
	}

	if m.backgroundCompactors == nil {
		n := numBackgroundCompactorsStandard
		m.backgroundCompactors = make(chan *compaction.Compactor, n)
		for i := 0; i < n; i++ {
			backgroundCompactor, err := compaction.NewCompactor(metadataPool,
				MetadataArrayPoolCapacity,
				m.opts.SegmentBuilderOptions(),
				m.opts.FSTSegmentOptions(),
				compaction.CompactorOptions{
					MmapDocsData: m.blockOpts.BackgroundCompactorMmapDocsData,
				})
			if err != nil {
				return err
			}
			m.backgroundCompactors <- backgroundCompactor
		}
	}

	return nil
}

func (m *mutableSegmentsCompact) allocBackgroundCompactorsGarbageCollect() (
	chan *compaction.Compactor,
	error,
) {
	metadataPool := m.opts.MetadataArrayPool()
	n := numBackgroundCompactorsGarbageCollect
	compactors := make(chan *compaction.Compactor, n)
	for i := 0; i < n; i++ {
		backgroundCompactor, err := compaction.NewCompactor(metadataPool,
			MetadataArrayPoolCapacity,
			m.opts.SegmentBuilderOptions(),
			m.opts.FSTSegmentOptions(),
			compaction.CompactorOptions{
				MmapDocsData: m.blockOpts.BackgroundCompactorMmapDocsData,
			})
		if err != nil {
			return nil, err
		}
		compactors <- backgroundCompactor
	}
	return compactors, nil
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
