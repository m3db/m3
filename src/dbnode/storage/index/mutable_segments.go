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
	"bytes"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/m3db/bloom/v4"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/dbnode/storage/index/segments"
	m3ninxindex "github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/builder"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/mmap"
	xresource "github.com/m3db/m3/src/x/resource"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	errUnableToWriteBlockConcurrent            = errors.New("unable to write, index block is being written to already")
	errMutableSegmentsAlreadyClosed            = errors.New("mutable segments already closed")
	errForegroundCompactorNoPlan               = errors.New("index foreground compactor failed to generate a plan")
	errForegroundCompactorBadPlanFirstTask     = errors.New("index foreground compactor generated plan without mutable segment in first task")
	errForegroundCompactorBadPlanSecondaryTask = errors.New("index foreground compactor generated plan with mutable segment a secondary task")

	numBackgroundCompactors = int(math.Min(4, float64(runtime.NumCPU())/2))
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

	foregroundSegments                 []*readableSeg
	backgroundSegments                 []*readableSeg
	indexedSnapshot                    *indexedBloomFilterSnapshot
	backgroundCompactActiveBlockStarts []xtime.UnixNano
	backgroundCompactIndexedSnapshot   *indexedBloomFilterSnapshot

	compact                  mutableSegmentsCompact
	blockStart               time.Time
	blockSize                time.Duration
	blockOpts                BlockOptions
	opts                     Options
	iopts                    instrument.Options
	optsListener             xresource.SimpleCloser
	writeIndexingConcurrency int

	indexedBloomFilterByTimeLock sync.RWMutex
	indexedBloomFilterByTime     map[xtime.UnixNano]*indexedBloomFilter

	metrics mutableSegmentsMetrics
	logger  *zap.Logger
}

type indexedBloomFilter struct {
	doNotWrite    *builder.IDsMap
	writes        *bloom.BloomFilter
	snapshotDirty bool
	snapshot      *bytes.Buffer
}

var (
	// Estimate bloom values for 1million and 1% false positive rate.
	// Roughly 1mb size with k:7 (hash 7 times on insert/lookup).
	bloomM, bloomK = bloom.EstimateFalsePositiveRate(1<<20, 0.01)
)

func newIndexedBloomFilter() *indexedBloomFilter {
	bf := bloom.NewBloomFilter(bloomM, bloomK)
	snapshot := bytes.NewBuffer(nil)
	_ = bf.BitSet().Write(snapshot)
	return &indexedBloomFilter{
		doNotWrite: builder.NewIDsMap(builder.IDsMapOptions{
			InitialSize: 4096,
		}),
		writes:   bf,
		snapshot: snapshot,
	}
}

func (f *indexedBloomFilter) ContainsWithNoFalsePositive(id []byte) bool {
	return f.doNotWrite.Contains(id)
}

func (f *indexedBloomFilter) Write(id []byte) {
	f.doNotWrite.SetUnsafe(id, struct{}{}, builder.IDsMapSetUnsafeOptions{
		NoCopyKey:     true,
		NoFinalizeKey: true,
	})
	f.writes.Add(id)
	f.snapshotDirty = true
}

func (f *indexedBloomFilter) UpdateSnapshotIfRequired() {
	if !f.snapshotDirty {
		return
	}
	f.snapshot.Truncate(0)
	_ = f.writes.BitSet().Write(f.snapshot)
	f.snapshotDirty = false
}

func (f *indexedBloomFilter) MergeSnapshot(
	snap *indexedBloomFilterSnapshot,
) {
	data := f.snapshot.Bytes()
	size := len(data)
	if cap(snap.buffer) < size {
		// Grow buffer if required.
		snap.buffer = make([]byte, size)
	} else {
		snap.buffer = snap.buffer[:size]
	}

	for i := range snap.buffer {
		snap.buffer[i] |= data[i]
	}
}

type indexedBloomFilterSnapshot struct {
	buffer      []byte
	bloomFilter *bloom.ReadOnlyBloomFilter
}

func newIndexedBloomFilterSnapshot() *indexedBloomFilterSnapshot {
	return &indexedBloomFilterSnapshot{}
}

func (s *indexedBloomFilterSnapshot) Reset() {
	for i := range s.buffer {
		s.buffer[i] = 0
	}
}

func (s *indexedBloomFilterSnapshot) ReadOnlyBloomFilter() *bloom.ReadOnlyBloomFilter {
	// In future would be good to update read only bloom filter instead
	// of having to create a new one with the buffer (even though it's just
	// a wrapper over the buffer.).
	return bloom.NewReadOnlyBloomFilter(bloomM, bloomK, s.buffer)
}

type mutableSegmentsMetrics struct {
	foregroundCompactionPlanRunLatency tally.Timer
	foregroundCompactionTaskRunLatency tally.Timer
	backgroundCompactionPlanRunLatency tally.Timer
	backgroundCompactionTaskRunLatency tally.Timer
	activeBlockIndexNew                tally.Counter
	activeBlockIndexExists             tally.Counter
	activeBlockBloomNew                tally.Counter
	activeBlockBloomExists             tally.Counter
	activeBlockBloomUpdate             tally.Counter
	activeBlockGarbageCollectSegment   tally.Counter
	activeBlockGarbageCollectSeries    tally.Counter
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
		activeBlockIndexExists: activeBlockScope.Tagged(map[string]string{
			"result_type": "exists",
		}).Counter("index-result"),
		activeBlockBloomNew: activeBlockScope.Tagged(map[string]string{
			"result_type": "new",
		}).Counter("bloom-result"),
		activeBlockBloomExists: activeBlockScope.Tagged(map[string]string{
			"result_type": "exists",
		}).Counter("bloom-result"),
		activeBlockBloomUpdate: activeBlockScope.Tagged(map[string]string{
			"result_type": "update",
		}).Counter("bloom-result"),
		activeBlockGarbageCollectSegment: activeBlockScope.Counter("gc-segment"),
		activeBlockGarbageCollectSeries:  activeBlockScope.Counter("gc-series"),
	}
}

// newMutableSegments returns a new Block, representing a complete reverse index
// for the duration of time specified. It is backed by one or more segments.
func newMutableSegments(
	md namespace.Metadata,
	blockStart time.Time,
	opts Options,
	blockOpts BlockOptions,
	namespaceRuntimeOptsMgr namespace.RuntimeOptionsManager,
	iopts instrument.Options,
) *mutableSegments {
	m := &mutableSegments{
		blockStart:               blockStart,
		blockSize:                md.Options().IndexOptions().BlockSize(),
		opts:                     opts,
		blockOpts:                blockOpts,
		iopts:                    iopts,
		indexedBloomFilterByTime: make(map[xtime.UnixNano]*indexedBloomFilter),
		metrics:                  newMutableSegmentsMetrics(iopts.MetricsScope()),
		logger:                   iopts.Logger(),
	}
	m.optsListener = namespaceRuntimeOptsMgr.RegisterListener(m)
	return m
}

func (m *mutableSegments) NotifySealedBlocks(
	sealed []xtime.UnixNano,
) error {
	if len(sealed) == 0 {
		return nil
	}

	m.indexedBloomFilterByTimeLock.Lock()
	for _, blockStart := range sealed {
		_, exists := m.indexedBloomFilterByTime[blockStart]
		if !exists {
			continue
		}
		// Remove indexed set if block now sealed.
		delete(m.indexedBloomFilterByTime, blockStart)
	}
	m.indexedBloomFilterByTimeLock.Unlock()

	m.Lock()
	m.maybeBackgroundCompactWithLock()
	m.Unlock()

	return nil
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
	err := m.compact.allocLazyBuilderAndCompactorsWithLock(m.writeIndexingConcurrency,
		m.blockOpts, m.opts)
	if err != nil {
		m.Unlock()
		return MutableSegmentsStats{}, err
	}

	m.compact.compactingForeground = true
	builder := m.compact.segmentBuilder
	if m.indexedSnapshot == nil {
		m.indexedSnapshot = newIndexedBloomFilterSnapshot()
	}
	m.indexedSnapshot.Reset()
	m.Unlock()

	// Updsate indexedBloomFilterByTime if needed.
	var activeBlockStarts []xtime.UnixNano
	if m.blockOpts.InMemoryBlock {
		// Take references to the pending entries docs
		// and make sure not to touch sort order until later.
		entries := inserts.PendingEntries()
		docs := inserts.PendingDocs()

		m.indexedBloomFilterByTimeLock.Lock()
		// Remove for indexing anything already indexed and
		// also update the tracking of what things have been indexed
		// for what block starts.
		for i := range entries {
			blockStart := entries[i].indexBlockStart(m.blockSize)
			needsIndex := true
			needsBloomFilterWrite := true
			for bloomFilterBlockStart, bloomFilter := range m.indexedBloomFilterByTime {
				if bloomFilter.ContainsWithNoFalsePositive(docs[i].ID) {
					// Already indexed, do not need to index.
					needsIndex = false
					if blockStart == bloomFilterBlockStart {
						// Do not need to update the fact that this
						// ID is contained by this block start.
						needsBloomFilterWrite = false
						break
					}
				}
			}

			if !needsIndex {
				// Mark the fact that it doesn't need indexing.
				inserts.MarkEntrySuccess(i)
				m.metrics.activeBlockIndexExists.Inc(1)
			} else {
				m.metrics.activeBlockIndexNew.Inc(1)
			}

			if !needsBloomFilterWrite {
				// No need to update the bloom filter.
				m.metrics.activeBlockBloomExists.Inc(1)
				continue
			}

			if !needsIndex {
				m.metrics.activeBlockBloomUpdate.Inc(1)
			} else {
				m.metrics.activeBlockBloomNew.Inc(1)
			}

			bloomFilter, ok := m.indexedBloomFilterByTime[blockStart]
			if !ok {
				bloomFilter = newIndexedBloomFilter()
				m.indexedBloomFilterByTime[blockStart] = bloomFilter
			}
			bloomFilter.Write(docs[i].ID)
		}
		// Update bloom filter snapshots if required and also
		// track the active block starts.
		activeBlockStarts = make([]xtime.UnixNano, 0, len(m.indexedBloomFilterByTime))
		for blockStart, bloomFilter := range m.indexedBloomFilterByTime {
			activeBlockStarts = append(activeBlockStarts, blockStart)
			bloomFilter.UpdateSnapshotIfRequired()
			bloomFilter.MergeSnapshot(m.indexedSnapshot)
		}
		m.indexedBloomFilterByTimeLock.Unlock()
	}

	defer func() {
		m.Lock()
		// Check if any segments needs filtering.
		// Prepare the bloom filter to merge into from the live time windows.
		m.backgroundCompactActiveBlockStarts = activeBlockStarts
		if m.backgroundCompactIndexedSnapshot == nil {
			m.backgroundCompactIndexedSnapshot = newIndexedBloomFilterSnapshot()
		}
		m.backgroundCompactIndexedSnapshot.buffer =
			append(m.backgroundCompactIndexedSnapshot.buffer[:0], m.indexedSnapshot.buffer...)

		m.compact.compactingForeground = false
		m.cleanupForegroundCompactWithLock()
		m.Unlock()
	}()

	builder.Reset()
	insertResultErr := builder.InsertBatch(m3ninxindex.Batch{
		Docs:                inserts.PendingDocs(),
		AllowPartialUpdates: true,
	})
	if len(builder.Docs()) == 0 {
		// No inserts, no need to compact.
		return MutableSegmentsStats{}, insertResultErr
	}

	// We inserted some documents, need to compact immediately into a
	// foreground segment from the segment builder before we can serve reads
	// from an FST segment.
	result, err := m.foregroundCompactWithBuilder(builder, activeBlockStarts)
	if err != nil {
		return MutableSegmentsStats{}, err
	}

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
	if m.compact.compactingBackground {
		return
	}

	// Create a logical plan.
	segs := make([]compaction.Segment, 0, len(m.backgroundSegments))
	for _, seg := range m.backgroundSegments {
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
		activeBlockStarts []xtime.UnixNano
		activeBloomFilter *bloom.ReadOnlyBloomFilter
	)
	if m.blockOpts.InMemoryBlock && m.backgroundCompactIndexedSnapshot != nil {
		// Only set the bloom filter to actively filter series out
		// if there were any segments that need the active block starts
		// updated.
		activeBlockStarts = m.backgroundCompactActiveBlockStarts
		activeBloomFilter = m.backgroundCompactIndexedSnapshot.ReadOnlyBloomFilter()

		// Now check which segments need filtering.
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

			activeBlockStartsOutdated := false
			for _, blockStart := range seg.containedBlockStarts {
				found := false
				for _, activeBlockStart := range activeBlockStarts {
					if activeBlockStart == blockStart {
						found = true
						break
					}
				}
				if !found {
					// Contains an active block start that should be removed.
					activeBlockStartsOutdated = true
					break
				}
			}

			if !activeBlockStartsOutdated {
				continue
			}

			// The active block starts are outdated, need to compact
			// and remove any old data from the segment.
			plan.Tasks = append(plan.Tasks, compaction.Task{
				Segments: []compaction.Segment{
					{
						Age:     seg.Age(),
						Size:    seg.Segment().Size(),
						Type:    segments.FSTType,
						Segment: seg.Segment(),
					},
				},
			})
		}
	}

	if len(plan.Tasks) == 0 {
		return
	}

	// Kick off compaction.
	m.compact.compactingBackground = true
	go func() {
		m.backgroundCompactWithPlan(plan, activeBlockStarts, activeBloomFilter)

		m.Lock()
		m.compact.compactingBackground = false
		m.cleanupBackgroundCompactWithLock()
		m.Unlock()
	}()
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

	backgroundCompactors := m.compact.backgroundCompactors
	close(backgroundCompactors)

	m.compact.backgroundCompactors = nil

	for compactor := range backgroundCompactors {
		if err := compactor.Close(); err != nil {
			instrument.EmitAndLogInvariantViolation(m.iopts, func(l *zap.Logger) {
				l.Error("error closing index block background compactor", zap.Error(err))
			})
		}
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
	activeBlockStarts []xtime.UnixNano,
	activeBloomFilter *bloom.ReadOnlyBloomFilter,
) {
	sw := m.metrics.backgroundCompactionPlanRunLatency.Start()
	defer sw.Stop()

	n := m.compact.numBackground
	m.compact.numBackground++

	logger := m.logger.With(
		zap.Time("blockStart", m.blockStart),
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
		compactor := <-m.compact.backgroundCompactors
		go func() {
			defer func() {
				m.compact.backgroundCompactors <- compactor
				wg.Done()
			}()
			err := m.backgroundCompactWithTask(task, activeBlockStarts,
				activeBloomFilter, compactor, log, logger.With(zap.Int("task", i)))
			if err != nil {
				instrument.EmitAndLogInvariantViolation(m.iopts, func(l *zap.Logger) {
					l.Error("error compacting segments", zap.Error(err))
				})
			}
		}()
	}

	wg.Wait()
}

func (m *mutableSegments) newReadThroughSegment(seg fst.Segment) segment.Segment {
	var (
		plCaches = ReadThroughSegmentCaches{
			SegmentPostingsListCache: m.opts.PostingsListCache(),
			SearchPostingsListCache:  m.opts.SearchPostingsListCache(),
		}
		readThroughOpts = m.opts.ReadThroughSegmentOptions()
	)
	return NewReadThroughSegment(seg, plCaches, readThroughOpts)
}

func (m *mutableSegments) backgroundCompactWithTask(
	task compaction.Task,
	activeBlockStarts []xtime.UnixNano,
	activeBloomFilter *bloom.ReadOnlyBloomFilter,
	compactor *compaction.Compactor,
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
	compacted, err := compactor.Compact(segments,
		activeBloomFilter,
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
	empty := err == compaction.ErrCompactorBuilderEmpty
	if empty {
		// Don't return the error since we need to remove the old segments
		// by calling addCompactedSegmentFromSegmentsWithLock.
		err = nil
	}
	if err != nil {
		return err
	}

	var replaceSegment segment.Segment
	if !empty {
		// Add a read through cache for repeated expensive queries against
		// background compacted segments since they can live for quite some
		// time and accrue a large set of documents.
		replaceSegment = m.newReadThroughSegment(compacted)
	}

	// Rotate out the replaced frozen segments and add the compacted one.
	m.Lock()
	defer m.Unlock()

	result := m.addCompactedSegmentFromSegmentsWithLock(m.backgroundSegments,
		segments, replaceSegment, activeBlockStarts)
	m.backgroundSegments = result

	return nil
}

func (m *mutableSegments) addCompactedSegmentFromSegmentsWithLock(
	current []*readableSeg,
	segmentsJustCompacted []segment.Segment,
	compacted segment.Segment,
	activeBlockStarts []xtime.UnixNano,
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
		// Compacted segment was empty.
		return result
	}

	// Return all the ones we kept plus the new compacted segment
	return append(result, newReadableSeg(compacted, activeBlockStarts, m.opts))
}

func (m *mutableSegments) foregroundCompactWithBuilder(
	builder segment.DocumentsBuilder,
	activeBlockStarts []xtime.UnixNano,
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
		zap.Time("blockStart", m.blockStart),
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
		activeBlockStarts, log, logger.With(zap.Int("task", 0)))
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
			activeBlockStarts, log, logger.With(zap.Int("task", i)))
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
	activeBlockStarts []xtime.UnixNano,
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
		segments, segment, activeBlockStarts)
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
	if !m.compact.compactingBackground {
		m.cleanupBackgroundCompactWithLock()
	}
}

// mutableSegmentsCompact has several lazily allocated compaction components.
type mutableSegmentsCompact struct {
	segmentBuilder       segment.CloseableDocumentsBuilder
	foregroundCompactor  *compaction.Compactor
	backgroundCompactors chan *compaction.Compactor
	compactingForeground bool
	compactingBackground bool
	numForeground        int
	numBackground        int
}

func (m *mutableSegmentsCompact) allocLazyBuilderAndCompactorsWithLock(
	concurrency int,
	blockOpts BlockOptions,
	opts Options,
) error {
	var (
		err      error
		docsPool = opts.DocumentArrayPool()
	)
	if m.segmentBuilder == nil {
		builderOpts := opts.SegmentBuilderOptions().
			SetConcurrency(concurrency)

		m.segmentBuilder, err = builder.NewBuilderFromDocuments(builderOpts)
		if err != nil {
			return err
		}
	}

	if m.foregroundCompactor == nil {
		m.foregroundCompactor, err = compaction.NewCompactor(docsPool,
			DocumentArrayPoolCapacity,
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

	if m.backgroundCompactors == nil {
		m.backgroundCompactors = make(chan *compaction.Compactor, numBackgroundCompactors)
		for i := 0; i < numBackgroundCompactors; i++ {
			backgroundCompactor, err := compaction.NewCompactor(docsPool,
				DocumentArrayPoolCapacity,
				opts.SegmentBuilderOptions(),
				opts.FSTSegmentOptions(),
				compaction.CompactorOptions{
					MmapDocsData: blockOpts.BackgroundCompactorMmapDocsData,
				})
			if err != nil {
				return err
			}
			m.backgroundCompactors <- backgroundCompactor
		}
	}

	return nil
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
