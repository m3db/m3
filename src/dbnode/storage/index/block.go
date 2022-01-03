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
	"io"
	"math"
	"runtime"
	"sync"
	"time"

	opentracinglog "github.com/opentracing/opentracing-go/log"
	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/limits"
	"github.com/m3db/m3/src/dbnode/tracepoint"
	"github.com/m3db/m3/src/m3ninx/doc"
	m3ninxindex "github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/m3ninx/search"
	"github.com/m3db/m3/src/m3ninx/search/executor"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xresource "github.com/m3db/m3/src/x/resource"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"
)

var (
	// ErrUnableToQueryBlockClosed is returned when querying closed block.
	ErrUnableToQueryBlockClosed = errors.New("unable to query, index block is closed")
	// ErrUnableReportStatsBlockClosed is returned from Stats when the block is closed.
	ErrUnableReportStatsBlockClosed = errors.New("unable to report stats, block is closed")

	errUnableToWriteBlockClosed     = errors.New("unable to write, index block is closed")
	errUnableToWriteBlockSealed     = errors.New("unable to write, index block is sealed")
	errUnableToBootstrapBlockClosed = errors.New("unable to bootstrap, block is closed")
	errUnableToTickBlockClosed      = errors.New("unable to tick, block is closed")
	errBlockAlreadyClosed           = errors.New("unable to close, block already closed")

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

	mmapIndexBlockName = "mmap.index.block"
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

type shardRangesSegmentsByVolumeType map[persist.IndexVolumeType][]blockShardRangesSegments

func (s shardRangesSegmentsByVolumeType) forEachSegment(cb func(segment segment.Segment) error) error {
	return s.forEachSegmentGroup(func(group blockShardRangesSegments) error {
		for _, seg := range group.segments {
			if err := cb(seg); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s shardRangesSegmentsByVolumeType) forEachSegmentGroup(cb func(group blockShardRangesSegments) error) error {
	for _, shardRangesSegments := range s {
		for _, group := range shardRangesSegments {
			if err := cb(group); err != nil {
				return err
			}
		}
	}
	return nil
}

type addAggregateResultsFn func(
	ctx context.Context,
	results AggregateResults,
	batch []AggregateResultsEntry,
	source []byte,
) ([]AggregateResultsEntry, int, int, error)

// nolint: maligned
type block struct {
	sync.RWMutex

	state blockState

	cachedSearchesWorkers xsync.WorkerPool

	mutableSegments                 *mutableSegments
	coldMutableSegments             []*mutableSegments
	shardRangesSegmentsByVolumeType shardRangesSegmentsByVolumeType
	newFieldsAndTermsIteratorFn     newFieldsAndTermsIteratorFn
	newExecutorWithRLockFn          newExecutorFn
	addAggregateResultsFn           addAggregateResultsFn
	blockStart                      xtime.UnixNano
	blockEnd                        xtime.UnixNano
	blockSize                       time.Duration
	opts                            Options
	iopts                           instrument.Options
	blockOpts                       BlockOptions
	nsMD                            namespace.Metadata
	namespaceRuntimeOptsMgr         namespace.RuntimeOptionsManager
	fetchDocsLimit                  limits.LookbackLimit
	aggDocsLimit                    limits.LookbackLimit

	metrics blockMetrics
	logger  *zap.Logger
}

type blockMetrics struct {
	rotateActiveSegment             tally.Counter
	rotateActiveSegmentAge          tally.Timer
	rotateActiveSegmentSize         tally.Histogram
	segmentFreeMmapSuccess          tally.Counter
	segmentFreeMmapError            tally.Counter
	segmentFreeMmapSkipNotImmutable tally.Counter
	querySeriesMatched              tally.Histogram
	queryDocsMatched                tally.Histogram
	aggregateSeriesMatched          tally.Histogram
	aggregateDocsMatched            tally.Histogram
	entryReconciledOnQuery          tally.Counter
	entryUnreconciledOnQuery        tally.Counter
}

func newBlockMetrics(s tally.Scope) blockMetrics {
	segmentFreeMmap := "segment-free-mmap"
	buckets := append(tally.ValueBuckets{0}, tally.MustMakeExponentialValueBuckets(100, 2, 16)...)
	return blockMetrics{
		rotateActiveSegment:     s.Counter("rotate-active-segment"),
		rotateActiveSegmentAge:  s.Timer("rotate-active-segment-age"),
		rotateActiveSegmentSize: s.Histogram("rotate-active-segment-size", buckets),
		segmentFreeMmapSuccess: s.Tagged(map[string]string{
			"result":    "success",
			"skip_type": "none",
		}).Counter(segmentFreeMmap),
		segmentFreeMmapError: s.Tagged(map[string]string{
			"result":    "error",
			"skip_type": "none",
		}).Counter(segmentFreeMmap),
		segmentFreeMmapSkipNotImmutable: s.Tagged(map[string]string{
			"result":    "skip",
			"skip_type": "not-immutable",
		}).Counter(segmentFreeMmap),

		querySeriesMatched:       s.Histogram("query-series-matched", buckets),
		queryDocsMatched:         s.Histogram("query-docs-matched", buckets),
		aggregateSeriesMatched:   s.Histogram("aggregate-series-matched", buckets),
		aggregateDocsMatched:     s.Histogram("aggregate-docs-matched", buckets),
		entryReconciledOnQuery:   s.Counter("entry-reconciled-on-query"),
		entryUnreconciledOnQuery: s.Counter("entry-unreconciled-on-query"),
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
	ActiveBlock                     bool
}

// NewBlockFn is a new block constructor.
type NewBlockFn func(
	blockStart xtime.UnixNano,
	md namespace.Metadata,
	blockOpts BlockOptions,
	namespaceRuntimeOptsMgr namespace.RuntimeOptionsManager,
	opts Options,
) (Block, error)

// Ensure NewBlock implements NewBlockFn.
var _ NewBlockFn = NewBlock

// NewBlock returns a new Block, representing a complete reverse index for the
// duration of time specified. It is backed by one or more segments.
func NewBlock(
	blockStart xtime.UnixNano,
	md namespace.Metadata,
	blockOpts BlockOptions,
	namespaceRuntimeOptsMgr namespace.RuntimeOptionsManager,
	opts Options,
) (Block, error) {
	blockSize := md.Options().IndexOptions().BlockSize()
	iopts := opts.InstrumentOptions()
	scope := iopts.MetricsScope().SubScope("index").SubScope("block")
	iopts = iopts.SetMetricsScope(scope)

	cpus := int(math.Max(1, math.Ceil(0.25*float64(runtime.GOMAXPROCS(0)))))
	cachedSearchesWorkers := xsync.NewWorkerPool(cpus)
	cachedSearchesWorkers.Init()

	segs := newMutableSegments(
		md,
		blockStart,
		opts,
		blockOpts,
		cachedSearchesWorkers,
		namespaceRuntimeOptsMgr,
		iopts,
	)

	coldSegs := newMutableSegments(
		md,
		blockStart,
		opts,
		blockOpts,
		cachedSearchesWorkers,
		namespaceRuntimeOptsMgr,
		iopts,
	)

	// NB(bodu): The length of coldMutableSegments is always at least 1.
	coldMutableSegments := []*mutableSegments{coldSegs}
	b := &block{
		state:                           blockStateOpen,
		blockStart:                      blockStart,
		blockEnd:                        blockStart.Add(blockSize),
		blockSize:                       blockSize,
		blockOpts:                       blockOpts,
		cachedSearchesWorkers:           cachedSearchesWorkers,
		mutableSegments:                 segs,
		coldMutableSegments:             coldMutableSegments,
		shardRangesSegmentsByVolumeType: make(shardRangesSegmentsByVolumeType),
		opts:                            opts,
		iopts:                           iopts,
		nsMD:                            md,
		namespaceRuntimeOptsMgr:         namespaceRuntimeOptsMgr,
		metrics:                         newBlockMetrics(scope),
		logger:                          iopts.Logger(),
		fetchDocsLimit:                  opts.QueryLimits().FetchDocsLimit(),
		aggDocsLimit:                    opts.QueryLimits().AggregateDocsLimit(),
	}
	b.newFieldsAndTermsIteratorFn = newFieldsAndTermsIterator
	b.newExecutorWithRLockFn = b.executorWithRLock
	b.addAggregateResultsFn = b.addAggregateResults

	return b, nil
}

func (b *block) StartTime() xtime.UnixNano {
	return b.blockStart
}

func (b *block) EndTime() xtime.UnixNano {
	return b.blockEnd
}

// BackgroundCompact background compacts eligible segments.
func (b *block) BackgroundCompact() {
	b.mutableSegments.BackgroundCompact()
}

func (b *block) WriteBatch(inserts *WriteBatch) (WriteBatchResult, error) {
	b.RLock()
	if !b.writesAcceptedWithRLock() {
		b.RUnlock()
		return b.writeBatchResult(inserts, MutableSegmentsStats{},
			b.writeBatchErrorInvalidState(b.state))
	}
	if b.state == blockStateSealed {
		coldBlock := b.coldMutableSegments[len(b.coldMutableSegments)-1]
		b.RUnlock()
		_, err := coldBlock.WriteBatch(inserts)
		// Don't pass stats back from insertion into a cold block,
		// we only care about warm mutable segments stats.
		return b.writeBatchResult(inserts, MutableSegmentsStats{}, err)
	}
	b.RUnlock()
	stats, err := b.mutableSegments.WriteBatch(inserts)
	return b.writeBatchResult(inserts, stats, err)
}

func (b *block) writeBatchResult(
	inserts *WriteBatch,
	stats MutableSegmentsStats,
	err error,
) (WriteBatchResult, error) {
	if err == nil {
		inserts.MarkUnmarkedEntriesSuccess()
		return WriteBatchResult{
			NumSuccess:           int64(inserts.Len()),
			MutableSegmentsStats: stats,
		}, nil
	}

	partialErr, ok := err.(*m3ninxindex.BatchPartialError)
	if !ok {
		// NB: marking all the inserts as failure, cause we don't know which ones failed.
		inserts.MarkUnmarkedEntriesError(err)
		return WriteBatchResult{
			NumError:             int64(inserts.Len()),
			MutableSegmentsStats: stats,
		}, err
	}

	numErr := len(partialErr.Errs())
	for _, err := range partialErr.Errs() {
		// Avoid marking these as success.
		inserts.MarkUnmarkedEntryError(err.Err, err.Idx)
	}

	// Mark all non-error inserts success, so we don't repeatedly index them.
	inserts.MarkUnmarkedEntriesSuccess()
	return WriteBatchResult{
		NumSuccess:           int64(inserts.Len() - numErr),
		NumError:             int64(numErr),
		MutableSegmentsStats: stats,
	}, partialErr
}

func (b *block) writesAcceptedWithRLock() bool {
	if b.state == blockStateOpen {
		return true
	}
	return b.state == blockStateSealed &&
		b.nsMD.Options().ColdWritesEnabled()
}

func (b *block) executorWithRLock() (search.Executor, error) {
	readers, err := b.segmentReadersWithRLock()
	if err != nil {
		return nil, err
	}

	indexReaders := make([]m3ninxindex.Reader, 0, len(readers))
	for _, r := range readers {
		indexReaders = append(indexReaders, r)
	}

	return executor.NewExecutor(indexReaders), nil
}

func (b *block) segmentReadersWithRLock() ([]segment.Reader, error) {
	expectedReaders := b.mutableSegments.Len()
	for _, coldSeg := range b.coldMutableSegments {
		expectedReaders += coldSeg.Len()
	}
	b.shardRangesSegmentsByVolumeType.forEachSegmentGroup(func(group blockShardRangesSegments) error {
		expectedReaders += len(group.segments)
		return nil
	})

	var (
		readers = make([]segment.Reader, 0, expectedReaders)
		success = false
		err     error
	)
	defer func() {
		// Cleanup in case any of the readers below fail.
		if !success {
			for _, reader := range readers {
				reader.Close()
			}
		}
	}()

	// Add mutable segments.
	readers, err = b.mutableSegments.AddReaders(readers)
	if err != nil {
		return nil, err
	}

	// Add cold mutable segments.
	for _, coldSeg := range b.coldMutableSegments {
		readers, err = coldSeg.AddReaders(readers)
		if err != nil {
			return nil, err
		}
	}

	// Loop over the segments associated to shard time ranges.
	if err := b.shardRangesSegmentsByVolumeType.forEachSegment(func(seg segment.Segment) error {
		reader, err := seg.Reader()
		if err != nil {
			return err
		}
		readers = append(readers, reader)
		return nil
	}); err != nil {
		return nil, err
	}

	success = true
	return readers, nil
}

// QueryIter acquires a read lock on the block to get the set of segments for the returned iterator. However, the
// segments are searched and results are processed lazily in the returned iterator. The segments are finalized when
// the ctx is finalized to ensure the mmaps are not freed until the ctx closes. This allows the returned results to
// reference data in the mmap without copying.
func (b *block) QueryIter(ctx context.Context, query Query) (QueryIterator, error) {
	b.RLock()
	defer b.RUnlock()

	if b.state == blockStateClosed {
		return nil, ErrUnableToQueryBlockClosed
	}
	exec, err := b.newExecutorWithRLockFn()
	if err != nil {
		return nil, err
	}

	// FOLLOWUP(prateek): push down QueryOptions to restrict results
	docIter, err := exec.Execute(ctx, query.Query.SearchQuery())
	if err != nil {
		b.closeAsync(exec)
		return nil, err
	}

	// Register the executor to close when context closes
	// so can avoid copying the results into the map and just take
	// references to it.
	ctx.RegisterFinalizer(xresource.FinalizerFn(func() {
		b.closeAsync(exec)
	}))

	return NewQueryIter(docIter), nil
}

// nolint: dupl
func (b *block) QueryWithIter(
	ctx context.Context,
	opts QueryOptions,
	iter QueryIterator,
	results DocumentResults,
	deadline time.Time,
	logFields []opentracinglog.Field,
) error {
	ctx, sp := ctx.StartTraceSpan(tracepoint.BlockQuery)
	sp.LogFields(logFields...)
	defer sp.Finish()

	err := b.queryWithSpan(ctx, opts, iter, results, deadline)
	if err != nil {
		sp.LogFields(opentracinglog.Error(err))
	}
	if iter.Done() {
		docs, series := iter.Counts()
		b.metrics.queryDocsMatched.RecordValue(float64(docs))
		b.metrics.querySeriesMatched.RecordValue(float64(series))
	}
	return err
}

func (b *block) queryWithSpan(
	ctx context.Context,
	opts QueryOptions,
	iter QueryIterator,
	results DocumentResults,
	deadline time.Time,
) error {
	var (
		err             error
		source          = opts.Source
		sizeBefore      = results.Size()
		docsCountBefore = results.TotalDocsCount()
		size            = sizeBefore
		docsCount       = docsCountBefore
		docsPool        = b.opts.DocumentArrayPool()
		batch           = docsPool.Get()
		batchSize       = cap(batch)
	)
	if batchSize == 0 {
		batchSize = defaultQueryDocsBatchSize
	}

	// Register local data structures that need closing.
	defer docsPool.Put(batch)

	for time.Now().Before(deadline) && iter.Next(ctx) {
		if opts.LimitsExceeded(size, docsCount) {
			break
		}

		// the caller (nsIndex) has canceled this before the query has timed out.
		// only check once per batch to limit the overhead. worst case nsIndex will need to wait for an additional batch
		// to be processed after the query timeout. we check when the batch is empty to cover 2 cases, the initial doc
		// when includes the search time, and subsequent batch resets.
		if len(batch) == 0 {
			select {
			case <-ctx.GoContext().Done():
				// indexNs will log something useful.
				return ctx.GoContext().Err()
			default:
			}
		}

		// Ensure that the block contains any of the relevant time segments for the query range.
		doc := iter.Current()
		if !b.docWithinQueryRange(doc, opts) {
			continue
		}

		batch = append(batch, doc)
		if len(batch) < batchSize {
			continue
		}

		batch, size, docsCount, err = b.addQueryResults(ctx, results, batch, source)
		if err != nil {
			return err
		}
	}
	if err := iter.Err(); err != nil {
		return err
	}

	// Add last batch to results if remaining.
	if len(batch) > 0 {
		batch, size, docsCount, err = b.addQueryResults(ctx, results, batch, source)
		if err != nil {
			return err
		}
	}

	iter.AddSeries(size - sizeBefore)
	iter.AddDocs(docsCount - docsCountBefore)

	return nil
}

func (b *block) docWithinQueryRange(doc doc.Document, opts QueryOptions) bool {
	md, ok := doc.Metadata()
	if !ok || md.OnIndexSeries == nil {
		return true
	}

	onIndexSeries, closer, reconciled := md.OnIndexSeries.ReconciledOnIndexSeries()
	if reconciled {
		b.metrics.entryReconciledOnQuery.Inc(1)
	} else {
		b.metrics.entryUnreconciledOnQuery.Inc(1)
	}

	defer closer.Close()

	var (
		inBlock                bool
		currentBlock           = opts.StartInclusive.Truncate(b.blockSize)
		endExclusive           = opts.EndExclusive
		minIndexed, maxIndexed = onIndexSeries.IndexedRange()
	)
	if maxIndexed == 0 {
		// Empty range.
		return false
	}

	// Narrow down the range of blocks to scan because the client could have
	// queried for an arbitrary wide range.
	if currentBlock.Before(minIndexed) {
		currentBlock = minIndexed
	}
	maxIndexedExclusive := maxIndexed.Add(time.Nanosecond)
	if endExclusive.After(maxIndexedExclusive) {
		endExclusive = maxIndexedExclusive
	}

	for !inBlock && currentBlock.Before(endExclusive) {
		inBlock = onIndexSeries.IndexedForBlockStart(currentBlock)
		currentBlock = currentBlock.Add(b.blockSize)
	}

	return inBlock
}

func (b *block) closeAsync(closer io.Closer) {
	if err := closer.Close(); err != nil {
		// Note: This only happens if closing the readers isn't clean.
		instrument.EmitAndLogInvariantViolation(
			b.iopts,
			func(l *zap.Logger) {
				l.Error("could not close query index block resource", zap.Error(err))
			})
	}
}

func (b *block) addQueryResults(
	ctx context.Context,
	results DocumentResults,
	batch []doc.Document,
	source []byte,
) ([]doc.Document, int, int, error) {
	// update recently queried docs to monitor memory.
	if results.EnforceLimits() {
		if err := b.fetchDocsLimit.Inc(len(batch), source); err != nil {
			return batch, 0, 0, err
		}
	}

	_, sp := ctx.StartTraceSpan(tracepoint.NSIdxBlockQueryAddDocuments)
	defer sp.Finish()
	// try to add the docs to the resource.
	size, docsCount, err := results.AddDocuments(batch)

	// reset batch.
	var emptyDoc doc.Document
	for i := range batch {
		batch[i] = emptyDoc
	}
	batch = batch[:0]

	// return results.
	return batch, size, docsCount, err
}

// AggregateIter acquires a read lock on the block to get the set of segments for the returned iterator. However, the
// segments are searched and results are processed lazily in the returned iterator. The segments are finalized when
// the ctx is finalized to ensure the mmaps are not freed until the ctx closes. This allows the returned results to
// reference data in the mmap without copying.
func (b *block) AggregateIter(ctx context.Context, aggOpts AggregateResultsOptions) (AggregateIterator, error) {
	b.RLock()
	defer b.RUnlock()

	if b.state == blockStateClosed {
		return nil, ErrUnableToQueryBlockClosed
	}

	iterateOpts := fieldsAndTermsIteratorOpts{
		restrictByQuery: aggOpts.RestrictByQuery,
		iterateTerms:    aggOpts.Type == AggregateTagNamesAndValues,
		allowFn: func(field []byte) bool {
			// skip any field names that we shouldn't allow.
			if bytes.Equal(field, doc.IDReservedFieldName) {
				return false
			}
			return aggOpts.FieldFilter.Allow(field)
		},
		fieldIterFn: func(r segment.Reader) (segment.FieldsPostingsListIterator, error) {
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
				return r.FieldsPostingsList()
			}
			return newFilterFieldsIterator(r, aggOpts.FieldFilter)
		},
	}
	readers, err := b.segmentReadersWithRLock()
	if err != nil {
		return nil, err
	}
	// Make sure to close readers at end of query since results can
	// include references to the underlying bytes from the index segment
	// read by the readers.
	for _, reader := range readers {
		reader := reader // Capture for inline function.
		ctx.RegisterFinalizer(xresource.FinalizerFn(func() {
			b.closeAsync(reader)
		}))
	}

	return &aggregateIter{
		readers:     readers,
		iterateOpts: iterateOpts,
		newIterFn:   b.newFieldsAndTermsIteratorFn,
	}, nil
}

// nolint: dupl
func (b *block) AggregateWithIter(
	ctx context.Context,
	iter AggregateIterator,
	opts QueryOptions,
	results AggregateResults,
	deadline time.Time,
	logFields []opentracinglog.Field,
) error {
	ctx, sp := ctx.StartTraceSpan(tracepoint.BlockAggregate)
	sp.LogFields(logFields...)
	defer sp.Finish()

	err := b.aggregateWithSpan(ctx, iter, opts, results, deadline)
	if err != nil {
		sp.LogFields(opentracinglog.Error(err))
	}
	if iter.Done() {
		docs, series := iter.Counts()
		b.metrics.aggregateDocsMatched.RecordValue(float64(docs))
		b.metrics.aggregateSeriesMatched.RecordValue(float64(series))
	}

	return err
}

func (b *block) aggregateWithSpan(
	ctx context.Context,
	iter AggregateIterator,
	opts QueryOptions,
	results AggregateResults,
	deadline time.Time,
) error {
	var (
		err           error
		source        = opts.Source
		size          = results.Size()
		docsCount     = results.TotalDocsCount()
		batch         = b.opts.AggregateResultsEntryArrayPool().Get()
		maxBatch      = cap(batch)
		fieldAppended bool
		termAppended  bool
		lastField     []byte
		batchedFields int
		currFields    int
		currTerms     int
	)
	if maxBatch == 0 {
		maxBatch = defaultAggregateResultsEntryBatchSize
	}

	// cleanup at the end
	defer b.opts.AggregateResultsEntryArrayPool().Put(batch)

	if opts.SeriesLimit > 0 && opts.SeriesLimit < maxBatch {
		maxBatch = opts.SeriesLimit
	}

	if opts.DocsLimit > 0 && opts.DocsLimit < maxBatch {
		maxBatch = opts.DocsLimit
	}

	for time.Now().Before(deadline) && iter.Next(ctx) {
		if opts.LimitsExceeded(size, docsCount) {
			break
		}

		// the caller (nsIndex) has canceled this before the query has timed out.
		// only check once per batch to limit the overhead. worst case nsIndex will need to wait for an additional
		// batch to be processed after the query timeout. we check when the batch is empty to cover 2 cases, the
		// initial result when includes the search time, and subsequent batch resets.
		if len(batch) == 0 {
			select {
			case <-ctx.GoContext().Done():
				return ctx.GoContext().Err()
			default:
			}
		}

		field, term := iter.Current()

		// TODO: remove this legacy doc tracking implementation when alternative
		// limits are in place.
		if results.EnforceLimits() {
			if lastField == nil {
				lastField = append(lastField, field...)
				batchedFields++
				if err := b.fetchDocsLimit.Inc(1, source); err != nil {
					return err
				}
			} else if !bytes.Equal(lastField, field) {
				lastField = lastField[:0]
				lastField = append(lastField, field...)
				batchedFields++
				if err := b.fetchDocsLimit.Inc(1, source); err != nil {
					return err
				}
			}

			// NB: this logic increments the doc count to account for where the
			// legacy limits would have been updated. It increments by two to
			// reflect the term appearing as both the last element of the previous
			// batch, as well as the first element in the next batch.
			if batchedFields > maxBatch {
				if err := b.fetchDocsLimit.Inc(2, source); err != nil {
					return err
				}

				batchedFields = 1
			}
		}

		batch, fieldAppended, termAppended = b.appendFieldAndTermToBatch(batch, field, term,
			iter.fieldsAndTermsIteratorOpts().iterateTerms)
		if fieldAppended {
			currFields++
		}
		if termAppended {
			currTerms++
		}
		// continue appending to the batch until we hit our max batch size.
		if currFields+currTerms < maxBatch {
			continue
		}

		batch, size, docsCount, err = b.addAggregateResultsFn(ctx, results, batch, source)
		if err != nil {
			return err
		}

		currFields = 0
		currTerms = 0
	}

	if err := iter.Err(); err != nil {
		return err
	}

	// Add last batch to results if remaining.
	for len(batch) > 0 {
		batch, size, docsCount, err = b.addAggregateResultsFn(ctx, results, batch, source)
		if err != nil {
			return err
		}
	}

	iter.AddSeries(size)
	iter.AddDocs(docsCount)

	return nil
}

// appendFieldAndTermToBatch adds the provided field / term onto the batch,
// optionally reusing the last element of the batch if it pertains to the same field.
// First boolean result indicates that a unique field was added to the batch
// and the second boolean indicates if a unique term was added.
func (b *block) appendFieldAndTermToBatch(
	batch []AggregateResultsEntry,
	field, term []byte,
	includeTerms bool,
) ([]AggregateResultsEntry, bool, bool) {
	// NB(prateek): we make a copy of the (field, term) entries returned
	// by the iterator during traversal, because the []byte are only valid per entry during
	// the traversal (i.e. calling Next() invalidates the []byte). We choose to do this
	// instead of checking if the entry is required (duplicates may exist in the results map
	// already), as it reduces contention on the map itself. Further, the ownership of these
	// idents is transferred to the results map, which either hangs on to them (if they are new),
	// or finalizes them if they are duplicates.
	var (
		entry                       AggregateResultsEntry
		lastField                   []byte
		lastFieldIsValid            bool
		reuseLastEntry              bool
		newFieldAdded, newTermAdded bool
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
		newFieldAdded = true
		// allocate id because this is the first time we've seen it
		// NB(r): Iterating fields FST, this byte slice is only temporarily available
		// since we are pushing/popping characters from the stack as we iterate
		// the fields FST and reusing the same byte slice.
		entry.Field = b.pooledID(field)
	}

	if includeTerms {
		newTermAdded = true
		// terms are always new (as far we know without checking the map for duplicates), so we allocate
		// NB(r): Iterating terms FST, this byte slice is only temporarily available
		// since we are pushing/popping characters from the stack as we iterate
		// the terms FST and reusing the same byte slice.
		entry.Terms = append(entry.Terms, b.pooledID(term))
	}

	if reuseLastEntry {
		batch[len(batch)-1] = entry
	} else {
		batch = append(batch, entry)
	}

	return batch, newFieldAdded, newTermAdded
}

func (b *block) pooledID(id []byte) ident.ID {
	data := b.opts.CheckedBytesPool().Get(len(id))
	data.IncRef()
	data.AppendAll(id)
	data.DecRef()
	return b.opts.IdentifierPool().BinaryID(data)
}

// addAggregateResults adds the fields on the batch
// to the provided results and resets the batch to be reused.
func (b *block) addAggregateResults(
	ctx context.Context,
	results AggregateResults,
	batch []AggregateResultsEntry,
	source []byte,
) ([]AggregateResultsEntry, int, int, error) {
	_, sp := ctx.StartTraceSpan(tracepoint.NSIdxBlockAggregateQueryAddDocuments)
	defer sp.Finish()
	// try to add the docs to the resource.
	size, docsCount := results.AddFields(batch)

	aggDocs := len(batch)
	for i := range batch {
		aggDocs += len(batch[i].Terms)
	}

	// update recently queried docs to monitor memory.
	if results.EnforceLimits() {
		if err := b.aggDocsLimit.Inc(aggDocs, source); err != nil {
			return batch, 0, 0, err
		}
	}

	// reset batch.
	var emptyField AggregateResultsEntry
	for i := range batch {
		batch[i] = emptyField
	}
	batch = batch[:0]

	// return results.
	return batch, size, docsCount, nil
}

func (b *block) AddResults(
	resultsByVolumeType result.IndexBlockByVolumeType,
) error {
	b.Lock()
	defer b.Unlock()

	multiErr := xerrors.NewMultiError()
	for volumeType, results := range resultsByVolumeType.Iter() {
		multiErr = multiErr.Add(b.addResults(volumeType, results))
	}

	return multiErr.FinalError()
}

func (b *block) addResults(
	volumeType persist.IndexVolumeType,
	results result.IndexBlock,
) error {
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

	shardRangesSegments, ok := b.shardRangesSegmentsByVolumeType[volumeType]
	if !ok {
		shardRangesSegments = make([]blockShardRangesSegments, 0)
		b.shardRangesSegmentsByVolumeType[volumeType] = shardRangesSegments
	}

	var (
		plCaches = ReadThroughSegmentCaches{
			SegmentPostingsListCache: b.opts.PostingsListCache(),
			SearchPostingsListCache:  b.opts.SearchPostingsListCache(),
		}
		readThroughOpts = b.opts.ReadThroughSegmentOptions()
		segments        = results.Segments()
	)
	readThroughSegments := make([]segment.Segment, 0, len(segments))
	for _, seg := range segments {
		elem := seg.Segment()
		if immSeg, ok := elem.(segment.ImmutableSegment); ok {
			// only wrap the immutable segments with a read through cache.
			elem = NewReadThroughSegment(immSeg, plCaches, readThroughOpts)
		}
		readThroughSegments = append(readThroughSegments, elem)
	}

	entry := blockShardRangesSegments{
		shardTimeRanges: results.Fulfilled(),
		segments:        readThroughSegments,
	}

	// first see if this block can cover all our current blocks covering shard
	// time ranges.
	currFulfilled := result.NewShardTimeRanges()
	for _, existing := range shardRangesSegments {
		currFulfilled.AddRanges(existing.shardTimeRanges)
	}

	unfulfilledBySegments := currFulfilled.Copy()
	unfulfilledBySegments.Subtract(results.Fulfilled())
	if !unfulfilledBySegments.IsEmpty() {
		// This is the case where it cannot wholly replace the current set of blocks
		// so simply append the segments in this case.
		b.shardRangesSegmentsByVolumeType[volumeType] = append(shardRangesSegments, entry)
		return nil
	}

	// This is the case where the new segments can wholly replace the
	// current set of blocks since unfullfilled by the new segments is zero.
	multiErr := xerrors.NewMultiError()
	for i, group := range shardRangesSegments {
		for _, seg := range group.segments {
			// Make sure to close the existing segments.
			multiErr = multiErr.Add(seg.Close())
		}
		shardRangesSegments[i] = blockShardRangesSegments{}
	}
	b.shardRangesSegmentsByVolumeType[volumeType] = append(shardRangesSegments[:0], entry)

	return multiErr.FinalError()
}

func (b *block) Tick(c context.Cancellable) (BlockTickResult, error) {
	b.Lock()
	defer b.Unlock()
	result := BlockTickResult{}
	if b.state == blockStateClosed {
		return result, errUnableToTickBlockClosed
	}

	// Add foreground/background segments.
	numSegments, numDocs := b.mutableSegments.NumSegmentsAndDocs()
	for _, coldSeg := range b.coldMutableSegments {
		coldNumSegments, coldNumDocs := coldSeg.NumSegmentsAndDocs()
		numSegments += coldNumSegments
		numDocs += coldNumDocs
	}
	result.NumSegments += numSegments
	result.NumSegmentsMutable += numSegments
	result.NumDocs += numDocs

	multiErr := xerrors.NewMultiError()

	// Any segments covering persisted shard ranges.
	b.shardRangesSegmentsByVolumeType.forEachSegment(func(seg segment.Segment) error {
		result.NumSegments++
		result.NumSegmentsBootstrapped++
		result.NumDocs += seg.Size()

		immSeg, ok := seg.(segment.ImmutableSegment)
		if !ok {
			b.metrics.segmentFreeMmapSkipNotImmutable.Inc(1)
			return nil
		}

		// TODO(bodu): Revist this and implement a more sophisticated free strategy.
		if err := immSeg.FreeMmap(); err != nil {
			multiErr = multiErr.Add(err)
			b.metrics.segmentFreeMmapError.Inc(1)
			return nil
		}

		result.FreeMmap++
		b.metrics.segmentFreeMmapSuccess.Inc(1)
		return nil
	})

	return result, multiErr.FinalError()
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

	b.mutableSegments.Stats(reporter)
	for _, coldSeg := range b.coldMutableSegments {
		// TODO(bodu): Cold segment stats should prob be of a
		// diff type or something.
		coldSeg.Stats(reporter)
	}

	b.shardRangesSegmentsByVolumeType.forEachSegment(func(seg segment.Segment) error {
		_, mutable := seg.(segment.MutableSegment)
		reporter.ReportSegmentStats(BlockSegmentStats{
			Type:    FlushedSegment,
			Mutable: mutable,
			Size:    seg.Size(),
		})
		return nil
	})
	return nil
}

func (b *block) IsOpen() bool {
	b.RLock()
	defer b.RUnlock()
	return b.state == blockStateOpen
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

	// Check any mutable segments that can be evicted after a flush.
	anyMutableSegmentNeedsEviction := b.mutableSegments.NeedsEviction()

	// Check bootstrapped segments and to see if any of them need an eviction.
	b.shardRangesSegmentsByVolumeType.forEachSegment(func(seg segment.Segment) error {
		if mutableSeg, ok := seg.(segment.MutableSegment); ok {
			anyMutableSegmentNeedsEviction = anyMutableSegmentNeedsEviction || mutableSeg.Size() > 0
		}
		return nil
	})

	return anyMutableSegmentNeedsEviction
}

func (b *block) EvictMutableSegments() error {
	b.Lock()
	defer b.Unlock()
	if b.state != blockStateSealed {
		return fmt.Errorf("unable to evict mutable segments, block must be sealed, found: %v", b.state)
	}

	b.mutableSegments.Close()

	// Close any other mutable segments that was added.
	multiErr := xerrors.NewMultiError()
	for _, shardRangesSegments := range b.shardRangesSegmentsByVolumeType {
		for idx := range shardRangesSegments {
			segments := make([]segment.Segment, 0, len(shardRangesSegments[idx].segments))
			for _, seg := range shardRangesSegments[idx].segments {
				mutableSeg, ok := seg.(segment.MutableSegment)
				if !ok {
					segments = append(segments, seg)
					continue
				}
				multiErr = multiErr.Add(mutableSeg.Close())
			}
			shardRangesSegments[idx].segments = segments
		}
	}

	return multiErr.FinalError()
}

func (b *block) NeedsColdMutableSegmentsEvicted() bool {
	b.RLock()
	defer b.RUnlock()
	var anyColdMutableSegmentNeedsEviction bool
	for _, coldSeg := range b.coldMutableSegments {
		anyColdMutableSegmentNeedsEviction = anyColdMutableSegmentNeedsEviction || coldSeg.NeedsEviction()
	}
	return b.state == blockStateSealed && anyColdMutableSegmentNeedsEviction
}

func (b *block) EvictColdMutableSegments() error {
	b.Lock()
	defer b.Unlock()
	if b.state != blockStateSealed {
		return fmt.Errorf("unable to evict cold mutable segments, block must be sealed, found: %v", b.state)
	}

	// Evict/remove all but the most recent cold mutable segment (That is the one we are actively writing to).
	for i, coldSeg := range b.coldMutableSegments {
		if i < len(b.coldMutableSegments)-1 {
			coldSeg.Close()
			b.coldMutableSegments[i] = nil
		}
	}
	// Swap last w/ first and truncate the slice.
	lastIdx := len(b.coldMutableSegments) - 1
	b.coldMutableSegments[0], b.coldMutableSegments[lastIdx] = b.coldMutableSegments[lastIdx], b.coldMutableSegments[0]
	b.coldMutableSegments = b.coldMutableSegments[:1]
	return nil
}

func (b *block) RotateColdMutableSegments() error {
	b.Lock()
	defer b.Unlock()
	coldSegs := newMutableSegments(
		b.nsMD,
		b.blockStart,
		b.opts,
		b.blockOpts,
		b.cachedSearchesWorkers,
		b.namespaceRuntimeOptsMgr,
		b.iopts,
	)
	b.coldMutableSegments = append(b.coldMutableSegments, coldSegs)
	return nil
}

func (b *block) MemorySegmentsData(ctx context.Context) ([]fst.SegmentData, error) {
	b.RLock()
	defer b.RUnlock()
	if b.state == blockStateClosed {
		return nil, errBlockAlreadyClosed
	}
	data, err := b.mutableSegments.MemorySegmentsData(ctx)
	if err != nil {
		return nil, err
	}
	for _, coldSeg := range b.coldMutableSegments {
		coldData, err := coldSeg.MemorySegmentsData(ctx)
		if err != nil {
			return nil, err
		}
		data = append(data, coldData...)
	}
	return data, nil
}

func (b *block) Close() error {
	b.Lock()
	defer b.Unlock()
	if b.state == blockStateClosed {
		return errBlockAlreadyClosed
	}
	b.state = blockStateClosed

	b.mutableSegments.Close()
	for _, coldSeg := range b.coldMutableSegments {
		coldSeg.Close()
	}

	// Close any other added segments too.
	var multiErr xerrors.MultiError
	b.shardRangesSegmentsByVolumeType.forEachSegment(func(seg segment.Segment) error {
		multiErr = multiErr.Add(seg.Close())
		return nil
	})

	for volumeType := range b.shardRangesSegmentsByVolumeType {
		b.shardRangesSegmentsByVolumeType[volumeType] = nil
	}

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
