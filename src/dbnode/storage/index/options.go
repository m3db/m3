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

	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/dbnode/storage/limits"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index/segment/builder"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/m3ninx/index/segment/mem"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/mmap"
	"github.com/m3db/m3/src/x/pool"
)

const (
	// defaultIndexInsertMode sets the default indexing mode to synchronous.
	defaultIndexInsertMode = InsertSync

	// metadataArrayPool size in general: 256*256*sizeof(doc.Metadata)
	// = 256 * 256 * 48
	// =~ 3mb
	// TODO(r): Make this configurable in a followup change.
	metadataArrayPoolSize = 256
	// MetadataArrayPoolCapacity is the capacity of the metadata array pool.
	MetadataArrayPoolCapacity    = 256
	metadataArrayPoolMaxCapacity = 256 // Do not allow grows, since we know the size

	// documentArrayPool size in general: 256*256*sizeof(doc.Document)
	// = 256 * 256 * 80
	// =~ 5mb
	documentArrayPoolSize = 256
	// DocumentArrayPoolCapacity is the capacity of the encoded document array pool.
	DocumentArrayPoolCapacity    = 256
	documentArrayPoolMaxCapacity = 256 // Do not allow grows, since we know the size

	// aggregateResultsEntryArrayPool size in general: 256*256*sizeof(doc.Field)
	// = 256 * 256 * 48
	// =~ 3mb
	// TODO(prateek): Make this configurable in a followup change.
	aggregateResultsEntryArrayPoolSize        = 256
	aggregateResultsEntryArrayPoolCapacity    = 256
	aggregateResultsEntryArrayPoolMaxCapacity = 256 // Do not allow grows, since we know the size
)

var (
	errOptionsIdentifierPoolUnspecified      = errors.New("identifier pool is unset")
	errOptionsBytesPoolUnspecified           = errors.New("checkedbytes pool is unset")
	errOptionsResultsPoolUnspecified         = errors.New("results pool is unset")
	errOptionsAggResultsPoolUnspecified      = errors.New("aggregate results pool is unset")
	errOptionsAggValuesPoolUnspecified       = errors.New("aggregate values pool is unset")
	errOptionsDocPoolUnspecified             = errors.New("docs array pool is unset")
	errOptionsDocContainerPoolUnspecified    = errors.New("doc container array pool is unset")
	errOptionsAggResultsEntryPoolUnspecified = errors.New("aggregate results entry array pool is unset")
	errIDGenerationDisabled                  = errors.New("id generation is disabled")
	errPostingsListCacheUnspecified          = errors.New("postings list cache is unset")

	defaultForegroundCompactionOpts compaction.PlannerOptions
	defaultBackgroundCompactionOpts compaction.PlannerOptions
)

func init() {
	// Foreground compaction opts are the same as background compaction
	// but with only a single level, 1/4 the size of the first level of
	// the background compaction.
	defaultForegroundCompactionOpts = compaction.DefaultOptions
	defaultForegroundCompactionOpts.Levels = []compaction.Level{
		{
			MinSizeInclusive: 0,
			MaxSizeExclusive: 1 << 12,
		},
	}
	defaultBackgroundCompactionOpts = compaction.DefaultOptions
	defaultBackgroundCompactionOpts.Levels = []compaction.Level{
		{
			MinSizeInclusive: 0,
			MaxSizeExclusive: 1 << 18,
		},
		{
			MinSizeInclusive: 1 << 18,
			MaxSizeExclusive: 1 << 20,
		},
		{
			MinSizeInclusive: 1 << 20,
			MaxSizeExclusive: 1 << 22,
		},
	}
}

// nolint: maligned
type options struct {
	forwardIndexThreshold                   float64
	forwardIndexProbability                 float64
	insertMode                              InsertMode
	clockOpts                               clock.Options
	instrumentOpts                          instrument.Options
	builderOpts                             builder.Options
	memOpts                                 mem.Options
	fstOpts                                 fst.Options
	idPool                                  ident.Pool
	bytesPool                               pool.CheckedBytesPool
	resultsPool                             QueryResultsPool
	aggResultsPool                          AggregateResultsPool
	aggValuesPool                           AggregateValuesPool
	docArrayPool                            doc.DocumentArrayPool
	metadataArrayPool                       doc.MetadataArrayPool
	aggResultsEntryArrayPool                AggregateResultsEntryArrayPool
	foregroundCompactionPlannerOpts         compaction.PlannerOptions
	backgroundCompactionPlannerOpts         compaction.PlannerOptions
	postingsListCache                       *PostingsListCache
	searchPostingsListCache                 *PostingsListCache
	readThroughSegmentOptions               ReadThroughSegmentOptions
	aggregateFieldFilterRegexCompileEnabled bool
	mmapReporter                            mmap.Reporter
	queryLimits                             limits.QueryLimits
}

var undefinedUUIDFn = func() ([]byte, error) { return nil, errIDGenerationDisabled }

// NewOptions returns a new Options object with default properties.
func NewOptions() Options {
	resultsPool := NewQueryResultsPool(pool.NewObjectPoolOptions())
	aggResultsPool := NewAggregateResultsPool(pool.NewObjectPoolOptions())
	aggValuesPool := NewAggregateValuesPool(pool.NewObjectPoolOptions())

	bytesPool := pool.NewCheckedBytesPool(nil, nil, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, nil)
	})
	bytesPool.Init()

	idPool := ident.NewPool(bytesPool, ident.PoolOptions{})

	docArrayPool := doc.NewDocumentArrayPool(doc.DocumentArrayPoolOpts{
		Options: pool.NewObjectPoolOptions().
			SetSize(documentArrayPoolSize),
		Capacity:    DocumentArrayPoolCapacity,
		MaxCapacity: documentArrayPoolMaxCapacity,
	})
	docArrayPool.Init()

	metadataArrayPool := doc.NewMetadataArrayPool(doc.MetadataArrayPoolOpts{
		Options: pool.NewObjectPoolOptions().
			SetSize(metadataArrayPoolSize),
		Capacity:    MetadataArrayPoolCapacity,
		MaxCapacity: metadataArrayPoolMaxCapacity,
	})
	metadataArrayPool.Init()

	aggResultsEntryArrayPool := NewAggregateResultsEntryArrayPool(AggregateResultsEntryArrayPoolOpts{
		Options: pool.NewObjectPoolOptions().
			SetSize(aggregateResultsEntryArrayPoolSize),
		Capacity:    aggregateResultsEntryArrayPoolCapacity,
		MaxCapacity: aggregateResultsEntryArrayPoolMaxCapacity,
	})
	aggResultsEntryArrayPool.Init()

	instrumentOpts := instrument.NewOptions()
	opts := &options{
		insertMode:                      defaultIndexInsertMode,
		clockOpts:                       clock.NewOptions(),
		instrumentOpts:                  instrumentOpts,
		builderOpts:                     builder.NewOptions().SetNewUUIDFn(undefinedUUIDFn),
		memOpts:                         mem.NewOptions().SetNewUUIDFn(undefinedUUIDFn),
		fstOpts:                         fst.NewOptions().SetInstrumentOptions(instrumentOpts),
		bytesPool:                       bytesPool,
		idPool:                          idPool,
		resultsPool:                     resultsPool,
		aggResultsPool:                  aggResultsPool,
		aggValuesPool:                   aggValuesPool,
		docArrayPool:                    docArrayPool,
		metadataArrayPool:               metadataArrayPool,
		aggResultsEntryArrayPool:        aggResultsEntryArrayPool,
		foregroundCompactionPlannerOpts: defaultForegroundCompactionOpts,
		backgroundCompactionPlannerOpts: defaultBackgroundCompactionOpts,
		queryLimits:                     limits.NoOpQueryLimits(),
	}
	resultsPool.Init(func() QueryResults {
		return NewQueryResults(nil, QueryResultsOptions{}, opts)
	})
	aggResultsPool.Init(func() AggregateResults {
		return NewAggregateResults(nil, AggregateResultsOptions{}, opts)
	})
	aggValuesPool.Init(func() AggregateValues { return NewAggregateValues(opts) })
	return opts
}

func (o *options) Validate() error {
	if o.idPool == nil {
		return errOptionsIdentifierPoolUnspecified
	}
	if o.bytesPool == nil {
		return errOptionsBytesPoolUnspecified
	}
	if o.resultsPool == nil {
		return errOptionsResultsPoolUnspecified
	}
	if o.aggResultsPool == nil {
		return errOptionsAggResultsPoolUnspecified
	}
	if o.aggValuesPool == nil {
		return errOptionsAggValuesPoolUnspecified
	}
	if o.docArrayPool == nil {
		return errOptionsDocPoolUnspecified
	}
	if o.metadataArrayPool == nil {
		return errOptionsDocContainerPoolUnspecified
	}
	if o.aggResultsEntryArrayPool == nil {
		return errOptionsAggResultsEntryPoolUnspecified
	}
	if o.postingsListCache == nil {
		return errPostingsListCacheUnspecified
	}
	return nil
}

func (o *options) SetInsertMode(value InsertMode) Options {
	opts := *o
	opts.insertMode = value
	return &opts
}

func (o *options) InsertMode() InsertMode {
	return o.insertMode
}

func (o *options) SetClockOptions(value clock.Options) Options {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *options) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	memOpts := opts.MemSegmentOptions().SetInstrumentOptions(value)
	fstOpts := opts.FSTSegmentOptions().SetInstrumentOptions(value)
	opts.instrumentOpts = value
	opts.memOpts = memOpts
	opts.fstOpts = fstOpts
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetSegmentBuilderOptions(value builder.Options) Options {
	opts := *o
	opts.builderOpts = value
	return &opts
}

func (o *options) SegmentBuilderOptions() builder.Options {
	return o.builderOpts
}

func (o *options) SetMemSegmentOptions(value mem.Options) Options {
	opts := *o
	opts.memOpts = value
	return &opts
}

func (o *options) MemSegmentOptions() mem.Options {
	return o.memOpts
}

func (o *options) SetFSTSegmentOptions(value fst.Options) Options {
	opts := *o
	opts.fstOpts = value
	return &opts
}

func (o *options) FSTSegmentOptions() fst.Options {
	return o.fstOpts
}

func (o *options) SetIdentifierPool(value ident.Pool) Options {
	opts := *o
	opts.idPool = value
	return &opts
}

func (o *options) IdentifierPool() ident.Pool {
	return o.idPool
}

func (o *options) SetCheckedBytesPool(value pool.CheckedBytesPool) Options {
	opts := *o
	opts.bytesPool = value
	return &opts
}

func (o *options) CheckedBytesPool() pool.CheckedBytesPool {
	return o.bytesPool
}

func (o *options) SetQueryResultsPool(value QueryResultsPool) Options {
	opts := *o
	opts.resultsPool = value
	return &opts
}

func (o *options) QueryResultsPool() QueryResultsPool {
	return o.resultsPool
}

func (o *options) SetAggregateResultsPool(value AggregateResultsPool) Options {
	opts := *o
	opts.aggResultsPool = value
	return &opts
}

func (o *options) AggregateResultsPool() AggregateResultsPool {
	return o.aggResultsPool
}

func (o *options) SetAggregateValuesPool(value AggregateValuesPool) Options {
	opts := *o
	opts.aggValuesPool = value
	return &opts
}

func (o *options) AggregateValuesPool() AggregateValuesPool {
	return o.aggValuesPool
}

func (o *options) SetDocumentArrayPool(value doc.DocumentArrayPool) Options {
	opts := *o
	opts.docArrayPool = value
	return &opts
}

func (o *options) DocumentArrayPool() doc.DocumentArrayPool {
	return o.docArrayPool
}

func (o *options) SetMetadataArrayPool(value doc.MetadataArrayPool) Options {
	opts := *o // nolint:govet
	opts.metadataArrayPool = value
	return &opts
}

func (o *options) MetadataArrayPool() doc.MetadataArrayPool {
	return o.metadataArrayPool
}

func (o *options) SetAggregateResultsEntryArrayPool(value AggregateResultsEntryArrayPool) Options {
	opts := *o
	opts.aggResultsEntryArrayPool = value
	return &opts
}

func (o *options) AggregateResultsEntryArrayPool() AggregateResultsEntryArrayPool {
	return o.aggResultsEntryArrayPool
}

func (o *options) SetForegroundCompactionPlannerOptions(value compaction.PlannerOptions) Options {
	opts := *o
	opts.foregroundCompactionPlannerOpts = value
	return &opts
}

func (o *options) ForegroundCompactionPlannerOptions() compaction.PlannerOptions {
	return o.foregroundCompactionPlannerOpts
}

func (o *options) SetBackgroundCompactionPlannerOptions(value compaction.PlannerOptions) Options {
	opts := *o
	opts.backgroundCompactionPlannerOpts = value
	return &opts
}

func (o *options) BackgroundCompactionPlannerOptions() compaction.PlannerOptions {
	return o.backgroundCompactionPlannerOpts
}

func (o *options) SetPostingsListCache(value *PostingsListCache) Options {
	opts := *o
	opts.postingsListCache = value
	return &opts
}

func (o *options) PostingsListCache() *PostingsListCache {
	return o.postingsListCache
}

func (o *options) SetSearchPostingsListCache(value *PostingsListCache) Options {
	opts := *o
	opts.searchPostingsListCache = value
	return &opts
}

func (o *options) SearchPostingsListCache() *PostingsListCache {
	return o.searchPostingsListCache
}

func (o *options) SetReadThroughSegmentOptions(value ReadThroughSegmentOptions) Options {
	opts := *o
	opts.readThroughSegmentOptions = value
	return &opts
}

func (o *options) ReadThroughSegmentOptions() ReadThroughSegmentOptions {
	return o.readThroughSegmentOptions
}

func (o *options) SetForwardIndexProbability(value float64) Options {
	opts := *o
	opts.forwardIndexProbability = value
	return &opts
}

func (o *options) ForwardIndexProbability() float64 {
	return o.forwardIndexProbability
}

func (o *options) SetForwardIndexThreshold(value float64) Options {
	opts := *o
	opts.forwardIndexThreshold = value
	return &opts
}

func (o *options) ForwardIndexThreshold() float64 {
	return o.forwardIndexThreshold
}

func (o *options) SetAggregateFieldFilterRegexCompileEnabled(value bool) Options {
	opts := *o
	opts.aggregateFieldFilterRegexCompileEnabled = value
	return &opts
}

func (o *options) AggregateFieldFilterRegexCompileEnabled() bool {
	return o.aggregateFieldFilterRegexCompileEnabled
}

func (o *options) SetMmapReporter(mmapReporter mmap.Reporter) Options {
	opts := *o
	opts.mmapReporter = mmapReporter
	return &opts
}

func (o *options) MmapReporter() mmap.Reporter {
	return o.mmapReporter
}

func (o *options) SetQueryLimits(value limits.QueryLimits) Options {
	opts := *o
	opts.queryLimits = value
	return &opts
}

func (o *options) QueryLimits() limits.QueryLimits {
	return o.queryLimits
}
