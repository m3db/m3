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

package m3

import (
	"context"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	queryts "github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	xresource "github.com/m3db/m3/src/x/resource"
	"github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"
)

// Cleanup is a cleanup function to be called after resources are freed.
type Cleanup func() error

func noop() error {
	return nil
}

// Storage provides an interface for reading and writing to the TSDB.
type Storage interface {
	storage.Storage
	Querier
}

// Querier handles queries against an M3 instance.
type Querier interface {
	// FetchCompressedResult fetches timeseries data based on a query.
	FetchCompressedResult(
		ctx context.Context,
		query *storage.FetchQuery,
		options *storage.FetchOptions,
	) (consolidators.SeriesFetchResult, Cleanup, error)

	// SearchCompressed fetches matching tags based on a query.
	SearchCompressed(
		ctx context.Context,
		query *storage.FetchQuery,
		options *storage.FetchOptions,
	) (consolidators.TagResult, Cleanup, error)

	// CompleteTagsCompressed returns autocompleted tag results.
	CompleteTagsCompressed(
		ctx context.Context,
		query *storage.CompleteTagsQuery,
		options *storage.FetchOptions,
	) (*consolidators.CompleteTagsResult, error)
}

// DynamicClusterNamespaceConfiguration is the configuration for
// dynamically fetching namespace configuration.
type DynamicClusterNamespaceConfiguration struct {
	// session is an active session connected to an M3DB cluster.
	session client.Session

	// nsInitializer is the initializer used to watch for namespace changes.
	nsInitializer namespace.Initializer
}

// DynamicClusterOptions is the options for a new dynamic Cluster.
type DynamicClusterOptions interface {
	// Validate validates the DynamicClusterOptions.
	Validate() error

	// SetDynamicClusterNamespaceConfiguration sets the configuration for the dynamically fetching cluster namespaces.
	SetDynamicClusterNamespaceConfiguration(value []DynamicClusterNamespaceConfiguration) DynamicClusterOptions

	// DynamicClusterNamespaceConfiguration returns the configuration for the dynamically fetching cluster namespaces.
	DynamicClusterNamespaceConfiguration() []DynamicClusterNamespaceConfiguration

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) DynamicClusterOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetClusterNamespacesWatcher sets the namespaces watcher which alerts components that
	// need to regenerate configuration when the ClusterNamespaces change.
	SetClusterNamespacesWatcher(value ClusterNamespacesWatcher) DynamicClusterOptions

	// ClusterNamespacesWatcher returns the namespaces watcher which alerts components that
	// need to regenerate configuration when the ClusterNamespaces change.
	ClusterNamespacesWatcher() ClusterNamespacesWatcher
}

// ClusterNamespacesWatcher allows interested parties to watch for changes
// to the cluster namespaces and register callbacks to be invoked
// when changes are detected.
type ClusterNamespacesWatcher interface {
	// Update updates the current namespaces.
	Update(namespaces ClusterNamespaces) error

	// Get returns the current namespaces.
	Get() ClusterNamespaces

	// RegisterListener registers a listener for updates to cluster namespaces.
	// If a value is currently present, it will synchronously call back the listener.
	RegisterListener(listener ClusterNamespacesListener) xresource.SimpleCloser

	// Close closes the watcher and all descendent watches.
	Close()
}

// ClusterNamespacesListener is a listener for receiving updates from a
// ClusterNamespacesWatcher.
type ClusterNamespacesListener interface {
	// OnUpdate is called when updates have occurred passing in the new namespaces.
	OnUpdate(namespaces ClusterNamespaces)
}

// Options describes the options for encoded block converters.
// These options are generally config-backed and don't usually change across
// queries, unless certain query string parameters are present.
type Options interface {
	// SetSplitSeriesByBlock determines if the converter will split the series
	// by blocks, or if it will instead treat the entire series as a single block.
	SetSplitSeriesByBlock(bool) Options
	// SplittingSeriesByBlock returns true iff lookback duration is 0, and the
	// options has not been forced to return a single block.
	SplittingSeriesByBlock() bool
	// SetLookbackDuration sets the lookback duration.
	SetLookbackDuration(time.Duration) Options
	// LookbackDuration returns the lookback duration.
	LookbackDuration() time.Duration
	// SetConsolidationFunc sets the consolidation function for the converter.
	SetConsolidationFunc(consolidators.ConsolidationFunc) Options
	// ConsolidationFunc returns the consolidation function.
	ConsolidationFunc() consolidators.ConsolidationFunc
	// SetTagOptions sets the tag options for the converter.
	SetTagOptions(models.TagOptions) Options
	// TagOptions returns the tag options.
	TagOptions() models.TagOptions
	// TagsTransform returns the transform to apply to tags before storage.
	TagsTransform() TagsTransform
	// SetTagsTransform sets the TagsTransform.
	SetTagsTransform(value TagsTransform) Options
	// SetRateLimiter sets the RateLimiter
	SetRateLimiter(value RateLimiter) Options
	// RateLimiter returns the rate limiter.
	RateLimiter() RateLimiter
	// SetIteratorPools sets the iterator pools for the converter.
	SetIteratorPools(encoding.IteratorPools) Options
	// IteratorPools returns the iterator pools for the converter.
	IteratorPools() encoding.IteratorPools
	// SetCheckedBytesPool sets the checked bytes pool for the converter.
	SetCheckedBytesPool(pool.CheckedBytesPool) Options
	// CheckedBytesPool returns the checked bytes pools for the converter.
	CheckedBytesPool() pool.CheckedBytesPool
	// SetReadWorkerPool sets the read worker pool for the converter.
	SetReadWorkerPool(sync.PooledWorkerPool) Options
	// ReadWorkerPool returns the read worker pool for the converter.
	ReadWorkerPool() sync.PooledWorkerPool
	// SetWriteWorkerPool sets the write worker pool for the converter.
	SetWriteWorkerPool(sync.PooledWorkerPool) Options
	// WriteWorkerPool returns the write worker pool for the converter.
	WriteWorkerPool() sync.PooledWorkerPool
	// SetSeriesConsolidationMatchOptions sets series consolidation options.
	SetSeriesConsolidationMatchOptions(value consolidators.MatchOptions) Options
	// SeriesConsolidationMatchOptions sets series consolidation options.
	SeriesConsolidationMatchOptions() consolidators.MatchOptions
	// SetSeriesIteratorProcessor sets the series iterator processor.
	SetSeriesIteratorProcessor(SeriesIteratorProcessor) Options
	// SeriesIteratorProcessor returns the series iterator processor.
	SeriesIteratorProcessor() SeriesIteratorProcessor
	// SetIteratorBatchingFn sets the batching function for the converter.
	SetIteratorBatchingFn(IteratorBatchingFn) Options
	// IteratorBatchingFn returns the batching function for the converter.
	IteratorBatchingFn() IteratorBatchingFn
	// SetBlockSeriesProcessor set the block series processor.
	SetBlockSeriesProcessor(value BlockSeriesProcessor) Options
	// BlockSeriesProcessor returns the block series processor.
	BlockSeriesProcessor() BlockSeriesProcessor
	// SetCustomAdminOptions sets custom admin options.
	SetCustomAdminOptions([]client.CustomAdminOption) Options
	// CustomAdminOptions gets custom admin options.
	CustomAdminOptions() []client.CustomAdminOption
	// SetInstrumented marks if the encoding step should have instrumentation enabled.
	SetInstrumented(bool) Options
	// Instrumented returns if the encoding step should have instrumentation enabled.
	Instrumented() bool
	// SetPromConvertOptions sets options for converting raw series iterators
	// to a Prometheus-compatible result.
	SetPromConvertOptions(storage.PromConvertOptions) Options
	// PromConvertOptions returns options for converting raw series iterators
	// to a Prometheus-compatible result.
	PromConvertOptions() storage.PromConvertOptions
	// Validate ensures that the given block options are valid.
	Validate() error
}

// RateLimiter rate limits write requests to the db nodes.
type RateLimiter interface {
	// Limit returns a boolean indicating whether or not the storage write may proceed.
	Limit(ClusterNamespace, queryts.Datapoints, []models.Tag) bool
}

// noopRateLimiter skips rate limiting.
type noopRateLimiter struct{}

// Limit ignores rate limiting by always returning true.
func (f *noopRateLimiter) Limit(ClusterNamespace, queryts.Datapoints, []models.Tag) bool {
	return true
}

// SeriesIteratorProcessor optionally defines methods to process series iterators.
type SeriesIteratorProcessor interface {
	// InspectSeries inspects SeriesIterator slices for a given query.
	InspectSeries(
		ctx context.Context,
		query index.Query,
		queryOpts index.QueryOptions,
		seriesIterators []encoding.SeriesIterator,
	) error
}

// IteratorBatchingFn determines how the iterator is split into batches.
type IteratorBatchingFn func(
	concurrency int,
	seriesBlockIterators []encoding.SeriesIterator,
	seriesMetas []block.SeriesMeta,
	meta block.Metadata,
	opts Options,
) ([]block.SeriesIterBatch, error)

// GraphiteBlockIteratorsFn returns block iterators for graphite decoding.
type GraphiteBlockIteratorsFn func(
	block.Block,
) ([]block.SeriesIterBatch, error)

type peekValue struct {
	started  bool
	finished bool
	point    ts.Datapoint
}

// TagsTransform transforms a set of tags.
type TagsTransform func(context.Context, ClusterNamespace, []models.Tag) ([]models.Tag, error)

// narrowing allows to restrict query time range based on namespace configuration.
type narrowing struct {
	start, end xtime.UnixNano
}
