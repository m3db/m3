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

package fanout

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/policy/filter"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/query/util/execution"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
)

const (
	initMetricMapSize     = 10
	fetchDataWarningError = "fetch_data_error"
)

type fanoutStorage struct {
	stores             []storage.Storage
	fetchFilter        filter.Storage
	writeFilter        filter.Storage
	completeTagsFilter filter.StorageCompleteTags
	tagOptions         models.TagOptions
	opts               m3.Options
	instrumentOpts     instrument.Options
}

// NewStorage creates a new fanout Storage instance.
func NewStorage(
	stores []storage.Storage,
	fetchFilter filter.Storage,
	writeFilter filter.Storage,
	completeTagsFilter filter.StorageCompleteTags,
	tagOptions models.TagOptions,
	opts m3.Options,
	instrumentOpts instrument.Options,
) storage.Storage {
	return &fanoutStorage{
		stores:             stores,
		fetchFilter:        fetchFilter,
		writeFilter:        writeFilter,
		completeTagsFilter: completeTagsFilter,
		tagOptions:         tagOptions,
		opts:               opts,
		instrumentOpts:     instrumentOpts,
	}
}

func (s *fanoutStorage) QueryStorageMetadataAttributes(
	ctx context.Context,
	queryStart, queryEnd time.Time,
	opts *storage.FetchOptions,
) ([]storagemetadata.Attributes, error) {
	// Optimization for the single store case
	if len(s.stores) == 1 {
		return s.stores[0].QueryStorageMetadataAttributes(ctx, queryStart, queryEnd, opts)
	}

	found := make(map[storagemetadata.Attributes]bool)
	for _, store := range s.stores {
		attrs, err := store.QueryStorageMetadataAttributes(ctx, queryStart, queryEnd, opts)
		if err != nil {
			return nil, err
		}
		for _, attr := range attrs {
			found[attr] = true
		}
	}

	attrs := make([]storagemetadata.Attributes, 0, len(found))
	for attr := range found {
		attrs = append(attrs, attr)
	}

	return attrs, nil
}

func (s *fanoutStorage) FetchProm(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (storage.PromResult, error) {
	stores := filterStores(s.stores, s.fetchFilter, query)
	// Optimization for the single store case
	if len(stores) == 1 {
		return stores[0].FetchProm(ctx, query, options)
	}

	var (
		mu         sync.Mutex
		wg         sync.WaitGroup
		multiErr   xerrors.MultiError
		numWarning int
	)

	wg.Add(len(stores))

	fanout := consolidators.NamespaceCoversAllQueryRange
	pools := s.opts.IteratorPools()
	matchOpts := s.opts.SeriesConsolidationMatchOptions()
	tagOpts := s.opts.TagOptions()
	limitOpts := consolidators.LimitOptions{
		Limit:             options.SeriesLimit,
		RequireExhaustive: options.RequireExhaustive,
	}
	accumulator := consolidators.NewMultiFetchResult(fanout, pools, matchOpts, tagOpts, limitOpts)
	defer func() {
		_ = accumulator.Close()
	}()
	for _, store := range stores {
		store := store
		go func() {
			defer wg.Done()

			storeResult, err := store.FetchCompressed(ctx, query, options)

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				warning, err := storage.IsWarning(store, err)
				if !warning {
					multiErr = multiErr.Add(err)
					s.instrumentOpts.Logger().Error(
						"fanout to store returned error",
						zap.Error(err),
						zap.String("store", store.Name()),
						zap.String("function", "FetchProm"))
					return
				}

				// Is warning, add to accumulator but also process any results.
				accumulator.AddWarnings(block.Warning{
					Name:    store.Name(),
					Message: fetchDataWarningError,
				})
				numWarning++
				s.instrumentOpts.Logger().Warn(
					"partial results: fanout to store returned warning",
					zap.Error(err),
					zap.String("store", store.Name()),
					zap.String("function", "FetchProm"))
			}

			if storeResult == nil {
				return
			}

			for _, r := range storeResult.Results() {
				accumulator.Add(r)
			}
		}()
	}

	wg.Wait()
	// NB: Check multiError first; if any hard error storages errored, the entire
	// query must be errored.
	if err := multiErr.FinalError(); err != nil {
		return storage.PromResult{}, err
	}

	// If there were no successful results at all, return a normal error.
	if numWarning > 0 && numWarning == len(stores) {
		return storage.PromResult{}, errors.ErrNoValidResults
	}

	result, attrs, err := accumulator.FinalResultWithAttrs()
	if err != nil {
		return storage.PromResult{}, err
	}

	resolutions := make([]time.Duration, 0, len(attrs))
	for _, attr := range attrs {
		resolutions = append(resolutions, attr.Resolution)
	}

	result.Metadata.Resolutions = resolutions
	return storage.SeriesIteratorsToPromResult(ctx, result,
		s.opts.ReadWorkerPool(), s.opts.TagOptions(), s.opts.PromConvertOptions(), options)
}

func (s *fanoutStorage) FetchCompressed(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (consolidators.MultiFetchResult, error) {
	stores := filterStores(s.stores, s.fetchFilter, query)
	// Optimization for the single store case
	if len(stores) == 1 {
		return stores[0].FetchCompressed(ctx, query, options)
	}

	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		multiErr xerrors.MultiError
	)

	wg.Add(len(stores))

	fanout := consolidators.NamespaceCoversAllQueryRange
	pools := s.opts.IteratorPools()
	matchOpts := s.opts.SeriesConsolidationMatchOptions()
	tagOpts := s.opts.TagOptions()
	limitOpts := consolidators.LimitOptions{
		Limit:             options.SeriesLimit,
		RequireExhaustive: options.RequireExhaustive,
	}
	accumulator := consolidators.NewMultiFetchResult(fanout, pools, matchOpts, tagOpts, limitOpts)
	defer func() {
		_ = accumulator.Close()
	}()
	for _, store := range stores {
		store := store
		go func() {
			defer wg.Done()
			storeResult, err := store.FetchCompressed(ctx, query, options)
			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				multiErr = multiErr.Add(err)
				s.instrumentOpts.Logger().Error(
					"fanout to store returned error",
					zap.Error(err),
					zap.String("store", store.Name()),
					zap.String("function", "FetchProm"))
				return
			}

			for _, r := range storeResult.Results() {
				accumulator.Add(r)
			}
		}()
	}

	wg.Wait()
	// NB: Check multiError first; if any hard error storages errored, the entire
	// query must be errored.
	if err := multiErr.FinalError(); err != nil {
		return nil, err
	}

	return accumulator, nil
}

func (s *fanoutStorage) FetchBlocks(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (block.Result, error) {
	stores := filterStores(s.stores, s.fetchFilter, query)
	// Optimization for the single store case
	if len(stores) == 1 {
		return stores[0].FetchBlocks(ctx, query, options)
	}

	var (
		mu         sync.Mutex
		wg         sync.WaitGroup
		multiErr   xerrors.MultiError
		numWarning int
	)

	// TODO(arnikola): update this to use a genny map
	blockResult := make(map[string]block.Block, len(stores))
	wg.Add(len(stores))
	resultMeta := block.NewResultMetadata()
	for _, store := range stores {
		store := store
		go func() {
			defer wg.Done()

			result, err := store.FetchBlocks(ctx, query, options)

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				if warning, err := storage.IsWarning(store, err); warning {
					resultMeta.AddWarning(store.Name(), fetchDataWarningError)
					numWarning++
					s.instrumentOpts.Logger().Warn(
						"partial results: fanout to store returned warning",
						zap.Error(err),
						zap.String("store", store.Name()),
						zap.String("function", "FetchBlocks"))
					return
				}

				multiErr = multiErr.Add(err)
				s.instrumentOpts.Logger().Error(
					"fanout to store returned error",
					zap.Error(err),
					zap.String("store", store.Name()),
					zap.String("function", "FetchBlocks"))
				return
			}

			resultMeta = resultMeta.CombineMetadata(result.Metadata)
			for _, bl := range result.Blocks {
				key := bl.Meta().String()
				foundBlock, found := blockResult[key]
				if !found {
					blockResult[key] = bl
					continue
				}

				// This block exists. Check to see if it's already an appendable block.
				blockType := foundBlock.Info().Type()
				if blockType != block.BlockContainer {
					var err error
					blockResult[key], err = block.NewContainerBlock(foundBlock, bl)
					if err != nil {
						multiErr = multiErr.Add(err)
						return
					}

					continue
				}

				accumulator, ok := foundBlock.(block.AccumulatorBlock)
				if !ok {
					multiErr = multiErr.Add(fmt.Errorf("container block has incorrect type"))
					return
				}

				// Already an accumulator block, add current block.
				if err := accumulator.AddBlock(bl); err != nil {
					multiErr = multiErr.Add(err)
					return
				}
			}
		}()
	}

	wg.Wait()
	// NB: Check multiError first; if any hard error storages errored, the entire
	// query must be errored.
	if err := multiErr.FinalError(); err != nil {
		return block.Result{}, err
	}

	// If there were no successful results at all, return a normal error.
	if numWarning > 0 && numWarning == len(stores) {
		return block.Result{}, errors.ErrNoValidResults
	}

	blocks := make([]block.Block, 0, len(blockResult))
	updateResultMeta := func(meta block.Metadata) block.Metadata {
		meta.ResultMetadata = meta.ResultMetadata.CombineMetadata(resultMeta)
		return meta
	}

	lazyOpts := block.NewLazyOptions().SetMetaTransform(updateResultMeta)
	for _, bl := range blockResult {
		// Update constituent blocks with combined resultMetadata if it has been
		// changed.
		if !resultMeta.IsDefault() {
			bl = block.NewLazyBlock(bl, lazyOpts)
		}

		blocks = append(blocks, bl)
	}

	return block.Result{
		Blocks:   blocks,
		Metadata: resultMeta,
	}, nil
}

func (s *fanoutStorage) SearchSeries(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (*storage.SearchResults, error) {
	// TODO: arnikola use a genny map here instead, or better yet, hide this
	// behind an accumulator.
	metricMap := make(map[string]models.Metric, initMetricMapSize)
	stores := filterStores(s.stores, s.fetchFilter, query)
	metadata := block.NewResultMetadata()
	for _, store := range stores {
		results, err := store.SearchSeries(ctx, query, options)
		if err != nil {
			if warning, err := storage.IsWarning(store, err); warning {
				metadata.AddWarning(store.Name(), fetchDataWarningError)
				s.instrumentOpts.Logger().Warn(
					"partial results: fanout to store returned warning",
					zap.Error(err),
					zap.String("store", store.Name()),
					zap.String("function", "SearchSeries"))
				continue
			}

			s.instrumentOpts.Logger().Error(
				"fanout to store returned error",
				zap.Error(err),
				zap.String("store", store.Name()),
				zap.String("function", "SearchSeries"))
			return nil, err
		}

		metadata = metadata.CombineMetadata(results.Metadata)
		for _, metric := range results.Metrics {
			id := string(metric.ID)
			if existing, found := metricMap[id]; found {
				existing.Tags = existing.Tags.AddTagsIfNotExists(metric.Tags.Tags)
				metricMap[id] = existing
			} else {
				metricMap[id] = metric
			}
		}
	}

	metrics := make(models.Metrics, 0, len(metricMap))
	for _, v := range metricMap {
		metrics = append(metrics, v)
	}

	result := &storage.SearchResults{
		Metrics:  metrics,
		Metadata: metadata,
	}

	return result, nil
}

func (s *fanoutStorage) CompleteTags(
	ctx context.Context,
	query *storage.CompleteTagsQuery,
	options *storage.FetchOptions,
) (*consolidators.CompleteTagsResult, error) {
	stores := filterCompleteTagsStores(s.stores, s.completeTagsFilter, *query)

	var completeTagsResult consolidators.CompleteTagsResult

	// short circuit complete tags
	if len(stores) == 1 {
		result, err := stores[0].CompleteTags(ctx, query, options)
		if err != nil {
			return result, err
		}
		completeTagsResult = *result
	} else {
		accumulatedTags := consolidators.NewCompleteTagsResultBuilder(
			query.CompleteNameOnly, s.tagOptions)
		metadata := block.NewResultMetadata()
		for _, store := range stores {
			result, err := store.CompleteTags(ctx, query, options)
			if err != nil {
				if warning, err := storage.IsWarning(store, err); warning {
					metadata.AddWarning(store.Name(), fetchDataWarningError)
					s.instrumentOpts.Logger().Warn(
						"partial results: fanout to store returned warning",
						zap.Error(err),
						zap.String("store", store.Name()),
						zap.String("function", "CompleteTags"))
					continue
				}

				s.instrumentOpts.Logger().Error(
					"fanout to store returned error",
					zap.Error(err),
					zap.String("store", store.Name()),
					zap.String("function", "CompleteTags"))

				return nil, err
			}

			metadata = metadata.CombineMetadata(result.Metadata)
			err = accumulatedTags.Add(result)
			if err != nil {
				return nil, err
			}
		}

		completeTagsResult = accumulatedTags.Build()
		completeTagsResult.Metadata = metadata
	}

	completeTagsResult = applyOptions(completeTagsResult, options)
	return &completeTagsResult, nil
}

func applyOptions(
	result consolidators.CompleteTagsResult,
	opts *storage.FetchOptions,
) consolidators.CompleteTagsResult {
	if opts.RestrictQueryOptions == nil {
		return result
	}

	filter := opts.RestrictQueryOptions.GetRestrictByTag().GetFilterByNames()
	if len(filter) > 0 {
		// Filter out unwanted tags inplace.
		filteredList := result.CompletedTags[:0]
		for _, s := range result.CompletedTags {
			skip := false
			for _, name := range filter {
				if bytes.Equal(s.Name, name) {
					skip = true
					break
				}
			}

			if skip {
				continue
			}

			filteredList = append(filteredList, s)
		}

		result.CompletedTags = filteredList
	}

	return result
}

func (s *fanoutStorage) Write(ctx context.Context,
	query *storage.WriteQuery) error {
	// TODO: Consider removing this lookup on every write by maintaining
	//  different read/write lists
	stores := filterStores(s.stores, s.writeFilter, query)
	// short circuit writes
	if len(stores) == 1 {
		return stores[0].Write(ctx, query)
	}

	requests := make([]execution.Request, 0, len(stores))
	for _, store := range stores {
		requests = append(requests, newWriteRequest(store, query))
	}

	return execution.ExecuteParallel(ctx, requests)
}

func (s *fanoutStorage) ErrorBehavior() storage.ErrorBehavior {
	return storage.BehaviorFail
}

func (s *fanoutStorage) Type() storage.Type {
	return storage.TypeMultiDC
}

func (s *fanoutStorage) Name() string {
	inner := make([]string, 0, len(s.stores))
	for _, store := range s.stores {
		inner = append(inner, store.Name())
	}

	return fmt.Sprintf("fanout_store, inner: %v", inner)
}

func (s *fanoutStorage) Close() error {
	var lastErr error
	for idx, store := range s.stores {
		// Keep going on error to close all storages
		if err := store.Close(); err != nil {
			s.instrumentOpts.Logger().Error("unable to close storage",
				zap.Int("store", int(store.Type())), zap.Int("index", idx))
			lastErr = err
		}
	}

	return lastErr
}

func filterStores(
	stores []storage.Storage,
	filterPolicy filter.Storage,
	query storage.Query,
) []storage.Storage {
	filtered := make([]storage.Storage, 0, len(stores))
	for _, s := range stores {
		if filterPolicy(query, s) {
			filtered = append(filtered, s)
		}
	}

	return filtered
}

func filterCompleteTagsStores(
	stores []storage.Storage,
	filterPolicy filter.StorageCompleteTags,
	query storage.CompleteTagsQuery,
) []storage.Storage {
	filtered := make([]storage.Storage, 0, len(stores))
	for _, s := range stores {
		if filterPolicy(query, s) {
			filtered = append(filtered, s)
		}
	}

	return filtered
}

type writeRequest struct {
	store storage.Storage
	query *storage.WriteQuery
}

func newWriteRequest(store storage.Storage, query *storage.WriteQuery) execution.Request {
	return &writeRequest{
		store: store,
		query: query,
	}
}

func (f *writeRequest) Process(ctx context.Context) error {
	return f.store.Write(ctx, f.query)
}
