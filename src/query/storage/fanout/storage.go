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
	"context"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/policy/filter"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util/execution"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"

	"go.uber.org/zap"
)

type fanoutStorage struct {
	stores             []storage.Storage
	fetchFilter        filter.Storage
	writeFilter        filter.Storage
	completeTagsFilter filter.StorageCompleteTags
	instrumentOpts     instrument.Options
}

// NewStorage creates a new fanout Storage instance.
func NewStorage(
	stores []storage.Storage,
	fetchFilter filter.Storage,
	writeFilter filter.Storage,
	completeTagsFilter filter.StorageCompleteTags,
	instrumentOpts instrument.Options,
) storage.Storage {
	return &fanoutStorage{
		stores:             stores,
		fetchFilter:        fetchFilter,
		writeFilter:        writeFilter,
		completeTagsFilter: completeTagsFilter,
		instrumentOpts:     instrumentOpts,
	}
}

func (s *fanoutStorage) Fetch(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (*storage.FetchResult, error) {
	stores := filterStores(s.stores, s.fetchFilter, query)
	requests := make([]execution.Request, len(stores))
	for idx, store := range stores {
		requests[idx] = newFetchRequest(store, query, options)
	}

	err := execution.ExecuteParallel(ctx, requests)
	if err != nil {
		return nil, err
	}

	return handleFetchResponses(requests)
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

	// TODO(arnikola): use a genny map here instead, also execute in parallel.
	blockResult := make(map[string]block.Block, 10)
	for _, store := range stores {
		result, err := store.FetchBlocks(ctx, query, options)
		if err != nil {
			return block.Result{}, err
		}

		for _, bl := range result.Blocks {
			it, err := bl.SeriesIter()
			if err != nil {
				return block.Result{}, err
			}

			key := it.Meta().Bounds.String()
			if foundBlock, found := blockResult[key]; found {
				// this block exists. Check to see if it's already an appendable block.
				if acc, ok := foundBlock.(block.AccumulatorBlock); ok {
					// already an accumulator block, add current block.
					if err := acc.AddBlock(bl); err != nil {
						return block.Result{}, err
					}
				} else {
					blockResult[key] = block.NewContainerBlock(foundBlock, bl)
				}
			} else {
				blockResult[key] = bl
			}
		}
	}

	blocks := make([]block.Block, 0, len(blockResult))
	for _, bl := range blockResult {
		blocks = append(blocks, bl)
	}

	return block.Result{Blocks: blocks}, nil
}

func handleFetchResponses(requests []execution.Request) (*storage.FetchResult, error) {
	seriesList := make([]*ts.Series, 0, len(requests))
	result := &storage.FetchResult{SeriesList: seriesList, LocalOnly: true}
	for _, req := range requests {
		fetchreq, ok := req.(*fetchRequest)
		if !ok {
			return nil, errors.ErrFetchRequestType
		}

		if fetchreq.result == nil {
			return nil, errors.ErrInvalidFetchResult
		}

		if fetchreq.store.Type() != storage.TypeLocalDC {
			result.LocalOnly = false
		}

		result.SeriesList = append(result.SeriesList, fetchreq.result.SeriesList...)
	}

	return result, nil
}

func (s *fanoutStorage) SearchSeries(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (*storage.SearchResults, error) {
	var metrics models.Metrics

	stores := filterStores(s.stores, s.fetchFilter, query)
	for _, store := range stores {
		results, err := store.SearchSeries(ctx, query, options)
		if err != nil {
			return nil, err
		}
		metrics = append(metrics, results.Metrics...)
	}

	result := &storage.SearchResults{Metrics: metrics}

	return result, nil
}

func (s *fanoutStorage) CompleteTags(
	ctx context.Context,
	query *storage.CompleteTagsQuery,
	options *storage.FetchOptions,
) (*storage.CompleteTagsResult, error) {
	stores := filterCompleteTagsStores(s.stores, s.completeTagsFilter, *query)
	// short circuit complete tags
	if len(stores) == 1 {
		return stores[0].CompleteTags(ctx, query, options)
	}

	accumulatedTags := storage.NewCompleteTagsResultBuilder(query.CompleteNameOnly)
	for _, store := range stores {
		result, err := store.CompleteTags(ctx, query, options)
		if err != nil {
			return nil, err
		}

		accumulatedTags.Add(result)
	}

	built := accumulatedTags.Build()
	return &built, nil
}

func (s *fanoutStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	// TODO: Consider removing this lookup on every write by maintaining different read/write lists
	stores := filterStores(s.stores, s.writeFilter, query)
	// short circuit writes
	if len(stores) == 1 {
		return stores[0].Write(ctx, query)
	}

	requests := make([]execution.Request, len(stores))
	for idx, store := range stores {
		requests[idx] = newWriteRequest(store, query)
	}

	return execution.ExecuteParallel(ctx, requests)
}

func (s *fanoutStorage) Type() storage.Type {
	return storage.TypeMultiDC
}

func (s *fanoutStorage) Close() error {
	var lastErr error
	for idx, store := range s.stores {
		// Keep going on error to close all storages
		if err := store.Close(); err != nil {
			logging.WithContext(context.Background(), s.instrumentOpts).
				Error("unable to close storage",
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

type fetchRequest struct {
	store   storage.Storage
	query   *storage.FetchQuery
	options *storage.FetchOptions
	result  *storage.FetchResult
}

func newFetchRequest(
	store storage.Storage,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) execution.Request {
	return &fetchRequest{
		store:   store,
		query:   query,
		options: options,
	}
}

func (f *fetchRequest) Process(ctx context.Context) error {
	result, err := f.store.Fetch(ctx, f.query, f.options)
	if err != nil {
		return err
	}

	f.result = result
	return nil
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
