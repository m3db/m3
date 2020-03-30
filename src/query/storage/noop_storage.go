package storage

import (
	"context"

	"github.com/m3db/m3/src/query/block"
)

// NewNoopStorage returns a fake implementation of Storage that accepts all writes
// and returns no results for all queries.
func NewNoopStorage() Storage {
	return noopStorage{}
}

type noopStorage struct{}

func (noopStorage) Fetch(ctx context.Context, query *FetchQuery, options *FetchOptions) (*FetchResult, error) {
	return &FetchResult{}, nil
}

func (noopStorage) FetchProm(ctx context.Context, query *FetchQuery, options *FetchOptions) (PromResult, error) {
	return PromResult{}, nil
}

// FetchBlocks fetches timeseries as blocks based on a query.
func (noopStorage) FetchBlocks(ctx context.Context, query *FetchQuery, options *FetchOptions) (block.Result, error) {
	return block.Result{}, nil
}

// SearchSeries returns series IDs matching the current query.
func (noopStorage) SearchSeries(ctx context.Context, query *FetchQuery, options *FetchOptions) (*SearchResults, error) {
	return &SearchResults{}, nil
}

// CompleteTags returns autocompleted tag results.
func (noopStorage) CompleteTags(ctx context.Context, query *CompleteTagsQuery, options *FetchOptions) (*CompleteTagsResult, error) {
	return &CompleteTagsResult{}, nil
}

// Write writes a batched set of datapoints to storage based on the provided
// query.
func (noopStorage) Write(ctx context.Context, query *WriteQuery) error {
	return nil
}

// Type identifies the type of the underlying
func (noopStorage) Type() Type {
	return TypeLocalDC
}

// Close is used to close the underlying storage and free up resources.
func (noopStorage) Close() error {
	return nil
}

// ErrorBehavior dictates what fanout storage should do when this storage
// encounters an error.
func (noopStorage) ErrorBehavior() ErrorBehavior {
	return BehaviorWarn
}

// Name gives the plaintext name for this storage, used for logging purposes.
func (noopStorage) Name() string {
	return "noopStorage"
}
