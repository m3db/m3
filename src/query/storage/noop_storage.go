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

package storage

import (
	"context"
	"errors"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
)

var errNoopClient = errors.New("operation not valid for noop client")

// NewNoopStorage returns a fake implementation of Storage that rejects all
// writes and returns errors for all queries.
func NewNoopStorage() Storage {
	return noopStorage{}
}

type noopStorage struct{}

func (noopStorage) QueryStorageMetadataAttributes(
	ctx context.Context,
	queryStart, queryEnd time.Time,
	opts *FetchOptions,
) ([]storagemetadata.Attributes, error) {
	return nil, errNoopClient
}

func (noopStorage) Fetch(ctx context.Context, query *FetchQuery, options *FetchOptions) (*FetchResult, error) {
	return nil, errNoopClient
}

func (noopStorage) FetchProm(ctx context.Context, query *FetchQuery, options *FetchOptions) (PromResult, error) {
	return PromResult{}, errNoopClient
}

// FetchBlocks fetches timeseries as blocks based on a query.
func (noopStorage) FetchBlocks(ctx context.Context, query *FetchQuery, options *FetchOptions) (block.Result, error) {
	return block.Result{}, errNoopClient
}

// SearchSeries returns series IDs matching the current query.
func (noopStorage) SearchSeries(ctx context.Context, query *FetchQuery, options *FetchOptions) (*SearchResults, error) {
	return nil, errNoopClient
}

// CompleteTags returns autocompleted tag results.
func (noopStorage) CompleteTags(ctx context.Context, query *CompleteTagsQuery, options *FetchOptions) (*consolidators.CompleteTagsResult, error) {
	return nil, errNoopClient
}

// Write writes a batched set of datapoints to storage based on the provided
// query.
func (noopStorage) Write(ctx context.Context, query *WriteQuery) error {
	return errNoopClient
}

// Type identifies the type of the underlying
func (noopStorage) Type() Type {
	return TypeLocalDC
}

// Close is used to close the underlying storage and free up resources.
func (noopStorage) Close() error {
	return errNoopClient
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
