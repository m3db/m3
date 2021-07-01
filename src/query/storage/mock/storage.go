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

package mock

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
)

// Storage implements storage.Storage and provides methods to help
// read what was written and set what to retrieve.
type Storage interface {
	storage.Storage

	SetTypeResult(storage.Type)
	SetErrorBehavior(storage.ErrorBehavior)
	LastFetchOptions() *storage.FetchOptions
	SetFetchResult(*storage.FetchResult, error)
	SetFetchResults(...*storage.FetchResult)
	SetSearchSeriesResult(*storage.SearchResults, error)
	SetCompleteTagsResult(*consolidators.CompleteTagsResult, error)
	SetWriteResult(error)
	SetFetchBlocksResult(block.Result, error)
	SetQueryStorageMetadataAttributesResult([]storagemetadata.Attributes, error)
	SetCloseResult(error)
	Writes() []*storage.WriteQuery
}

type mockStorage struct {
	sync.RWMutex
	typeResult struct {
		result storage.Type
	}
	errorBehavior    storage.ErrorBehavior
	lastFetchOptions *storage.FetchOptions
	fetchResult      struct {
		results []*storage.FetchResult
		idx     int
		err     error
	}
	fetchTagsResult struct {
		result *storage.SearchResults
		err    error
	}
	writeResult struct {
		err error
	}
	fetchBlocksResult struct {
		result block.Result
		err    error
	}
	completeTagsResult struct {
		result *consolidators.CompleteTagsResult
		err    error
	}
	closeResult struct {
		err error
	}
	queryStorageMetadataAttributesResult struct {
		attrs []storagemetadata.Attributes
		err   error
	}
	writes []*storage.WriteQuery
}

// NewMockStorage creates a new mock Storage instance.
func NewMockStorage() Storage {
	return &mockStorage{}
}

func (s *mockStorage) SetTypeResult(result storage.Type) {
	s.Lock()
	defer s.Unlock()
	s.typeResult.result = result
}

func (s *mockStorage) SetErrorBehavior(b storage.ErrorBehavior) {
	s.Lock()
	defer s.Unlock()
	s.errorBehavior = b
}

func (s *mockStorage) SetFetchResult(result *storage.FetchResult, err error) {
	s.Lock()
	defer s.Unlock()
	s.fetchResult.results = []*storage.FetchResult{result}
	s.fetchResult.err = err
}

func (s *mockStorage) SetFetchResults(results ...*storage.FetchResult) {
	s.Lock()
	defer s.Unlock()
	s.fetchResult.results = append(
		make([]*storage.FetchResult, 0, len(results)),
		results...,
	)
}

func (s *mockStorage) SetSearchSeriesResult(result *storage.SearchResults, err error) {
	s.Lock()
	defer s.Unlock()
	s.fetchTagsResult.result = result
	s.fetchTagsResult.err = err
}

func (s *mockStorage) SetWriteResult(err error) {
	s.Lock()
	defer s.Unlock()
	s.writeResult.err = err
}

func (s *mockStorage) SetFetchBlocksResult(result block.Result, err error) {
	s.Lock()
	defer s.Unlock()
	s.fetchBlocksResult.result = result
	s.fetchBlocksResult.err = err
}

func (s *mockStorage) SetCompleteTagsResult(result *consolidators.CompleteTagsResult, err error) {
	s.Lock()
	defer s.Unlock()
	s.completeTagsResult.result = result
	s.completeTagsResult.err = err
}

func (s *mockStorage) SetQueryStorageMetadataAttributesResult(attrs []storagemetadata.Attributes, err error) {
	s.Lock()
	defer s.Unlock()
	s.queryStorageMetadataAttributesResult.attrs = attrs
	s.queryStorageMetadataAttributesResult.err = err
}

func (s *mockStorage) SetCloseResult(err error) {
	s.Lock()
	defer s.Unlock()
	s.closeResult.err = err
}

func (s *mockStorage) Writes() []*storage.WriteQuery {
	s.RLock()
	defer s.RUnlock()
	return s.writes
}

func (s *mockStorage) LastFetchOptions() *storage.FetchOptions {
	s.RLock()
	defer s.RUnlock()
	return s.lastFetchOptions
}

func (s *mockStorage) Fetch(
	ctx context.Context,
	query *storage.FetchQuery,
	opts *storage.FetchOptions,
) (*storage.FetchResult, error) {
	s.Lock()
	defer s.Unlock()
	s.lastFetchOptions = opts
	idx := s.fetchResult.idx
	if idx >= len(s.fetchResult.results) {
		idx = 0
	}

	s.fetchResult.idx = s.fetchResult.idx + 1
	return s.fetchResult.results[idx], s.fetchResult.err
}

func (s *mockStorage) FetchProm(
	ctx context.Context,
	query *storage.FetchQuery,
	opts *storage.FetchOptions,
) (storage.PromResult, error) {
	return storage.PromResult{}, errors.New("not implemented")
}

func (s *mockStorage) FetchBlocks(
	ctx context.Context,
	query *storage.FetchQuery,
	opts *storage.FetchOptions,
) (block.Result, error) {
	s.RLock()
	defer s.RUnlock()
	s.lastFetchOptions = opts
	return s.fetchBlocksResult.result, s.fetchBlocksResult.err
}

func (s *mockStorage) SearchSeries(
	ctx context.Context,
	query *storage.FetchQuery,
	opts *storage.FetchOptions,
) (*storage.SearchResults, error) {
	s.RLock()
	defer s.RUnlock()
	s.lastFetchOptions = opts
	return s.fetchTagsResult.result, s.fetchTagsResult.err
}

func (s *mockStorage) CompleteTags(
	ctx context.Context,
	query *storage.CompleteTagsQuery,
	opts *storage.FetchOptions,
) (*consolidators.CompleteTagsResult, error) {
	s.RLock()
	defer s.RUnlock()
	s.lastFetchOptions = opts
	return s.completeTagsResult.result, s.completeTagsResult.err
}

func (s *mockStorage) QueryStorageMetadataAttributes(
	_ context.Context,
	_, _ time.Time,
	opts *storage.FetchOptions,
) ([]storagemetadata.Attributes, error) {
	s.RLock()
	defer s.RUnlock()
	s.lastFetchOptions = opts
	return s.queryStorageMetadataAttributesResult.attrs, s.queryStorageMetadataAttributesResult.err
}

func (s *mockStorage) Write(
	ctx context.Context,
	query *storage.WriteQuery,
) error {
	s.Lock()
	defer s.Unlock()
	s.writes = append(s.writes, query)
	return s.writeResult.err
}

func (s *mockStorage) Type() storage.Type {
	s.RLock()
	defer s.RUnlock()
	return s.typeResult.result
}

func (s *mockStorage) Name() string {
	return "mock"
}

func (s *mockStorage) ErrorBehavior() storage.ErrorBehavior {
	s.RLock()
	defer s.RUnlock()
	return s.errorBehavior
}

func (s *mockStorage) Close() error {
	s.RLock()
	defer s.RUnlock()
	return s.closeResult.err
}
