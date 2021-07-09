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

package test

import (
	"context"
	"errors"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
)

// slowStorage slows down a request by delay
type slowStorage struct {
	storage storage.Storage
	delay   time.Duration
}

// NewSlowStorage creates a new slow storage
func NewSlowStorage(
	storage storage.Storage,
	delay time.Duration,
) storage.Storage {
	return &slowStorage{storage: storage, delay: delay}
}

func (s *slowStorage) FetchProm(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (storage.PromResult, error) {
	time.Sleep(s.delay)
	return s.storage.FetchProm(ctx, query, options)
}

func (s *slowStorage) FetchBlocks(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (block.Result, error) {
	time.Sleep(s.delay)
	return s.storage.FetchBlocks(ctx, query, options)
}

func (s *slowStorage) SearchSeries(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (*storage.SearchResults, error) {
	time.Sleep(s.delay)
	return s.storage.SearchSeries(ctx, query, options)
}

func (s *slowStorage) CompleteTags(
	ctx context.Context,
	query *storage.CompleteTagsQuery,
	options *storage.FetchOptions,
) (*consolidators.CompleteTagsResult, error) {
	time.Sleep(s.delay)
	return s.storage.CompleteTags(ctx, query, options)
}

func (s *slowStorage) Write(
	ctx context.Context,
	query *storage.WriteQuery,
) error {
	time.Sleep(s.delay)
	return s.storage.Write(ctx, query)
}

func (s *slowStorage) QueryStorageMetadataAttributes(
	ctx context.Context,
	queryStart, queryEnd time.Time,
	opts *storage.FetchOptions,
) ([]storagemetadata.Attributes, error) {
	return nil, errors.New("not implemented")
}

func (s *slowStorage) Type() storage.Type {
	return storage.TypeMultiDC
}

func (s *slowStorage) Name() string {
	return "slow"
}

func (s *slowStorage) ErrorBehavior() storage.ErrorBehavior {
	return storage.BehaviorFail
}

func (s *slowStorage) Close() error {
	return nil
}
