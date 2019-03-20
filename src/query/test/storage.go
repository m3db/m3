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
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
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

func (s *slowStorage) Fetch(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (*storage.FetchResult, error) {
	time.Sleep(s.delay)
	return s.storage.Fetch(ctx, query, options)
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
) (*storage.CompleteTagsResult, error) {
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

func (s *slowStorage) Type() storage.Type {
	return storage.TypeMultiDC
}

func (s *slowStorage) Close() error {
	return nil
}
