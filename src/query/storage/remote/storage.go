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

package remote

import (
	"context"
	"fmt"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/remote"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
)

// Options contains options for remote clients.
type Options struct {
	// ErrorBehavior determines the error behavior for this remote storage.
	ErrorBehavior storage.ErrorBehavior
	// Name is this storage's name.
	Name string
}

type remoteStorage struct {
	client remote.Client
	opts   Options
}

// NewStorage creates a new remote Storage instance.
func NewStorage(c remote.Client, opts Options) storage.Storage {
	return &remoteStorage{client: c, opts: opts}
}

func (s *remoteStorage) FetchProm(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (storage.PromResult, error) {
	return s.client.FetchProm(ctx, query, options)
}

func (s *remoteStorage) FetchCompressed(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (consolidators.MultiFetchResult, error) {
	return s.client.FetchCompressed(ctx, query, options)
}

func (s *remoteStorage) FetchBlocks(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (block.Result, error) {
	return s.client.FetchBlocks(ctx, query, options)
}

func (s *remoteStorage) SearchSeries(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (*storage.SearchResults, error) {
	return s.client.SearchSeries(ctx, query, options)
}

func (s *remoteStorage) CompleteTags(
	ctx context.Context,
	query *storage.CompleteTagsQuery,
	options *storage.FetchOptions,
) (*consolidators.CompleteTagsResult, error) {
	return s.client.CompleteTags(ctx, query, options)
}

func (s *remoteStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	return errors.ErrRemoteWriteQuery
}

func (s *remoteStorage) ErrorBehavior() storage.ErrorBehavior {
	return s.opts.ErrorBehavior
}

func (s *remoteStorage) Type() storage.Type {
	return storage.TypeRemoteDC
}

func (s *remoteStorage) Name() string {
	return fmt.Sprintf("remote_store_%s", s.opts.Name)
}

func (s *remoteStorage) Close() error {
	return nil
}
