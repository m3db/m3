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

	"github.com/m3db/m3coordinator/errors"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/tsdb/remote"
)

type remoteStorage struct {
	client remote.Client
}

// NewStorage creates a new remote Storage instance.
func NewStorage(c remote.Client) storage.Storage {
	return &remoteStorage{client: c}
}

func (s *remoteStorage) Fetch(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (*storage.FetchResult, error) {
	return s.client.Fetch(ctx, query, options)
}

func (s *remoteStorage) FetchTags(ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (*storage.SearchResults, error) {
	// todo (braskin): implement remote FetchTags
	return nil, errors.ErrNotImplemented
}

func (s *remoteStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	return s.client.Write(ctx, query)
}

func (s *remoteStorage) Type() storage.Type {
	return storage.TypeRemoteDC
}

func (s *remoteStorage) Close() error {
	return nil
}

func (s *remoteStorage) FetchBlocks(
	ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (storage.BlockResult, error) {
	return storage.BlockResult{}, errors.ErrNotImplemented
}
