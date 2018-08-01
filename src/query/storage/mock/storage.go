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

	"github.com/m3db/m3db/src/query/block"
	"github.com/m3db/m3db/src/query/storage"
)

type mockStorage struct {
	sType  storage.Type
	blocks []block.Block
}

// NewMockStorage creates a new mock Storage instance.
func NewMockStorage() storage.Storage {
	return &mockStorage{sType: storage.Type(0)}
}

// NewMockStorageWithBlocks creates a new mock Storage instance with blocks.
func NewMockStorageWithBlocks(blocks []block.Block) storage.Storage {
	return &mockStorage{sType: storage.Type(0), blocks: blocks}
}

// NewMockStorageWithType creates a new mock Storage instance.
func NewMockStorageWithType(sType storage.Type) storage.Storage {
	return &mockStorage{sType: sType}
}

func (s *mockStorage) Fetch(ctx context.Context, query *storage.FetchQuery, _ *storage.FetchOptions) (*storage.FetchResult, error) {
	return nil, nil
}

func (s *mockStorage) FetchTags(ctx context.Context, query *storage.FetchQuery, _ *storage.FetchOptions) (*storage.SearchResults, error) {
	return nil, nil
}

func (s *mockStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	return nil
}

func (s *mockStorage) Type() storage.Type {
	return s.sType
}

func (s *mockStorage) Close() error {
	return nil
}

func (s *mockStorage) FetchBlocks(
	ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (block.Result, error) {
	return block.Result{
		Blocks: s.blocks,
	}, nil
}
