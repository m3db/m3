// Copyright (c) 2021  Uber Technologies, Inc.
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

package promremote

import (
	"context"
	"fmt"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
)

func (p *promStorage) FetchProm(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (storage.PromResult, error) {
	return storage.PromResult{}, unimplementedErr("FetchProm")
}

func (p *promStorage) FetchBlocks(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (block.Result, error) {
	return block.Result{}, unimplementedErr("FetchBlocks")
}

func (p *promStorage) FetchCompressed(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (consolidators.MultiFetchResult, error) {
	return nil, unimplementedErr("FetchCompressed")
}

func (p *promStorage) SearchSeries(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (*storage.SearchResults, error) {
	return nil, unimplementedErr("SearchSeries")
}

func (p *promStorage) CompleteTags(
	_ context.Context,
	_ *storage.CompleteTagsQuery,
	_ *storage.FetchOptions,
) (*consolidators.CompleteTagsResult, error) {
	return nil, unimplementedErr("CompleteTags")
}

func (p *promStorage) QueryStorageMetadataAttributes(
	_ context.Context,
	_, _ time.Time,
	_ *storage.FetchOptions,
) ([]storagemetadata.Attributes, error) {
	return nil, unimplementedErr("QueryStorageMetadataAttributes")
}

func unimplementedErr(name string) error {
	return fmt.Errorf("promStorage: %s method is not supported", name)
}
