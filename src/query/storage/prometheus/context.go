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

package prometheus

import (
	"context"
	"errors"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
)

// ContextKey is the context key type.
type ContextKey string

const (
	// FetchOptionsContextKey is the context key for fetch options.
	FetchOptionsContextKey ContextKey = "fetch-options"

	// BlockResultMetadataFnKey is the context key for a function to receive block metadata results.
	BlockResultMetadataFnKey ContextKey = "block-meta-result-fn"
)

// RemoteReadFlags is a set of flags for storage remote read requests.
type RemoteReadFlags struct {
	Limited bool
}

func fetchOptions(ctx context.Context) (*storage.FetchOptions, error) {
	fetchOptions := ctx.Value(FetchOptionsContextKey)
	if f, ok := fetchOptions.(*storage.FetchOptions); ok {
		return f, nil
	}
	return nil, errors.New("fetch options not available")
}

func resultMetadataReceiveFn(ctx context.Context) (func(m block.ResultMetadata), error) {
	value := ctx.Value(BlockResultMetadataFnKey)
	if v, ok := value.(func(m block.ResultMetadata)); ok {
		return v, nil
	}
	return nil, errors.New("block result metadata not available")
}
