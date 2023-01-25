// Copyright (c) 2019 Uber Technologies, Inc.
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

package common

import (
	"github.com/m3db/m3/src/query/graphite/storage"
)

// QueryEngine is the generic engine interface.
type QueryEngine interface {
	// FetchByQuery retrieves one or more time series based on a query.
	FetchByQuery(
		ctx *Context,
		query string,
		options storage.FetchOptions,
	) (*storage.FetchResult, error)

	// Storage returns the engine's storage object.
	Storage() storage.Storage
}

// The Engine for running queries
type Engine struct {
	storage storage.Storage
}

// NewEngine creates a new query engine
func NewEngine(storage storage.Storage) *Engine {
	return &Engine{
		storage: storage,
	}
}

func (e *Engine) FetchByQuery(
	ctx *Context,
	query string,
	options storage.FetchOptions,
) (*storage.FetchResult, error) {
	ctx.TrackFetch()
	return e.storage.FetchByQuery(ctx, query, options)
}

func (e *Engine) Storage() storage.Storage {
	return e.storage
}
