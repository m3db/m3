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

package native

import (
	"github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/graphite/storage"
)

// The Engine for running queries.
type Engine struct {
	storage     storage.Storage
	compileOpts CompileOptions
}

// NewEngine creates a new query engine.
func NewEngine(store storage.Storage, compileOpts CompileOptions) *Engine {
	// TODO: take pooling details from config
	// (https://github.com/m3db/m3/issues/2092)
	return &Engine{
		storage:     store,
		compileOpts: compileOpts,
	}
}

// FetchByQuery retrieves one or more time series based on a query.
func (e *Engine) FetchByQuery(
	ctx context.Context,
	query string,
	options storage.FetchOptions,
) (*storage.FetchResult, error) {
	return e.storage.FetchByQuery(ctx, query, options)
}

// Compile compiles an expression from an expression string
func (e *Engine) Compile(s string) (Expression, error) {
	return Compile(s, e.compileOpts)
}

// Storage returns the engine's storage object
func (e *Engine) Storage() storage.Storage {
	return e.storage
}
