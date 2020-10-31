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

package executor

import (
	"context"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/instrument"
)

// Engine executes a Query.
type Engine interface {
	// ExecuteProm runs the query and returns the result in a
	// Prometheus-compatible format (primarily used for remote read paths).
	ExecuteProm(
		ctx context.Context,
		query *storage.FetchQuery,
		opts *QueryOptions,
		fetchOpts *storage.FetchOptions,
	) (storage.PromResult, error)

	// ExecuteExpr runs the query DAG and closes the results channel once done.
	ExecuteExpr(
		ctx context.Context,
		parser parser.Parser,
		opts *QueryOptions,
		fetchOpts *storage.FetchOptions,
		params models.RequestParams,
	) (block.Block, error)

	// Options returns the currently configured options.
	Options() EngineOptions

	// Close kills all running queries and prevents new queries from being attached.
	Close() error
}

// EngineOptions are used to create an engine.
type EngineOptions interface {
	// InstrumentOptions returns the instrument options and scope used
	// for metrics.
	InstrumentOptions() instrument.Options
	// SetInstrumentOptions sets the instrument options and scope used
	// for metrics.
	SetInstrumentOptions(instrument.Options) EngineOptions

	// Store returns the storage.
	Store() storage.Storage
	// SetStore sets the storage.
	SetStore(storage.Storage) EngineOptions

	// LookbackDuration returns the query lookback duration.
	LookbackDuration() time.Duration
	// SetLookbackDuration sets the query lookback duration.
	SetLookbackDuration(time.Duration) EngineOptions

	// ParseOptions returns the parse options.
	ParseOptions() promql.ParseOptions
	// SetParseOptions sets the parse options.
	SetParseOptions(p promql.ParseOptions) EngineOptions
}
