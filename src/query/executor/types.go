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

	qcost "github.com/m3db/m3/src/query/cost"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/storage"
	"github.com/uber-go/tally"
)

// Engine executes a Query.
type Engine interface {
	// Execute runs the query and closes the results channel once done.
	Execute(
		ctx context.Context,
		query *storage.FetchQuery,
		results chan *storage.QueryResult,
	)

	// ExecuteExpr runs the query DAG and closes the results channel once done.
	// nolint: unparam
	ExecuteExpr(
		ctx context.Context,
		parser parser.Parser,
		params models.RequestParams,
		results chan Query,
	)

	// Close kills all running queries and prevents new queries from being attached.
	Close() error

	Opts() EngineOptions
	SetOpts(EngineOptions) Engine
}

// EngineOptions can be used to pass custom flags to engine.
type EngineOptions interface {
	QueryContextOptions() models.QueryContextOptions
	SetQueryContextOptions(models.QueryContextOptions) EngineOptions
	CostScope() tally.Scope
	SetCostScope(tally.Scope) EngineOptions
	GlobalEnforcer() qcost.ChainedEnforcer
	SetGlobalEnforcer(qcost.ChainedEnforcer) EngineOptions
	Store() storage.Storage
	SetStore(storage.Storage) EngineOptions
	LookbackDuration() time.Duration
	SetLookbackDuration(time.Duration) EngineOptions
}
