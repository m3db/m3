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
//

package models

import (
	"github.com/m3db/m3/src/query/cost"

	"github.com/uber-go/tally"
)

// QueryContext provides all external state needed to execute and track a query. It acts as a hook back into the
// execution engine for things like cost accounting.
type QueryContext struct {
	Enforcer cost.ChainedEnforcer
	Scope    tally.Scope
}

// NewQueryContext constructs a QueryContext using the given Enforcer to enforce per query limits.
func NewQueryContext(scope tally.Scope, enforcer cost.ChainedEnforcer) *QueryContext {
	return &QueryContext{
		Scope:    scope,
		Enforcer: enforcer,
	}
}

// NoopQueryContext returns a query context with no active components.
func NoopQueryContext() *QueryContext {
	return NewQueryContext(tally.NoopScope, cost.NoopChainedEnforcer())
}
