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

package m3

import (
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/cost"

	"github.com/uber-go/tally"
)

// AccountedSeriesIter wraps a series iterator to track and enforce limits on datapoint usage. Datapoint usage
// is tracked on each call to Next().
type AccountedSeriesIter struct {
	encoding.SeriesIterator

	scope       tally.Scope
	fetchedDps  tally.Counter
	enforcer    cost.ChainedEnforcer
	enforcerErr error
}

// NewAccountedSeriesIter constructs an AccountedSeriesIter which uses wrapped as its source.
func NewAccountedSeriesIter(wrapped encoding.SeriesIterator, enforcer cost.ChainedEnforcer, scope tally.Scope) *AccountedSeriesIter {
	return &AccountedSeriesIter{
		SeriesIterator: wrapped,
		enforcer:       enforcer,
		scope:          scope,
		fetchedDps:     scope.Tagged(map[string]string{"type": "fetched"}).Counter("datapoints"),
	}
}

// Err returns the underlying iterator's error if present, or any limit exceeded error.
func (as *AccountedSeriesIter) Err() error {
	if err := as.SeriesIterator.Err(); err != nil {
		return err
	}
	return as.enforcerErr
}

// Next advances the underlying iterator and adds to the datapoint count. If that count exceeds the limit, it will
// set this iterator's error.
func (as *AccountedSeriesIter) Next() bool {
	if as.enforcerErr != nil {
		return false
	}

	hasNext := as.SeriesIterator.Next()

	if !hasNext {
		// nothing left; don't inform the enforcer
		return false
	}

	as.fetchedDps.Inc(1)
	// we actually advanced the iterator; inform the enforcer
	r := as.enforcer.Add(1.0)

	if err := r.Error; err != nil {
		as.enforcerErr = err
	}

	return hasNext
}

// Close closes the underlying iterator, and marks datapoints as released to our enforcer.
func (as *AccountedSeriesIter) Close() {
	as.SeriesIterator.Close()
	as.enforcer.Release()
}
