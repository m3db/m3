// Copyright (c) 2017 Uber Technologies, Inc.
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

package rules

import (
	"time"

	"github.com/m3db/m3metrics/rules"
)

// Source is a datasource providing match results.
type Source interface {
	// Match returns the match result for an given id.
	Match(id []byte, t time.Time) rules.MatchResult
}

// Cache caches the rule matching result associated with metrics.
type Cache interface {
	// Match returns the rule matching result associated with a metric id.
	Match(namespace []byte, id []byte) rules.MatchResult

	// Register sets the result source for a given namespace.
	Register(namespace []byte, source Source)

	// Unregister deletes the cached results for a given namespace.
	Unregister(namespace []byte)

	// Close closes the cache.
	Close() error
}
