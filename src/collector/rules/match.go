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
	"github.com/m3db/m3cluster/kv/util/runtime"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/log"
)

// Matcher matches rules against metric IDs.
type Matcher interface {
	// Match matches rules against metric ID and returns the match result.
	Match(id id.ID) rules.MatchResult

	// Close closes the matcher.
	Close() error
}

type matcher struct {
	opts             Options
	namespaceTag     []byte
	defaultNamespace []byte

	namespaces *namespaces
	cache      Cache
}

// NewMatcher creates a new rule matcher.
func NewMatcher(cache Cache, opts Options) (Matcher, error) {
	scope := opts.InstrumentOptions().MetricsScope()
	key := opts.NamespacesKey()
	namespaces := newNamespaces(key, cache, opts)
	if err := namespaces.Watch(); err != nil {
		errCreateWatch, ok := err.(runtime.CreateWatchError)
		if ok {
			scope.Counter("create-watch-errors").Inc(1)
			return nil, errCreateWatch
		}
		// NB(xichen): we managed to watch the key but weren't able
		// to initialize the value. In this case, log the error instead
		// to be more resilient to error conditions preventing process
		// from starting up.
		scope.Counter("init-watch-errors").Inc(1)
		log := opts.InstrumentOptions().Logger()
		log.WithFields(
			xlog.NewLogField("key", key),
			xlog.NewLogErrField(err),
		).Error("error initializing namespaces values")
	}
	return &matcher{
		opts:             opts,
		namespaceTag:     opts.NamespaceTag(),
		defaultNamespace: opts.DefaultNamespace(),
		namespaces:       namespaces,
		cache:            cache,
	}, nil
}

func (m *matcher) Match(id id.ID) rules.MatchResult {
	ns, found := id.TagValue(m.namespaceTag)
	if !found {
		ns = m.defaultNamespace
	}
	return m.cache.Match(ns, id.Bytes())
}

func (m *matcher) Close() error {
	m.namespaces.Close()
	return m.cache.Close()
}
