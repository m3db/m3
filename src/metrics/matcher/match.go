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

package matcher

import (
	"errors"
	"time"

	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/metrics/matcher/cache"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/rules"
)

var errNotOpened = errors.New("matcher not opened")

// Matcher matches rules against metric IDs.
// A Matcher is not thread safe and should not be shared across goroutines.
type Matcher interface {
	// Open the matcher. Must be called before any match calls.
	Open() error

	// ForwardMatch matches rules against metric ID for time range [fromNanos, toNanos)
	// and returns the match result.
	ForwardMatch(id id.ID, fromNanos, toNanos int64) (rules.MatchResult, error)

	// Close closes the matcher.
	Close() error
}

type matcher struct {
	namespaceResolver namespaceResolver
	namespaces        Namespaces
	cache             cache.Cache
	metrics           matcherMetrics
	opened            bool
}

type namespaceResolver struct {
	namespaceTag     []byte
	defaultNamespace []byte
}

func (r namespaceResolver) Resolve(id id.ID) []byte {
	ns, found := id.TagValue(r.namespaceTag)
	if !found {
		ns = r.defaultNamespace
	}
	return ns
}

// NewMatcherFn creates a new Matcher
type NewMatcherFn func() Matcher

// NewMatcherFnForOptions creates a function that creates Matchers with the provided Options.
func NewMatcherFnForOptions(opts Options) NewMatcherFn {
	return func() Matcher {
		return NewMatcher(opts)
	}
}

// NewMatcher creates a new rule matcher.
// The Matcher must be Opened before using.
func NewMatcher(opts Options) Matcher {
	nsResolver := namespaceResolver{
		namespaceTag:     opts.NamespaceTag(),
		defaultNamespace: opts.DefaultNamespace(),
	}

	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope()
	iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("namespaces"))
	namespacesOpts := opts.SetInstrumentOptions(iOpts)

	cache := opts.Cache()
	if cache != nil {
		namespacesOpts = namespacesOpts.
			SetOnNamespaceAddedFn(func(namespace []byte, ruleSet RuleSet) {
				cache.Register(namespace, ruleSet)
			}).
			SetOnNamespaceRemovedFn(func(namespace []byte) {
				cache.Unregister(namespace)
			}).
			SetOnRuleSetUpdatedFn(func(namespace []byte, ruleSet RuleSet) {
				cache.Refresh(namespace, ruleSet)
			})
	}

	namespaces := NewNamespaces(opts.NamespacesKey(), namespacesOpts)
	if cache == nil {
		return &noCacheMatcher{
			namespaceResolver: nsResolver,
			namespaces:        namespaces,
			metrics:           newMatcherMetrics(scope.SubScope("matcher")),
		}
	}

	return &matcher{
		namespaceResolver: nsResolver,
		namespaces:        namespaces,
		cache:             cache,
		metrics:           newMatcherMetrics(scope.SubScope("cached-matcher")),
	}
}

func (m *matcher) Open() error {
	if m.opened {
		return nil
	}
	if err := m.namespaces.Open(); err != nil {
		return err
	}
	m.opened = true
	return nil
}

func (m *matcher) ForwardMatch(
	id id.ID,
	fromNanos, toNanos int64) (rules.MatchResult, error) {
	if !m.opened {
		return rules.MatchResult{}, errNotOpened
	}
	sw := m.metrics.matchLatency.Start()
	defer sw.Stop()
	return m.cache.ForwardMatch(m.namespaceResolver.Resolve(id), id.Bytes(), fromNanos, toNanos), nil
}

func (m *matcher) Close() error {
	m.namespaces.Close()
	return m.cache.Close()
}

type noCacheMatcher struct {
	opened            bool
	namespaces        Namespaces
	namespaceResolver namespaceResolver
	metrics           matcherMetrics
}

type matcherMetrics struct {
	matchLatency tally.Histogram
}

func newMatcherMetrics(scope tally.Scope) matcherMetrics {
	return matcherMetrics{
		matchLatency: scope.Histogram(
			"match-latency",
			append(
				tally.DurationBuckets{0},
				tally.MustMakeExponentialDurationBuckets(time.Millisecond, 1.5, 15)...,
			),
		),
	}
}

func (m *noCacheMatcher) Open() error {
	if m.opened {
		return nil
	}
	if err := m.namespaces.Open(); err != nil {
		return err
	}
	m.opened = true
	return nil
}

func (m *noCacheMatcher) ForwardMatch(
	id id.ID,
	fromNanos, toNanos int64) (rules.MatchResult, error) {
	if !m.opened {
		return rules.MatchResult{}, errNotOpened
	}
	sw := m.metrics.matchLatency.Start()
	defer sw.Stop()
	return m.namespaces.ForwardMatch(m.namespaceResolver.Resolve(id), id.Bytes(), fromNanos, toNanos), nil
}

func (m *noCacheMatcher) Close() error {
	m.namespaces.Close()
	return nil
}
