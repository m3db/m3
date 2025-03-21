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
	"time"

	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/matcher/cache"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/rules"
	"github.com/m3db/m3/src/metrics/rules/view"
)

// Matcher matches rules against metric IDs.
type Matcher interface {
	rules.ActiveSet

	// Close closes the matcher.
	Close() error
}

type matcher struct {
	namespaces Namespaces
	cache      cache.Cache
	metrics    matcherMetrics
}

// NewMatcher creates a new rule matcher, optionally with a cache.
func NewMatcher(cache cache.Cache, opts Options) (Matcher, error) {
	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope()
	iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("namespaces"))
	namespacesOpts := opts.SetInstrumentOptions(iOpts)

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
	if err := namespaces.Open(); err != nil {
		return nil, err
	}

	if cache == nil {
		return &noCacheMatcher{
			namespaces: namespaces,
			metrics:    newMatcherMetrics(scope.SubScope("matcher")),
		}, nil
	}

	return &matcher{
		namespaces: namespaces,
		cache:      cache,
		metrics:    newMatcherMetrics(scope.SubScope("cached-matcher")),
	}, nil
}

func (m *matcher) LatestRollupRules(namespace []byte, timeNanos int64) ([]view.RollupRule, error) {
	return m.namespaces.LatestRollupRules(namespace, timeNanos)
}

func (m *matcher) ForwardMatch(
	id id.ID,
	fromNanos, toNanos int64,
	opts rules.MatchOptions,
) (rules.MatchResult, error) {
	sw := m.metrics.matchLatency.Start()
	defer sw.Stop()
	return m.cache.ForwardMatch(id, fromNanos, toNanos, opts)
}

func (m *matcher) ReverseMatch(
	id id.ID,
	fromNanos int64,
	toNanos int64,
	mt metric.Type,
	at aggregation.Type,
	isMultiAggregationTypesAllowed bool,
	aggTypesOpts aggregation.TypesOptions,
	matchOpts rules.MatchOptions,
) (rules.MatchResult, error) {
	sw := m.metrics.matchLatency.Start()
	defer sw.Stop()
	// Cache does not support reverse matching.
	return m.namespaces.ReverseMatch(
		id,
		fromNanos,
		toNanos,
		mt,
		at,
		isMultiAggregationTypesAllowed,
		aggTypesOpts,
		matchOpts,
	)
}

func (m *matcher) Close() error {
	m.namespaces.Close()
	return m.cache.Close()
}

type noCacheMatcher struct {
	namespaces Namespaces
	metrics    matcherMetrics
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

func (m *noCacheMatcher) LatestRollupRules(namespace []byte, timeNanos int64) ([]view.RollupRule, error) {
	return m.namespaces.LatestRollupRules(namespace, timeNanos)
}

func (m *noCacheMatcher) ForwardMatch(
	id id.ID,
	fromNanos, toNanos int64,
	opts rules.MatchOptions,
) (rules.MatchResult, error) {
	sw := m.metrics.matchLatency.Start()
	defer sw.Stop()
	return m.namespaces.ForwardMatch(id, fromNanos, toNanos, opts)
}

func (m *noCacheMatcher) ReverseMatch(
	id id.ID,
	fromNanos int64,
	toNanos int64,
	mt metric.Type,
	at aggregation.Type,
	isMultiAggregationTypesAllowed bool,
	aggTypesOpts aggregation.TypesOptions,
	matchOpts rules.MatchOptions,
) (rules.MatchResult, error) {
	sw := m.metrics.matchLatency.Start()
	defer sw.Stop()
	return m.namespaces.ReverseMatch(
		id,
		fromNanos,
		toNanos,
		mt,
		at,
		isMultiAggregationTypesAllowed,
		aggTypesOpts,
		matchOpts,
	)
}

func (m *noCacheMatcher) Close() error {
	m.namespaces.Close()
	return nil
}
