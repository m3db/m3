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
	"sync"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/util/runtime"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"

	"github.com/uber-go/tally"
)

// RuleSet manages runtime updates to registered rules and provides
// API to match metic ids against rules in the corresponding ruleset.
type RuleSet interface {
	runtime.Value
	rules.Matcher

	// Namespace returns the namespace of the ruleset.
	Namespace() []byte

	// Version returns the current version of the ruleset.
	Version() int

	// CutoverNanos returns the cutover time of the ruleset.
	CutoverNanos() int64

	// Tombstoned returns whether the ruleset is tombstoned.
	Tombstoned() bool
}

type ruleSetMetrics struct {
	match      instrument.MethodMetrics
	nilMatcher tally.Counter
	updated    tally.Counter
}

func newRuleSetMetrics(scope tally.Scope, samplingRate float64) ruleSetMetrics {
	return ruleSetMetrics{
		match:      instrument.NewMethodMetrics(scope, "match", samplingRate),
		nilMatcher: scope.Counter("nil-matcher"),
		updated:    scope.Counter("updated"),
	}
}

// ruleSet contains the list of rules for a namespace.
type ruleSet struct {
	sync.RWMutex
	runtime.Value

	namespace          []byte
	key                string
	store              kv.Store
	opts               Options
	nowFn              clock.NowFn
	matchRangePast     time.Duration
	ruleSetOpts        rules.Options
	onRuleSetUpdatedFn OnRuleSetUpdatedFn

	proto        *schema.RuleSet
	version      int
	cutoverNanos int64
	tombstoned   bool
	matcher      rules.Matcher
	metrics      ruleSetMetrics
}

func newRuleSet(
	namespace []byte,
	key string,
	opts Options,
) RuleSet {
	instrumentOpts := opts.InstrumentOptions()
	r := &ruleSet{
		namespace:          namespace,
		key:                key,
		opts:               opts,
		store:              opts.KVStore(),
		nowFn:              opts.ClockOptions().NowFn(),
		matchRangePast:     opts.MatchRangePast(),
		ruleSetOpts:        opts.RuleSetOptions(),
		onRuleSetUpdatedFn: opts.OnRuleSetUpdatedFn(),
		proto:              &schema.RuleSet{},
		version:            kv.UninitializedVersion,
		metrics:            newRuleSetMetrics(instrumentOpts.MetricsScope(), instrumentOpts.MetricsSamplingRate()),
	}
	valueOpts := runtime.NewOptions().
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetInitWatchTimeout(opts.InitWatchTimeout()).
		SetKVStore(r.store).
		SetUnmarshalFn(r.toRuleSet).
		SetProcessFn(r.process)
	r.Value = runtime.NewValue(key, valueOpts)
	return r
}

func (r *ruleSet) Namespace() []byte {
	r.RLock()
	namespace := r.namespace
	r.RUnlock()
	return namespace
}

func (r *ruleSet) Version() int {
	r.RLock()
	version := r.version
	r.RUnlock()
	return version
}

func (r *ruleSet) CutoverNanos() int64 {
	r.RLock()
	cutoverNanos := r.cutoverNanos
	r.RUnlock()
	return cutoverNanos
}

func (r *ruleSet) Tombstoned() bool {
	r.RLock()
	tombstoned := r.tombstoned
	r.RUnlock()
	return tombstoned
}

func (r *ruleSet) ForwardMatch(id []byte, fromNanos, toNanos int64) rules.MatchResult {
	callStart := r.nowFn()
	r.RLock()
	if r.matcher == nil {
		r.RUnlock()
		r.metrics.nilMatcher.Inc(1)
		return rules.EmptyMatchResult
	}
	res := r.matcher.ForwardMatch(id, fromNanos, toNanos)
	r.RUnlock()
	r.metrics.match.ReportSuccess(r.nowFn().Sub(callStart))
	return res
}

func (r *ruleSet) ReverseMatch(id []byte, fromNanos, toNanos int64, mt metric.Type, at policy.AggregationType) rules.MatchResult {
	callStart := r.nowFn()
	r.RLock()
	if r.matcher == nil {
		r.RUnlock()
		r.metrics.nilMatcher.Inc(1)
		return rules.EmptyMatchResult
	}
	res := r.matcher.ReverseMatch(id, fromNanos, toNanos, mt, at)
	r.RUnlock()
	r.metrics.match.ReportSuccess(r.nowFn().Sub(callStart))
	return res
}

func (r *ruleSet) toRuleSet(value kv.Value) (interface{}, error) {
	r.Lock()
	defer r.Unlock()

	if value == nil {
		return nil, errNilValue
	}
	r.proto.Reset()
	if err := value.Unmarshal(r.proto); err != nil {
		return nil, err
	}
	return rules.NewRuleSetFromSchema(value.Version(), r.proto, r.ruleSetOpts)
}

// process processes an ruleset update.
func (r *ruleSet) process(value interface{}) error {
	r.Lock()
	defer r.Unlock()

	ruleSet := value.(rules.RuleSet)
	r.version = ruleSet.Version()
	r.cutoverNanos = ruleSet.CutoverNanos()
	r.tombstoned = ruleSet.Tombstoned()
	r.matcher = ruleSet.ActiveSet(r.nowFn().Add(-r.matchRangePast).UnixNano())
	if r.onRuleSetUpdatedFn != nil {
		r.onRuleSetUpdatedFn(r.namespace, r)
	}
	r.metrics.updated.Inc(1)
	return nil
}
