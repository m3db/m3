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

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/util/runtime"
	"github.com/m3db/m3/src/metrics/generated/proto/rulepb"
	"github.com/m3db/m3/src/metrics/rules"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"

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

	// Reset the RuleSet so it can be used for a new request.
	Reset()
}

type ruleSetMetrics struct {
	match      instrument.MethodMetrics
	nilMatcher tally.Counter
	updated    tally.Counter
}

func newRuleSetMetrics(scope tally.Scope, opts instrument.TimerOptions) ruleSetMetrics {
	return ruleSetMetrics{
		match:      instrument.NewMethodMetrics(scope, "match", opts),
		nilMatcher: scope.Counter("nil-matcher"),
		updated:    scope.Counter("updated"),
	}
}

// ruleSet contains the list of rules for a namespace.
type ruleSet struct {
	sync.Mutex
	runtime.Value

	namespace          []byte
	key                string
	store              kv.Store
	opts               Options
	nowFn              clock.NowFn
	matchRangePast     time.Duration
	ruleSetOpts        rules.Options
	onRuleSetUpdatedFn OnRuleSetUpdatedFn

	version      int
	cutoverNanos int64
	tombstoned   bool
	nextRuleSet  rules.RuleSet
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
		version:            kv.UninitializedVersion,
		metrics: newRuleSetMetrics(instrumentOpts.MetricsScope(),
			instrumentOpts.TimerOptions()),
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
	return r.namespace
}

func (r *ruleSet) Version() int {
	return r.version
}

func (r *ruleSet) CutoverNanos() int64 {
	return r.cutoverNanos
}

func (r *ruleSet) Tombstoned() bool {
	return r.tombstoned
}

func (r *ruleSet) ForwardMatch(id []byte, fromNanos, toNanos int64) rules.MatchResult {
	callStart := r.nowFn()
	if r.matcher == nil {
		r.metrics.nilMatcher.Inc(1)
		return rules.EmptyMatchResult
	}
	res := r.matcher.ForwardMatch(id, fromNanos, toNanos)
	r.metrics.match.ReportSuccess(r.nowFn().Sub(callStart))
	return res
}

func (r *ruleSet) toRuleSet(value kv.Value) (interface{}, error) {
	if value == nil {
		return nil, errNilValue
	}
	var proto rulepb.RuleSet
	if err := value.Unmarshal(&proto); err != nil {
		return nil, err
	}
	return rules.NewRuleSetFromProto(value.Version(), &proto, r.ruleSetOpts)
}

// process processes an ruleset update.
func (r *ruleSet) process(value interface{}) error {
	r.Lock()
	r.nextRuleSet = value.(rules.RuleSet)
	r.Unlock()
	return nil
}

func (r *ruleSet) Reset() {
	r.Lock()
	ruleSet := r.nextRuleSet
	r.Unlock()
	if ruleSet == nil {
		return
	}
	r.version = ruleSet.Version()
	r.cutoverNanos = ruleSet.CutoverNanos()
	r.tombstoned = ruleSet.Tombstoned()
	r.matcher = ruleSet.ActiveSet(r.nowFn().Add(-r.matchRangePast).UnixNano())

	// NB: calling the update callback outside the ruleset lock to avoid circular
	// lock dependency causing a deadlock.
	if r.onRuleSetUpdatedFn != nil {
		r.onRuleSetUpdatedFn(r.namespace, r)
	}
	r.metrics.updated.Inc(1)
}
