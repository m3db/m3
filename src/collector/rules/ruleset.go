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
	"sync"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/util/runtime"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/clock"
)

// ruleSet contains the list of rules for a namespace.
type ruleSet struct {
	sync.RWMutex
	runtime.Value

	namespace       []byte
	key             string
	cache           Cache
	store           kv.Store
	opts            Options
	nowFn           clock.NowFn
	maxNegativeSkew time.Duration
	ruleSetOpts     rules.Options

	proto        *schema.RuleSet
	version      int
	cutoverNanos int64
	tombstoned   bool
	matcher      rules.Matcher
}

func newRuleSet(
	namespace []byte,
	key string,
	cache Cache,
	opts Options,
) *ruleSet {
	clockOpts := opts.ClockOptions()
	r := &ruleSet{
		namespace:       namespace,
		key:             key,
		cache:           cache,
		store:           opts.KVStore(),
		opts:            opts,
		nowFn:           clockOpts.NowFn(),
		maxNegativeSkew: clockOpts.MaxNegativeSkew(),
		ruleSetOpts:     opts.RuleSetOptions(),
		proto:           &schema.RuleSet{},
		version:         kv.UninitializedVersion,
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

func (r *ruleSet) Match(id []byte, from time.Time, to time.Time) rules.MatchResult {
	r.RLock()
	defer r.RUnlock()

	if r.matcher == nil {
		return rules.EmptyMatchResult
	}
	return r.matcher.MatchAll(id, from, to)
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
	return rules.NewRuleSet(value.Version(), r.proto, r.ruleSetOpts)
}

// process processes an ruleset update.
func (r *ruleSet) process(value interface{}) error {
	r.Lock()
	defer r.Unlock()

	ruleSet := value.(rules.RuleSet)
	r.version = ruleSet.Version()
	r.cutoverNanos = ruleSet.CutoverNanos()
	r.tombstoned = ruleSet.TombStoned()
	r.matcher = ruleSet.ActiveSet(r.nowFn().Add(-r.maxNegativeSkew))
	r.cache.Register(r.namespace, r)
	return nil
}
