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
	"fmt"
	"math"
	"time"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/metrics/matcher/cache"
	"github.com/m3db/m3/src/metrics/rules"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
)

const (
	defaultInitWatchTimeout = 10 * time.Second
	defaultNamespacesKey    = "/namespaces"
	defaultRuleSetKeyFormat = "/ruleset/%s"
	defaultMatchRangePast   = time.Duration(math.MaxInt64)
)

var (
	defaultNamespaceTag     = []byte("namespace")
	defaultDefaultNamespace = []byte("default")
)

// RuleSetKeyFn generates the ruleset key for a given namespace.
type RuleSetKeyFn func(namespace []byte) string

// OnNamespaceAddedFn is called when a namespace is added.
type OnNamespaceAddedFn func(namespace []byte, ruleSet RuleSet)

// OnNamespaceRemovedFn is called when a namespace is removed.
type OnNamespaceRemovedFn func(namespace []byte)

// OnRuleSetUpdatedFn is called when a ruleset is updated.
type OnRuleSetUpdatedFn func(namespace []byte, ruleSet RuleSet)

// Options provide a set of options for the matcher.
type Options interface {
	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) Options

	// ClockOptions returns the clock options.
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetRuleSetOptions sets the ruleset options.
	SetRuleSetOptions(value rules.Options) Options

	// RuleSetOptions returns the ruleset options.
	RuleSetOptions() rules.Options

	// SetInitWatchTimeout sets the initial watch timeout.
	SetInitWatchTimeout(value time.Duration) Options

	// InitWatchTimeout returns the initial watch timeout.
	InitWatchTimeout() time.Duration

	// SetKVStore sets the kv store.
	SetKVStore(value kv.Store) Options

	// KVStore returns the kv store.
	KVStore() kv.Store

	// SetNamespacesKey sets the key for the full list of namespaces.
	SetNamespacesKey(value string) Options

	// NamespacesKey returns the key for the full list of namespaces.
	NamespacesKey() string

	// SetRuleSetKeyFn sets the function to generate ruleset keys.
	SetRuleSetKeyFn(value RuleSetKeyFn) Options

	// RuleSetKeyFn returns the function to generate ruleset keys.
	RuleSetKeyFn() RuleSetKeyFn

	// SetNamespaceTag sets the namespace tag.
	SetNamespaceTag(value []byte) Options

	// NamespaceTag returns the namespace tag.
	NamespaceTag() []byte

	// SetDefaultNamespace sets the default namespace for ids without a namespace.
	SetDefaultNamespace(value []byte) Options

	// DefaultNamespace returns the default namespace for ids without a namespace.
	DefaultNamespace() []byte

	// SetMatchRangePast sets the limit on the earliest time eligible for rule matching.
	SetMatchRangePast(value time.Duration) Options

	// MatchRangePast returns the limit on the earliest time eligible for rule matching.
	MatchRangePast() time.Duration

	// SetOnNamespaceAddedFn sets the function to be called when a namespace is added.
	SetOnNamespaceAddedFn(value OnNamespaceAddedFn) Options

	// OnNamespaceAddedFn returns the function to be called when a namespace is added.
	OnNamespaceAddedFn() OnNamespaceAddedFn

	// SetOnNamespaceRemovedFn sets the function to be called when a namespace is removed.
	SetOnNamespaceRemovedFn(value OnNamespaceRemovedFn) Options

	// OnNamespaceRemovedFn returns the function to be called when a namespace is removed.
	OnNamespaceRemovedFn() OnNamespaceRemovedFn

	// SetOnRuleSetUpdatedFn sets the function to be called when a ruleset is updated.
	SetOnRuleSetUpdatedFn(value OnRuleSetUpdatedFn) Options

	// OnRuleSetUpdatedFn returns the function to be called when a ruleset is updated.
	OnRuleSetUpdatedFn() OnRuleSetUpdatedFn

	// SetRequireNamespaceWatchOnInit sets the flag to ensure matcher is initialized with a loaded namespace watch.
	SetRequireNamespaceWatchOnInit(value bool) Options

	// RequireNamespaceWatchOnInit returns the flag to ensure matcher is initialized with a loaded namespace watch.
	RequireNamespaceWatchOnInit() bool

	// InterruptedCh returns the interrupted channel.
	InterruptedCh() <-chan struct{}

	// SetInterruptedCh sets the interrupted channel.
	SetInterruptedCh(value <-chan struct{}) Options

	// Cache for match results. If nil, no cache is used.
	Cache() cache.Cache

	// SetCache sets Cache.
	SetCache(value cache.Cache) Options
}

type options struct {
	clockOpts                   clock.Options
	instrumentOpts              instrument.Options
	ruleSetOpts                 rules.Options
	initWatchTimeout            time.Duration
	kvStore                     kv.Store
	namespacesKey               string
	ruleSetKeyFn                RuleSetKeyFn
	namespaceTag                []byte
	defaultNamespace            []byte
	matchRangePast              time.Duration
	onNamespaceAddedFn          OnNamespaceAddedFn
	onNamespaceRemovedFn        OnNamespaceRemovedFn
	onRuleSetUpdatedFn          OnRuleSetUpdatedFn
	requireNamespaceWatchOnInit bool
	interruptedCh               <-chan struct{}
	cache                       cache.Cache
}

// NewOptions creates a new set of options.
func NewOptions() Options {
	return &options{
		clockOpts:        clock.NewOptions(),
		instrumentOpts:   instrument.NewOptions(),
		ruleSetOpts:      rules.NewOptions(),
		initWatchTimeout: defaultInitWatchTimeout,
		kvStore:          mem.NewStore(),
		namespacesKey:    defaultNamespacesKey,
		ruleSetKeyFn:     defaultRuleSetKeyFn,
		namespaceTag:     defaultNamespaceTag,
		defaultNamespace: defaultDefaultNamespace,
		matchRangePast:   defaultMatchRangePast,
		// disable the cache by default.
		// This is due to discovering that there is a lot of contention
		// used by the cache and the fact that most coordinators are used
		// in a stateless manner with a central deployment which in turn
		// leads to an extremely low cache hit ratio anyway.
		cache: nil,
	}
}

func (o *options) SetClockOptions(value clock.Options) Options {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *options) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetRuleSetOptions(value rules.Options) Options {
	opts := *o
	opts.ruleSetOpts = value
	return &opts
}

func (o *options) RuleSetOptions() rules.Options {
	return o.ruleSetOpts
}

func (o *options) SetInitWatchTimeout(value time.Duration) Options {
	opts := *o
	opts.initWatchTimeout = value
	return &opts
}

func (o *options) InitWatchTimeout() time.Duration {
	return o.initWatchTimeout
}

func (o *options) SetKVStore(value kv.Store) Options {
	opts := *o
	opts.kvStore = value
	return &opts
}

func (o *options) KVStore() kv.Store {
	return o.kvStore
}

func (o *options) SetNamespacesKey(value string) Options {
	opts := *o
	opts.namespacesKey = value
	return &opts
}

func (o *options) NamespacesKey() string {
	return o.namespacesKey
}

func (o *options) SetRuleSetKeyFn(value RuleSetKeyFn) Options {
	opts := *o
	opts.ruleSetKeyFn = value
	return &opts
}

func (o *options) RuleSetKeyFn() RuleSetKeyFn {
	return o.ruleSetKeyFn
}

func (o *options) SetNamespaceTag(value []byte) Options {
	opts := *o
	opts.namespaceTag = value
	return &opts
}

func (o *options) NamespaceTag() []byte {
	return o.namespaceTag
}

func (o *options) SetDefaultNamespace(value []byte) Options {
	opts := *o
	opts.defaultNamespace = value
	return &opts
}

func (o *options) DefaultNamespace() []byte {
	return o.defaultNamespace
}

func (o *options) SetMatchRangePast(value time.Duration) Options {
	opts := *o
	opts.matchRangePast = value
	return &opts
}

func (o *options) MatchRangePast() time.Duration {
	return o.matchRangePast
}

func (o *options) SetOnNamespaceAddedFn(value OnNamespaceAddedFn) Options {
	opts := *o
	opts.onNamespaceAddedFn = value
	return &opts
}

func (o *options) OnNamespaceAddedFn() OnNamespaceAddedFn {
	return o.onNamespaceAddedFn
}

func (o *options) SetOnNamespaceRemovedFn(value OnNamespaceRemovedFn) Options {
	opts := *o
	opts.onNamespaceRemovedFn = value
	return &opts
}

func (o *options) OnNamespaceRemovedFn() OnNamespaceRemovedFn {
	return o.onNamespaceRemovedFn
}

func (o *options) SetOnRuleSetUpdatedFn(value OnRuleSetUpdatedFn) Options {
	opts := *o
	opts.onRuleSetUpdatedFn = value
	return &opts
}

func (o *options) OnRuleSetUpdatedFn() OnRuleSetUpdatedFn {
	return o.onRuleSetUpdatedFn
}

// SetRequireNamespaceWatchOnInit sets the flag to ensure matcher is initialized with a loaded namespace watch.
func (o *options) SetRequireNamespaceWatchOnInit(value bool) Options {
	opts := *o
	opts.requireNamespaceWatchOnInit = value
	return &opts
}

// RequireNamespaceWatchOnInit returns the flag to ensure matcher is initialized with a loaded namespace watch.
func (o *options) RequireNamespaceWatchOnInit() bool {
	return o.requireNamespaceWatchOnInit
}

func (o *options) SetInterruptedCh(ch <-chan struct{}) Options {
	o.interruptedCh = ch
	return o
}

func (o *options) InterruptedCh() <-chan struct{} {
	return o.interruptedCh
}

func (o *options) Cache() cache.Cache {
	return o.cache
}

func (o *options) SetCache(value cache.Cache) Options {
	opts := *o
	opts.cache = value
	return &opts
}

func defaultRuleSetKeyFn(namespace []byte) string {
	return fmt.Sprintf(defaultRuleSetKeyFormat, namespace)
}
