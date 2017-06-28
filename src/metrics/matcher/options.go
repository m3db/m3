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

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
)

const (
	defaultInitWatchTimeout = 10 * time.Second
	defaultValueRetryPeriod = 10 * time.Second
	defaultValueRetryExpiry = 3 * time.Hour
	defaultNamespacesKey    = "/namespaces"
	defaultRuleSetKeyFormat = "/ruleset/%s"
	defaultMatchRangePast   = time.Duration(math.MaxInt64)
)

var (
	defaultNamespaceTag     = []byte("namespace")
	defaultDefaultNamespace = []byte("defaultNamespace")
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
	// SetMatchMode sets the match mode.
	SetMatchMode(value rules.MatchMode) Options

	// MatchMode returns the match mode.
	MatchMode() rules.MatchMode

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
}

type options struct {
	matchMode            rules.MatchMode
	clockOpts            clock.Options
	instrumentOpts       instrument.Options
	ruleSetOpts          rules.Options
	initWatchTimeout     time.Duration
	kvStore              kv.Store
	namespacesKey        string
	ruleSetKeyFn         RuleSetKeyFn
	namespaceTag         []byte
	defaultNamespace     []byte
	matchRangePast       time.Duration
	onNamespaceAddedFn   OnNamespaceAddedFn
	onNamespaceRemovedFn OnNamespaceRemovedFn
	onRuleSetUpdatedFn   OnRuleSetUpdatedFn
}

// NewOptions creates a new set of options.
func NewOptions() Options {
	return &options{
		matchMode:        rules.ForwardMatch,
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
	}
}

func (o *options) SetMatchMode(value rules.MatchMode) Options {
	opts := *o
	opts.matchMode = value
	return &opts
}

func (o *options) MatchMode() rules.MatchMode {
	return o.matchMode
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

func defaultRuleSetKeyFn(namespace []byte) string {
	return fmt.Sprintf(defaultRuleSetKeyFormat, namespace)
}
