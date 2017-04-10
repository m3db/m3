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
	"fmt"
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
)

var (
	defaultNamespaceTag     = []byte("namespace")
	defaultDefaultNamespace = []byte("defaultNamespace")
)

func defaultRuleSetKeyFn(namespace []byte) string {
	return fmt.Sprintf(defaultRuleSetKeyFormat, namespace)
}

// RuleSetKeyFn generates the ruleset key for a given namespace.
type RuleSetKeyFn func(namespace []byte) string

// Options provide a set of options for the msgpack-based reporter.
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
}

type options struct {
	clockOpts        clock.Options
	instrumentOpts   instrument.Options
	ruleSetOpts      rules.Options
	initWatchTimeout time.Duration
	kvStore          kv.Store
	namespacesKey    string
	ruleSetKeyFn     RuleSetKeyFn
	namespaceTag     []byte
	defaultNamespace []byte
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
