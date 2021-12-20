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
	"time"

	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/metrics/matcher/cache"
	"github.com/m3db/m3/src/metrics/matcher/namespace"
	"github.com/m3db/m3/src/metrics/metric/id/m3"
	"github.com/m3db/m3/src/metrics/rules"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
)

// Configuration is config used to create a Matcher.
type Configuration struct {
	InitWatchTimeout      time.Duration                `yaml:"initWatchTimeout"`
	RulesKVConfig         kv.OverrideConfiguration     `yaml:"rulesKVConfig"`
	NamespacesKey         string                       `yaml:"namespacesKey" validate:"nonzero"`
	RuleSetKeyFmt         string                       `yaml:"ruleSetKeyFmt" validate:"nonzero"`
	NamespaceTag          string                       `yaml:"namespaceTag" validate:"nonzero"`
	DefaultNamespace      string                       `yaml:"defaultNamespace" validate:"nonzero"`
	NameTagKey            string                       `yaml:"nameTagKey" validate:"nonzero"`
	MatchRangePast        *time.Duration               `yaml:"matchRangePast"`
	SortedTagIteratorPool pool.ObjectPoolConfiguration `yaml:"sortedTagIteratorPool"`
}

// NewNamespaces creates a matcher.Namespaces.
func (cfg *Configuration) NewNamespaces(
	kvCluster client.Client,
	clockOpts clock.Options,
	instrumentOpts instrument.Options,
) (Namespaces, error) {
	opts, err := cfg.NewOptions(kvCluster, clockOpts, instrumentOpts)
	if err != nil {
		return nil, err
	}

	namespaces := NewNamespaces(opts.NamespacesKey(), opts)
	return namespaces, nil
}

// NewMatcher creates a Matcher.
func (cfg *Configuration) NewMatcher(
	cache cache.Cache,
	kvCluster client.Client,
	clockOpts clock.Options,
	instrumentOpts instrument.Options,
) (Matcher, error) {
	opts, err := cfg.NewOptions(kvCluster, clockOpts, instrumentOpts)
	if err != nil {
		return nil, err
	}

	return NewMatcher(cache, opts)
}

// NewOptions creates a Options.
func (cfg *Configuration) NewOptions(
	kvCluster client.Client,
	clockOpts clock.Options,
	instrumentOpts instrument.Options,
) (Options, error) {
	// Configure rules kv store.
	kvOpts, err := cfg.RulesKVConfig.NewOverrideOptions()
	if err != nil {
		return nil, err
	}

	rulesStore, err := kvCluster.Store(kvOpts)
	if err != nil {
		return nil, err
	}

	// Configure rules options.
	tagsFilterOptions := filters.TagsFilterOptions{
		NameTagKey: []byte(cfg.NameTagKey),
	}

	ruleSetOpts := rules.NewOptions().
		SetTagsFilterOptions(tagsFilterOptions).
		SetNewRollupIDFn(m3.NewRollupID)

	// Configure ruleset key function.
	ruleSetKeyFn := func(namespace []byte) string {
		return fmt.Sprintf(cfg.RuleSetKeyFmt, namespace)
	}

	opts := NewOptions().
		SetClockOptions(clockOpts).
		SetInstrumentOptions(instrumentOpts).
		SetRuleSetOptions(ruleSetOpts).
		SetKVStore(rulesStore).
		SetNamespacesKey(cfg.NamespacesKey).
		SetRuleSetKeyFn(ruleSetKeyFn).
		SetNamespaceResolver(namespace.NewResolver([]byte(cfg.NamespaceTag), []byte(cfg.DefaultNamespace)))

	if cfg.InitWatchTimeout != 0 {
		opts = opts.SetInitWatchTimeout(cfg.InitWatchTimeout)
	}
	if cfg.MatchRangePast != nil {
		opts = opts.SetMatchRangePast(*cfg.MatchRangePast)
	}

	return opts, nil
}
