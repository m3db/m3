// Copyright (c) 2024 Uber Technologies, Inc.
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
	"errors"

	"github.com/m3db/m3/src/cluster/kv"
)

var (
	defaultNamespace       = "/attribution/rules"
	errRuleSetNotAvailable = errors.New("ruleSet is not available")
)

// Service provides accessibility to the ruleset.
type Service interface {
	// Get returns the RuleSet and version for the given name.
	Get(name string) (RuleSet, error)

	// CheckAndSet sets the RuleSet for the name if the version matches.
	CheckAndSet(rs RuleSet, version int) (RuleSet, error)

	// Watch returns a RuleSet watch.
	Watch(name string) (Watch, error)
}

type service struct {
	store kv.Store
}

// NewService creates a RuleSet service.
func NewService(sOpts ServiceOptions) (Service, error) {
	kvOpts := sanitizeKVOptions(sOpts.KVOverrideOptions())
	store, err := sOpts.ConfigService().Store(kvOpts)
	if err != nil {
		return nil, err
	}
	return &service{
		store: store,
	}, nil
}

func (s *service) Get(name string) (RuleSet, error) {
	value, err := s.store.Get(key(name))
	if err != nil {
		return nil, err
	}
	t, err := NewRuleSetFromValue(value)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (s *service) CheckAndSet(rs RuleSet, version int) (RuleSet, error) {
	if err := rs.Validate(); err != nil {
		return nil, err
	}
	rspb, err := NewRuleSetToProto(rs)
	if err != nil {
		return nil, err
	}
	version, err = s.store.CheckAndSet(key(rs.Name()), version, rspb)
	if err != nil {
		return nil, err
	}
	return rs.SetVersion(version), nil
}

func (s *service) Watch(name string) (Watch, error) {
	w, err := s.store.Watch(key(name))
	if err != nil {
		return nil, err
	}
	return NewWatch(w), nil
}

func key(name string) string {
	return defaultNamespace + "/" + name
}

func sanitizeKVOptions(opts kv.OverrideOptions) kv.OverrideOptions {
	if opts.Namespace() == "" {
		opts = opts.SetNamespace(defaultNamespace)
	}
	return opts
}

// Watch watches the updates of a ruleset.
type Watch interface {
	// C returns the notification channel.
	C() <-chan struct{}

	// Get returns the latest version of the RuleSet.
	Get() (RuleSet, error)

	// Close stops watching for RuleSet updates.
	Close()
}

// NewWatch creates a new ruleset watch.
func NewWatch(w kv.ValueWatch) Watch {
	return &watch{w}
}

type watch struct {
	kv.ValueWatch
}

func (w *watch) Get() (RuleSet, error) {
	value := w.ValueWatch.Get()
	if value == nil {
		return nil, errRuleSetNotAvailable
	}
	rs, err := NewRuleSetFromValue(value)
	if err != nil {
		return nil, err
	}
	return rs, nil
}
