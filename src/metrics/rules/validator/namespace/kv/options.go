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

package kv

import (
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3x/instrument"
)

const (
	defaultInitWatchTimeout = 10 * time.Second
)

// NamespaceValidatorOptions provide a set of options for the KV-backed namespace validator.
type NamespaceValidatorOptions interface {
	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) NamespaceValidatorOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetKVStore sets the KV store for namespace validation.
	SetKVStore(value kv.Store) NamespaceValidatorOptions

	// KVStore returns the KV store for namespace validation.
	KVStore() kv.Store

	// SetInitWatchTimeout sets the initial watch timeout.
	SetInitWatchTimeout(value time.Duration) NamespaceValidatorOptions

	// InitWatchTimeout returns the initial watch timeout.
	InitWatchTimeout() time.Duration

	// SetValidNamespacesKey sets the KV key associated with valid namespaces.
	SetValidNamespacesKey(value string) NamespaceValidatorOptions

	// ValidNamespacesKey returns the KV key associated with valid namespaces.
	ValidNamespacesKey() string

	// SetDefaultValidNamespaces sets the default list of valid namespaces.
	SetDefaultValidNamespaces(value []string) NamespaceValidatorOptions

	// DefaultValidNamespaces returns the default list of valid namespaces.
	DefaultValidNamespaces() []string
}

type namespaceValidatorOptions struct {
	instrumentOpts         instrument.Options
	kvStore                kv.Store
	initWatchTimeout       time.Duration
	validNamespacesKey     string
	defaultValidNamespaces []string
}

// NewNamespaceValidatorOptions create a new set of namespace validator options.
func NewNamespaceValidatorOptions() NamespaceValidatorOptions {
	return &namespaceValidatorOptions{
		instrumentOpts:   instrument.NewOptions(),
		initWatchTimeout: defaultInitWatchTimeout,
	}
}

func (o *namespaceValidatorOptions) SetInstrumentOptions(value instrument.Options) NamespaceValidatorOptions {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *namespaceValidatorOptions) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *namespaceValidatorOptions) SetKVStore(value kv.Store) NamespaceValidatorOptions {
	opts := *o
	opts.kvStore = value
	return &opts
}

func (o *namespaceValidatorOptions) KVStore() kv.Store {
	return o.kvStore
}

func (o *namespaceValidatorOptions) SetInitWatchTimeout(value time.Duration) NamespaceValidatorOptions {
	opts := *o
	opts.initWatchTimeout = value
	return &opts
}

func (o *namespaceValidatorOptions) InitWatchTimeout() time.Duration {
	return o.initWatchTimeout
}

func (o *namespaceValidatorOptions) SetValidNamespacesKey(value string) NamespaceValidatorOptions {
	opts := *o
	opts.validNamespacesKey = value
	return &opts
}

func (o *namespaceValidatorOptions) ValidNamespacesKey() string {
	return o.validNamespacesKey
}

func (o *namespaceValidatorOptions) SetDefaultValidNamespaces(value []string) NamespaceValidatorOptions {
	cloned := make([]string, len(value))
	copy(cloned, value)
	opts := *o
	opts.defaultValidNamespaces = cloned
	return &opts
}

func (o *namespaceValidatorOptions) DefaultValidNamespaces() []string {
	return o.defaultValidNamespaces
}
