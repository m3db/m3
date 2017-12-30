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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3cluster/kv"
	kvutil "github.com/m3db/m3cluster/kv/util"
	"github.com/m3db/m3metrics/rules/validator/namespace"
	"github.com/m3db/m3x/log"
)

var (
	errValidatorClosed = errors.New("validator is closed")
)

type validator struct {
	sync.RWMutex

	opts               NamespaceValidatorOptions
	logger             log.Logger
	kvStore            kv.Store
	validNamespacesKey string
	initWatchTimeout   time.Duration

	closed          bool
	doneCh          chan struct{}
	wg              sync.WaitGroup
	validNamespaces map[string]struct{}
}

// NewNamespaceValidator creates a new namespace validator.
func NewNamespaceValidator(opts NamespaceValidatorOptions) (namespace.Validator, error) {
	v := &validator{
		opts:               opts,
		logger:             opts.InstrumentOptions().Logger(),
		kvStore:            opts.KVStore(),
		validNamespacesKey: opts.ValidNamespacesKey(),
		initWatchTimeout:   opts.InitWatchTimeout(),
		doneCh:             make(chan struct{}),
		validNamespaces:    toStringSet(opts.DefaultValidNamespaces()),
	}
	if err := v.watchRuntimeConfig(); err != nil {
		return nil, err
	}
	return v, nil
}

// Validate validates whether a given namespace is valid.
func (v *validator) Validate(ns string) error {
	v.RLock()
	defer v.RUnlock()

	if v.closed {
		return errValidatorClosed
	}
	if _, exists := v.validNamespaces[ns]; !exists {
		return fmt.Errorf("%s is not a valid namespace", ns)
	}
	return nil
}

func (v *validator) Close() {
	v.Lock()
	if v.closed {
		v.Unlock()
		return
	}
	v.closed = true
	close(v.doneCh)
	v.Unlock()

	// NB: Must wait outside the lock to avoid a circular dependency
	// between the goroutine closing the validator and the goroutine
	// processing updates.
	v.wg.Wait()
}

func (v *validator) watchRuntimeConfig() error {
	watch, err := v.kvStore.Watch(v.validNamespacesKey)
	if err != nil {
		return err
	}
	kvOpts := kvutil.NewOptions().SetLogger(v.logger)
	select {
	case <-watch.C():
		v.processUpdate(watch.Get(), kvOpts)
	case <-time.After(v.initWatchTimeout):
		v.logger.WithFields(
			log.NewField("key", v.validNamespacesKey),
			log.NewField("timeout", v.initWatchTimeout),
			log.NewField("default", v.opts.DefaultValidNamespaces()),
		).Warnf("timed out waiting for initial valid namespaces")
	}

	v.wg.Add(1)
	go func() {
		defer v.wg.Done()

		for {
			select {
			case <-watch.C():
				v.processUpdate(watch.Get(), kvOpts)
			case <-v.doneCh:
				return
			}
		}
	}()
	return nil
}

func (v *validator) processUpdate(value kv.Value, opts kvutil.Options) {
	strs, err := kvutil.StringArrayFromValue(value, v.validNamespacesKey, v.opts.DefaultValidNamespaces(), opts)
	if err != nil {
		// kvutil already logged the error.
		return
	}
	m := toStringSet(strs)
	v.Lock()
	v.validNamespaces = m
	v.Unlock()
}

func toStringSet(strs []string) map[string]struct{} {
	m := make(map[string]struct{}, len(strs))
	for _, s := range strs {
		m[s] = struct{}{}
	}
	return m
}
