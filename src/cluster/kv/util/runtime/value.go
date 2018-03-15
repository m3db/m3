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

package runtime

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3x/log"
)

var (
	errInitWatchTimeout = errors.New("init watch timeout")
	errNilValue         = errors.New("nil kv value")
)

// Value is a value that can be updated during runtime.
type Value interface {
	// Key is the key associated with value.
	Key() string

	// Watch starts watching for value updates.
	Watch() error

	// Unwatch stops watching for value updates.
	Unwatch()
}

// UnmarshalFn unmarshals a kv value and extracts its payload.
type UnmarshalFn func(value kv.Value) (interface{}, error)

// ProcessFn processes a value.
type ProcessFn func(value interface{}) error

// updateWithLockFn updates a value while holding a lock.
type updateWithLockFn func(value kv.Value) error

type valueStatus int

const (
	valueNotWatching valueStatus = iota
	valueWatching
)

type value struct {
	sync.RWMutex

	key              string
	store            kv.Store
	opts             Options
	log              log.Logger
	unmarshalFn      UnmarshalFn
	processFn        ProcessFn
	updateWithLockFn updateWithLockFn

	status    valueStatus
	watch     kv.ValueWatch
	currValue kv.Value
}

// NewValue creates a new value.
func NewValue(
	key string,
	opts Options,
) Value {
	v := &value{
		key:         key,
		opts:        opts,
		store:       opts.KVStore(),
		log:         opts.InstrumentOptions().Logger(),
		unmarshalFn: opts.UnmarshalFn(),
		processFn:   opts.ProcessFn(),
	}
	v.updateWithLockFn = v.updateWithLock
	return v
}

func (v *value) Key() string { return v.key }

func (v *value) Watch() error {
	v.Lock()
	defer v.Unlock()

	if v.status == valueWatching {
		return nil
	}
	watch, err := v.store.Watch(v.key)
	if err != nil {
		return CreateWatchError{innerError: err}
	}
	v.status = valueWatching
	v.watch = watch

	select {
	case <-watch.C():
	case <-time.After(v.opts.InitWatchTimeout()):
		err = errInitWatchTimeout
	}

	if err == nil {
		err = v.updateWithLockFn(watch.Get())
	}

	// NB(xichen): we want to start watching updates even though
	// we may fail to initialize the value temporarily (e.g., during
	// a network partition) so the value will be updated when the
	// error condition is resolved.
	go v.watchUpdates(v.watch)
	if err != nil {
		return InitValueError{innerError: err}
	}
	return nil
}

func (v *value) Unwatch() {
	v.Lock()
	defer v.Unlock()

	// If status is nil, it means we are not watching.
	if v.status == valueNotWatching {
		return
	}
	v.watch.Close()
	v.status = valueNotWatching
	v.watch = nil
}

func (v *value) watchUpdates(watch kv.ValueWatch) {
	for range watch.C() {
		v.Lock()
		// If we are not watching, or we are watching with a different
		// watch because we stopped the current watch and started a new
		// one, return immediately.
		if v.status != valueWatching || v.watch != watch {
			v.Unlock()
			return
		}
		if err := v.updateWithLockFn(watch.Get()); err != nil {
			v.log.Errorf("error updating value: %v", err)
		}
		v.Unlock()
	}
}

func (v *value) updateWithLock(update kv.Value) error {
	if update == nil {
		return errNilValue
	}
	if v.currValue != nil && !update.IsNewer(v.currValue) {
		v.log.Warnf("ignore kv update with version %d which is not newer than the version of the current value: %d", update.Version(), v.currValue.Version())
		return nil
	}
	v.log.Infof("received kv update with version %d for key %s", update.Version(), v.key)
	latest, err := v.unmarshalFn(update)
	if err != nil {
		err = fmt.Errorf("error unmarshalling value for version %d: %v", update.Version(), err)
		return err
	}
	if err := v.processFn(latest); err != nil {
		return err
	}
	v.currValue = update
	return nil
}

// CreateWatchError is returned when encountering an error creating a watch.
type CreateWatchError struct {
	innerError error
}

func (e CreateWatchError) Error() string {
	return fmt.Sprintf("create watch error:%v", e.innerError)
}

// InitValueError is returned when encountering an error when initializing a value.
type InitValueError struct {
	innerError error
}

func (e InitValueError) Error() string {
	return fmt.Sprintf("initializing value error:%v", e.innerError)
}
