// Copyright (c) 2016 Uber Technologies, Inc.
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

	xwatch "github.com/m3db/m3x/watch"
)

var (
	errEmptyNamespace   = errors.New("empty kv namespace")
	errEmptyEnvironment = errors.New("empty kv environment")
)

type valueWatch struct {
	w xwatch.Watch
}

// newValueWatch creates a new ValueWatch
func newValueWatch(w xwatch.Watch) ValueWatch {
	return &valueWatch{w: w}
}

func (v *valueWatch) Close() {
	v.w.Close()
}

func (v *valueWatch) C() <-chan struct{} {
	return v.w.C()
}

func (v *valueWatch) Get() Value {
	return valueFromWatch(v.w.Get())
}

type valueWatchable struct {
	w xwatch.Watchable
}

// NewValueWatchable creates a new ValueWatchable
func NewValueWatchable() ValueWatchable {
	return &valueWatchable{w: xwatch.NewWatchable()}
}

func (w *valueWatchable) IsClosed() bool {
	return w.w.IsClosed()
}

func (w *valueWatchable) Close() {
	w.w.Close()
}

func (w *valueWatchable) Get() Value {
	return valueFromWatch(w.w.Get())
}

func (w *valueWatchable) Watch() (Value, ValueWatch, error) {
	value, watch, err := w.w.Watch()
	if err != nil {
		return nil, nil, err
	}

	return valueFromWatch(value), newValueWatch(watch), nil
}

func (w *valueWatchable) NumWatches() int {
	return w.w.NumWatches()
}

func (w *valueWatchable) Update(v Value) error {
	return w.w.Update(v)
}

func valueFromWatch(value interface{}) Value {
	if value != nil {
		return value.(Value)
	}

	return nil
}

type options struct {
	namespace string
	env       string
}

// NewOptions creates a new kv Options.
func NewOptions() Options {
	return options{}
}

func (opts options) Namespace() string {
	return opts.namespace
}

func (opts options) SetNamespace(namespace string) Options {
	opts.namespace = namespace
	return opts
}

func (opts options) Environment() string {
	return opts.env
}

func (opts options) SetEnvironment(env string) Options {
	opts.env = env
	return opts
}

func (opts options) Validate() error {
	if opts.env == "" {
		return errEmptyEnvironment
	}
	if opts.namespace == "" {
		return errEmptyNamespace
	}
	return nil
}
