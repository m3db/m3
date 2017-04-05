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

package storage

import (
	"github.com/m3db/m3x/watch"
)

type runtimeOptionsManager struct {
	watchable xwatch.Watchable
}

// NewRuntimeOptionsManager creates a new runtime options manager
func NewRuntimeOptionsManager(initialValue RuntimeOptions) RuntimeOptionsManager {
	watchable := xwatch.NewWatchable()
	watchable.Update(initialValue)
	return &runtimeOptionsManager{
		watchable: watchable,
	}
}

func (w *runtimeOptionsManager) Update(value RuntimeOptions) {
	w.watchable.Update(value)
}

func (w *runtimeOptionsManager) Get() RuntimeOptions {
	return w.watchable.Get().(RuntimeOptions)
}

func (w *runtimeOptionsManager) GetAndWatch() (RuntimeOptions, RuntimeOptionsWatch) {
	_, watch, _ := w.watchable.Watch()
	// We always initialize the watchable so always read
	// the first notification value
	<-watch.C()

	result := &runtimeOptionsWatch{watch: watch}
	return result.Get(), result
}

func (w *runtimeOptionsManager) Close() {
	w.watchable.Close()
}

type runtimeOptionsWatch struct {
	watch xwatch.Watch
}

func (w *runtimeOptionsWatch) C() <-chan struct{} {
	return w.watch.C()
}

func (w *runtimeOptionsWatch) Get() RuntimeOptions {
	return w.watch.Get().(RuntimeOptions)
}

func (w *runtimeOptionsWatch) Close() {
	w.watch.Close()
}
