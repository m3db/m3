// Copyright (c) 2018 Uber Technologies, Inc.
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

package etcd

import (
	"github.com/m3db/m3cluster/services"
	xwatch "github.com/m3db/m3x/watch"
)

type serviceWatchable interface {
	watches() int
	update(v services.Service)
	get() services.Service
	watch() (services.Service, services.Watch, error)
}

type watchable struct {
	value xwatch.Watchable
}

func newServiceWatchable() serviceWatchable {
	return &watchable{value: xwatch.NewWatchable()}
}

func (w *watchable) watches() int {
	return w.value.NumWatches()
}

func (w *watchable) update(v services.Service) {
	w.value.Update(v)
}

func (w *watchable) get() services.Service {
	return w.value.Get().(services.Service)
}

func (w *watchable) watch() (services.Service, services.Watch, error) {
	curr, newWatch, err := w.value.Watch()
	if err != nil {
		return nil, nil, err
	}
	return curr.(services.Service), &watch{value: newWatch}, nil
}

type watch struct {
	value xwatch.Watch
}

func (w *watch) Close() {
	w.value.Close()
}

func (w *watch) C() <-chan struct{} {
	return w.value.C()
}

func (w *watch) Get() services.Service {
	return w.value.Get().(services.Service)
}
