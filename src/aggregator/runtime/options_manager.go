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
	xresource "github.com/m3db/m3/src/x/resource"
	"github.com/m3db/m3/src/x/watch"
)

// OptionsManager manages runtime options.
type OptionsManager interface {
	// SetRuntimeOptions sets the runtime options.
	SetRuntimeOptions(value Options)

	// RuntimeOptions returns the current runtime options.
	RuntimeOptions() Options

	// RegisterWatcher registers a watcher that watches updates to runtime options.
	// When an update arrives, the manager will deliver the update to all registered
	// watchers.
	RegisterWatcher(l OptionsWatcher) xresource.SimpleCloser

	// Close closes the watcher and all descendent watches
	Close()
}

// OptionsWatcher watches for updates to runtime options.
type OptionsWatcher interface {
	// SetRuntimeOptions is called for registerer watchers when the runtime options
	// are updated, passing in the updated options as the argument.
	SetRuntimeOptions(value Options)
}

type optionsManager struct {
	watchable watch.Watchable
}

// NewOptionsManager creates a new runtime options manager.
func NewOptionsManager(initialValue Options) OptionsManager {
	watchable := watch.NewWatchable()
	watchable.Update(initialValue)
	return &optionsManager{
		watchable: watchable,
	}
}

func (w *optionsManager) SetRuntimeOptions(value Options) {
	w.watchable.Update(value)
}

func (w *optionsManager) RuntimeOptions() Options {
	return w.watchable.Get().(Options)
}

func (w *optionsManager) RegisterWatcher(
	watcher OptionsWatcher,
) xresource.SimpleCloser {
	_, watch, _ := w.watchable.Watch()

	// The watchable is always initialized so it's okay to do a blocking read.
	<-watch.C()

	// Deliver the current runtime options.
	watcher.SetRuntimeOptions(watch.Get().(Options))

	// Spawn a new goroutine that will terminate when the
	// watchable terminates on the close of the runtime options manager.
	go func() {
		for range watch.C() {
			watcher.SetRuntimeOptions(watch.Get().(Options))
		}
	}()

	return watch
}

func (w *optionsManager) Close() {
	w.watchable.Close()
}
