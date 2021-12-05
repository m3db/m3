// Copyright (c) 2020  Uber Technologies, Inc.
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

package m3

import (
	xresource "github.com/m3db/m3/src/x/resource"
	xwatch "github.com/m3db/m3/src/x/watch"
)

type clusterNamespacesWatcher struct {
	watchable xwatch.Watchable
}

// NewClusterNamespacesWatcher creates a new ClusterNamespacesWatcher.
func NewClusterNamespacesWatcher() ClusterNamespacesWatcher {
	watchable := xwatch.NewWatchable()
	return &clusterNamespacesWatcher{watchable: watchable}
}

func (n *clusterNamespacesWatcher) Update(namespaces ClusterNamespaces) error {
	return n.watchable.Update(namespaces)
}

func (n *clusterNamespacesWatcher) Get() ClusterNamespaces {
	value := n.watchable.Get()
	if value == nil {
		return nil
	}

	return value.(ClusterNamespaces)
}

// RegisterListener registers the provided listener and returns a closer to clean up the watch.
// If the watcher is already closed, nothing is registered.
func (n *clusterNamespacesWatcher) RegisterListener(
	listener ClusterNamespacesListener,
) xresource.SimpleCloser {
	if n.watchable.IsClosed() {
		return xresource.SimpleCloserFn(func() {})
	}
	_, watch, _ := n.watchable.Watch()

	namespaces := watch.Get()

	// Deliver the current cluster namespaces, synchronously, if already set.
	if namespaces != nil {
		<-watch.C() // Consume initial notification
		listener.OnUpdate(namespaces.(ClusterNamespaces))
	}

	// Spawn a new goroutine to listen for updates.
	go func() {
		for range watch.C() {
			listener.OnUpdate(watch.Get().(ClusterNamespaces))
		}
	}()

	return watch
}

func (n *clusterNamespacesWatcher) Close() {
	n.watchable.Close()
}
