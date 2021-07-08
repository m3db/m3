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

package mocks

import (
	"sync"

	"go.etcd.io/etcd/client/v3"
	"golang.org/x/net/context"
)

// watcher mocks an etcd client that just blackholes a few watch requests
type watcher struct {
	sync.Mutex

	failed    int
	failTotal int
	c         *clientv3.Client
	onFail    func()
}

// NewBlackholeWatcher returns a watcher that mimics blackholing
func NewBlackholeWatcher(c *clientv3.Client, failTotal int, onFail func()) clientv3.Watcher {
	return &watcher{
		failed:    0,
		failTotal: failTotal,
		c:         c,
		onFail:    onFail,
	}
}

// Watch is implementing etcd clientv3 Watcher interface
func (m *watcher) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	m.Lock()

	if m.failed < m.failTotal {
		m.failed++
		m.Unlock()

		m.onFail()
		return nil
	}
	m.Unlock()

	return m.c.Watch(ctx, key, opts...)
}

// RequestProgress is implementing etcd clientv3 Watcher interface
func (m *watcher) RequestProgress(ctx context.Context) error {
	return nil
}

// Close is implementing etcd clientv3 Watcher interface
func (m *watcher) Close() error {
	return m.c.Close()
}
