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

package pools

import (
	"errors"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
)

// PoolWrapper is an asynchronous wrapper for iterator pools
type PoolWrapper struct {
	mu          sync.Mutex
	watchers    []chan encoding.IteratorPools
	watchersErr []chan error
	pools       encoding.IteratorPools
	err         error
}

// NewPoolsWrapper creates an initialized pool wrapper
func NewPoolsWrapper(pools encoding.IteratorPools) *PoolWrapper {
	return &PoolWrapper{pools: pools}
}

// NewAsyncPoolsWrapper creates a pool wrapper which must be initialized
func NewAsyncPoolsWrapper() *PoolWrapper {
	return &PoolWrapper{}
}

// Init initializes the wrapper with iterator pools and notifies any watchers
func (w *PoolWrapper) Init(
	sessionPools encoding.IteratorPools,
	err error,
) {
	w.mu.Lock()
	w.pools, w.err = sessionPools, err
	for _, watcher := range w.watchers {
		watcher <- w.pools
	}
	for _, watcherErr := range w.watchersErr {
		watcherErr <- w.err
	}
	w.mu.Unlock()
}

// nolint
// IteratorPools either returns iterator pools and errors, or channels that will be notified
// when iterator pools become available
func (w *PoolWrapper) IteratorPools() (
	bool, encoding.IteratorPools, error, <-chan encoding.IteratorPools, <-chan error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.pools != nil || w.err != nil {
		return true, w.pools, w.err, nil, nil
	}
	watcher := make(chan encoding.IteratorPools)
	w.watchers = append(w.watchers, watcher)
	watcherErr := make(chan error)
	w.watchersErr = append(w.watchersErr, watcherErr)
	return false, nil, nil, watcher, watcherErr
}

// WaitForIteratorPools will block until iterator pools are available.
// If given a timeout of 0, will block indefinitely
func (w *PoolWrapper) WaitForIteratorPools(
	timeout time.Duration,
) (encoding.IteratorPools, error) {
	available, pools, err, poolCh, errCh := w.IteratorPools()
	if err != nil {
		return nil, err
	}

	if !available {
		if timeout == 0 {
			select {
			case pools = <-poolCh:
			case err = <-errCh:
				return nil, err
			}
		}

		select {
		case pools = <-poolCh:
		case err = <-errCh:
			return nil, err
		case <-time.After(timeout):
			return nil, errors.New("timeout waiting for iterator pools")
		}
	}

	return pools, err
}
