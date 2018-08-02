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

package m3db

import (
	"errors"
	"fmt"
	"sync"

	clusterclient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/services"
)

const (
	errNewClientFailFmt = "M3 cluster client failed to initialize: %v"
)

var (
	errClientUninitialized = errors.New("M3 cluster client not yet initialized")
)

// AsyncClient is a thin wrapper around a cluster client that
// allows for lazy initialization.
type AsyncClient struct {
	sync.RWMutex
	client clusterclient.Client
	done   chan<- struct{}
	err    error
}

// NewClientFn creates a cluster client.
type NewClientFn func() (clusterclient.Client, error)

// NewAsyncClient returns a new AsyncClient.
func NewAsyncClient(
	fn NewClientFn,
	done chan<- struct{},
) clusterclient.Client {
	asyncClient := &AsyncClient{
		done: done,
		err:  errClientUninitialized,
	}

	go func() {
		if asyncClient.done != nil {
			defer func() {
				asyncClient.done <- struct{}{}
			}()
		}

		client, err := fn()

		asyncClient.Lock()
		defer asyncClient.Unlock()
		if err != nil {
			asyncClient.err = fmt.Errorf(errNewClientFailFmt, err)
			return
		}

		asyncClient.client = client
		asyncClient.err = nil
	}()

	return asyncClient
}

// Services returns access to the set of services.
func (c *AsyncClient) Services(opts services.OverrideOptions) (services.Services, error) {
	c.RLock()
	defer c.RUnlock()
	if c.err != nil {
		return nil, c.err
	}
	return c.client.Services(opts)
}

// KV returns access to the distributed configuration store.
// To be deprecated.
func (c *AsyncClient) KV() (kv.Store, error) {
	c.RLock()
	defer c.RUnlock()
	if c.err != nil {
		return nil, c.err
	}
	return c.client.KV()
}

// Txn returns access to the transaction store.
// To be deprecated.
func (c *AsyncClient) Txn() (kv.TxnStore, error) {
	c.RLock()
	defer c.RUnlock()
	if c.err != nil {
		return nil, c.err
	}
	return c.client.Txn()
}

// Store returns access to the distributed configuration store with a namespace.
func (c *AsyncClient) Store(opts kv.OverrideOptions) (kv.Store, error) {
	c.RLock()
	defer c.RUnlock()
	if c.err != nil {
		return nil, c.err
	}
	return c.client.Store(opts)
}

// TxnStore returns access to the transaction store with a namespace.
func (c *AsyncClient) TxnStore(opts kv.OverrideOptions) (kv.TxnStore, error) {
	c.RLock()
	defer c.RUnlock()
	if c.err != nil {
		return nil, c.err
	}
	return c.client.TxnStore(opts)
}
