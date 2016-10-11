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

package context

import (
	"sync"
)

type dependency struct {
	closers      []Closer
	dependencies sync.WaitGroup
}

// NB(r): using golang.org/x/net/context is too GC expensive
type ctx struct {
	sync.RWMutex
	pool   contextPool
	closed bool
	dep    *dependency
}

// NewContext creates a new context
func NewContext() Context {
	return newPooledContext(nil)
}

// NewPooledContext returns a new context that is returned to a pool when closed
func newPooledContext(pool contextPool) Context {
	return &ctx{pool: pool}
}

func (c *ctx) ensureDependencies(initClosers bool) {
	if c.dep != nil {
		return
	}
	c.dep = &dependency{}
	if !initClosers {
		return
	}
	if c.pool != nil {
		c.dep.closers = c.pool.GetClosers()
	} else {
		c.dep.closers = allocateClosers()
	}
}

func (c *ctx) RegisterCloser(closer Closer) {
	c.Lock()
	if c.closed {
		c.Unlock()
		return
	}
	c.ensureDependencies(true)
	c.dep.closers = append(c.dep.closers, closer)
	c.Unlock()
}

func (c *ctx) DependsOn(blocker Context) {
	c.Lock()
	closed := c.closed
	if !closed {
		c.ensureDependencies(false)
		c.dep.dependencies.Add(1)
		blocker.RegisterCloser(c)
	}
	c.Unlock()
}

// OnClose handles a call from another context that was depended upon closing
func (c *ctx) OnClose() {
	c.dep.dependencies.Done()
}

func (c *ctx) Close() {
	c.close(false)
}

func (c *ctx) BlockingClose() {
	c.close(true)
}

func (c *ctx) close(blocking bool) {
	var closers []Closer

	c.Lock()
	if c.closed {
		c.Unlock()
		return
	}
	c.closed = true
	if c.dep != nil {
		closers = c.dep.closers[:]
	}
	c.Unlock()

	if len(closers) > 0 {
		// NB(xichen): might be worth using a worker pool for the go routines.
		finalize := func() {
			// Wait for dependencies
			c.dep.dependencies.Wait()
			// Now call closers
			for _, closer := range closers {
				closer.OnClose()
			}
			c.returnToPool()
		}
		if blocking {
			finalize()
		} else {
			go finalize()
		}
		return
	}

	c.returnToPool()
}

func (c *ctx) IsClosed() bool {
	c.RLock()
	closed := c.closed
	c.RUnlock()
	return closed
}

func (c *ctx) Reset() {
	c.Lock()
	c.closed = false
	if c.dep != nil {
		if c.pool != nil {
			c.pool.PutClosers(c.dep.closers)
		}
		c.dep.closers = nil
		c.dep.dependencies = sync.WaitGroup{}
	}
	c.Unlock()
}

func (c *ctx) returnToPool() {
	if c.pool != nil {
		c.Reset()
		c.pool.Put(c)
	}
}
