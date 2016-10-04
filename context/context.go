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

const (
	defaultClosersCapacity = 32
)

type dependency struct {
	closers      []Closer
	dependencies sync.WaitGroup
}

// NB(r): using golang.org/x/net/context is too GC expensive
type ctx struct {
	sync.RWMutex
	pool   Pool
	closed bool
	dep    *dependency
}

// NewContext creates a new context
func NewContext() Context {
	return NewPooledContext(nil)
}

// NewPooledContext returns a new context that is returned to a pool when closed
func NewPooledContext(pool Pool) Context {
	return &ctx{pool: pool}
}

func (c *ctx) ensureDependencies() {
	if c.dep != nil {
		return
	}
	// TODO(r): return these to a pool on reset, otherwise over time
	// all contexts in a shared pool will acquire a dependency object
	c.dep = &dependency{
		closers: make([]Closer, 0, defaultClosersCapacity),
	}
}

func (c *ctx) RegisterCloser(closer Closer) {
	c.Lock()
	if c.closed {
		c.Unlock()
		return
	}
	c.ensureDependencies()
	c.dep.closers = append(c.dep.closers, closer)
	c.Unlock()
}

func (c *ctx) DependsOn(blocker Context) {
	c.Lock()
	closed := c.closed
	if !closed {
		c.ensureDependencies()
		c.dep.dependencies.Add(1)
	}
	c.Unlock()

	if !closed {
		blocker.RegisterCloser(func() {
			c.dep.dependencies.Done()
		})
	}
}

func (c *ctx) Close() {
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
		go func() {
			// Wait for dependencies
			c.dep.dependencies.Wait()
			// Now call closers
			for _, closer := range closers {
				closer()
			}
			c.returnToPool()
		}()
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
		c.dep.closers = c.dep.closers[:0]
		c.dep.dependencies = sync.WaitGroup{}
	}
	c.Unlock()
}

func (c *ctx) returnToPool() {
	if c.dep != nil && len(c.dep.closers) > defaultClosersCapacity {
		// Free any large arrays that are created
		c.dep.closers = nil
	}
	if c.pool != nil {
		c.Reset()
		c.pool.Put(c)
	}
}
