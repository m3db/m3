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

package context

import (
	"sync"

	"github.com/m3db/m3x/resource"
)

// NB(r): using golang.org/x/net/context is too GC expensive.
type ctx struct {
	sync.RWMutex

	pool       contextPool
	done       bool
	wg         sync.WaitGroup
	finalizers []resource.Finalizer
}

// NewContext creates a new context.
func NewContext() Context {
	return newPooledContext(nil)
}

// NewPooledContext returns a new context that is returned to a pool when closed.
func newPooledContext(pool contextPool) Context {
	return &ctx{pool: pool}
}

func (c *ctx) IsClosed() bool {
	c.RLock()
	done := c.done
	c.RUnlock()

	return done
}

func (c *ctx) RegisterFinalizer(f resource.Finalizer) {
	if c.Lock(); c.done {
		c.Unlock()
		return
	}

	if c.finalizers != nil {
		c.finalizers = append(c.finalizers, f)
		c.Unlock()
		return
	}

	if c.pool != nil {
		c.finalizers = append(c.pool.GetFinalizers(), f)
	} else {
		c.finalizers = append(allocateFinalizers(), f)
	}

	c.Unlock()
}

func allocateFinalizers() []resource.Finalizer {
	return make([]resource.Finalizer, 0, defaultInitFinalizersCap)
}

func (c *ctx) DependsOn(blocker Context) {
	c.Lock()

	if !c.done {
		c.wg.Add(1)
		blocker.RegisterFinalizer(c)
	}

	c.Unlock()
}

// Finalize handles a call from another context that was depended upon closing.
func (c *ctx) Finalize() {
	c.wg.Done()
}

type closeMode int

const (
	closeAsync closeMode = iota
	closeBlock
)

func (c *ctx) Close() {
	c.close(closeAsync)
}

func (c *ctx) BlockingClose() {
	c.close(closeBlock)
}

func (c *ctx) close(mode closeMode) {
	if c.Lock(); c.done {
		c.Unlock()
		return
	}

	c.done = true
	c.Unlock()

	if len(c.finalizers) == 0 {
		c.returnToPool()
		return
	}

	switch mode {
	case closeAsync:
		go c.finalize()
	case closeBlock:
		c.finalize()
	}
}

func (c *ctx) finalize() {
	// Wait for dependencies.
	c.wg.Wait()

	// Now call finalizers.
	for i := range c.finalizers {
		c.finalizers[i].Finalize()
	}

	c.returnToPool()
}

func (c *ctx) Reset() {
	c.Lock()

	if c.pool != nil && c.finalizers != nil {
		c.pool.PutFinalizers(c.finalizers)
	}

	c.done, c.finalizers = false, nil

	c.Unlock()
}

func (c *ctx) returnToPool() {
	if c.pool == nil {
		return
	}

	c.Reset()
	c.pool.Put(c)
}
