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
	stdctx "context"
	"sync"

	xopentracing "github.com/m3db/m3x/opentracing"
	"github.com/m3db/m3x/resource"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
)

// NB(r): using golang.org/x/net/context is too GC expensive.
// Instead, we just embed one.
type ctx struct {
	sync.RWMutex

	goCtx                stdctx.Context
	pool                 contextPool
	done                 bool
	wg                   sync.WaitGroup
	finalizeables        []finalizeable
	parent               Context
	checkedAndNotSampled bool
}

type finalizeable struct {
	finalizer resource.Finalizer
	closer    resource.Closer
}

// NewContext creates a new context.
func NewContext() Context {
	return newContext()
}

// NewPooledContext returns a new context that is returned to a pool when closed.
func newPooledContext(pool contextPool) Context {
	return &ctx{pool: pool}
}

// newContext returns an empty ctx
func newContext() *ctx {
	return &ctx{}
}

func (c *ctx) GoContext() (stdctx.Context, bool) {
	if c.goCtx == nil {
		return nil, false
	}

	return c.goCtx, true
}

func (c *ctx) SetGoContext(v stdctx.Context) {
	c.goCtx = v
}

func (c *ctx) IsClosed() bool {
	parent := c.parentCtx()
	if parent != nil {
		return parent.IsClosed()
	}

	c.RLock()
	done := c.done
	c.RUnlock()

	return done
}

func (c *ctx) RegisterFinalizer(f resource.Finalizer) {
	parent := c.parentCtx()
	if parent != nil {
		parent.RegisterFinalizer(f)
		return
	}

	c.registerFinalizeable(finalizeable{finalizer: f})
}

func (c *ctx) RegisterCloser(f resource.Closer) {
	parent := c.parentCtx()
	if parent != nil {
		parent.RegisterCloser(f)
		return
	}

	c.registerFinalizeable(finalizeable{closer: f})
}

func (c *ctx) registerFinalizeable(f finalizeable) {
	if c.Lock(); c.done {
		c.Unlock()
		return
	}

	if c.finalizeables != nil {
		c.finalizeables = append(c.finalizeables, f)
		c.Unlock()
		return
	}

	if c.pool != nil {
		c.finalizeables = append(c.pool.getFinalizeables(), f)
	} else {
		c.finalizeables = append(allocateFinalizeables(), f)
	}

	c.Unlock()
}

func allocateFinalizeables() []finalizeable {
	return make([]finalizeable, 0, defaultInitFinalizersCap)
}

func (c *ctx) DependsOn(blocker Context) {
	parent := c.parentCtx()
	if parent != nil {
		parent.DependsOn(blocker)
		return
	}

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
	parent := c.parentCtx()
	if parent != nil {
		if !parent.IsClosed() {
			parent.Close()
		}
		c.returnToPool()
		return
	}

	c.close(closeAsync)
}

func (c *ctx) BlockingClose() {
	parent := c.parentCtx()
	if parent != nil {
		if !parent.IsClosed() {
			parent.BlockingClose()
		}
		c.returnToPool()
		return
	}

	c.close(closeBlock)
}

func (c *ctx) close(mode closeMode) {
	if c.Lock(); c.done {
		c.Unlock()
		return
	}

	c.done = true
	c.Unlock()

	if c.finalizeables == nil {
		c.returnToPool()
		return
	}

	// Capture finalizeables to avoid concurrent r/w if Reset
	// is used after a caller waits for the finalizers to finish
	f := c.finalizeables
	c.finalizeables = nil

	switch mode {
	case closeAsync:
		go c.finalize(f)
	case closeBlock:
		c.finalize(f)
	}
}

func (c *ctx) finalize(f []finalizeable) {
	// Wait for dependencies.
	c.wg.Wait()

	// Now call finalizers.
	for i := range f {
		if f[i].finalizer != nil {
			f[i].finalizer.Finalize()
			f[i].finalizer = nil
		}
		if f[i].closer != nil {
			f[i].closer.Close()
			f[i].closer = nil
		}
	}

	if c.pool != nil {
		c.pool.putFinalizeables(f)
	}

	c.returnToPool()
}

func (c *ctx) Reset() {
	parent := c.parentCtx()
	if parent != nil {
		parent.Reset()
		return
	}

	c.Lock()
	c.done, c.finalizeables, c.goCtx, c.checkedAndNotSampled = false, nil, nil, false
	c.Unlock()
}

func (c *ctx) returnToPool() {
	if c.pool == nil {
		return
	}

	c.Reset()
	c.pool.Put(c)
}

func (c *ctx) newChildContext() Context {
	var childCtx *ctx
	if c.pool != nil {
		pooled, ok := c.pool.Get().(*ctx)
		if ok {
			childCtx = pooled
		}
	}

	if childCtx == nil {
		childCtx = newContext()
	}

	childCtx.setParentCtx(c)
	return childCtx
}

func (c *ctx) setParentCtx(parentCtx Context) {
	c.Lock()
	c.parent = parentCtx
	c.Unlock()
}

func (c *ctx) parentCtx() Context {
	c.RLock()
	parent := c.parent
	c.RUnlock()

	return parent
}

// Until OpenTracing supports the `IsSampled()` method, we need to cast to a Jaeger span.
// See https://github.com/opentracing/specification/issues/92 for more information.
func (c *ctx) spanIsSampled(sp opentracing.Span) bool {
	jaegerSpan, ok := sp.(*jaeger.Span)
	if !ok {
		return false
	}

	jaegerCtx := jaegerSpan.Context()
	spanCtx, ok := jaegerCtx.(*jaeger.SpanContext)
	if !ok {
		return false
	}

	if !spanCtx.IsSampled() {
		return false
	}

	return true
}

func (c *ctx) StartTraceSpan(name string) (Context, opentracing.Span, bool) {
	goCtx, exists := c.GoContext()
	if !exists || c.checkedAndNotSampled {
		return c, nil, false
	}

	var (
		sp    opentracing.Span
		spCtx stdctx.Context
	)

	sp = opentracing.SpanFromContext(goCtx)
	if sp == nil {
		sp, spCtx = xopentracing.StartSpanFromContext(goCtx, name)
		if c.spanIsSampled(sp) {
			child := c.newChildContext()
			child.SetGoContext(spCtx)
			return child, sp, true
		}
		c.checkedAndNotSampled = true
		return c, nil, false
	}

	sp, spCtx = xopentracing.StartSpanFromContext(goCtx, name)
	child := c.newChildContext()
	child.SetGoContext(spCtx)
	return child, sp, true
}
