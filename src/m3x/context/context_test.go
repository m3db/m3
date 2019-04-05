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
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3x/resource"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
)

func TestRegisterFinalizerWithChild(t *testing.T) {
	xCtx := NewContext().(*ctx)
	assert.Nil(t, xCtx.parentCtx())

	childCtx := xCtx.newChildContext().(*ctx)
	assert.NotNil(t, childCtx.parentCtx())

	var (
		wg          sync.WaitGroup
		childClosed = false
	)

	wg.Add(1)
	childCtx.RegisterFinalizer(resource.FinalizerFn(func() {
		childClosed = true
		wg.Done()
	}))

	assert.Equal(t, 0, len(childCtx.finalizeables))

	childCtx.Close()
	wg.Wait()

	// validate closing childCtx, closes the parent
	assert.True(t, xCtx.IsClosed())
	assert.True(t, childClosed)
}

func TestRegisterFinalizer(t *testing.T) {
	var (
		wg     sync.WaitGroup
		closed = false
		ctx    = NewContext().(*ctx)
	)

	wg.Add(1)
	ctx.RegisterFinalizer(resource.FinalizerFn(func() {
		closed = true
		wg.Done()
	}))

	assert.Equal(t, 1, len(ctx.finalizeables))

	ctx.Close()
	wg.Wait()

	assert.Equal(t, true, closed)
}

func TestRegisterCloserWithChild(t *testing.T) {
	xCtx := NewContext().(*ctx)
	assert.Nil(t, xCtx.parentCtx())

	childCtx := xCtx.newChildContext().(*ctx)
	assert.NotNil(t, childCtx.parentCtx())

	var (
		wg          sync.WaitGroup
		childClosed = false
	)

	wg.Add(1)
	childCtx.RegisterCloser(resource.CloserFn(func() {
		childClosed = true
		wg.Done()
	}))

	assert.Equal(t, 0, len(childCtx.finalizeables))

	childCtx.Close()
	wg.Wait()

	// validate closing childCtx, closes the parent
	assert.True(t, xCtx.IsClosed())
	assert.True(t, childClosed)
}

func TestRegisterCloser(t *testing.T) {
	var (
		wg     sync.WaitGroup
		closed = false
		ctx    = NewContext().(*ctx)
	)

	wg.Add(1)
	ctx.RegisterCloser(resource.CloserFn(func() {
		closed = true
		wg.Done()
	}))

	assert.Equal(t, 1, len(ctx.finalizeables))

	ctx.Close()
	wg.Wait()

	assert.Equal(t, true, closed)
}

func TestDoesNotRegisterFinalizerWhenClosed(t *testing.T) {
	ctx := NewContext().(*ctx)
	ctx.Close()
	ctx.RegisterFinalizer(resource.FinalizerFn(func() {}))

	assert.Equal(t, 0, len(ctx.finalizeables))
}

func TestDoesNotCloseTwice(t *testing.T) {
	ctx := NewContext().(*ctx)

	var closed int32
	ctx.RegisterFinalizer(resource.FinalizerFn(func() {
		atomic.AddInt32(&closed, 1)
	}))

	ctx.Close()
	ctx.Close()

	// Give some time for a bug to occur.
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, int32(1), atomic.LoadInt32(&closed))
}

func TestDependsOnNoCloserAllocation(t *testing.T) {
	ctx := NewContext().(*ctx)
	ctx.DependsOn(NewContext())
	assert.Nil(t, ctx.finalizeables)
}

func TestDependsOn(t *testing.T) {
	ctx := NewContext().(*ctx)
	testDependsOn(t, ctx)
}

func TestDependsOnWithReset(t *testing.T) {
	ctx := NewContext().(*ctx)

	testDependsOn(t, ctx)

	// Reset and test works again.
	ctx.Reset()

	testDependsOn(t, ctx)
}

func testDependsOn(t *testing.T, c *ctx) {
	var wg sync.WaitGroup
	var closed int32

	other := NewContext().(*ctx)

	wg.Add(1)
	c.RegisterFinalizer(resource.FinalizerFn(func() {
		atomic.AddInt32(&closed, 1)
		wg.Done()
	}))

	c.DependsOn(other)
	c.Close()

	// Give some time for a bug to occur.
	time.Sleep(100 * time.Millisecond)

	// Ensure still not closed
	assert.Equal(t, int32(0), atomic.LoadInt32(&closed))

	// Now close the context ctx is dependent on.
	other.BlockingClose()

	wg.Wait()

	// Ensure closed now.
	assert.Equal(t, int32(1), atomic.LoadInt32(&closed))
}

func TestDependsOnWithChild(t *testing.T) {
	var (
		c     = NewContext().(*ctx)
		child = c.newChildContext().(*ctx)
		other = NewContext().(*ctx)

		wg     sync.WaitGroup
		closed int32
	)

	wg.Add(1)
	c.RegisterFinalizer(resource.FinalizerFn(func() {
		atomic.AddInt32(&closed, 1)
		wg.Done()
	}))

	child.DependsOn(other)
	child.Close()

	// Give some time for a bug to occur.
	time.Sleep(100 * time.Millisecond)

	// Ensure still not closed even though child ctx has been closed
	assert.Equal(t, int32(0), atomic.LoadInt32(&closed))

	// Now close the context ctx is dependent on.
	other.BlockingClose()

	wg.Wait()

	// Ensure closed now.
	assert.Equal(t, int32(1), atomic.LoadInt32(&closed))
	assert.True(t, c.IsClosed())
}

func TestTraceSpan(t *testing.T) {
	var (
		xCtx  = NewContext()
		goCtx = stdctx.Background()
		sp    opentracing.Span
		spCtx Context
		ok    bool
	)

	// use a mock tracer to ensure sampling rate is set to 1.
	mktr := mocktracer.New()
	sp = mktr.StartSpan("test_op")
	defer sp.Finish()

	spGoCtx := opentracing.ContextWithSpan(goCtx, sp)

	xCtx.SetGoContext(spGoCtx)
	spCtx, sp, ok = xCtx.StartTraceSpan("test_op_2")
	assert.True(t, ok)
	assert.NotNil(t, sp)
	defer sp.Finish()

	childCtx := spCtx.(*ctx)
	assert.NotNil(t, childCtx.parentCtx())
}

func TestGoContext(t *testing.T) {
	goCtx := stdctx.Background()
	xCtx := NewContext()

	var (
		exists    bool
		returnCtx stdctx.Context
	)

	returnCtx, exists = xCtx.GoContext()
	assert.False(t, exists)
	assert.Nil(t, returnCtx)

	xCtx.SetGoContext(goCtx)

	returnCtx, exists = xCtx.GoContext()
	assert.True(t, exists)
	assert.Equal(t, goCtx, returnCtx)

	xCtx.Reset()
	returnCtx, exists = xCtx.GoContext()
	assert.False(t, exists)
	assert.Nil(t, returnCtx)
}
