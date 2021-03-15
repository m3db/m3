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
	"fmt"
	"github.com/m3db/m3/vendor/github.com/opentracing/opentracing-go"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	xresource "github.com/m3db/m3/src/x/resource"
)

func TestRegisterFinalizerWithChild(t *testing.T) {
	xCtx := NewBackground().(*ctx)
	assert.Nil(t, xCtx.parentCtx())

	childCtx := xCtx.newChildContext().(*ctx)
	assert.NotNil(t, childCtx.parentCtx())

	var (
		wg          sync.WaitGroup
		childClosed = false
	)

	wg.Add(1)
	childCtx.RegisterFinalizer(xresource.FinalizerFn(func() {
		childClosed = true
		wg.Done()
	}))

	assert.Equal(t, 0, childCtx.numFinalizeables())

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
		ctx    = NewBackground().(*ctx)
	)

	wg.Add(1)
	ctx.RegisterFinalizer(xresource.FinalizerFn(func() {
		closed = true
		wg.Done()
	}))

	assert.Equal(t, 1, ctx.numFinalizeables())

	ctx.Close()
	wg.Wait()

	assert.Equal(t, true, closed)
}

func TestRegisterCloserWithChild(t *testing.T) {
	xCtx := NewBackground().(*ctx)
	assert.Nil(t, xCtx.parentCtx())

	childCtx := xCtx.newChildContext().(*ctx)
	assert.NotNil(t, childCtx.parentCtx())

	var (
		wg          sync.WaitGroup
		childClosed = false
	)

	wg.Add(1)
	childCtx.RegisterCloser(xresource.SimpleCloserFn(func() {
		childClosed = true
		wg.Done()
	}))

	assert.Equal(t, 0, childCtx.numFinalizeables())

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
		ctx    = NewBackground().(*ctx)
	)

	wg.Add(1)
	ctx.RegisterCloser(xresource.SimpleCloserFn(func() {
		closed = true
		wg.Done()
	}))

	assert.Equal(t, 1, ctx.numFinalizeables())

	ctx.Close()
	wg.Wait()

	assert.Equal(t, true, closed)
}

func TestDoesNotRegisterFinalizerWhenClosed(t *testing.T) {
	ctx := NewBackground().(*ctx)
	ctx.Close()
	ctx.RegisterFinalizer(xresource.FinalizerFn(func() {}))

	assert.Equal(t, 0, ctx.numFinalizeables())
}

func TestDoesNotCloseTwice(t *testing.T) {
	ctx := NewBackground().(*ctx)

	var closed int32
	ctx.RegisterFinalizer(xresource.FinalizerFn(func() {
		atomic.AddInt32(&closed, 1)
	}))

	ctx.Close()
	ctx.Close()

	// Give some time for a bug to occur.
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, int32(1), atomic.LoadInt32(&closed))
}

func TestDependsOnNoCloserAllocation(t *testing.T) {
	ctx := NewBackground().(*ctx)
	ctx.DependsOn(NewBackground())
	assert.Equal(t, 0, ctx.numFinalizeables())
}

func TestDependsOn(t *testing.T) {
	ctx := NewBackground().(*ctx)
	testDependsOn(t, ctx)
}

func TestDependsOnWithReset(t *testing.T) {
	ctx := NewBackground().(*ctx)

	testDependsOn(t, ctx)

	// Reset and test works again.
	ctx.Reset()

	testDependsOn(t, ctx)
}

func testDependsOn(t *testing.T, c *ctx) {
	var wg sync.WaitGroup
	var closed int32

	other := NewBackground().(*ctx)

	wg.Add(1)
	c.RegisterFinalizer(xresource.FinalizerFn(func() {
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
		c     = NewBackground().(*ctx)
		child = c.newChildContext().(*ctx)
		other = NewBackground().(*ctx)

		wg     sync.WaitGroup
		closed int32
	)

	wg.Add(1)
	c.RegisterFinalizer(xresource.FinalizerFn(func() {
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

func TestSampledTraceSpan(t *testing.T) {
	var (
		xCtx  = NewBackground()
		goCtx = stdctx.Background()
		sp    opentracing.Span
		spCtx Context
	)

	// use a mock tracer to ensure sampling rate is set to 1.
	tracer := mocktracer.New()
	sp = tracer.StartSpan("foo")
	defer sp.Finish()

	mockSpan, ok := sp.(*mocktracer.MockSpan)
	require.True(t, ok)

	mockSpan.SpanContext.Sampled = true

	spGoCtx := opentracing.ContextWithSpan(goCtx, sp)

	xCtx.SetGoContext(spGoCtx)
	spCtx, sp = xCtx.StartTraceSpan("test_op_2")
	assert.NotNil(t, sp)
	defer sp.Finish()

	childCtx := spCtx.(*ctx)
	assert.NotNil(t, childCtx.parentCtx())
}

func TestGoContext(t *testing.T) {
	goCtx, cancel := stdctx.WithTimeout(stdctx.Background(), time.Minute)
	defer cancel()
	xCtx := NewWithGoContext(goCtx)

	var (
		returnCtx stdctx.Context
	)

	returnCtx = xCtx.GoContext()
	assert.Equal(t, goCtx, returnCtx)

	xCtx.Reset()
	returnCtx = xCtx.GoContext()
	assert.Equal(t, stdctx.Background(), returnCtx)
}

// Test that too deep span tree created is prevented
// Span is signalled as error in that case that which is well defined
func TestTooDeepSpanTreeIsPreventedAndMarked(t *testing.T) {
	// use a mock tracer to ensure sampling rate is set to 1.
	tracer := mocktracer.New()

	span := tracer.StartSpan("root-span")
	defer span.Finish()

	context := NewContext()
	context.SetGoContext(opentracing.ContextWithSpan(stdctx.Background(), span))

	var (
		lastChildSpanCreated    opentracing.Span = nil
		lastChildContextCreated                  = context
	)
	for i := 1; i <= maxDistanceFromRootContext; i++ {
		lastChildContextCreated, lastChildSpanCreated, _ =
			lastChildContextCreated.StartSampledTraceSpan(fmt.Sprintf("test-action-depth-%d", i))
	}

	mockSpan := lastChildSpanCreated.(*mocktracer.MockSpan)

	errorTagValue := mockSpan.Tag("error")
	assert.NotNil(t, errorTagValue)
	assert.Equal(t, true, errorTagValue)
	spanLogs := mockSpan.Logs()
	assert.Len(t, spanLogs, 1)
	spanLog := spanLogs[0]
	assert.True(t, fieldsContains(spanLog.Fields, "event", "error") &&
		fieldsContains(spanLog.Fields, "error.object", errSpanTooDeep.Error()))

	childContext, span, _ := lastChildContextCreated.StartSampledTraceSpan("another-span-beyond-max-depth")
	assert.Equal(t, lastChildContextCreated, childContext)
}

func fieldsContains(fields []mocktracer.MockKeyValue, key string, value string) bool {
	for _, field := range fields {
		if field.Key == key && field.ValueString == value {
			return true
		}
	}
	return false
}
