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
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3x/resource"

	"github.com/stretchr/testify/assert"
)

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

	assert.Equal(t, 1, len(ctx.finalizers))

	ctx.Close()
	wg.Wait()

	assert.Equal(t, true, closed)
}

func TestDoesNotRegisterFinalizerWhenClosed(t *testing.T) {
	ctx := NewContext().(*ctx)
	ctx.Close()
	ctx.RegisterFinalizer(resource.FinalizerFn(func() {}))

	assert.Equal(t, 0, len(ctx.finalizers))
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
	assert.Nil(t, ctx.finalizers)
}

func TestDependsOnWithReset(t *testing.T) {
	ctx := NewContext().(*ctx)

	testDependsOn(ctx, t)

	// Reset and test works again.
	ctx.Reset()

	testDependsOn(ctx, t)
}

func testDependsOn(c *ctx, t *testing.T) {
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
	other.Close()

	wg.Wait()

	// Ensure closed now.
	assert.Equal(t, int32(1), atomic.LoadInt32(&closed))
}
