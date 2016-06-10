package context

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRegisterCloser(t *testing.T) {
	var wg sync.WaitGroup
	closed := false

	ctx := NewContext()

	wg.Add(1)
	ctx.RegisterCloser(func() {
		closed = true
		wg.Done()
	})

	ctx.Close()

	wg.Wait()

	assert.Equal(t, true, closed)
}

func TestDoesNotRegisterCloserWhenClosed(t *testing.T) {
	ctx := NewContext().(*ctx)
	ctx.Close()
	ctx.RegisterCloser(func() {})

	assert.Nil(t, ctx.dep)
}

func TestDoesNotCloseTwice(t *testing.T) {
	ctx := NewContext().(*ctx)

	var closed int32
	ctx.RegisterCloser(func() {
		atomic.AddInt32(&closed, 1)
	})

	ctx.Close()
	ctx.Close()

	// Give some time for a bug to occur
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, int32(1), atomic.LoadInt32(&closed))
}

func TestDependsOnWithReset(t *testing.T) {
	ctx := NewContext().(*ctx)

	testDependsOn(ctx, t)

	// Reset and test works again
	ctx.Reset()

	testDependsOn(ctx, t)
}

func testDependsOn(c *ctx, t *testing.T) {
	var wg sync.WaitGroup
	var closed int32

	other := NewContext().(*ctx)

	wg.Add(1)
	c.RegisterCloser(func() {
		atomic.AddInt32(&closed, 1)
		wg.Done()
	})

	c.DependsOn(other)
	c.Close()

	// Give some time for a bug to occur
	time.Sleep(100 * time.Millisecond)

	// Ensure still not closed
	assert.Equal(t, int32(0), atomic.LoadInt32(&closed))

	// Now close the context ctx is dependent on
	other.Close()

	wg.Wait()

	// Ensure closed now
	assert.Equal(t, int32(1), atomic.LoadInt32(&closed))
}
