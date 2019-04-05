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

	"github.com/m3db/m3x/pool"
	"github.com/m3db/m3x/resource"

	"github.com/opentracing/opentracing-go"
)

// Cancellable is an object that can be cancelled
type Cancellable interface {
	// IsCancelled determines whether the object is cancelled
	IsCancelled() bool

	// Cancel cancels the object
	Cancel()

	// Reset resets the object
	Reset()
}

// Context provides context to an operation.
type Context interface {
	// IsClosed returns whether the context is closed.
	IsClosed() bool

	// RegisterFinalizer will register a resource finalizer.
	RegisterFinalizer(resource.Finalizer)

	// RegisterCloser will register a resource closer.
	RegisterCloser(resource.Closer)

	// DependsOn will register a blocking context that
	// must complete first before finalizers can be called.
	DependsOn(Context)

	// Close will close the context.
	Close()

	// BlockingClose will close the context and call the
	// registered finalizers in a blocking manner after waiting
	// for any dependent contexts to close. After calling
	// the context becomes safe to reset and reuse again.
	BlockingClose()

	// Reset will reset the context for reuse.
	Reset()

	// GoContext returns the Go std context
	GoContext() (stdctx.Context, bool)

	// SetGoContext sets the Go std context
	SetGoContext(stdctx.Context)

	// StartTraceSpan starts a new span and returns a child ctx
	// if the span is being sampled.
	StartTraceSpan(name string) (Context, opentracing.Span, bool)
}

// Pool provides a pool for contexts.
type Pool interface {
	// Get provides a context from the pool.
	Get() Context

	// Put returns a context to the pool.
	Put(Context)
}

// Options controls knobs for context pooling.
type Options interface {
	// SetContextPoolOptions sets the context pool options.
	SetContextPoolOptions(pool.ObjectPoolOptions) Options

	// ContextPoolOptions returns the context pool options.
	ContextPoolOptions() pool.ObjectPoolOptions

	// SetFinalizerPoolOptions sets the finalizer pool options.
	SetFinalizerPoolOptions(pool.ObjectPoolOptions) Options

	// FinalizerPoolOptions returns the finalizer pool options.
	FinalizerPoolOptions() pool.ObjectPoolOptions

	// SetMaxPooledFinalizerCapacity sets the maximum capacity allowed
	// for a finalizer to be pooled.
	SetMaxPooledFinalizerCapacity(int) Options

	// MaxPooledFinalizerCapacity returns the maximum capacity allowed
	// for a finalizer to be pooled.
	MaxPooledFinalizerCapacity() int

	// SetInitPooledFinalizerCapacity sets the capacity finalizers are
	// initialized to.
	SetInitPooledFinalizerCapacity(int) Options

	// InitPooledFinalizerCapacity return the capacity finalizers are
	// initialized to.
	InitPooledFinalizerCapacity() int
}

// contextPool is the internal pool interface for contexts.
type contextPool interface {
	Pool

	// getFinalizeables provides a finalizeables slice from the pool.
	getFinalizeables() []finalizeable

	// putFinalizeables returns the finalizers to pool.
	putFinalizeables([]finalizeable)
}
