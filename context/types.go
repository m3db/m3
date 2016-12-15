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

// Finalizer finalizes a resource.
type Finalizer interface {
	Finalize()
}

// FinalizerFn is a function literal that is a finalizer.
type FinalizerFn func()

// Finalize will call the function literal as a finalizer.
func (fn FinalizerFn) Finalize() {
	fn()
}

// Context provides context to an operation.
type Context interface {
	// IsClosed returns whether the context is closed.
	IsClosed() bool

	// RegisterFinalizer will register a resource finalizer.
	RegisterFinalizer(Finalizer)

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
}

// Pool provides a pool for contexts.
type Pool interface {
	// Get provides a context from the pool.
	Get() Context

	// Put returns a context to the pool.
	Put(Context)
}

// contextPool is the internal pool interface for contexts.
type contextPool interface {
	Pool

	// GetFinalizers provides a finalizer slice from the pool.
	GetFinalizers() []Finalizer

	// PutFinalizers returns the finalizers to pool.
	PutFinalizers([]Finalizer)
}
