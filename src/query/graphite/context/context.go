// Copyright (c) 2019 Uber Technologies, Inc.
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
	ctx "context"
	"sync"
)

// Closer is an interface implemented by objects that should be closed
// when a context completes
type Closer interface {
	Close() error
}

// Context is an execution context.  A Context holds deadlines and request
// scoped values.  Contexts should be scoped to a request;
// created when the request begins and closed when the request completes.
// Contexts are safe for use by multiple goroutines, but should not span
// multiple requests.
type Context interface {
	Closer

	// SetRequestContext sets the given context as the request context for this
	// execution context. This is used for calls to the m3 storage wrapper.
	SetRequestContext(ctx.Context)

	// RequestContext will provide the wrapped request context. Used for calls
	// to m3 storage wrapper.
	RequestContext() ctx.Context

	// RegisterCloser registers an object that should be closed when this
	// context is closed.  Can be used to cleanup per-request objects.
	RegisterCloser(closer Closer)

	// AddAsyncTask allows asynchronous tasks to be enqueued that will
	// ensure this context does not call its registered closers until
	// the tasks are all complete
	AddAsyncTasks(count int)

	// DoneAsyncTask signals that an asynchronous task is complete, when
	// all asynchronous tasks complete if the context has been closed and
	// avoided calling its registered closers it will finally call them
	DoneAsyncTask()
}

// New creates a new context
func New() Context {
	return &context{}
}

type contextStatus int

const (
	contextStatusOpen contextStatus = iota
	contextStatusClosed
)

type context struct {
	sync.RWMutex
	closers    []Closer
	status     contextStatus
	asyncTasks int
	reqCtx     ctx.Context
}

// Close closes the context
func (c *context) Close() error {
	finalize := false

	c.Lock()
	if c.status == contextStatusOpen {
		if c.asyncTasks == 0 {
			finalize = true
		}
		c.status = contextStatusClosed
	}
	c.Unlock()

	if finalize {
		return c.callClosers()
	}
	return nil
}

// SetRequestContext sets the given context as the request context for this
// execution context. This is used for calls to the m3 storage wrapper.
func (c *context) SetRequestContext(reqCtx ctx.Context) {
	c.Lock()
	c.reqCtx = reqCtx
	c.Unlock()
}

// RequestContext will provide the wrapped request context. Used for calls
// to m3 storage wrapper.
func (c *context) RequestContext() ctx.Context {
	c.RLock()
	r := c.reqCtx
	c.RUnlock()
	return r
}

// RegisterCloser registers a new Closer with the context
func (c *context) RegisterCloser(closer Closer) {
	c.Lock()
	c.closers = append(c.closers, closer)
	c.Unlock()
}

// AddAsyncTasks adds tracked asynchronous task(s)
func (c *context) AddAsyncTasks(count int) {
	c.Lock()
	c.asyncTasks += count
	c.Unlock()
}

// DoneAsyncTask marks a single tracked asynchronous task complete
func (c *context) DoneAsyncTask() {
	finalize := false

	c.Lock()
	c.asyncTasks--
	if c.asyncTasks == 0 && c.status == contextStatusClosed {
		finalize = true
	}
	c.Unlock()

	if finalize {
		c.callClosers()
	}
}

func (c *context) callClosers() error {
	var firstErr error
	c.RLock()
	for _, closer := range c.closers {
		if err := closer.Close(); err != nil {
			//FIXME: log.Errorf("could not close %v: %v", closer, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	c.RUnlock()
	return firstErr
}
