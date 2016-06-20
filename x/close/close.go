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

package client

import (
	"errors"
)

var (
	// ErrNotCloseable is returned when trying to close a resource
	// that does not conform to a closeable interface
	ErrNotCloseable = errors.New("not a closeable resource")
)

// Closeable is a resource that can close
type Closeable interface {
	// Close resource
	Close()
}

// CloseableResult is a closeable resource that returns result of a close
type CloseableResult interface {
	// Close resource and return result
	Close() error
}

// TryClose attempts to close a resource, the resource is expected to
// implement either Closeable or CloseableResult.
func TryClose(r interface{}) error {
	if r, ok := r.(Closeable); ok {
		r.Close()
		return nil
	}
	if r, ok := r.(CloseableResult); ok {
		return r.Close()
	}
	return ErrNotCloseable
}
