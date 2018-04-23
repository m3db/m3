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

package errors

import (
	"errors"
)

var (
	// ErrNilWriteQuery is returned when trying to write a nil query
	ErrNilWriteQuery = errors.New("nil write query")

	// ErrNotImplemented is returned when the storage endpoint is not implemented
	ErrNotImplemented = errors.New("not implemented")

	// ErrInvalidFetchResponse is returned when fetch fails from storage.
	ErrInvalidFetchResponse = errors.New("invalid response from fetch")

	// ErrFetchResponseOrder is returned fetch responses are not in order.
	ErrFetchResponseOrder = errors.New("responses out of order for fetch")

	// ErrFetchRequestType is an error returned when response from fetch has invalid type.
	ErrFetchRequestType = errors.New("invalid request type")

	// ErrInvalidFetchResult is an error returned when fetch result is invalid.
	ErrInvalidFetchResult = errors.New("invalid fetch result")
)
