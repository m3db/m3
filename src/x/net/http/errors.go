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

package xhttp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"

	xerrors "github.com/m3db/m3/src/x/errors"
)

// ErrorRewriteFn is a function for rewriting response error.
type ErrorRewriteFn func(error) error

var (
	errorRewriteFn     ErrorRewriteFn = func(err error) error { return err }
	errorRewriteFnLock sync.RWMutex
)

// Error is an HTTP JSON error that also sets a return status code.
type Error interface {
	// Fulfill error interface.
	error

	// Embedding ContainedError allows for the inner error
	// to be retrieved with all existing error helpers.
	xerrors.ContainedError

	// Code returns the status code to return to end users.
	Code() int
}

// NewError creates a new error with an explicit status code
// which will override any wrapped error to return specifically
// the exact error code desired.
func NewError(err error, status int) Error {
	return errorWithCode{err: err, status: status}
}

type errorWithCode struct {
	err    error
	status int
}

func (e errorWithCode) Error() string {
	return e.err.Error()
}

func (e errorWithCode) InnerError() error {
	return e.err
}

func (e errorWithCode) Code() int {
	return e.status
}

// ErrorResponse is a generic response for an HTTP error.
type ErrorResponse struct {
	Status string `json:"status"`
	Error  string `json:"error"`
}

type options struct {
	response []byte
}

// WriteErrorOption is an option to pass to WriteError.
type WriteErrorOption func(*options)

// WithErrorResponse specifies a response to add the WriteError method.
func WithErrorResponse(b []byte) WriteErrorOption {
	return func(o *options) {
		o.response = b
	}
}

// WriteError will serve an HTTP error.
func WriteError(w http.ResponseWriter, err error, opts ...WriteErrorOption) {
	var o options
	for _, fn := range opts {
		fn(&o)
	}

	errorRewriteFnLock.RLock()
	err = errorRewriteFn(err)
	errorRewriteFnLock.RUnlock()

	statusCode := getStatusCode(err)
	if o.response == nil {
		w.Header().Set(HeaderContentType, ContentTypeJSON)
		w.WriteHeader(statusCode)
		json.NewEncoder(w).Encode(ErrorResponse{Status: "error", Error: err.Error()}) //nolint:errcheck
	} else {
		w.WriteHeader(statusCode)
		w.Write(o.response)
	}
}

// SetErrorRewriteFn sets error rewrite function.
func SetErrorRewriteFn(f ErrorRewriteFn) ErrorRewriteFn {
	errorRewriteFnLock.Lock()
	defer errorRewriteFnLock.Unlock()

	res := errorRewriteFn
	errorRewriteFn = f
	return res
}

func getStatusCode(err error) int {
	switch v := err.(type) {
	case Error:
		return v.Code()
	case error:
		if xerrors.IsInvalidParams(v) {
			return http.StatusBadRequest
		} else if errors.Is(err, context.DeadlineExceeded) {
			return http.StatusGatewayTimeout
		}
	}
	return http.StatusInternalServerError
}

// IsClientError returns true if this error would result in 4xx status code.
func IsClientError(err error) bool {
	code := getStatusCode(err)
	return code >= 400 && code < 500
}
