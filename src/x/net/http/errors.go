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
	"encoding/json"
	"net/http"

	xerrors "github.com/m3db/m3/src/x/errors"
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
	Error string `json:"error"`
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

	switch v := err.(type) {
	case Error:
		w.WriteHeader(v.Code())
	case error:
		if xerrors.IsInvalidParams(v) {
			w.WriteHeader(http.StatusBadRequest)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}

	if o.response != nil {
		w.Write(o.response)
		return
	}

	json.NewEncoder(w).Encode(ErrorResponse{Error: err.Error()})
}
