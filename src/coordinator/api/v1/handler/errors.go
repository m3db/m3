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

package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

var (
	// ErrInvalidParams is returned when input parameters are invalid
	ErrInvalidParams = errors.New("invalid request params")
)

// Error will serve an HTTP error
func Error(w http.ResponseWriter, err error, code int) {
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(struct {
		Error string `json:"error"`
	}{
		Error: err.Error(),
	})
}

// ParseError is the error from parsing requests
type ParseError struct {
	inner error
	code  int
}

// NewParseError creates a new parse error
func NewParseError(inner error, code int) *ParseError {
	return &ParseError{inner, code}
}

// Errors returns the error object
func (e *ParseError) Error() error {
	return e.inner
}

// Code returns the parse error type
func (e *ParseError) Code() int {
	return e.code
}

// Code returns the error code
func (e *ParseError) String() string {
	return fmt.Sprintf("error: %v, code: %d", e.inner, e.code)
}

// IsInvalidParams returns true if this is an invalid params error
func IsInvalidParams(err error) bool {
	if err == nil {
		return false
	}

	if strings.HasPrefix(err.Error(), ErrInvalidParams.Error()) {
		return true
	}

	return false
}
