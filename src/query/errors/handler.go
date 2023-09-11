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
	"context"
	"errors"
	"net/http"

	"github.com/m3db/m3/src/dbnode/client"
	xerrors "github.com/m3db/m3/src/x/errors"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

var (
	// ErrNotFound is returned when something is not found, this might be used for direct comparison
	ErrNotFound = xhttp.NewError(xerrors.NewInvalidParamsError(errors.New("not found")), http.StatusNotFound)
	// ErrHeaderNotFound is returned when a header is not found
	ErrHeaderNotFound = xerrors.NewInvalidParamsError(errors.New("header not found"))
	// ErrBatchQuery is returned when a batch query is found
	ErrBatchQuery = xerrors.NewInvalidParamsError(errors.New("batch queries are currently not supported"))
	// ErrNoQueryFound is returned when a target is not found
	ErrNoQueryFound = xerrors.NewInvalidParamsError(errors.New("no query found"))
	// ErrInvalidResultParamError is returned when result field for complete tag request
	// is an unexpected value
	ErrInvalidResultParamError = xerrors.NewInvalidParamsError(errors.New("invalid 'result' type for complete tag request"))
	// ErrNoName is returned when no name param is provided in the resource path
	ErrNoName = xerrors.NewInvalidParamsError(errors.New("invalid path with no name present"))
	// ErrInvalidMatchers is returned when invalid matchers are provided
	ErrInvalidMatchers = xerrors.NewInvalidParamsError(errors.New("invalid matchers"))
	// ErrNamesOnly is returned when label values results are name only
	ErrNamesOnly = xerrors.NewInvalidParamsError(errors.New("can not render label values; result has label names only"))
	// ErrWithNames is returned when label values results are name only
	ErrWithNames = xerrors.NewInvalidParamsError(errors.New("can not render label list; result has label names and values"))
	// ErrMultipleResults is returned when there are multiple label values results
	ErrMultipleResults = xerrors.NewInvalidParamsError(errors.New("can not render label values; multiple results detected"))
)

// ErrQueryTimeout is returned when a query times out executing.
type ErrQueryTimeout struct {
	cause error
}

// Error returns the error string of the causing error.
func (e *ErrQueryTimeout) Error() string {
	return e.cause.Error()
}

// Code returns an HTTP 504.
func (e *ErrQueryTimeout) Code() int {
	return http.StatusGatewayTimeout
}

// InnerError returns the cause of the query timeout.
func (e *ErrQueryTimeout) InnerError() error {
	return e.cause
}

// NewErrQueryTimeout wraps the provided causing error as an ErrQueryTimeout.
func NewErrQueryTimeout(err error) *ErrQueryTimeout {
	if err == nil {
		return nil
	}
	return &ErrQueryTimeout{cause: err}
}

// IsTimeout returns true if the error was caused by a timeout.
func IsTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || client.IsTimeoutError(err)
}
