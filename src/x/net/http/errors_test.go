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
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	xerrors "github.com/m3db/m3/src/x/errors"
)

func TestErrorRewrite(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		expectedStatus int
		expectedBody   string
	}{
		{
			name:           "error that should not be rewritten",
			err:            errors.New("random error"),
			expectedStatus: 500,
			expectedBody:   `{"status":"error","error":"random error"}`,
		},
		{
			name:           "error that should be rewritten",
			err:            xerrors.NewInvalidParamsError(errors.New("to be rewritten")),
			expectedStatus: 500,
			expectedBody:   `{"status":"error","error":"rewritten error"}`,
		},
	}

	SetErrorRewriteFn(func(err error) error {
		if xerrors.IsInvalidParams(err) {
			return errors.New("rewritten error")
		}
		return err
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			WriteError(recorder, tt.err)
			assert.Equal(t, tt.expectedStatus, recorder.Code)
			assert.JSONEq(t, tt.expectedBody, recorder.Body.String())
		})
	}
}

func TestIsClientError(t *testing.T) {
	tests := []struct {
		err      error
		expected bool
	}{
		{NewError(fmt.Errorf("xhttp.Error(400)"), 400), true},
		{NewError(fmt.Errorf("xhttp.Error(499)"), 499), true},
		{xerrors.NewInvalidParamsError(fmt.Errorf("InvalidParamsError")), true},
		{xerrors.NewRetryableError(xerrors.NewInvalidParamsError(
			fmt.Errorf("InvalidParamsError insde RetyrableError"))), true},

		{NewError(fmt.Errorf("xhttp.Error(399)"), 399), false},
		{NewError(fmt.Errorf("xhttp.Error(500)"), 500), false},
		{xerrors.NewRetryableError(fmt.Errorf("any error inside RetryableError")), false},
		{fmt.Errorf("any error"), false},
	}

	for _, tt := range tests {
		t.Run(tt.err.Error(), func(t *testing.T) {
			require.Equal(t, tt.expected, IsClientError(tt.err))
		})
	}
}
