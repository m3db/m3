// Copyright (c) 2017 Uber Technologies, Inc.
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
package r2

import (
	"net/http"
	"testing"

	"github.com/m3db/m3ctl/auth"

	"github.com/stretchr/testify/require"
)

func TestDefaultAuthorizationTypeForHTTPMethodGet(t *testing.T) {
	actual, err := defaultAuthorizationTypeForHTTPMethod(http.MethodGet)
	require.NoError(t, err)
	require.EqualValues(t, auth.ReadOnlyAuthorization, actual)
}
func TestDefaultAuthorizationTypeForHTTPMethodPost(t *testing.T) {
	actual, err := defaultAuthorizationTypeForHTTPMethod(http.MethodPost)
	require.NoError(t, err)
	require.EqualValues(t, auth.ReadWriteAuthorization, actual)
}

func TestDefaultAuthorizationTypeForHTTPMethodPut(t *testing.T) {
	actual, err := defaultAuthorizationTypeForHTTPMethod(http.MethodPut)
	require.NoError(t, err)
	require.EqualValues(t, auth.ReadWriteAuthorization, actual)
}

func TestDefaultAuthorizationTypeForHTTPMethodPatch(t *testing.T) {
	actual, err := defaultAuthorizationTypeForHTTPMethod(http.MethodPatch)
	require.NoError(t, err)
	require.EqualValues(t, auth.ReadWriteAuthorization, actual)
}

func TestDefaultAuthorizationTypeForHTTPMethodDelete(t *testing.T) {
	actual, err := defaultAuthorizationTypeForHTTPMethod(http.MethodDelete)
	require.NoError(t, err)
	require.EqualValues(t, auth.ReadWriteAuthorization, actual)

}

func TestDefaultAuthorizationTypeForHTTPMethodUnrecognizedMethod(t *testing.T) {
	actual, err := defaultAuthorizationTypeForHTTPMethod(http.MethodOptions)
	require.Error(t, err)
	require.EqualValues(t, auth.UnknownAuthorization, actual)
}
