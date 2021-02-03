// Copyright (c) 2020 Uber Technologies, Inc.
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
	"fmt"
	"testing"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseErrorsFromString(t *testing.T) {
	tests := []*rpc.Error{
		NewResourceExhaustedError(errors.New(
			"query aborted due to limit: name=docs-matched, limit=1, current=2, within=1s")),
		NewBadRequestError(errors.New("")),
		NewInternalError(errors.New("boom")),
	}

	for i, err := range tests {
		t.Run(fmt.Sprintf("index %d", i), func(t *testing.T) {
			actual, aerr := ParseErrorFromString(err.String())
			require.NoError(t, aerr)
			require.Equal(t, err, actual)
		})
	}
}

func TestParseErrorsFromStringFails(t *testing.T) {
	_, err := ParseErrorFromString("blah")
	require.Error(t, err)

	_, err = ParseErrorFromString("Error({Type:FOO Message:query aborted due to " +
		"limit: name=docs-matched, limit=1, current=2, within=1s Flags:1})")
	require.Error(t, err)
}

func TestErrorsAreRecognized(t *testing.T) {
	someError := errors.New("some inner error")

	tests := []struct {
		name  string
		value bool
	}{
		{
			name:  "internal error",
			value: IsInternalError(NewInternalError(someError)),
		},
		{
			name:  "bad request error",
			value: IsBadRequestError(NewBadRequestError(someError)),
		},
		{
			name:  "resource exhausted error",
			value: IsBadRequestError(NewResourceExhaustedError(someError)),
		},
		{
			name:  "resource exhausted flag",
			value: IsResourceExhaustedErrorFlag(NewResourceExhaustedError(someError)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.True(t, tt.value)
		})
	}
}
