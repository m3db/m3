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

package limits

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	xerrors "github.com/m3db/m3/src/x/errors"
)

func TestIsQueryLimitExceededError(t *testing.T) {
	randomErr := xerrors.NewNonRetryableError(errors.New("random error"))
	limitExceededErr := NewQueryLimitExceededError("query limit exceeded")

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			"not query limit exceeded",
			randomErr,
			false,
		},
		{
			"query limit exceeded",
			limitExceededErr,
			true,
		},
		{
			"inner non query limit exceeded",
			xerrors.NewInvalidParamsError(randomErr),
			false,
		},
		{
			"inner query limit exceeded",
			xerrors.NewInvalidParamsError(limitExceededErr),
			true,
		},
		{
			"empty multi error",
			multiError(),
			false,
		},
		{
			"multi error without query limit exceeded",
			multiError(randomErr),
			false,
		},
		{
			"multi error with only query limit exceeded",
			multiError(limitExceededErr),
			true,
		},
		{
			"multi error with query limit exceeded",
			multiError(randomErr, xerrors.NewRetryableError(limitExceededErr)),
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsQueryLimitExceededError(tt.err))
		})
	}
}

func multiError(errs ...error) error {
	multiErr := xerrors.NewMultiError()
	for _, e := range errs {
		multiErr = multiErr.Add(e)
	}
	return multiErr.FinalError()
}
