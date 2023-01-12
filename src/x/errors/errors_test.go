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

package errors

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWrap(t *testing.T) {
	inner := errors.New("detailed error message")
	err := NewInvalidParamsError(inner)
	wrappedErr := Wrap(err, "context about params error")
	assert.Error(t, wrappedErr)
	assert.Equal(t, "context about params error: detailed error message", wrappedErr.Error())
	assert.True(t, IsInvalidParams(wrappedErr))

	err = NewResourceExhaustedError(inner)
	wrappedErr = Wrap(err, "context about resource exhausted error")
	assert.Error(t, wrappedErr)
	assert.Equal(t, "context about resource exhausted error: detailed error message", wrappedErr.Error())
	assert.True(t, IsResourceExhausted(wrappedErr))

	err = NewRetryableError(inner)
	wrappedErr = Wrap(err, "context about retryable error")
	assert.Error(t, wrappedErr)
	assert.Equal(t, "context about retryable error: detailed error message", wrappedErr.Error())
	assert.True(t, IsRetryableError(wrappedErr))

	err = NewNonRetryableError(inner)
	wrappedErr = Wrap(err, "context about nonretryable error")
	assert.Error(t, wrappedErr)
	assert.Equal(t, "context about nonretryable error: detailed error message", wrappedErr.Error())
	assert.True(t, IsNonRetryableError(wrappedErr))
}

func TestWrapf(t *testing.T) {
	inner := errors.New("detailed error message")
	err := NewInvalidParamsError(inner)
	wrappedErr := Wrapf(err, "context about %s error", "params")
	assert.Error(t, wrappedErr)
	assert.Equal(t, "context about params error: detailed error message", wrappedErr.Error())
	assert.True(t, IsInvalidParams(wrappedErr))

	err = NewResourceExhaustedError(inner)
	wrappedErr = Wrapf(err, "context about %s error", "resource exhausted")
	assert.Error(t, wrappedErr)
	assert.Equal(t, "context about resource exhausted error: detailed error message", wrappedErr.Error())
	assert.True(t, IsResourceExhausted(wrappedErr))

	err = NewRetryableError(inner)
	wrappedErr = Wrapf(err, "context about %s error", "retryable")
	assert.Error(t, wrappedErr)
	assert.Equal(t, "context about retryable error: detailed error message", wrappedErr.Error())
	assert.True(t, IsRetryableError(wrappedErr))

	err = NewNonRetryableError(inner)
	wrappedErr = Wrapf(err, "context about %s error", "nonretryable")
	assert.Error(t, wrappedErr)
	assert.Equal(t, "context about nonretryable error: detailed error message", wrappedErr.Error())
	assert.True(t, IsNonRetryableError(wrappedErr))
}

func TestMultiErrorNoError(t *testing.T) {
	err := NewMultiError()
	require.Nil(t, err.FinalError())
	require.Nil(t, err.LastError())
	require.Equal(t, "", err.Error())
	require.True(t, err.Empty())
	require.Equal(t, 0, err.NumErrors())
}

func TestMultiErrorOneError(t *testing.T) {
	err := NewMultiError()
	err = err.Add(errors.New("foo"))
	final := err.FinalError()
	require.NotNil(t, final)
	require.Equal(t, "foo", final.Error())
	last := err.LastError()
	require.NotNil(t, last)
	require.Equal(t, "foo", last.Error())
	require.False(t, err.Empty())
	require.Equal(t, 1, err.NumErrors())
}

func TestMultiErrorMultipleErrors(t *testing.T) {
	err := NewMultiError()
	for _, errMsg := range []string{"foo", "bar", "baz"} {
		err = err.Add(errors.New(errMsg))
	}
	err = err.Add(nil)
	final := err.FinalError()
	require.NotNil(t, final)
	require.Equal(t, final.Error(), "foo\nbar\nbaz")
	last := err.LastError()
	require.NotNil(t, last)
	require.Equal(t, last.Error(), "baz")
	require.False(t, err.Empty())
	require.Equal(t, 3, err.NumErrors())
	require.Equal(t, 3, len(err.Errors()))
}

func TestErrorsIsAnErrorAndFormatsErrors(t *testing.T) {
	errs := error(Errors{
		fmt.Errorf("some error: foo=2, bar=baz"),
		fmt.Errorf("some other error: foo=42, bar=qux"),
	})
	assert.Equal(t, "[<some error: foo=2, bar=baz>, "+
		"<some other error: foo=42, bar=qux>]", errs.Error())
}
