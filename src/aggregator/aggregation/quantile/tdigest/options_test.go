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

package tdigest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	testOpts = NewOptions()
)

func TestOptionsValidateSuccess(t *testing.T) {
	require.NoError(t, testOpts.Validate())
}

func TestOptionsValidateInvalidCompression(t *testing.T) {
	opts := testOpts.SetCompression(minCompression - 1)
	require.Equal(t, errInvalidCompression, opts.Validate())

	opts = testOpts.SetCompression(maxCompression + 1)
	require.Equal(t, errInvalidCompression, opts.Validate())
}

func TestOptionsValidateInvalidPrecision(t *testing.T) {
	opts := testOpts.SetPrecision(minPrecision - 1)
	require.Equal(t, errInvalidPrecision, opts.Validate())

	opts = testOpts.SetPrecision(maxPrecision + 1)
	require.Equal(t, errInvalidPrecision, opts.Validate())
}

func TestOptionsValidateNoCentroidsPool(t *testing.T) {
	opts := testOpts.SetCentroidsPool(nil)
	require.Equal(t, errNoCentroidsPool, opts.Validate())
}
