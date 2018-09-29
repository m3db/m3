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

package ident

import (
	"testing"

	"github.com/m3db/m3x/checked"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConstructorEquality(t *testing.T) {
	a := StringID("abc")
	b := BinaryID(checked.NewBytes([]byte{'a', 'b', 'c'}, nil))

	require.Equal(t, a.String(), "abc")

	assert.True(t, a.Equal(b))
	assert.Equal(t, a.String(), b.String())
	assert.Equal(t, a.Bytes(), b.Bytes())
}

func TestNoFinalize(t *testing.T) {
	v := StringID("abc")

	checkValid := func() {
		require.NotNil(t, v.Bytes())
		assert.True(t, v.Equal(StringID("abc")))
	}
	checkValid()
	assert.True(t, v.IsNoFinalize())

	v.NoFinalize()
	checkValid()
	assert.True(t, v.IsNoFinalize())

	for i := 0; i < 2; i++ {
		v.Finalize()
		checkValid()
	}
}
