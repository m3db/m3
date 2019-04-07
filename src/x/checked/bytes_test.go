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

package checked

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBytes(t *testing.T) {
	raw := make([]byte, 3, 5)
	copy(raw, []byte{'a', 'b', 'c'})

	var onFinalize BytesFinalizerFn
	finalizer := BytesFinalizerFn(func(finalizing Bytes) {
		onFinalize(finalizing)
	})

	b := NewBytes(raw, NewBytesOptions().SetFinalizer(finalizer))
	b.IncRef()

	assert.Equal(t, []byte("abc"), b.Bytes())
	assert.Equal(t, 3, b.Len())
	assert.Equal(t, 5, b.Cap())

	b.Append('d')
	b.AppendAll([]byte{'e', 'f'})

	assert.Equal(t, []byte("abcdef"), b.Bytes())
	assert.Equal(t, 6, b.Len())

	b.Resize(4)
	assert.Equal(t, []byte("abcd"), b.Bytes())
	assert.Equal(t, 4, b.Len())

	b.Reset([]byte{'x', 'y', 'z'})
	assert.Equal(t, []byte("xyz"), b.Bytes())
	assert.Equal(t, 3, b.Len())

	b.DecRef()

	finalizerCalls := 0
	onFinalize = func(finalizing Bytes) {
		// Ensure closing the ref we created
		assert.Equal(t, b, finalizing)
		finalizing.IncRef()
		assert.Equal(t, []byte("xyz"), finalizing.Bytes())
		finalizing.DecRef()
		finalizerCalls++
	}

	b.Finalize()
	assert.Equal(t, 1, finalizerCalls)
}
