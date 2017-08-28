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

package msgpack

import (
	"testing"

	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/require"
)

func TestBufferedEncoderPool(t *testing.T) {
	poolOpts := pool.NewObjectPoolOptions().SetSize(1)
	opts := NewBufferedEncoderPoolOptions().
		SetObjectPoolOptions(poolOpts)

	p := NewBufferedEncoderPool(opts)
	p.Init(func() BufferedEncoder {
		return NewPooledBufferedEncoder(p)
	})

	// Retrieve an encoder from the pool.
	encoder := p.Get()
	encoder.Buffer().Write([]byte{1, 2, 3})
	require.Equal(t, 3, encoder.Buffer().Len())

	// Closing the encoder should put it back to the pool.
	encoder.Close()

	// Retrieve the encoder and assert it's the same encoder.
	encoder = p.Get()
	require.Equal(t, 3, encoder.Buffer().Len())

	// Reset the encoder and assert it's been reset.
	encoder.Reset()
	require.Equal(t, 0, encoder.Buffer().Len())
}

func TestBufferedEncoderPoolMaxCapacity(t *testing.T) {
	poolOpts := pool.NewObjectPoolOptions().SetSize(1)
	opts := NewBufferedEncoderPoolOptions().
		SetMaxCapacity(2).
		SetObjectPoolOptions(poolOpts)

	p := NewBufferedEncoderPool(opts)
	p.Init(func() BufferedEncoder {
		return NewPooledBufferedEncoder(p)
	})

	// Retrieve an encoder from the pool.
	encoder := p.Get()
	encoder.Buffer().Write([]byte{1, 2, 3})
	require.Equal(t, 3, encoder.Buffer().Len())

	// Closing the encoder should put it back to the pool.
	encoder.Close()

	// Retrieve an encoder and assert it's a different encoder since
	// the previous one exceeded the maximum capacity of the pool.
	encoder = p.Get()
	require.Equal(t, 0, encoder.Buffer().Cap())
}
