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

package repair

import (
	"testing"

	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/require"
)

func TestHostBlockMetadataSlicePoolResetOnPut(t *testing.T) {
	opts := pool.NewObjectPoolOptions().SetSize(1)
	p := NewHostBlockMetadataSlicePool(opts, 64)
	res := p.Get()

	// Make res non-empty
	res.Add(HostBlockMetadata{})
	require.Equal(t, 1, len(res.Metadata()))

	// Return res to pool
	p.Put(res)

	// Verify res has been reset
	res = p.Get()
	require.Equal(t, 0, len(res.Metadata()))
}
