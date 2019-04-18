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

package protobuf

import (
	"testing"

	"github.com/m3db/m3/src/x/pool"

	"github.com/stretchr/testify/require"
)

func TestDefaultUnaggregatedOptions(t *testing.T) {
	opts := NewUnaggregatedOptions()
	require.NotNil(t, opts.BytesPool())
	require.Equal(t, defaultInitBufferSize, opts.InitBufferSize())
	require.Equal(t, defaultMaxUnaggregatedMessageSize, opts.MaxMessageSize())

	buckets := []pool.Bucket{
		{Capacity: 16, Count: 1},
	}
	p := pool.NewBytesPool(buckets, nil)
	p.Init()
	opts = opts.SetBytesPool(p).SetInitBufferSize(100).SetMaxMessageSize(200)
	require.True(t, p == opts.BytesPool())
	require.Equal(t, 100, opts.InitBufferSize())
	require.Equal(t, 200, opts.MaxMessageSize())
}
