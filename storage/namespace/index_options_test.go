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

package namespace

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIndexOptionsEqual(t *testing.T) {
	opts := NewIndexOptions()
	require.True(t, opts.Equal(opts.SetEnabled(false)))
	require.False(t, opts.SetEnabled(true).Equal(opts.SetEnabled(false)))
	require.False(t, opts.SetBlockSize(time.Hour).Equal(
		opts.SetBlockSize(time.Hour*2)))
}

func TestIndexOptionsEnabled(t *testing.T) {
	opts := NewIndexOptions()
	require.True(t, opts.SetEnabled(true).Enabled())
	require.False(t, opts.SetEnabled(false).Enabled())
}

func TestIndexOptionsBlockSize(t *testing.T) {
	opts := NewIndexOptions()
	require.Equal(t, time.Hour, opts.SetBlockSize(time.Hour).BlockSize())
}
