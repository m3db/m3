// Copyright (c) 2017 Uber Technologies, Inc.
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

package time

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUnixNano(t *testing.T) {
	time := time.Unix(0, 1000)
	unixNano := ToUnixNano(time)
	require.Equal(t, UnixNano(1000), unixNano)
	require.Equal(t, time, unixNano.ToTime())
}

func TestUnixNanoBefore(t *testing.T) {
	t0 := UnixNano(0)
	t1 := UnixNano(1)

	require.Equal(t, true, t0.Before(t1))
	require.Equal(t, false, t1.Before(t0))
	require.Equal(t, false, t0.Before(t0))
}

func TestUnixNanoAfter(t *testing.T) {
	t0 := UnixNano(0)
	t1 := UnixNano(1)

	require.Equal(t, false, t0.After(t1))
	require.Equal(t, true, t1.After(t0))
	require.Equal(t, false, t0.After(t0))
}

func TestUnixNanoEqual(t *testing.T) {
	t0 := UnixNano(0)
	t1 := UnixNano(1)

	require.Equal(t, false, t0.Equal(t1))
	require.Equal(t, false, t1.Equal(t0))
	require.Equal(t, true, t0.Equal(t0))
}
