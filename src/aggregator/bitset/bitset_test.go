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

package bitset

import (
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBitSetSmallRangeSetAll(t *testing.T) {
	for i := 0; i <= 64; i++ {
		bs := New(uint(i))
		for j := 0; j < i; j++ {
			bs.Set(uint(j))
		}
		require.True(t, bs.All(uint(i)))
	}
}

func TestBitSetSmallRangeSetPartial(t *testing.T) {
	for i := 1; i <= 64; i++ {
		for skip := 0; skip < i; skip++ {
			bs := New(uint(i))
			for j := 0; j < i; j++ {
				if j == skip {
					continue
				}
				bs.Set(uint(j))
			}
			require.False(t, bs.All(uint(i)))
		}
	}
}

func TestBitSetLargeRangeSetAll(t *testing.T) {
	for i := 65; i <= 256; i++ {
		bs := New(uint(i))
		for j := 0; j < i; j++ {
			bs.Set(uint(j))
		}
		require.True(t, bs.All(uint(i)))
	}
}

func TestBitSetLargeRangeSetPartial(t *testing.T) {
	for i := 65; i <= 256; i++ {
		for skip := 0; skip < i; skip++ {
			bs := New(uint(i))
			for j := 0; j < i; j++ {
				if j == skip {
					continue
				}
				bs.Set(uint(j))
			}
			require.False(t, bs.All(uint(i)))
		}
	}
}

func TestRlimits(t *testing.T) {
	var l syscall.Rlimit
	syscall.Getrlimit(syscall.RLIMIT_NOFILE, &l)
	t.Logf("%+v\n", l)
	t.FailNow()
}
