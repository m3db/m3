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

package lookup

import (
	"testing"
	"time"

	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

var (
	initTime      = time.Date(2018, time.May, 12, 15, 55, 0, 0, time.UTC)
	testBlockSize = 24 * time.Hour
)

func newTime(n int) xtime.UnixNano {
	t := initTime.Truncate(testBlockSize).Add(time.Duration(n) * testBlockSize)
	return xtime.ToUnixNano(t)
}

func TestEntryIndexAttemptRotatesSlice(t *testing.T) {
	e := NewEntry(nil, 0)
	require.Equal(t, 3, cap(e.reverseIndex.states))
	for i := 0; i < 10; i++ {
		ti := newTime(i)
		require.True(t, e.NeedsIndexUpdate(ti))
		require.Equal(t, 3, cap(e.reverseIndex.states))
	}

	// ensure only the latest ones are held on to
	for i := 9; i >= 7; i-- {
		ti := newTime(i)
		require.False(t, e.NeedsIndexUpdate(ti))
	}
}
